package cache

import (
	"context"
	"encoding/binary"
	"fmt"
	"log/slog"
	"math"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

const (
	PrefixCache = "cache:v1:"
)

type QueryCache struct {
	client *redis.Client
	cfg    RetryConfig
	mu     sync.RWMutex // Add this to manage the index lifecycle
}

func NewQueryCache(client *redis.Client, cfg RetryConfig) *QueryCache {
	return &QueryCache{client: client, cfg: cfg}
}

// InitializeIndex creates the RediSearch schema if it doesn't exist
func (q *QueryCache) InitializeIndex(ctx context.Context) error {
	// 768 is the exact dimension size for Google's text-embedding-004 model
	err := q.client.Do(ctx,
		"FT.CREATE", "idx:cache",
		"ON", "HASH", "PREFIX", "1", PrefixCache,
		"SCHEMA",
		"query", "TEXT",
		"embedding", "VECTOR", "FLAT", "6",
		"TYPE", "FLOAT32",
		"DIM", "768",
		"DISTANCE_METRIC", "COSINE",
	).Err()

	// It's safe to run this on every boot. If the index is already there, it throws a harmless error.
	if err != nil && err.Error() != "Index already exists" {
		slog.Error("⚠️ Failed to create Redis Vector Index", "error", err)
		return err
	}
	slog.Info("🔍 Redis Vector Index initialized (or already exists)")
	return nil
}
func (q *QueryCache) GetCache(ctx context.Context, queryVector []float32, threshold float64) (string, bool) {
	// Suspect #2: Dimension Mismatch. RediSearch silently drops vectors that don't match the schema DIM.
	if len(queryVector) != 768 {
		slog.Error("❌ [Cache] Vector dimension mismatch! RediSearch will ignore this.", "expected", 768, "got", len(queryVector))
		return "", false
	}

	vectorBytes := float32ToByte(queryVector)
	searchQuery := "*=>[KNN 1 @embedding $vec AS distance]"

	// 🚨 Fix 1: Explicitly define results as a slice of interfaces
	// 1. Use .Result() so go-redis just gives us whatever raw interface it receives
	var res interface{}
	err := executeWithRetry(ctx, q.cfg, func(opCtx context.Context) error {
		var innerErr error
		res, innerErr = q.client.Do(opCtx,
			"FT.SEARCH", "idx:cache", searchQuery,
			"PARAMS", "2", "vec", vectorBytes,
			"RETURN", "2", "answer", "distance",
			"DIALECT", "2",
		).Result()
		return innerErr
	})

	if err != nil {
		slog.Error("❌ Redis Vector Search Error", "error", err)
		return "", false
	}

	var answer string
	var distance float64
	var totalResults int64

	// 2. Safely parse the response based on the Protocol format (RESP2 vs RESP3)
	switch v := res.(type) {

	case []interface{}:
		// --- RESP2 PARSING (Older Flat Array Format) ---
		// local Redis 5 container (RESP2)
		if len(v) == 0 {
			return "", false
		}
		totalResults = v[0].(int64)
		if totalResults == 0 {
			slog.Info("💾 [Cache Miss] Index active, 0 matches found (RESP2)")
			return "", false
		}
		if len(v) >= 3 {
			fields := v[2].([]interface{})
			for i := 0; i < len(fields); i += 2 {
				key := fields[i].(string)
				val := fields[i+1].(string)
				if key == "answer" {
					answer = val
				}
				if key == "distance" {
					fmt.Sscanf(val, "%f", &distance)
				}
			}
		}

	case map[interface{}]interface{}:
		// Google Cloud Memorystore Redis 7 instance (RESP3)
		// --- RESP3 PARSING (Newer Map/Dictionary Format) ---

		// Safely extract total_results
		if total, ok := v["total_results"].(int64); ok {
			totalResults = total
		} else if total, ok := v["total_results"].(int); ok {
			totalResults = int64(total)
		}

		if totalResults == 0 {
			slog.Info("💾 [Cache Miss] Index active, 0 matches found (RESP3)")
			return "", false
		}

		// Extract the results array
		if resultsArr, ok := v["results"].([]interface{}); ok && len(resultsArr) > 0 {
			// Get the first document
			if doc, ok := resultsArr[0].(map[interface{}]interface{}); ok {
				// In RESP3, the return fields are nested inside "extra_attributes"
				fields := doc
				if extra, hasExtra := doc["extra_attributes"].(map[interface{}]interface{}); hasExtra {
					fields = extra
				}

				if ans, ok := fields["answer"].(string); ok {
					answer = ans
				}
				if dist, ok := fields["distance"].(string); ok {
					fmt.Sscanf(dist, "%f", &distance)
				}
			}
		}

	default:
		slog.Error("❌ [Cache] Unrecognized Redis response type", "type", fmt.Sprintf("%T", res))
		return "", false
	}

	// 3. Precision Math
	similarity := 1.0 - distance
	margin := similarity - threshold

	slog.Info("🧮 [Cache Evaluation]",
		"threshold", fmt.Sprintf("%.4f", threshold),
		"similarity", fmt.Sprintf("%.4f", similarity),
		"margin", fmt.Sprintf("%+.4f", margin),
	)

	if similarity >= threshold {
		slog.Info("🎯 [Cache Hit] Success!")
		return answer, true
	}

	slog.Info("📉 [Cache Miss] Score too low")
	return "", false
}

// SetCache stores the query, answer, and vector as a Redis Hash
func (q *QueryCache) SetCache(ctx context.Context, query string, answer string, vector []float32, ttl time.Duration) error {
	// Generate a unique key for this specific cache entry
	key := PrefixCache + uuid.New().String()
	vectorBytes := float32ToByte(vector)

	err := executeWithRetry(ctx, q.cfg, func(opCtx context.Context) error {
		pipe := q.client.Pipeline()
		// Store as a Redis Hash
		pipe.HSet(opCtx, key, map[string]interface{}{
			"query":     query,
			"answer":    answer,
			"embedding": vectorBytes,
		})
		// Set the expiration
		pipe.Expire(opCtx, key, ttl)
		_, innerErr := pipe.Exec(opCtx)
		return innerErr
	})

	if err != nil {
		slog.Error("❌ Redis Vector Set Error (Degraded)", "error", err)
		return err
	}

	slog.Debug("💾 Semantic Cache Stored", "key", key)
	return nil
}

func (q *QueryCache) FlushCache(ctx context.Context) error {
	slog.Warn("🧹 [Cache Management] Strategic Wipe Initiated...")

	// 1. Delete all keys with our prefix, but NOT the index schema itself
	// This keeps the 'idx:cache' definition alive so GetCache never throws "No such index"
	script := `
        local keys = redis.call('keys', ARGV[1])
        for i,k in ipairs(keys) do
            redis.call('del', k)
        end
        return #keys
    `
	err := q.client.Eval(ctx, script, []string{}, PrefixCache+"*").Err()
	if err != nil {
		// Fallback: If script fails, we do the nuclear option but immediately re-index
		q.client.FlushDB(ctx)
		q.InitializeIndex(ctx)
	}

	// 2. Also wipe the session history (different prefix)
	q.client.Eval(ctx, script, []string{}, "session:v1:*")

	slog.Info("✅ [Cache Management] Strategic wipe complete. Schema preserved.")
	return nil
}

func float32ToByte(f []float32) []byte {
	buf := make([]byte, len(f)*4)
	for i, v := range f {
		binary.LittleEndian.PutUint32(buf[i*4:], math.Float32bits(v))
	}
	return buf
}
