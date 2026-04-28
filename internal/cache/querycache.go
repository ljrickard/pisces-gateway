package cache

import (
	"context"
	"encoding/binary"
	"fmt"
	"log/slog"
	"math"
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
}

func NewQueryCache(client *redis.Client, cfg RetryConfig) *QueryCache {
	return &QueryCache{client: client, cfg: cfg}
}

// InitializeIndex creates the RediSearch schema if it doesn't exist
func (c *QueryCache) InitializeIndex(ctx context.Context) error {
	// 768 is the exact dimension size for Google's text-embedding-004 model
	err := c.client.Do(ctx,
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

// GetCache performs a Vector KNN search in Redis
func (q *QueryCache) GetCache(ctx context.Context, queryVector []float32, threshold float64) (string, bool) {
	vectorBytes := float32ToByte(queryVector)

	// FT.SEARCH query: Find the 1 nearest neighbor using the vector
	searchQuery := "*=>[KNN 1 @embedding $vec AS distance]"

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
		slog.Error("❌ Redis Vector Search Error (Degraded)", "error", err)
		return "", false
	}

	// Parse the raw RediSearch response
	results, ok := res.([]interface{})
	if !ok || len(results) == 0 || results[0].(int64) == 0 {
		slog.Debug("💾 Semantic Cache Miss (No entries found)")
		return "", false
	}

	// Extract the fields from the response array
	fields := results[2].([]interface{})
	var answer string
	var distance float64

	for i := 0; i < len(fields); i += 2 {
		key := fields[i].(string)
		val := fields[i+1].(string)
		if key == "answer" {
			answer = val
		} else if key == "distance" {
			fmt.Sscanf(val, "%f", &distance)
		}
	}

	// Convert Cosine Distance to Similarity (1.0 - distance)
	similarity := 1.0 - distance

	// Extensive Logging for Observability
	slog.Debug("🧮 Semantic Cache Evaluation",
		"redis_raw_distance", distance,
		"calculated_similarity", similarity,
	)

	if similarity >= threshold {
		slog.Info("🎯 Semantic Cache Hit",
			"similarity_score", fmt.Sprintf("%.4f", similarity),
		)
		return answer, true
	}

	slog.Info("💨 Semantic Cache Miss (Below Threshold)",
		"similarity_score", fmt.Sprintf("%.4f", similarity),
	)
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

// Helper: Redis requires vectors to be raw byte arrays
func float32ToByte(f []float32) []byte {
	buf := make([]byte, len(f)*4)
	for i, v := range f {
		binary.LittleEndian.PutUint32(buf[i*4:], math.Float32bits(v))
	}
	return buf
}
