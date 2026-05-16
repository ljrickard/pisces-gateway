package cache

import (
	"context"
	"encoding/binary"
	"fmt"
	"log/slog"
	"math"
	"sync"
	"time"

	"pisces-gateway/tracing"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

const (
	PrefixCache = "cache:v1:"
)

type QueryCache struct {
	client *redis.Client
	cfg    RetryConfig
	mu     sync.RWMutex
}

func NewQueryCache(client *redis.Client, cfg RetryConfig) *QueryCache {
	return &QueryCache{client: client, cfg: cfg}
}

func (q *QueryCache) InitializeIndex(ctx context.Context) error {
	traceID := tracing.GetTraceID(ctx)
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

	if err != nil && err.Error() != "Index already exists" {
		slog.Error("⚠️ Failed to create Redis Vector Index", "trace_id", traceID, "error", err)
		return err
	}
	slog.Info("🔍 Redis Vector Index initialized (or already exists)", "trace_id", traceID)
	return nil
}

func (q *QueryCache) GetCache(ctx context.Context, queryVector []float32, threshold float64) (string, bool) {
	traceID := tracing.GetTraceID(ctx)

	if len(queryVector) != 768 {
		slog.Error("❌ [Cache] Vector dimension mismatch! RediSearch will ignore this.", "expected", 768, "got", len(queryVector), "trace_id", traceID)
		return "", false
	}

	vectorBytes := float32ToByte(queryVector)
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
		slog.Error("❌ Redis Vector Search Error", "trace_id", traceID, "error", err)
		return "", false
	}

	var answer string
	var distance float64
	var totalResults int64

	switch v := res.(type) {
	case []interface{}:
		if len(v) == 0 {
			return "", false
		}
		totalResults = v[0].(int64)
		if totalResults == 0 {
			slog.Info("💾 [Cache Miss] Index active, 0 matches found (RESP2)", "trace_id", traceID)
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
		if total, ok := v["total_results"].(int64); ok {
			totalResults = total
		} else if total, ok := v["total_results"].(int); ok {
			totalResults = int64(total)
		}

		if totalResults == 0 {
			slog.Info("💾 [Cache Miss] Index active, 0 matches found (RESP3)", "trace_id", traceID)
			return "", false
		}

		if resultsArr, ok := v["results"].([]interface{}); ok && len(resultsArr) > 0 {
			if doc, ok := resultsArr[0].(map[interface{}]interface{}); ok {
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
		slog.Error("❌ [Cache] Unrecognized Redis response type", "type", fmt.Sprintf("%T", res), "trace_id", traceID)
		return "", false
	}

	similarity := 1.0 - distance
	margin := similarity - threshold

	slog.Info("🧮 [Cache Evaluation]",
		"threshold", fmt.Sprintf("%.4f", threshold),
		"similarity", fmt.Sprintf("%.4f", similarity),
		"margin", fmt.Sprintf("%+.4f", margin),
		"trace_id", traceID,
	)

	if similarity >= threshold {
		slog.Info("🎯 [Cache Hit] Success!", "trace_id", traceID)
		return answer, true
	}

	slog.Info("📉 [Cache Miss] Score too low", "trace_id", traceID)
	return "", false
}

func (q *QueryCache) SetCache(ctx context.Context, query string, answer string, vector []float32, ttl time.Duration) error {
	traceID := tracing.GetTraceID(ctx)
	key := PrefixCache + uuid.New().String()
	vectorBytes := float32ToByte(vector)

	err := executeWithRetry(ctx, q.cfg, func(opCtx context.Context) error {
		pipe := q.client.Pipeline()
		pipe.HSet(opCtx, key, map[string]interface{}{
			"query":     query,
			"answer":    answer,
			"embedding": vectorBytes,
		})
		pipe.Expire(opCtx, key, ttl)
		_, innerErr := pipe.Exec(opCtx)
		return innerErr
	})

	if err != nil {
		slog.Error("❌ Redis Vector Set Error (Degraded)", "trace_id", traceID, "error", err)
		return err
	}

	slog.Debug("💾 Semantic Cache Stored", "key", key, "trace_id", traceID)
	return nil
}

func (q *QueryCache) FlushCache(ctx context.Context) error {
	traceID := tracing.GetTraceID(ctx)
	slog.Warn("🧹 [Cache Management] Strategic Wipe Initiated...", "trace_id", traceID)

	script := `
        local keys = redis.call('keys', ARGV[1])
        for i,k in ipairs(keys) do
            redis.call('del', k)
        end
        return #keys
    `
	err := q.client.Eval(ctx, script, []string{}, PrefixCache+"*").Err()
	if err != nil {
		slog.Error("⚠️ Cache script wipe failed, using fallback FlushDB pipeline", "trace_id", traceID, "error", err)
		q.client.FlushDB(ctx)
		q.InitializeIndex(ctx)
	}

	q.client.Eval(ctx, script, []string{}, "session:v1:*")

	slog.Info("✅ [Cache Management] Strategic wipe complete. Schema preserved.", "trace_id", traceID)
	return nil
}

func float32ToByte(f []float32) []byte {
	buf := make([]byte, len(f)*4)
	for i, v := range f {
		binary.LittleEndian.PutUint32(buf[i*4:], math.Float32bits(v))
	}
	return buf
}
