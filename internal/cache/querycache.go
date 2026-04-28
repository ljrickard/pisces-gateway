package cache

import (
	"context"
	"log/slog"
	"time"

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

func (q *QueryCache) GetCache(ctx context.Context, key string) (string, bool) {
	fullKey := PrefixCache + key
	var val string

	err := executeWithRetry(ctx, q.cfg, func(opCtx context.Context) error {
		var innerErr error
		val, innerErr = q.client.Get(opCtx, fullKey).Result()
		return innerErr
	})

	if err == redis.Nil {
		slog.Debug("💾 Cache Miss", "key", fullKey)
		return "", false
	} else if err != nil {
		// Graceful Degradation: Log it and pretend it's a miss
		slog.Error("❌ Redis GetCache Error (Degraded)", "key", fullKey, "error", err)
		return "", false
	}

	slog.Info("🎯 Cache Hit", "key", fullKey)
	return val, true
}

func (q *QueryCache) SetCache(ctx context.Context, key string, value string, ttl time.Duration) error {
	fullKey := PrefixCache + key

	err := executeWithRetry(ctx, q.cfg, func(opCtx context.Context) error {
		return q.client.Set(opCtx, fullKey, value, ttl).Err()
	})

	if err != nil {
		// Graceful Degradation: Just log the failure, don't crash the pipeline
		slog.Error("❌ Redis SetCache Error (Degraded)", "key", fullKey, "error", err)
		return err
	}
	slog.Debug("💾 Cache Stored", "key", fullKey, "ttl", ttl)
	return nil
}
