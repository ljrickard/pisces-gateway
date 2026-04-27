package cache

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	PrefixCache   = "cache:v1:"
	PrefixSession = "session:v1:"
	SessionTTL    = 7 * 24 * time.Hour // 7 Days
)

type RedisClient struct {
	client *redis.Client
}

func NewRedisClient(addr string) (*RedisClient, error) {
	rdb := redis.NewClient(&redis.Options{Addr: addr})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := rdb.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("redis ping failed: %w", err)
	}

	slog.Info("✅ Redis connection established", "addr", addr)
	return &RedisClient{client: rdb}, nil
}

func (r *RedisClient) GetCache(ctx context.Context, key string) (string, bool) {
	fullKey := PrefixCache + key
	val, err := r.client.Get(ctx, fullKey).Result()

	if err == redis.Nil {
		slog.Debug("💾 Cache Miss", "key", fullKey)
		return "", false
	} else if err != nil {
		slog.Error("❌ Redis GetCache Error", "key", fullKey, "error", err)
		return "", false
	}

	slog.Info("🎯 Cache Hit", "key", fullKey)
	return val, true
}

func (r *RedisClient) SetCache(ctx context.Context, key string, value string, ttl time.Duration) error {
	fullKey := PrefixCache + key
	err := r.client.Set(ctx, fullKey, value, ttl).Err()
	if err != nil {
		slog.Error("❌ Redis SetCache Error", "key", fullKey, "error", err)
		return err
	}
	slog.Debug("💾 Cache Stored", "key", fullKey, "ttl", ttl)
	return nil
}

func (r *RedisClient) GetSession(ctx context.Context, sessionID string, limit int) ([]string, error) {
	fullKey := PrefixSession + sessionID
	slog.Debug("📜 Fetching Session History", "session_id", sessionID, "limit", limit)

	// We still use the limit to only pull what the Pipeline requested
	res, err := r.client.LRange(ctx, fullKey, 0, int64(limit-1)).Result()
	if err != nil {
		slog.Error("❌ Redis GetSession Error", "session_id", sessionID, "error", err)
		return nil, err
	}

	return res, nil
}

func (r *RedisClient) SaveSession(ctx context.Context, sessionID string, message string) error {
	fullKey := PrefixSession + sessionID

	// Pipeline RPush and Expire together
	pipe := r.client.Pipeline()
	pipe.RPush(ctx, fullKey, message)
	pipe.Expire(ctx, fullKey, SessionTTL) // Refresh the 7-day window

	_, err := pipe.Exec(ctx)
	if err != nil {
		slog.Error("❌ Redis SaveSession Error", "session_id", sessionID, "error", err)
	} else {
		slog.Debug("📝 Saved to Session", "session_id", sessionID)
	}
	return err
}
