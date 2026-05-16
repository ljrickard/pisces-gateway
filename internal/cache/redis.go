package cache

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"time"

	"pisces-gateway/tracing"

	"github.com/redis/go-redis/v9"
)

const (
	SessionTTL = 7 * 24 * time.Hour
)

type RetryConfig struct {
	Timeout    time.Duration
	MaxRetries int
	BaseDelay  time.Duration
}

func NewRedisConnection(ctx context.Context, addr string) (*redis.Client, error) {
	traceID := tracing.GetTraceID(ctx)
	rdb := redis.NewClient(&redis.Options{Addr: addr})

	if err := rdb.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("redis ping failed: %w", err)
	}

	slog.Info("✅ Redis connection established", "addr", addr, "trace_id", traceID)
	return rdb, nil
}

func executeWithRetry(ctx context.Context, cfg RetryConfig, operation func(context.Context) error) error {
	traceID := tracing.GetTraceID(ctx)
	for attempt := 0; attempt <= cfg.MaxRetries; attempt++ {
		opCtx, cancel := context.WithTimeout(ctx, cfg.Timeout)
		err := operation(opCtx)
		cancel()

		if err == nil {
			return nil
		}

		if err == redis.Nil {
			return err
		}

		if attempt == cfg.MaxRetries {
			return err
		}

		delay := cfg.BaseDelay * (1 << uint(attempt))
		jitter := time.Duration(rand.Int63n(int64(delay) / 4))
		wait := delay + jitter

		slog.Warn("⚠️ Redis transient network error, spinning up retry backoff", "trace_id", traceID, "error", err, "attempt", attempt+1)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(wait):
		}
	}
	return fmt.Errorf("max retries exceeded")
}
