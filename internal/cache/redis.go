package cache

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	SessionTTL = 7 * 24 * time.Hour
)

// RetryConfig matches our Gemini pattern, adding a hard Timeout
type RetryConfig struct {
	Timeout    time.Duration // Per-attempt timeout so a hung Redis doesn't freeze the Gateway
	MaxRetries int
	BaseDelay  time.Duration
}

// NewRedisConnection establishes the raw socket using the provided context
func NewRedisConnection(ctx context.Context, addr string) (*redis.Client, error) {
	rdb := redis.NewClient(&redis.Options{Addr: addr})

	// Use the injected context for the Ping! No more hardcoded 5 seconds.
	if err := rdb.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("redis ping failed: %w", err)
	}

	slog.Info("✅ Redis connection established", "addr", addr)
	return rdb, nil
}

// executeWithRetry handles exponential backoff and safely ignores redis.Nil
func executeWithRetry(ctx context.Context, cfg RetryConfig, operation func(context.Context) error) error {
	for attempt := 0; attempt <= cfg.MaxRetries; attempt++ {
		// Apply the strict per-operation timeout
		opCtx, cancel := context.WithTimeout(ctx, cfg.Timeout)
		err := operation(opCtx)
		cancel()

		if err == nil {
			return nil
		}

		// CRITICAL: A Cache Miss is not a failure. Do not retry!
		if err == redis.Nil {
			return err
		}

		if attempt == cfg.MaxRetries {
			return err
		}

		delay := cfg.BaseDelay * (1 << uint(attempt))
		jitter := time.Duration(rand.Int63n(int64(delay) / 4))
		wait := delay + jitter

		slog.Warn("⚠️ Redis transient error, retrying...", "error", err, "attempt", attempt+1)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(wait):
		}
	}
	return fmt.Errorf("max retries exceeded")
}
