package cache

import (
	"context"
	"log/slog"

	"pisces-gateway/tracing"

	"github.com/redis/go-redis/v9"
)

const (
	PrefixSession = "session:v1:"
)

type SessionStore struct {
	client *redis.Client
	cfg    RetryConfig
}

func NewSessionStore(client *redis.Client, cfg RetryConfig) *SessionStore {
	return &SessionStore{client: client, cfg: cfg}
}

func (r *SessionStore) GetSession(ctx context.Context, sessionID string, limit int) ([]string, error) {
	traceID := tracing.GetTraceID(ctx)
	fullKey := PrefixSession + sessionID
	slog.Debug("📜 Fetching Session History", "session_id", sessionID, "limit", limit, "trace_id", traceID)

	var res []string

	err := executeWithRetry(ctx, r.cfg, func(opCtx context.Context) error {
		var innerErr error
		res, innerErr = r.client.LRange(opCtx, fullKey, 0, int64(limit-1)).Result()
		return innerErr
	})

	if err != nil && err != redis.Nil {
		slog.Error("❌ Redis GetSession Error", "session_id", sessionID, "trace_id", traceID, "error", err)
		return nil, err
	}

	return res, nil
}

func (r *SessionStore) SaveSession(ctx context.Context, sessionID string, message string) error {
	traceID := tracing.GetTraceID(ctx)
	fullKey := PrefixSession + sessionID

	err := executeWithRetry(ctx, r.cfg, func(opCtx context.Context) error {
		pipe := r.client.Pipeline()
		pipe.RPush(opCtx, fullKey, message)
		pipe.Expire(opCtx, fullKey, SessionTTL)

		_, innerErr := pipe.Exec(opCtx)
		return innerErr
	})

	if err != nil {
		slog.Error("❌ Redis SaveSession Error (Degraded)", "session_id", sessionID, "trace_id", traceID, "error", err)
	} else {
		slog.Debug("📝 Saved to Session Store", "session_id", sessionID, "trace_id", traceID)
	}

	return err
}
