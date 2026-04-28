package cache

import (
	"context"
	"log/slog"

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
	fullKey := PrefixSession + sessionID
	slog.Debug("📜 Fetching Session History", "session_id", sessionID, "limit", limit)

	var res []string

	// Wrap the call in our robust retry logic!
	err := executeWithRetry(ctx, r.cfg, func(opCtx context.Context) error {
		var innerErr error
		res, innerErr = r.client.LRange(opCtx, fullKey, 0, int64(limit-1)).Result()
		return innerErr
	})

	if err != nil && err != redis.Nil {
		slog.Error("❌ Redis GetSession Error", "session_id", sessionID, "error", err)
		return nil, err
	}

	return res, nil
}

func (r *SessionStore) SaveSession(ctx context.Context, sessionID string, message string) error {
	fullKey := PrefixSession + sessionID

	err := executeWithRetry(ctx, r.cfg, func(opCtx context.Context) error {
		pipe := r.client.Pipeline()
		pipe.RPush(opCtx, fullKey, message)
		pipe.Expire(opCtx, fullKey, SessionTTL)

		_, innerErr := pipe.Exec(opCtx)
		return innerErr
	})

	if err != nil {
		// Graceful degradation: Log it, but don't crash the user's request
		slog.Error("❌ Redis SaveSession Error (Degraded)", "session_id", sessionID, "error", err)
	} else {
		slog.Debug("📝 Saved to Session", "session_id", sessionID)
	}

	return err
}
