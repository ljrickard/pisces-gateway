package pipeline

import (
	"context"
	"log/slog"
	"pisces-gateway/internal/config"
	"time"
)

type Normalizer interface {
	Process(ctx context.Context, query string) string
}

type Rewriter interface {
	Resolve(ctx context.Context, query string, history []string) string
}

type Cache interface {
	GetCache(ctx context.Context, key string) (string, bool)
	SetCache(ctx context.Context, key string, value string, ttl time.Duration) error
	GetSession(ctx context.Context, sessionID string, limit int) ([]string, error)
	SaveSession(ctx context.Context, sessionID string, message string) error
}

type Intent interface {
	Determine(ctx context.Context, query string) string
}

type Proxy interface {
	Forward(ctx context.Context, backend string, query string, flags config.FeatureState) string
}

type Pipeline struct {
	Normalizer Normalizer
	Rewriter   Rewriter
	Cache      Cache
	Intent     Intent
	Proxy      Proxy
}

func (p *Pipeline) Execute(ctx context.Context, rawQuery string, sessionID string, flags config.FeatureState) string {
	slog.Info("🚀 Pipeline Started", "session_id", sessionID, "raw_query", rawQuery)

	// 1. Normalize
	clean := p.Normalizer.Process(ctx, rawQuery)

	// 2. Fetch History
	history, err := p.Cache.GetSession(ctx, sessionID, flags.ContextHistoryLimit)
	if err != nil {
		slog.Error("❌ History Fetch Failed", "session_id", sessionID, "error", err)
	}
	slog.Debug("📜 Context Retrieved", "session_id", sessionID, "count", len(history))

	// 3. Rewrite (The critical visibility step)
	rewritten := p.Rewriter.Resolve(ctx, clean, history)

	// Compare clean vs rewritten in the logs
	if clean != rewritten {
		slog.Info("🔄 Query Contextualized",
			"original", clean,
			"rewritten", rewritten,
		)
	} else {
		slog.Debug("✅ No Rewrite Needed", "query", clean)
	}

	// 4. Cache Check
	if !flags.BypassCache {
		if cached, hit := p.Cache.GetCache(ctx, rewritten); hit {
			slog.Info("🎯 Semantic Cache Hit", "key", rewritten)
			return cached
		}
	}

	backend := p.Intent.Determine(ctx, rewritten)
	response := p.Proxy.Forward(ctx, backend, rewritten, flags)

	// Update everything in the background
	go func() {
		bgCtx := context.Background()
		if !flags.BypassCache {
			p.Cache.SetCache(bgCtx, rewritten, response, 1*time.Hour)
		}
		p.Cache.SaveSession(bgCtx, sessionID, "User: "+rawQuery)
		p.Cache.SaveSession(bgCtx, sessionID, "Frasier: "+response)
	}()

	return response
}
