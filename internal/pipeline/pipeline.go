package pipeline

import (
	"context"
	"log/slog"
	"pisces-gateway/internal/cache"
	"pisces-gateway/internal/config"
	"pisces-gateway/internal/intent"
	"pisces-gateway/internal/proxy"
	"pisces-gateway/internal/rewrite"
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

type Embedder interface {
	EmbedText(ctx context.Context, text string) ([]float32, error)
}

type Pipeline struct {
	Normalizer   Normalizer
	Rewriter     *rewrite.GeminiRewriter
	Intent       *intent.Classifier
	Embedder     Embedder
	Querycache   *cache.QueryCache
	Sessionstore *cache.SessionStore
	FrasierBot   *proxy.FrasierClient
}

func (p *Pipeline) Execute(ctx context.Context, rawQuery string, sessionID string, flags config.FeatureState, botConfigs map[string]any) string {
	// 1. Fetch History & Rewrite the Query
	rewritten := rawQuery
	if p.Rewriter != nil {
		history, err := p.Sessionstore.GetSession(ctx, sessionID, flags.ContextHistoryLimit)
		if err != nil {
			slog.Error("❌ GetSession returned an error", "error", err)
			return "I encountered an issue retrieving your session. Please try again."
		}
		rewritten = p.Rewriter.Resolve(ctx, rawQuery, history)
	}

	// 2. Generate the Embedding Vector
	var queryVector []float32
	if p.Embedder != nil && !flags.SkipCache {
		var err error
		queryVector, err = p.Embedder.EmbedText(ctx, rewritten)
		if err != nil {
			slog.Error("⚠️ Embedder failed, bypassing semantic cache", "error", err)
		}
	}

	// 3. Check the Gateway Cache using the dynamic threshold from flags!
	if !flags.SkipCache && queryVector != nil {
		if cached, hit := p.Querycache.GetCache(ctx, queryVector, flags.SimilarityThreshold); hit {
			return cached
		}
	}

	// 3. Intent Routing: Which bot should handle this?
	domain := "frasier" // Default assumption for now
	if p.Intent != nil {
		domain = p.Intent.Determine(ctx, rewritten)
	}

	var answer string

	// 4. Route based on Domain
	if domain == "frasier" {
		payload := map[string]any{
			"query":      rewritten,
			"session_id": sessionID,
		}

		if specificConfig, exists := botConfigs[domain]; exists {
			payload["config"] = specificConfig
			slog.Debug("Attached domain-specific config to downstream payload", "domain", domain)
		}

		botResponse, err := p.FrasierBot.ForwardChat(ctx, payload)
		if err != nil {
			slog.Error("❌ Failed to reach downstream service", "domain", domain, "error", err)
			return "I'm currently unable to reach the requested service. Please try again later."
		}

		var ok bool
		answer, ok = botResponse["answer"].(string)
		if !ok {
			slog.Error("❌ Unexpected response format from downstream service", "domain", domain)
			return "I received an invalid response from the backend service. Please try again."
		}
	} else {
		// Generic fallback for any unhandled domains
		slog.Info("🎙️ Query out of domain, using fallback", "domain", domain)
		answer = "I don't have a specialized bot configured for that topic yet!"
	}

	// 5. Update Cache and History (Only on success paths)
	go func() {
		bgCtx := context.Background()
		if !flags.SkipCache && queryVector != nil {
			// Now passing the vector along with the text!
			p.Querycache.SetCache(bgCtx, rewritten, answer, queryVector, 1*time.Hour)
		}
		p.Sessionstore.SaveSession(bgCtx, sessionID, "User: "+rawQuery)
		p.Sessionstore.SaveSession(bgCtx, sessionID, "Bot: "+answer)
	}()

	return answer
}
