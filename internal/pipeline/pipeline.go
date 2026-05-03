package pipeline

import (
	"context"
	"log/slog"
	"time"

	"pisces-gateway/internal/cache"
	"pisces-gateway/internal/config"
	"pisces-gateway/internal/intent"
	"pisces-gateway/internal/proxy"
	"pisces-gateway/internal/rewrite"
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

func (p *Pipeline) Execute(ctx context.Context, rawQuery string, sessionID string, flags config.FeatureState, botConfigs map[string]any) (string, []string) {
	slog.Info("🚀 [Pipeline] Request started", "session_id", sessionID, "raw_query", rawQuery)

	// 1. Fetch History & Rewrite the Query
	rewritten := rawQuery
	if p.Rewriter != nil {
		if flags.NoSession {
			slog.Info("⏩ [Session History] Read skipped via NoSession flag")
		} else {
			slog.Debug("🔍 [Session History] Fetching history...", "limit", flags.ContextHistoryLimit)
			history, err := p.Sessionstore.GetSession(ctx, sessionID, flags.ContextHistoryLimit)
			if err != nil {
				slog.Error("❌ [Session History] GetSession returned an error", "error", err)
				return "I encountered an issue retrieving your session. Please try again.", nil
			}

			slog.Info("📖 [Session History] Retrieved successfully", "messages_retrieved", len(history))

			rewritten = p.Rewriter.Resolve(ctx, rawQuery, history)
			slog.Info("✍️ [Query Rewriter] Processed query", "original", rawQuery, "rewritten", rewritten)
		}
	} else {
		slog.Info("⏩ [Query Rewriter] Disabled or Nil - Skipping")
	}

	// 2. Generate the Embedding Vector
	var queryVector []float32
	if p.Embedder != nil && !flags.SkipCache {
		slog.Debug("🧠 [Embedder] Generating vectors for cache search...")
		var err error
		queryVector, err = p.Embedder.EmbedText(ctx, rewritten)
		if err != nil {
			slog.Error("⚠️ [Embedder] Failed, bypassing semantic cache", "error", err)
		}
	}

	// 3. Check the Gateway Cache
	if !flags.SkipCache && queryVector != nil {
		slog.Info("🔎 [Semantic Cache] Executing vector search...", "active_threshold", flags.SimilarityThreshold)

		if cached, hit := p.Querycache.GetCache(ctx, queryVector, flags.SimilarityThreshold); hit {
			slog.Info("🛑 [Pipeline] Halted early: Returning cached response.")

			go func() {
				bgCtx := context.Background()
				if !flags.NoSession {
					slog.Debug("💾 [Session History] Saving cached conversation...")
					p.Sessionstore.SaveSession(bgCtx, sessionID, "User: "+rawQuery)
					p.Sessionstore.SaveSession(bgCtx, sessionID, "Bot: "+cached)
				}
			}()

			return cached, nil
		}
		slog.Info("💨 [Pipeline] Cache miss, proceeding to backend.")
	} else {
		slog.Info("⏩ [Semantic Cache] Skipped via configuration.")
	}

	// 4. Intent Routing: Which bot should handle this?
	domain := "frasier"
	if p.Intent != nil {
		domain = p.Intent.Determine(ctx, rewritten)
		slog.Info("🧭 [Intent] Classified request", "determined_domain", domain)
	} else {
		slog.Info("⏩ [Intent] Disabled or Nil - Defaulting to frasier")
	}

	var answer string
	contexts := make([]string, 0)

	// 5. Route based on Domain
	if domain == "frasier" {
		payload := map[string]any{
			"query":      rewritten,
			"session_id": sessionID,
		}

		if specificConfig, exists := botConfigs[domain]; exists {
			payload["config"] = specificConfig
			slog.Info("📦 [Payload] Attached specific bot config", "domain", domain)
		}

		slog.Info("📞 [Network] Forwarding payload to downstream bot...", "domain", domain)
		botResponse, err := p.FrasierBot.ForwardChat(ctx, payload)
		if err != nil {
			slog.Error("❌ [Network] Failed to reach downstream bot", "error", err)
			return "I'm having trouble reaching the Frasier bot right now. Please try again later.", contexts
		}

		slog.Debug("📦 [Network] Raw downstream response payload", "len", len(botResponse))

		if rawAns, ok := botResponse["answer"].(string); ok {
			answer = rawAns
		} else if rawResp, ok := botResponse["response"].(string); ok {
			answer = rawResp
		} else if nested, ok := botResponse["response"].(map[string]any); ok {
			if nestedAns, ok := nested["answer"].(string); ok {
				answer = nestedAns
			}
		} else {
			slog.Warn("⚠️ [Network] Could not find 'answer' or 'response' string in payload")
		}

		var rawArray []any
		var isArray bool

		if rawArray, isArray = botResponse["contexts"].([]any); !isArray {
			rawArray, isArray = botResponse["episodes"].([]any)
		}

		if isArray {
			for _, item := range rawArray {
				if strChunk, ok := item.(string); ok {
					contexts = append(contexts, strChunk)
				} else if mapChunk, ok := item.(map[string]any); ok {
					if contentStr, ok := mapChunk["content"].(string); ok {
						contexts = append(contexts, contentStr)
					} else if textStr, ok := mapChunk["text"].(string); ok {
						contexts = append(contexts, textStr)
					}
				}
			}
		} else {
			slog.Warn("⚠️ [Network] Could not find 'contexts' or 'episodes' array in payload")
		}

		slog.Info("✅ [Network] Downstream bot replied successfully.")
	} else {
		slog.Warn("🎙️ [Routing] Query out of domain, using fallback", "domain", domain)
		answer = "I don't have a specialized bot configured for that topic yet!"
	}

	// 6. Update Cache and History (Only on success paths)
	go func() {
		bgCtx := context.Background()
		if !flags.SkipCache && queryVector != nil {
			slog.Debug("💾 [Semantic Cache] Storing new entry...")
			p.Querycache.SetCache(bgCtx, rewritten, answer, queryVector, 1*time.Hour)
		}

		if !flags.NoSession {
			slog.Debug("💾 [Session History] Saving conversation...")
			p.Sessionstore.SaveSession(bgCtx, sessionID, "User: "+rawQuery)
			p.Sessionstore.SaveSession(bgCtx, sessionID, "Bot: "+answer)
		} else {
			slog.Info("⏩ [Session History] Write skipped via NoSession flag")
		}
	}()

	slog.Info("🏁 [Pipeline] Complete")
	return answer, contexts
}
