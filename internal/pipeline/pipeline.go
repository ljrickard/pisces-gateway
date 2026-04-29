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
	slog.Info("🚀 [Pipeline Start]", "session_id", sessionID, "raw_query", rawQuery)

	// 1. Fetch History & Rewrite the Query
	rewritten := rawQuery
	if p.Rewriter != nil {
		slog.Debug("Fetching session history...", "limit", flags.ContextHistoryLimit)
		history, err := p.Sessionstore.GetSession(ctx, sessionID, flags.ContextHistoryLimit)
		if err != nil {
			slog.Error("❌ GetSession returned an error", "error", err)
			return "I encountered an issue retrieving your session. Please try again.", nil
		}

		slog.Info("📖 [Session History]", "messages_retrieved", len(history), "history_content", history)

		rewritten = p.Rewriter.Resolve(ctx, rawQuery, history)
		slog.Info("✍️ [Query Rewriter]", "original", rawQuery, "rewritten", rewritten)
	} else {
		slog.Info("⏩ [Query Rewriter] Disabled or Nil - Skipping")
	}

	// 2. Generate the Embedding Vector
	var queryVector []float32
	if p.Embedder != nil && !flags.SkipCache {
		slog.Debug("Generating embeddings for vector cache search...")
		var err error
		queryVector, err = p.Embedder.EmbedText(ctx, rewritten)
		if err != nil {
			slog.Error("⚠️ Embedder failed, bypassing semantic cache", "error", err)
		}
	}

	// 3. Check the Gateway Cache
	if !flags.SkipCache && queryVector != nil {
		slog.Info("🔎 [Cache] Executing Vector Search...", "active_threshold", flags.SimilarityThreshold)

		if cached, hit := p.Querycache.GetCache(ctx, queryVector, flags.SimilarityThreshold); hit {
			slog.Info("🛑 [Pipeline Halted] Returning cached response early.")
			return cached, nil
		}
		slog.Info("💨 [Pipeline Continuing] Proceeding to backend routing.")
	} else {
		slog.Info("⏩ [Cache] Skipped via configuration.")
	}

	// 4. Intent Routing: Which bot should handle this?
	domain := "frasier" // Default assumption
	if p.Intent != nil {
		domain = p.Intent.Determine(ctx, rewritten)
		slog.Info("🧭 [Intent Classifier]", "determined_domain", domain, "evaluated_string", rewritten)
	} else {
		slog.Info("⏩ [Intent Classifier] Disabled or Nil - Defaulting to frasier")
	}

	var answer string
	contexts := make([]string, 0) // Prevents the JSON 'null' issue! Always serializes to []

	// 5. Route based on Domain
	if domain == "frasier" {
		payload := map[string]any{
			"query":      rewritten,
			"session_id": sessionID,
		}

		if specificConfig, exists := botConfigs[domain]; exists {
			payload["config"] = specificConfig
			slog.Info("📦 [Payload Generator] Attached specific bot config", "domain", domain, "config", specificConfig)
		}

		slog.Info("📞 [Network] Forwarding to downstream bot...", "domain", domain)
		botResponse, err := p.FrasierBot.ForwardChat(ctx, payload)
		if err != nil {
			slog.Error("❌ [Network] Failed to reach downstream bot", "error", err)
			return "I'm having trouble reaching the Frasier bot right now. Please try again later.", contexts
		}

		// Log exactly what the bot sent us so we can see the keys and structure
		slog.Debug("📦 [Gateway] Raw Downstream Payload", "len(payload)", len(botResponse))

		// --- 5a. Safely extract the Answer ---
		if rawAns, ok := botResponse["answer"].(string); ok {
			answer = rawAns
		} else if rawResp, ok := botResponse["response"].(string); ok {
			answer = rawResp
		} else if nested, ok := botResponse["response"].(map[string]any); ok {
			// If it got accidentally nested like {"response": {"answer": "..."}}
			if nestedAns, ok := nested["answer"].(string); ok {
				answer = nestedAns
			}
		} else {
			slog.Warn("⚠️ Gateway could not find a string answer in the bot payload", "payload", botResponse)
		}

		// --- 5b. Safely extract the Contexts ---
		var rawArray []any
		var isArray bool

		// Check both common keys
		if rawArray, isArray = botResponse["contexts"].([]any); !isArray {
			rawArray, isArray = botResponse["episodes"].([]any)
		}

		if isArray {
			for _, item := range rawArray {
				// CASE 1: The downstream bot sent a flat array of strings
				if strChunk, ok := item.(string); ok {
					contexts = append(contexts, strChunk)
				} else if mapChunk, ok := item.(map[string]any); ok {
					// CASE 2: The downstream bot sent an array of objects (like your logs show)
					// We need to pull out the actual text transcript field (usually "content" or "text")
					if contentStr, ok := mapChunk["content"].(string); ok {
						contexts = append(contexts, contentStr)
					} else if textStr, ok := mapChunk["text"].(string); ok {
						contexts = append(contexts, textStr)
					}
				}
			}
		} else {
			slog.Warn("⚠️ Gateway found neither 'contexts' nor 'episodes' as a valid array in the bot payload")
		}

		slog.Info("✅ [Network] Downstream bot replied successfully.")
	} else {
		// Generic fallback for any unhandled domains
		slog.Warn("🎙️ [Routing] Query out of domain, using fallback", "domain", domain)
		answer = "I don't have a specialized bot configured for that topic yet!"
	}

	// 6. Update Cache and History (Only on success paths)
	go func() {
		bgCtx := context.Background()
		if !flags.SkipCache && queryVector != nil {
			p.Querycache.SetCache(bgCtx, rewritten, answer, queryVector, 1*time.Hour)
		}

		slog.Debug("Saving conversation to Session Store...")
		p.Sessionstore.SaveSession(bgCtx, sessionID, "User: "+rawQuery)
		p.Sessionstore.SaveSession(bgCtx, sessionID, "Bot: "+answer)
	}()

	slog.Info("🏁 [Pipeline Complete]")
	return answer, contexts
}
