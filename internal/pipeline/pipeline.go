package pipeline

import (
	"context"
	"log/slog"
	"time"

	"pisces-gateway/internal/cache"
	"pisces-gateway/internal/config"
	"pisces-gateway/internal/proxy"
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
	Rewriter     Rewriter
	Intent       Intent
	Embedder     Embedder
	Querycache   *cache.QueryCache
	Sessionstore *cache.SessionStore
	FrasierBot   *proxy.FrasierClient
}

// ExecuteWithSession manages Redis state and wraps the pure pipeline[cite: 4]
func (p *Pipeline) ExecuteWithSession(ctx context.Context, rawQuery string, sessionID string, requestID string, flags config.FeatureState, botConfigs map[string]any) (string, []string, []string) {
	var history []string
	var err error

	// 1. Fetch History from Redis[cite: 4]
	if !flags.NoSession {
		slog.Debug("🔍 [Session History] Fetching history...", "session_id", sessionID, "request_id", requestID)
		history, err = p.Sessionstore.GetSession(ctx, sessionID, flags.ContextHistoryLimit)
		if err != nil {
			slog.Error("❌ [Session History] Error fetching session", "session_id", sessionID, "request_id", requestID, "error", err)
			return "I encountered an issue retrieving your session. Please try again.", nil, nil
		}
	} else {
		slog.Info("⏩ [Session History] Read skipped via NoSession flag", "request_id", requestID)
	}

	// 2. Call the core, pure pipeline (passing requestID instead of sessionID)[cite: 4]
	answer, contexts, rawContexts := p.Execute(ctx, rawQuery, history, requestID, flags, botConfigs)

	// 3. Save History to Redis[cite: 4]
	if !flags.NoSession {
		go func() {
			bgCtx := context.Background()
			p.Sessionstore.SaveSession(bgCtx, sessionID, "User: "+rawQuery)
			p.Sessionstore.SaveSession(bgCtx, sessionID, "Bot: "+answer)
		}()
	}

	return answer, contexts, rawContexts
}

// Execute is entirely stateless and focuses purely on text processing and tracing[cite: 4]
func (p *Pipeline) Execute(ctx context.Context, rawQuery string, history []string, requestID string, flags config.FeatureState, botConfigs map[string]any) (string, []string, []string) {
	slog.Info("🚀 [Pipeline] Request started", "request_id", requestID, "raw_query", rawQuery)

	// 1. Rewrite the Query[cite: 4]
	rewritten := rawQuery
	if p.Rewriter != nil && len(history) > 0 {
		rewritten = p.Rewriter.Resolve(ctx, rawQuery, history)
		slog.Info("✍️ [Query Rewriter] Processed query", "request_id", requestID, "original", rawQuery, "rewritten", rewritten)
	} else if p.Rewriter == nil {
		slog.Info("⏩ [Query Rewriter] Disabled or Nil - Skipping", "request_id", requestID)
	} else {
		slog.Info("⏩ [Query Rewriter] Skipped (No history provided)", "request_id", requestID)
	}

	// 2. Generate the Embedding Vector[cite: 4]
	var queryVector []float32
	if p.Embedder != nil && !flags.SkipCache {
		slog.Debug("🧠 [Embedder] Generating vectors for cache search...", "request_id", requestID)
		var err error
		queryVector, err = p.Embedder.EmbedText(ctx, rewritten)
		if err != nil {
			slog.Error("⚠️ [Embedder] Failed, bypassing semantic cache", "request_id", requestID, "error", err)
		}
	}

	// 3. Check the Gateway Semantic Cache[cite: 4]
	if !flags.SkipCache && queryVector != nil {
		slog.Info("🔎 [Semantic Cache] Executing vector search...", "request_id", requestID, "active_threshold", flags.SimilarityThreshold)

		if cached, hit := p.Querycache.GetCache(ctx, queryVector, flags.SimilarityThreshold); hit {
			slog.Info("🛑 [Pipeline] Halted early: Returning cached response.", "request_id", requestID)
			return cached, nil, nil
		}
		slog.Info("💨 [Pipeline] Cache miss, proceeding to backend.", "request_id", requestID)
	} else {
		slog.Info("⏩ [Semantic Cache] Skipped via configuration.", "request_id", requestID)
	}

	// 4. Intent Routing[cite: 4]
	domain := "frasier"
	if p.Intent != nil {
		domain = p.Intent.Determine(ctx, rewritten)
		slog.Info("🧭 [Intent] Classified request", "request_id", requestID, "domain", domain)
	} else {
		slog.Info("⏩ [Intent] Disabled or Nil - Defaulting to frasier", "request_id", requestID)
	}

	var answer string
	contexts := make([]string, 0)
	rawContexts := make([]string, 0) // New: slice for pre-rerank results[cite: 3]

	// 5. Route based on Domain[cite: 4]
	if domain == "frasier" {
		payload := map[string]any{
			"query":      rewritten,
			"request_id": requestID,
		}

		if specificConfig, exists := botConfigs[domain]; exists {
			payload["config"] = specificConfig
			slog.Info("📦 [Payload] Attached specific bot config", "request_id", requestID, "domain", domain)
		}

		slog.Info("📞 [Network] Forwarding payload to downstream bot...", "request_id", requestID, "domain", domain)
		botResponse, err := p.FrasierBot.ForwardChat(ctx, payload)
		if err != nil {
			slog.Error("❌ [Network] Downstream error", "request_id", requestID, "error", err)
			return "I'm having trouble reaching the Frasier bot right now. Please try again later.", contexts, rawContexts
		}

		// Extract Answer[cite: 4]
		if rawAns, ok := botResponse["answer"].(string); ok {
			answer = rawAns
		} else if rawResp, ok := botResponse["response"].(string); ok {
			answer = rawResp
		}

		// Extract Reranked Contexts (Top-K)[cite: 4]
		if rawArray, ok := botResponse["contexts"].([]any); ok {
			for _, item := range rawArray {
				if strChunk, ok := item.(string); ok {
					contexts = append(contexts, strChunk)
				} else if mapChunk, ok := item.(map[string]any); ok {
					if content, ok := mapChunk["content"].(string); ok {
						contexts = append(contexts, content)
					}
				}
			}
		}

		// Extract Raw Contexts (The full Fetch-K pool)[cite: 3]
		if rawCtxArray, ok := botResponse["raw_contexts"].([]any); ok {
			for _, item := range rawCtxArray {
				if strChunk, ok := item.(string); ok {
					rawContexts = append(rawContexts, strChunk)
				} else if mapChunk, ok := item.(map[string]any); ok {
					if content, ok := mapChunk["content"].(string); ok {
						rawContexts = append(rawContexts, content)
					}
				}
			}
		}

		slog.Info("✅ [Network] Downstream bot replied successfully.", "request_id", requestID)
	} else {
		slog.Warn("🎙️ [Routing] Query out of domain, using fallback", "request_id", requestID, "domain", domain)
		answer = "I don't have a specialized bot configured for that topic yet!"
	}

	// 6. Update Semantic Cache[cite: 4]
	go func() {
		if !flags.SkipCache && queryVector != nil {
			bgCtx := context.Background()
			slog.Debug("💾 [Semantic Cache] Storing new entry...", "request_id", requestID)
			p.Querycache.SetCache(bgCtx, rewritten, answer, queryVector, 1*time.Hour)
		}
	}()

	slog.Info("🏁 [Pipeline] Complete", "request_id", requestID)
	return answer, contexts, rawContexts
}
