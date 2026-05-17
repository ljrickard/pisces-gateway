package pipeline

import (
	"context"
	"log/slog"
	"pisces-gateway/internal/config"
	"pisces-gateway/tracing"
	"time"

	"go.opentelemetry.io/otel"
)

func (p *Pipeline) ExecuteWithSession(ctx context.Context, rawQuery string, sessionID string, requestID string, flags config.FeatureState, botConfigs map[string]any) (string, []string, []string) {
	tracer := otel.Tracer("pipeline-module")
	ctx, span := tracer.Start(ctx, "Pipeline.ExecuteWithSession")
	defer span.End()

	traceID := tracing.GetTraceID(ctx)
	var history []string
	var err error

	if !flags.NoSession {
		slog.Debug("🔍 [Session History] Fetching history...", "session_id", sessionID, "request_id", requestID, "trace_id", traceID)

		ctx, redisGetSpan := tracer.Start(ctx, "Redis.GetSession")
		history, err = p.Sessionstore.GetSession(ctx, sessionID, flags.ContextHistoryLimit)
		redisGetSpan.End()

		if err != nil {
			slog.Error("❌ [Session History] Error fetching session", "session_id", sessionID, "request_id", requestID, "trace_id", traceID, "error", err)
			return "I encountered an issue retrieving your session. Please try again.", nil, nil
		}
	} else {
		slog.Info("⏩ [Session History] Read skipped via NoSession flag", "request_id", requestID, "trace_id", traceID)
	}

	answer, contexts, rawContexts := p.Execute(ctx, rawQuery, history, requestID, flags, botConfigs)

	if !flags.NoSession {
		go func() {
			bgCtx := context.Background()
			p.Sessionstore.SaveSession(bgCtx, sessionID, "User: "+rawQuery)
			p.Sessionstore.SaveSession(bgCtx, sessionID, "Bot: "+answer)
		}()
	}

	return answer, contexts, rawContexts
}

func (p *Pipeline) Execute(ctx context.Context, rawQuery string, history []string, requestID string, flags config.FeatureState, botConfigs map[string]any) (string, []string, []string) {
	tracer := otel.Tracer("pipeline-module")
	ctx, span := tracer.Start(ctx, "Pipeline.Execute")
	defer span.End()

	traceID := tracing.GetTraceID(ctx)
	slog.Info("🚀 [Pipeline] Request started", "request_id", requestID, "raw_query", rawQuery, "trace_id", traceID)

	rewritten := rawQuery
	if p.Rewriter != nil && len(history) > 0 {
		ctx, rewriteSpan := tracer.Start(ctx, "Gemini.RewriteQuery")
		rewritten = p.Rewriter.Resolve(ctx, rawQuery, history)
		rewriteSpan.End()

		slog.Info("✍️ [Query Rewriter] Processed query", "request_id", requestID, "original", rawQuery, "rewritten", rewritten, "trace_id", traceID)
	} else if p.Rewriter == nil {
		slog.Info("⏩ [Query Rewriter] Disabled or Nil - Skipping", "request_id", requestID, "trace_id", traceID)
	} else {
		slog.Info("⏩ [Query Rewriter] Skipped (No history provided)", "request_id", requestID, "trace_id", traceID)
	}

	var queryVector []float32
	if p.Embedder != nil && !flags.SkipCache {
		slog.Debug("🧠 [Embedder] Generating vectors for cache search...", "request_id", requestID, "trace_id", traceID)

		ctx, embedSpan := tracer.Start(ctx, "Vertex.EmbedQuery")
		var err error
		queryVector, err = p.Embedder.EmbedText(ctx, rewritten)
		embedSpan.End()

		if err != nil {
			slog.Error("⚠️ [Embedder] Failed, bypassing semantic cache", "request_id", requestID, "trace_id", traceID, "error", err)
		}
	}

	if !flags.SkipCache && queryVector != nil {
		slog.Info("🔎 [Semantic Cache] Executing vector search...", "request_id", requestID, "active_threshold", flags.SimilarityThreshold, "trace_id", traceID)

		ctx, cacheSpan := tracer.Start(ctx, "Redis.SemanticCacheLookup")
		cached, hit := p.Querycache.GetCache(ctx, queryVector, flags.SimilarityThreshold)
		cacheSpan.End()

		if hit {
			slog.Info("🛑 [Pipeline] Halted early: Returning cached response.", "request_id", requestID, "trace_id", traceID)
			return cached, nil, nil
		}
		slog.Info("💨 [Pipeline] Cache miss, proceeding to backend.", "request_id", requestID, "trace_id", traceID)
	} else {
		slog.Info("⏩ [Semantic Cache] Skipped via configuration.", "request_id", requestID, "trace_id", traceID)
	}

	domain := "frasier"
	if p.Intent != nil {
		ctx, intentSpan := tracer.Start(ctx, "Gemini.ClassifyIntent")
		domain = p.Intent.Determine(ctx, rewritten)
		intentSpan.End()

		slog.Info("🧭 [Intent] Classified request", "request_id", requestID, "domain", domain, "trace_id", traceID)
	} else {
		slog.Info("⏩ [Intent] Disabled or Nil - Defaulting to frasier", "request_id", requestID, "trace_id", traceID)
	}

	var answer string
	contexts := make([]string, 0)
	rawContexts := make([]string, 0)

	if domain == "frasier" {
		payload := map[string]any{
			"query":      rewritten,
			"request_id": requestID,
		}

		if specificConfig, exists := botConfigs[domain]; exists {
			payload["config"] = specificConfig
			slog.Info("📦 [Payload] Attached specific bot config", "request_id", requestID, "domain", domain, "trace_id", traceID)
		}

		slog.Info("📞 [Network] Forwarding payload to downstream bot...", "request_id", requestID, "domain", domain, "trace_id", traceID)

		ctx, networkSpan := tracer.Start(ctx, "HTTP.FrasierBotCall")
		botResponse, err := p.FrasierBot.ForwardChat(ctx, payload)
		networkSpan.End()

		if err != nil {
			slog.Error("❌ [Network] Downstream error", "request_id", requestID, "trace_id", traceID, "error", err)
			return "I'm having trouble reaching the Frasier bot right now. Please try again later.", contexts, rawContexts
		}

		if rawAns, ok := botResponse["answer"].(string); ok {
			answer = rawAns
		} else if rawResp, ok := botResponse["response"].(string); ok {
			answer = rawResp
		}

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

		slog.Info("✅ [Network] Downstream bot replied successfully.", "request_id", requestID, "trace_id", traceID)
	} else {
		slog.Warn("🎙️ [Routing] Query out of domain, using fallback", "request_id", requestID, "domain", domain, "trace_id", traceID)
		answer = "I don't have a specialized bot configured for that topic yet!"
	}

	go func() {
		if !flags.SkipCache && queryVector != nil {
			bgCtx := context.Background()
			slog.Debug("💾 [Semantic Cache] Storing new entry...", "request_id", requestID)
			p.Querycache.SetCache(bgCtx, rewritten, answer, queryVector, 1*time.Hour)
		}
	}()

	slog.Info("🏁 [Pipeline] Complete", "request_id", requestID, "trace_id", traceID)
	return answer, contexts, rawContexts
}
