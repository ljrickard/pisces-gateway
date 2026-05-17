package pipeline

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"time"

	"pisces-gateway/internal/config"
	"pisces-gateway/tracing"

	"go.opentelemetry.io/otel"
)

func (p *Pipeline) ExecuteStreamWithSession(
	ctx context.Context,
	rawQuery string,
	sessionID string,
	requestID string,
	flags config.FeatureState,
	botConfigs map[string]any,
	statusCallback func(string),
) (io.ReadCloser, error) {
	tracer := otel.Tracer("pipeline-module")
	ctx, span := tracer.Start(ctx, "Pipeline.ExecuteStreamWithSession")
	defer span.End()

	traceID := tracing.GetTraceID(ctx)
	slog.Info("🚀 [Pipeline Stream] Request started", "request_id", requestID, "raw_query", rawQuery, "trace_id", traceID)

	var history []string
	var err error

	// 1. SESSION HISTORY
	if !flags.NoSession {
		tracing.SendGatewayStatus(statusCallback, "Redis.GetSession")
		slog.Debug("🔍 [Session History] Fetching history...", "session_id", sessionID, "request_id", requestID, "trace_id", traceID)

		ctx, redisGetSpan := tracer.Start(ctx, "Redis.GetSession")
		history, err = p.Sessionstore.GetSession(ctx, sessionID, flags.ContextHistoryLimit)
		redisGetSpan.End()

		if err != nil {
			slog.Error("❌ [Session History] Error fetching session", "session_id", sessionID, "request_id", requestID, "trace_id", traceID, "error", err)
			return nil, err
		}
	} else {
		slog.Info("⏩ [Session History] Read skipped via NoSession flag", "request_id", requestID, "trace_id", traceID)
	}

	// 2. QUERY REWRITING
	rewritten := rawQuery
	if p.Rewriter != nil && len(history) > 0 {
		tracing.SendGatewayStatus(statusCallback, "Gemini.RewriteQuery")
		ctx, rewriteSpan := tracer.Start(ctx, "Gemini.RewriteQuery")
		rewritten = p.Rewriter.Resolve(ctx, rawQuery, history)
		rewriteSpan.End()

		slog.Info("✍️ [Query Rewriter] Processed query", "request_id", requestID, "original", rawQuery, "rewritten", rewritten, "trace_id", traceID)
	} else if p.Rewriter == nil {
		slog.Info("⏩ [Query Rewriter] Disabled or Nil - Skipping", "request_id", requestID, "trace_id", traceID)
	} else {
		slog.Info("⏩ [Query Rewriter] Skipped (No history provided)", "request_id", requestID, "trace_id", traceID)
	}

	// 3. GENERATE VECTORS FOR SEMANTIC CACHE LOOKUP
	var queryVector []float32
	if p.Embedder != nil && !flags.SkipCache {
		tracing.SendGatewayStatus(statusCallback, "Vertex.EmbedQuery")
		slog.Debug("🧠 [Embedder] Generating vectors for cache search...", "request_id", requestID, "trace_id", traceID)

		ctx, embedSpan := tracer.Start(ctx, "Vertex.EmbedQuery")
		queryVector, err = p.Embedder.EmbedText(ctx, rewritten)
		embedSpan.End()

		if err != nil {
			slog.Error("⚠️ [Embedder] Failed, bypassing semantic cache", "request_id", requestID, "trace_id", traceID, "error", err)
		}
	}

	// 4. SEMANTIC CACHE SEARCH
	if !flags.SkipCache && queryVector != nil {
		slog.Info("🔎 [Semantic Cache] Executing vector search...", "request_id", requestID, "active_threshold", flags.SimilarityThreshold, "trace_id", traceID)

		ctx, cacheSpan := tracer.Start(ctx, "Redis.SemanticCacheLookup")
		cached, hit := p.Querycache.GetCache(ctx, queryVector, flags.SimilarityThreshold)
		cacheSpan.End()

		if hit {
			slog.Info("🛑 [Pipeline] Halted early: Returning cached response.", "request_id", requestID, "trace_id", traceID)

			if !flags.NoSession {
				go func() {
					bgCtx := context.Background()
					p.Sessionstore.SaveSession(bgCtx, sessionID, "User: "+rawQuery)
					p.Sessionstore.SaveSession(bgCtx, sessionID, "Bot: "+cached)
				}()
			}
			return io.NopCloser(strings.NewReader(fmt.Sprintf("data: %s\n\ndata: [DONE]\n\n", cached))), nil
		}
		slog.Info("💨 [Pipeline] Cache miss, proceeding to backend.", "request_id", requestID, "trace_id", traceID)
	} else {
		slog.Info("⏩ [Semantic Cache] Skipped via configuration.", "request_id", requestID, "trace_id", traceID)
	}

	// 5. INTENT CLASSIFICATION
	domain := "frasier"
	if p.Intent != nil {
		tracing.SendGatewayStatus(statusCallback, "Gemini.ClassifyIntent")
		ctx, intentSpan := tracer.Start(ctx, "Gemini.ClassifyIntent")
		domain = p.Intent.Determine(ctx, rewritten)
		intentSpan.End()

		slog.Info("🧭 [Intent] Classified request", "request_id", requestID, "domain", domain, "trace_id", traceID)
	} else {
		slog.Info("⏩ [Intent] Disabled or Nil - Defaulting to frasier", "request_id", requestID, "trace_id", traceID)
	}

	// 6. DISTRIBUTED ROUTING TARGET SELECTION
	if domain == "frasier" {
		tracing.SendGatewayStatus(statusCallback, "HTTP.FrasierBotCallStream")
		payload := map[string]any{
			"query":      rewritten,
			"request_id": requestID,
		}

		if specificConfig, exists := botConfigs[domain]; exists {
			payload["config"] = specificConfig
			slog.Info("📦 [Payload] Attached specific bot config", "request_id", requestID, "domain", domain, "trace_id", traceID)
		}

		slog.Info("📞 [Network] Forwarding payload to downstream bot...", "request_id", requestID, "domain", domain, "trace_id", traceID)

		ctx, networkSpan := tracer.Start(ctx, "HTTP.FrasierBotCallStream")
		streamBody, err := p.FrasierBot.ForwardChatStream(ctx, payload)
		networkSpan.End()
		if err != nil {
			slog.Error("❌ [Network] Downstream connection error", "request_id", requestID, "trace_id", traceID, "error", err)
			return nil, err
		}

		pr, pw := io.Pipe()
		go func() {
			defer streamBody.Close()
			defer pw.Close()

			var answerBuilder strings.Builder
			var lastEventType string

			// FIX: Swap Scanner for a dynamic Reader to lift the 64KB line limit
			reader := bufio.NewReader(streamBody)

			for {
				line, err := reader.ReadString('\n')
				if err != nil {
					// Handle any lingering trailing text before exiting at EOF
					if err == io.EOF && len(line) > 0 {
						fmt.Fprint(pw, line)
					}
					break
				}
				fmt.Fprint(pw, line) // Stream directly to client un-mangled

				// Trim the trailing newline for internal text processing parsing checks
				cleanLine := strings.TrimSuffix(line, "\n")

				if strings.HasPrefix(cleanLine, "event: ") {
					lastEventType = strings.TrimPrefix(cleanLine, "event: ")
					continue
				}

				if strings.HasPrefix(cleanLine, "data: ") {
					token := strings.TrimPrefix(cleanLine, "data: ")
					// Avoid caching raw metadata or done control sequences into the semantic layer
					if lastEventType != "status" && lastEventType != "metadata" && token != "[DONE]" && !strings.HasPrefix(token, "{") {
						answerBuilder.WriteString(token)
					}
				}

				if cleanLine == "" {
					lastEventType = ""
				}
			}

			finalAnswer := answerBuilder.String()
			if finalAnswer != "" {
				if !flags.NoSession {
					bgCtx := context.Background()
					p.Sessionstore.SaveSession(bgCtx, sessionID, "User: "+rawQuery)
					p.Sessionstore.SaveSession(bgCtx, sessionID, "Bot: "+finalAnswer)
				}

				if !flags.SkipCache && queryVector != nil {
					bgCtx := context.Background()
					slog.Debug("💾 [Semantic Cache] Storing new entry from stream...", "request_id", requestID)
					p.Querycache.SetCache(bgCtx, rewritten, finalAnswer, queryVector, 1*time.Hour)
				}
			}
			slog.Info("🏁 [Pipeline] Streaming complete", "request_id", requestID, "trace_id", traceID)
		}()

		return pr, nil
	}

	fallbackMsg := "I don't have a specialized bot configured for that topic yet!"
	return io.NopCloser(strings.NewReader(fmt.Sprintf("data: %s\n\ndata: [DONE]\n\n", fallbackMsg))), nil
}
