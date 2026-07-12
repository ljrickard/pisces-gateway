package api

import (
	"context"
	"log/slog"
	"net/http"
	"pisces-gateway/internal/cache"
	"pisces-gateway/internal/pipeline"
	"pisces-gateway/internal/pregel"
	"pisces-gateway/internal/proxy"
	"pisces-gateway/tracing"
	"sync"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

// Server holds all the cross-cutting dependencies your handlers need
type Server struct {
	PipelineV1    *pipeline.Pipeline
	GraphV2       *pregel.Graph[pregel.AgentState]
	NodesV2       *pregel.GatewayNodes
	FrasierClient *proxy.FrasierClient
	QueryCache    *cache.QueryCache
	BgWG          sync.WaitGroup // 👈 Exported (capitalized) so main.go can await draining
}

// Mount attaches all the HTTP handlers to the main router
func (s *Server) Mount(mux *http.ServeMux) {
	mux.HandleFunc("/", s.healthCheck)
	mux.HandleFunc("/health", s.healthCheck)
	mux.HandleFunc("/cache", s.cacheFlush)

	// V1 Routes
	mux.Handle("/chat", otelhttp.NewHandler(http.HandlerFunc(s.HandleChatV1), "POST /chat"))

	// V2 Routes (Ready for the new engine)
	mux.Handle("/v2/chat", otelhttp.NewHandler(http.HandlerFunc(s.HandleChatV2), "POST /v2/chat"))

	// OpenAI Compatibility
	mux.Handle("/v1/chat/completions", otelhttp.NewHandler(http.HandlerFunc(s.handleOpenAICompletions), "POST /v1/chat/completions"))
}

func (s *Server) saveToCacheAsync(parentCtx context.Context, state *pregel.AgentState) {
	if state.Flags.SkipCache || state.IsCacheHit || state.HasError || state.Query == "" || state.FinalAnswer == "" {
		if state.IsCacheHit {
			slog.Debug("💾 Bypassing cache write: answer was already served from semantic cache")
		}
		if state.HasError {
			slog.Warn("🛡️ Bypassing cache write: graph execution completed with an error flag")
		}
		return
	}

	query := state.Query
	answer := state.FinalAnswer
	traceID := tracing.GetTraceID(parentCtx)

	s.BgWG.Add(1)
	go func(q, ans, tid string) {
		defer s.BgWG.Done()

		bgCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		slog.Debug("Asynchronously embedding and writing query to semantic cache...", "query", q, "trace_id", tid)

		vector, err := s.NodesV2.LLM.EmbedText(bgCtx, q)
		if err != nil {
			slog.Warn("Failed to generate embedding for async cache write", "error", err, "trace_id", tid)
			return
		}

		if err := s.QueryCache.SetCache(bgCtx, q, ans, vector, 24*time.Hour); err != nil {
			slog.Warn("Failed to write synthesized answer to Redis cache", "error", err, "trace_id", tid)
		} else {
			slog.Info("💾 Successfully updated V2 semantic cache in Redis", "trace_id", tid)
		}
	}(query, answer, traceID)
}
