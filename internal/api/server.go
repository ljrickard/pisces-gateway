package api

import (
	"context"
	"log/slog"
	"net/http"
	"pisces-gateway/internal/cache"
	"pisces-gateway/internal/pipeline"
	"pisces-gateway/internal/pregel"
	"pisces-gateway/internal/proxy"
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

// saveToCacheAsync executes the embedding generation and Redis persistence
// in a detached background goroutine so the HTTP socket closes immediately.
func (s *Server) saveToCacheAsync(query string, answer string, skipCache bool) {
	if skipCache || query == "" || answer == "" {
		return
	}

	s.BgWG.Add(1) // 👈 Register the background task BEFORE spawning
	go func(q, ans string) {
		defer s.BgWG.Done() // 👈 Guarantee release when the write finishes or times out

		bgCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		slog.Debug("Asynchronously embedding and writing query to semantic cache...", "query", q)

		vector, err := s.NodesV2.LLM.EmbedText(bgCtx, q)
		if err != nil {
			slog.Warn("Failed to generate embedding for async cache write", "error", err)
			return
		}

		// 🚀 THE FIX: Pass query, answer, vector, and a 24-hour TTL to match the interface!
		if err := s.QueryCache.SetCache(bgCtx, q, ans, vector, 24*time.Hour); err != nil {
			slog.Warn("Failed to write synthesized answer to Redis cache", "error", err)
		} else {
			slog.Info("💾 Successfully updated V2 semantic cache in Redis")
		}
	}(query, answer)
}
