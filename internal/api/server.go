package api

import (
	"net/http"
	"pisces-gateway/internal/cache"
	"pisces-gateway/internal/pipeline"
	"pisces-gateway/internal/pregel"
	"pisces-gateway/internal/proxy"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

// Server holds all the cross-cutting dependencies your handlers need
type Server struct {
	PipelineV1    *pipeline.Pipeline
	GraphV2       *pregel.Graph[pregel.AgentState]
	NodesV2       *pregel.GatewayNodes
	FrasierClient *proxy.FrasierClient
	QueryCache    *cache.QueryCache
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
