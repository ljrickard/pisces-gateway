package api

import (
	"log/slog"
	"net/http"
	"pisces-gateway/tracing"
)

func (s *Server) cacheFlush(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	traceID := tracing.GetTraceID(ctx)

	if r.Method != http.MethodDelete {
		slog.Warn("Method not allowed on /cache", "method", r.Method, "trace_id", traceID)
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if err := s.QueryCache.FlushCache(ctx); err != nil {
		slog.Error("❌ Failed to flush Redis cache via API", "trace_id", traceID, "error", err)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"status": "success", "message": "Redis cache completely wiped"}`))
}
