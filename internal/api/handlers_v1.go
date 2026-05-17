package api

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"pisces-gateway/internal/config"
	"pisces-gateway/tracing"

	"github.com/google/uuid"
)

func (s *Server) HandleChatV1(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	traceID := tracing.GetTraceID(ctx)
	w.Header().Set("X-Trace-Id", traceID)

	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	requestID := r.Header.Get("X-Client-Request-Id")
	if requestID == "" {
		requestID = uuid.New().String()
	}

	var req ChatRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}

	metadata, valid := config.ParseRequestMetadata(r)
	if !valid {
		metadata.Flags.NoSession = true
	}

	if req.Stream {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		flusher, _ := w.(http.Flusher)

		sendGatewayStatus := func(msg string) {
			fmt.Fprintf(w, "event: status\ndata: %s\n\n", msg)
			flusher.Flush()
		}

		streamBody, err := s.PipelineV1.ExecuteStreamWithSession(ctx, req.Message, metadata.SessionID, requestID, metadata.Flags, req.Config, sendGatewayStatus)
		if err != nil {
			slog.Error("❌ [Pipeline Stream] Execution aborted via runtime crash", "trace_id", traceID, "error", err)
			return
		}
		defer streamBody.Close()

		// FIX: Dynamic Reader prevents large token packet drops on the gateway egress leg
		reader := bufio.NewReader(streamBody)
		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				if err == io.EOF && len(line) > 0 {
					fmt.Fprint(w, line)
					flusher.Flush()
				}
				break
			}
			fmt.Fprint(w, line)
			flusher.Flush() // Flush immediately to avoid network queue lag
		}
		return
	}

	// Standard, non-streaming blocking execution path remains untouched
	answer, contexts, rawContexts := s.PipelineV1.ExecuteWithSession(ctx, req.Message, metadata.SessionID, requestID, metadata.Flags, req.Config)

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("x-request-id", requestID)
	json.NewEncoder(w).Encode(map[string]any{
		"response":     answer,
		"contexts":     contexts,
		"raw_contexts": rawContexts,
		"trace_id":     traceID,
	})
}
