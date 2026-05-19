package api

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"pisces-gateway/internal/config"
	"pisces-gateway/internal/pregel"
	"pisces-gateway/tracing"
)

func (s *Server) HandleChatV2(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	traceID := tracing.GetTraceID(ctx)
	w.Header().Set("X-Trace-Id", traceID)

	var reqBody ChatRequest
	if err := json.NewDecoder(r.Body).Decode(&reqBody); err != nil {
		http.Error(w, "Invalid JSON payload", http.StatusBadRequest)
		return
	}

	metadata, _ := config.ParseRequestMetadata(r)

	state := &pregel.AgentState{
		Query:     reqBody.Message,
		IsStream:  reqBody.Stream,
		History:   []string{"User: Who is Niles?", "Bot: He is Frasier's brother."},
		Config:    metadata.Flags,
		LoopCount: 0,
	}

	// 🌊 STATUS STREAM INIT
	if state.IsStream {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")

		flusher, ok := w.(http.Flusher)
		if !ok {
			slog.Error("❌ [Handlers V2 STREAM INIT] Streaming unsupported by response socket writer", "trace_id", traceID)
			http.Error(w, "Streaming Unsupported", http.StatusInternalServerError)
			return
		}

		state.StatusStream = func(msg string) {
			fmt.Fprintf(w, "event: status\ndata: %s\n\n", msg)
			flusher.Flush()
		}
	}

	slog.Info("▶️ [V2 Engine] Starting graph execution", "trace_id", traceID)
	err := s.GraphV2.Run(ctx, state)
	if err != nil {
		slog.Error("❌ [V2 Engine] Graph Execution Failed", "trace_id", traceID, "error", err)
		http.Error(w, "Graph Execution Failed", http.StatusInternalServerError)
		return
	}
	slog.Info("⏹️ [V2 Engine] Graph execution finished", "trace_id", traceID, "loops", state.LoopCount)

	// 4. THE STREAMING PATH 🌊 (Final Answer Tokens)
	if state.StreamBody != nil {
		defer state.StreamBody.Close()
		flusher, ok := w.(http.Flusher)
		if !ok {
			slog.Error("❌ [Handlers V1 STREAMING PATH] Streaming unsupported by response socket writer", "trace_id", traceID)
			http.Error(w, "Streaming Unsupported", http.StatusInternalServerError)
			return
		}

		slog.Info("🌊 [Gateway] Pumping SSE stream to client", "trace_id", traceID)

		// 🐛 THE FIX: Raw Byte Reader, EXACTLY like V1!
		reader := bufio.NewReader(state.StreamBody)
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

	// 5. THE BLOCKING PATH 🧱
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"response": state.FinalAnswer,
		"domain":   state.Domain,
		"trace_id": traceID,
	})
}
