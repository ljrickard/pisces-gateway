package api

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

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

	var chatHistory []string
	if !metadata.Flags.NoSession && metadata.SessionID != "" {
		history, err := s.PipelineV1.Sessionstore.GetSession(ctx, metadata.SessionID, metadata.Flags.ContextHistoryLimit)
		if err != nil {
			slog.Warn("Failed to fetch session history from Redis", "error", err)
		} else {
			chatHistory = history
		}
	}

	state := &pregel.AgentState{
		Query:     reqBody.Message,
		SessionID: metadata.SessionID,
		IsStream:  reqBody.Stream,
		History:   chatHistory,
		Flags:     metadata.Flags,
		ReqConfig: reqBody.Config,
		LoopCount: 0,
	}

	// 🌊 STATUS STREAM INIT
	if state.IsStream {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")

		flusher, ok := w.(http.Flusher)
		if !ok {
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

	s.saveToCacheAsync(ctx, state)

	// 4. THE DOWNSTREAM STREAMING PATH 🌊 (Frasier Bot)
	if state.StreamBody != nil {
		defer state.StreamBody.Close()
		flusher, _ := w.(http.Flusher)

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
			flusher.Flush()
		}
		return
	}

	// 5. THE FALLBACK PATH 🧱 (Generic LLM or Semantic Cache Hits)
	if state.IsStream {
		flusher, _ := w.(http.Flusher)
		slog.Info("🌊 [Gateway] Pumping synthetic SSE stream for local graph result", "trace_id", traceID)

		// To satisfy the bash script's spinner, we chunk the text block into SSE events
		// This creates a fast, fake "typing" effect!
		words := strings.Split(state.FinalAnswer, " ")
		for i, word := range words {
			prefix := ""
			if i > 0 {
				prefix = " "
			}
			// Clean newlines to prevent SSE framing corruption
			safeWord := strings.ReplaceAll(prefix+word, "\n", " ")
			fmt.Fprintf(w, "data: %s\n\n", safeWord)
			flusher.Flush()
			time.Sleep(20 * time.Millisecond)
		}

		fmt.Fprintf(w, "data: [DONE]\n\n")
		flusher.Flush()
		return
	}

	// 6. THE BLOCKING PATH (Standard JSON)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"response": state.FinalAnswer,
		"domain":   state.Domain,
		"trace_id": traceID,
	})
}
