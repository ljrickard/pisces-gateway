package api

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"pisces-gateway/internal/config"
	"pisces-gateway/internal/proxy"
	"pisces-gateway/tracing"
	"strings"
	"time"

	"github.com/google/uuid"
)

func (s *Server) handleOpenAICompletions(w http.ResponseWriter, r *http.Request) {
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

	var req OpenAIChatRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		slog.Warn("Malformed OpenAI request body", "request_id", requestID, "trace_id", traceID, "error", err)
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}

	var lastUserMessage string
	if len(req.Messages) > 0 {
		lastUserMessage = req.Messages[len(req.Messages)-1].Content
	}

	if req.Stream {
		handleOpenAIStream(w, r, s.FrasierClient, req.Model, lastUserMessage, requestID, traceID)
		return
	}

	var history []string
	if len(req.Messages) > 0 {
		for i := 0; i < len(req.Messages)-1; i++ {
			msg := req.Messages[i]
			prefix := "User: "
			if msg.Role == "assistant" {
				prefix = "Bot: "
			} else if msg.Role == "system" {
				prefix = "System: "
			}
			history = append(history, prefix+msg.Content)
		}
	}

	metadata, _ := config.ParseRequestMetadata(r)
	metadata.Flags.NoSession = true

	answer, _, _ := s.PipelineV1.Execute(ctx, lastUserMessage, history, requestID, metadata.Flags, map[string]any{})

	resp := OpenAIChatResponse{
		ID:      fmt.Sprintf("chatcmpl-%s", requestID),
		Object:  "chat.completion",
		Created: time.Now().Unix(),
		Model:   req.Model,
		Choices: []OpenAIChoice{
			{
				Index: 0,
				Message: OpenAIMessage{
					Role:    "assistant",
					Content: answer,
				},
				FinishReason: "stop",
			},
		},
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("x-request-id", requestID)
	json.NewEncoder(w).Encode(resp)
}

func handleOpenAIStream(w http.ResponseWriter, r *http.Request, frasierClient *proxy.FrasierClient, modelName string, lastUserMessage string, requestID string, traceID string) {
	ctx := r.Context()

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	flusher, ok := w.(http.Flusher)
	if !ok {
		slog.Error("❌ [OpenAI Stream] Streaming unsupported by response socket writer", "trace_id", traceID)
		http.Error(w, "Streaming Unsupported", http.StatusInternalServerError)
		return
	}

	botPayload := map[string]any{
		"query":      lastUserMessage,
		"request_id": requestID,
	}

	streamBody, err := frasierClient.ForwardChatStream(ctx, botPayload)
	if err != nil {
		slog.Error("❌ [OpenAI Stream] Downstream route unreachable", "trace_id", traceID, "error", err)
		http.Error(w, "Downstream stream unreachable", http.StatusBadGateway)
		return
	}
	defer streamBody.Close()

	var lastEvent string
	scanner := bufio.NewScanner(streamBody)
	for scanner.Scan() {
		line := scanner.Text()

		if strings.HasPrefix(line, "event: ") {
			lastEvent = strings.TrimPrefix(line, "event: ")
			continue
		}

		if strings.HasPrefix(line, "data: ") {
			dataContent := strings.TrimPrefix(line, "data: ")

			if lastEvent == "metadata" {
				lastEvent = ""
				continue
			}

			if dataContent == "[DONE]" {
				fmt.Fprintf(w, "data: [DONE]\n\n")
				flusher.Flush()
				break
			}

			openaiDelta := OpenAIStreamResponse{
				ID:      fmt.Sprintf("chatcmpl-%s", requestID),
				Object:  "chat.completion.chunk",
				Created: time.Now().Unix(),
				Model:   modelName,
				Choices: []OpenAIStreamChoice{
					{
						Index: 0,
						Delta: OpenAIStreamDelta{
							Content: dataContent,
						},
						FinishReason: nil,
					},
				},
			}
			blob, _ := json.Marshal(openaiDelta)
			fmt.Fprintf(w, "data: %s\n\n", string(blob))
			flusher.Flush()

			lastEvent = ""
		}
	}

	if err := scanner.Err(); err != nil {
		slog.Error("⚠️ [OpenAI Stream] Interruption translating downstream context wire chunks", "trace_id", traceID, "error", err)
	}
}
