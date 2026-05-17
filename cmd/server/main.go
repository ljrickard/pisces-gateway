package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	texporter "github.com/GoogleCloudPlatform/opentelemetry-operations-go/exporter/trace"
	"github.com/google/uuid"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"

	"pisces-gateway/internal/cache"
	"pisces-gateway/internal/config"
	"pisces-gateway/internal/gemini"
	"pisces-gateway/internal/intent"
	"pisces-gateway/internal/pipeline"
	"pisces-gateway/internal/proxy"
	"pisces-gateway/internal/rewrite"
	"pisces-gateway/tracing"

	secretmanager "cloud.google.com/go/secretmanager/apiv1"
	"cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"
)

type ChatRequest struct {
	Message string         `json:"message"`
	Config  map[string]any `json:"config,omitempty"`
	Stream  bool           `json:"stream,omitempty"`
}

// --- OpenAI Spec Structs ---
type OpenAIMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type OpenAIChatRequest struct {
	Model    string          `json:"model"`
	Messages []OpenAIMessage `json:"messages"`
	Stream   bool            `json:"stream,omitempty"`
}

type OpenAIChoice struct {
	Index        int           `json:"index"`
	Message      OpenAIMessage `json:"message"`
	FinishReason string        `json:"finish_reason"`
}

type OpenAIStreamDelta struct {
	Content string `json:"content,omitempty"`
	Role    string `json:"role,omitempty"`
}

type OpenAIStreamChoice struct {
	Index        int               `json:"index"`
	Delta        OpenAIStreamDelta `json:"delta"`
	FinishReason *string           `json:"finish_reason"`
}

type OpenAIStreamResponse struct {
	ID      string               `json:"id"`
	Object  string               `json:"object"`
	Created int64                `json:"created"`
	Model   string               `json:"model"`
	Choices []OpenAIStreamChoice `json:"choices"`
}

type OpenAIUsage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
	TotalTokens      int `json:"total_tokens"`
}

type OpenAIChatResponse struct {
	ID      string         `json:"id"`
	Object  string         `json:"object"`
	Created int64          `json:"created"`
	Model   string         `json:"model"`
	Choices []OpenAIChoice `json:"choices"`
	Usage   OpenAIUsage    `json:"usage"`
}

type OpenAIModel struct {
	ID      string `json:"id"`
	Object  string `json:"object"`
	Created int64  `json:"created"`
	OwnedBy string `json:"owned_by"`
}

type OpenAIModelsResponse struct {
	Object string        `json:"object"`
	Data   []OpenAIModel `json:"data"`
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

func main() {
	level := slog.LevelInfo
	if os.Getenv("LOGGING_LEVEL") == "DEBUG" {
		level = slog.LevelDebug
	}
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level})))

	slog.Info("🐟 Starting Pisces API Gateway...")

	tp, err := initTracer("pisces-12")
	if err != nil {
		slog.Error("❌ Failed to initialize tracing", "error", err)
	} else {
		slog.Info("🔭 OpenTelemetry tracing enabled via GCP Cloud Trace")
		otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
		defer tp.Shutdown(context.Background())
	}

	startupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	projectID := "pisces-12"
	secretName := "gemini-api-key"
	apiKey, err := getSecret(startupCtx, projectID, secretName)
	if err != nil {
		log.Fatalf("Error loading API key from Secret Manager: %v", err)
	}

	geminiCfg := gemini.Config{
		APIKey:         apiKey,
		ProjectID:      os.Getenv("GEMINI_PROJECT"),
		Location:       os.Getenv("GEMINI_LOCATION"),
		TextModel:      os.Getenv("GEMINI_MODEL"),
		EmbeddingModel: os.Getenv("EMBEDDING_MODEL"),
		Retry: gemini.RetryConfig{
			MaxRetries: 3,
			BaseDelay:  2 * time.Second,
		},
	}

	if geminiCfg.TextModel == "" || geminiCfg.ProjectID == "" {
		slog.Error("❌ GEMINI_MODEL or GEMINI_PROJECT environment variable is not set")
		os.Exit(1)
	}

	geminiClient, err := gemini.NewClient(startupCtx, geminiCfg)
	if err != nil {
		slog.Error("❌ Critical failure: Gemini unreachable", "error", err)
		os.Exit(1)
	}

	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		slog.Error("REDIS_ADDR environment variable is required")
		os.Exit(1)
	}

	frasierURL := os.Getenv("FRASIER_BOT_URL")
	if frasierURL == "" {
		slog.Error("FRASIER_BOT_URL environment variable is required")
		os.Exit(1)
	}

	rawRedis, err := cache.NewRedisConnection(startupCtx, redisAddr)
	if err != nil {
		slog.Error("❌ Failed to connect to Redis", "error", err)
		os.Exit(1)
	}

	queryCache := cache.NewQueryCache(rawRedis, cache.RetryConfig{
		Timeout:    50 * time.Millisecond,
		MaxRetries: 0,
	})
	_ = queryCache.InitializeIndex(startupCtx)

	sessionStore := cache.NewSessionStore(rawRedis, cache.RetryConfig{
		Timeout:    100 * time.Millisecond,
		MaxRetries: 2,
		BaseDelay:  50 * time.Millisecond,
	})

	slog.Info("🔗 Connecting to downstream Frasier Bot...", "url", frasierURL)
	frasierClient, err := proxy.NewFrasierClient(frasierURL)
	if err != nil {
		slog.Error("❌ Critical failure: Downstream Frasier Bot is completely unreachable over network socket mesh", "error", err)
		os.Exit(1)
	}
	slog.Info("✅ Downstream Frasier Bot link verified and active.")

	p := &pipeline.Pipeline{
		Rewriter:     &rewrite.Rewriter{LLM: geminiClient},
		Intent:       &intent.Classifier{LLM: geminiClient},
		Embedder:     geminiClient,
		Sessionstore: sessionStore,
		Querycache:   queryCache,
		FrasierBot:   frasierClient,
	}

	mux := http.NewServeMux()

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("Pisces Gateway is Healthy"))
	})

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("Gateway OK"))
	})

	mux.HandleFunc("/cache", func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		traceID := tracing.GetTraceID(ctx)

		if r.Method != http.MethodDelete {
			slog.Warn("Method not allowed on /cache", "method", r.Method, "trace_id", traceID)
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		if err := queryCache.FlushCache(ctx); err != nil {
			slog.Error("❌ Failed to flush Redis cache via API", "trace_id", traceID, "error", err)
			http.Error(w, "Internal Server Error", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status": "success", "message": "Redis cache completely wiped"}`))
	})

	chatHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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

			streamBody, err := p.ExecuteStreamWithSession(ctx, req.Message, metadata.SessionID, requestID, metadata.Flags, req.Config, sendGatewayStatus)
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
		answer, contexts, rawContexts := p.ExecuteWithSession(ctx, req.Message, metadata.SessionID, requestID, metadata.Flags, req.Config)

		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("x-request-id", requestID)
		json.NewEncoder(w).Encode(map[string]any{
			"response":     answer,
			"contexts":     contexts,
			"raw_contexts": rawContexts,
			"trace_id":     traceID,
		})
	})

	mux.Handle("/chat", otelhttp.NewHandler(chatHandler, "POST /chat"))

	mux.HandleFunc("/v1/chat/completions", func(w http.ResponseWriter, r *http.Request) {
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
			handleOpenAIStream(w, r, frasierClient, req.Model, lastUserMessage, requestID, traceID)
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

		answer, _, _ := p.Execute(ctx, lastUserMessage, history, requestID, metadata.Flags, map[string]any{})

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
	})

	mux.HandleFunc("/v1/models", func(w http.ResponseWriter, r *http.Request) {
		resp := OpenAIModelsResponse{
			Object: "list",
			Data: []OpenAIModel{
				{ID: "pisces", Object: "model", Created: time.Now().Unix(), OwnedBy: "ljrickard"},
			},
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	})

	slog.Info("🚀 Pisces Gateway listening on :8080")
	_ = http.ListenAndServe(":8080", mux)
}

func getSecret(ctx context.Context, projectID, secretName string) (string, error) {
	smClient, err := secretmanager.NewClient(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to create Secret Manager client: %v", err)
	}
	defer smClient.Close()
	versionPath := fmt.Sprintf("projects/%s/secrets/%s/versions/latest", projectID, secretName)
	result, err := smClient.AccessSecretVersion(ctx, &secretmanagerpb.AccessSecretVersionRequest{Name: versionPath})
	if err != nil {
		return "", err
	}
	return string(result.Payload.Data), nil
}

func initTracer(projectID string) (*sdktrace.TracerProvider, error) {
	exporter, err := texporter.New(texporter.WithProjectID(projectID))
	if err != nil {
		return nil, err
	}
	res, err := resource.New(context.Background(), resource.WithAttributes(semconv.ServiceName("pisces-gateway")))
	if err != nil {
		return nil, err
	}
	tp := sdktrace.NewTracerProvider(sdktrace.WithBatcher(exporter), sdktrace.WithResource(res))
	otel.SetTracerProvider(tp)
	return tp, nil
}
