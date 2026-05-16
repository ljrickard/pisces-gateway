package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"os"
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

// ChatRequest represents the incoming JSON body from the user
type ChatRequest struct {
	Message string         `json:"message"`
	Config  map[string]any `json:"config,omitempty"`
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

// ---------------------------

func checkBotHealth(botURL string) error {
	client := http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(fmt.Sprintf("%s/health", botURL))
	if err != nil {
		return fmt.Errorf("network error reaching bot: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("bot returned non-200 status: %d", resp.StatusCode)
	}
	return nil
}

func main() {
	level := slog.LevelInfo
	if os.Getenv("LOGGING_LEVEL") == "DEBUG" {
		level = slog.LevelDebug
	}
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level})))

	slog.Info("🐟 Starting Pisces API Gateway...")

	tp, err := initTracer("pisces-12") // Your GCP Project ID
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

	slog.Info("Using Gemini configuration", slog.Any("config", map[string]any{
		"project_id":      geminiCfg.ProjectID,
		"location":        geminiCfg.Location,
		"text_model":      geminiCfg.TextModel,
		"embedding_model": geminiCfg.EmbeddingModel,
	}))

	geminiClient, err := gemini.NewClient(startupCtx, geminiCfg)
	if err != nil {
		slog.Error("❌ Critical failure: Gemini unreachable", "error", err)
		os.Exit(1)
	}

	slog.Info("🧠 Gemini Client established and verified", "project", geminiCfg.ProjectID,
		"text_model", geminiCfg.TextModel, "embedding_model", geminiCfg.EmbeddingModel)

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

	rawRedis, err := cache.NewRedisConnection(startupCtx, os.Getenv("REDIS_ADDR"))
	if err != nil {
		slog.Error("❌ Failed to connect to Redis", "error", err)
		os.Exit(1)
	}

	queryCache := cache.NewQueryCache(rawRedis, cache.RetryConfig{
		Timeout:    50 * time.Millisecond,
		MaxRetries: 0,
	})
	err = queryCache.InitializeIndex(startupCtx)
	if err != nil {
		slog.Error("❌ Query Cache Failed to Initialize Index", "error", err)
		os.Exit(1)
	}

	sessionStore := cache.NewSessionStore(rawRedis, cache.RetryConfig{
		Timeout:    100 * time.Millisecond,
		MaxRetries: 2,
		BaseDelay:  50 * time.Millisecond,
	})

	slog.Info("🔗 Connecting to downstream Frasier Bot...", "url", frasierURL)
	frasierClient, err := proxy.NewFrasierClient(frasierURL)
	if err != nil {
		slog.Error("❌ Downstream Frasier Bot is unhealthy or unreachable", "error", err)
		os.Exit(1)
	}
	slog.Info("✅ Downstream Frasier Bot is ALIVE.")

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

	chatHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		traceID := tracing.GetTraceID(ctx)
		w.Header().Set("X-Trace-Id", traceID)

		if r.Method != http.MethodPost {
			slog.Warn("Method not allowed on /chat", "method", r.Method, "trace_id", traceID)
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		metadata, valid := config.ParseRequestMetadata(r)
		if !valid {
			slog.Warn("Missing or invalid X-Pisces-Session-ID, defaulting to stateless mode (NoSession=true)", "trace_id", traceID)
			metadata.Flags.NoSession = true
		}

		requestID := r.Header.Get("X-Client-Request-Id")
		if requestID == "" {
			requestID = uuid.New().String()
		}

		var req ChatRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			slog.Warn("Malformed request body", "request_id", requestID, "trace_id", traceID, "error", err)
			http.Error(w, "Bad Request", http.StatusBadRequest)
			return
		}

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

	mux.HandleFunc("/v1/chat/completions", func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		traceID := tracing.GetTraceID(ctx)

		if r.Method != http.MethodPost {
			slog.Warn("Method not allowed on /v1/chat/completions", "method", r.Method, "trace_id", traceID)
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

		var history []string
		var lastUserMessage string

		if len(req.Messages) > 0 {
			lastUserMessage = req.Messages[len(req.Messages)-1].Content
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
			Usage: OpenAIUsage{
				PromptTokens:     0,
				CompletionTokens: 0,
				TotalTokens:      0,
			},
		}

		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("x-request-id", requestID)
		w.Header().Set("X-Trace-Id", traceID)
		json.NewEncoder(w).Encode(resp)
	})

	mux.HandleFunc("/v1/models", func(w http.ResponseWriter, r *http.Request) {
		resp := OpenAIModelsResponse{
			Object: "list",
			Data: []OpenAIModel{
				{
					ID:      "pisces",
					Object:  "model",
					Created: time.Now().Unix(),
					OwnedBy: "ljrickard",
				},
			},
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	})

	slog.Info("🚀 Pisces Gateway listening on :8080")
	if err := http.ListenAndServe(":8080", mux); err != nil {
		slog.Error("❌ Gateway server crashed", "error", err)
		os.Exit(1)
	}
}

func getSecret(ctx context.Context, projectID, secretName string) (string, error) {
	smClient, err := secretmanager.NewClient(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to create Secret Manager client: %v", err)
	}
	defer smClient.Close()

	versionPath := fmt.Sprintf("projects/%s/secrets/%s/versions/latest", projectID, secretName)
	req := &secretmanagerpb.AccessSecretVersionRequest{
		Name: versionPath,
	}

	result, err := smClient.AccessSecretVersion(ctx, req)
	if err != nil {
		return "", fmt.Errorf("failed to access secret version: %v", err)
	}

	return string(result.Payload.Data), nil
}

func initTracer(projectID string) (*sdktrace.TracerProvider, error) {
	exporter, err := texporter.New(texporter.WithProjectID(projectID))
	if err != nil {
		return nil, fmt.Errorf("failed to create GCP trace exporter: %w", err)
	}

	res, err := resource.New(context.Background(),
		resource.WithAttributes(
			semconv.ServiceName("pisces-gateway"),
		),
	)
	if err != nil {
		return nil, err
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)
	return tp, nil
}
