package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	"pisces-gateway/internal/cache"
	"pisces-gateway/internal/config"
	"pisces-gateway/internal/gemini"
	"pisces-gateway/internal/intent"
	"pisces-gateway/internal/pipeline"
	"pisces-gateway/internal/proxy"
	"pisces-gateway/internal/rewrite"
)

// ChatRequest represents the incoming JSON body from the user
type ChatRequest struct {
	Message string         `json:"message"`
	Config  map[string]any `json:"config,omitempty"`
}

// checkBotHealth ensures the downstream Frasier Chat service is alive before starting
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
	// 1. Logger Setup
	level := slog.LevelInfo
	if os.Getenv("LOGGING_LEVEL") == "DEBUG" {
		level = slog.LevelDebug
	}
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level})))

	slog.Info("🐟 Starting Pisces API Gateway...")

	geminiCfg := gemini.Config{
		ProjectID:      os.Getenv("GEMINI_PROJECT"),
		Location:       os.Getenv("GEMINI_LOCATION"),
		TextModel:      os.Getenv("GEMINI_MODEL"),
		EmbeddingModel: os.Getenv("EMBEDDING_MODEL"),
		Retry: gemini.RetryConfig{
			MaxRetries: 3,
			BaseDelay:  2 * time.Second,
		},
	}

	if geminiCfg.TextModel == "" {
		slog.Error("❌ GEMINI_MODEL environment variable is not set")
		os.Exit(1)
	}

	ctx := context.Background()
	geminiClient, err := gemini.NewClient(ctx, geminiCfg)
	if err != nil {
		slog.Error("❌ Critical failure: Gemini unreachable", "error", err)
		os.Exit(1)
	}

	slog.Info("🧠 Gemini Client established and verified", "project", geminiCfg.ProjectID,
		"TextModel", geminiCfg.TextModel, "EmbeddingModel", geminiCfg.EmbeddingModel)

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

	startupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 1. Establish the shared base connection
	rawRedis, err := cache.NewRedisConnection(startupCtx, os.Getenv("REDIS_ADDR"))
	if err != nil {
		slog.Error("❌ Failed to connect to Redis", "error", err)
		os.Exit(1)
	}

	// 2. Query Cache: Fail-Fast (50ms timeout, 0 retries)
	queryCache := cache.NewQueryCache(rawRedis, cache.RetryConfig{
		Timeout:    50 * time.Millisecond,
		MaxRetries: 0,
	})
	err = queryCache.InitializeIndex(startupCtx)
	if err != nil {
		slog.Error("❌ Query Cache Failed to Initialize Index", "error", err)
		os.Exit(1)
	}

	// 3. Session Store: Resilient (100ms timeout, 2 retries)
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
		Rewriter:     &rewrite.GeminiRewriter{LLM: geminiClient},
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

	mux.HandleFunc("/chat", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		metadata, valid := config.ParseRequestMetadata(r)
		if !valid {
			http.Error(w, "Missing or Invalid Metadata Headers", http.StatusBadRequest)
			return
		}

		var req ChatRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			slog.Warn("Malformed request body", "error", err)
			http.Error(w, "Bad Request", http.StatusBadRequest)
			return
		}

		answer := p.Execute(r.Context(), req.Message, metadata.SessionID, metadata.Flags, req.Config)

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{
			"response": answer,
		})
	})

	// DELETE /cache - Admin endpoint to wipe the Redis database for evaluation runs
	mux.HandleFunc("/cache", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		// Call the FlushCache method we added to QueryCache earlier
		if err := queryCache.FlushCache(r.Context()); err != nil {
			slog.Error("❌ Failed to flush Redis cache via API", "error", err)
			http.Error(w, "Internal Server Error", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status": "success", "message": "Redis cache completely wiped"}`))
	})

	slog.Info("🚀 Pisces Gateway listening on :8080")
	if err := http.ListenAndServe(":8080", mux); err != nil {
		slog.Error("❌ Gateway server crashed", "error", err)
		os.Exit(1)
	}
}
