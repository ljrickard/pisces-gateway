package main

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"os"
	"time"

	"pisces-gateway/internal/cache"
	"pisces-gateway/internal/config"
	"pisces-gateway/internal/intent"
	"pisces-gateway/internal/normalize"
	"pisces-gateway/internal/pipeline"
	"pisces-gateway/internal/proxy"
	"pisces-gateway/internal/rewrite"
)

type ChatRequest struct {
	Message   string `json:"message"`
	SessionID string `json:"session_id"`
}

func main() {
	level := slog.LevelInfo
	switch os.Getenv("LOGGING_LEVEL") {
	case "DEBUG":
		level = slog.LevelDebug
	case "WARN":
		level = slog.LevelWarn
	case "ERROR":
		level = slog.LevelError
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: level,
	}))
	slog.SetDefault(logger)

	slog.Info("🐟 Starting Pisces API Gateway", "level", level.String())

	slog.Info("🐟 Starting Pisces API Gateway")

	redisClient, err := cache.NewRedisClient(getEnv("REDIS_ADDR", "localhost:6379"))
	if err != nil {
		slog.Error("Redis failed to initialize", "error", err)
		os.Exit(1)
	}

	// Wire dependencies with Context-aware constructors if needed
	p := &pipeline.Pipeline{
		Normalizer: normalize.NewService(),
		Rewriter:   rewrite.NewClient(),
		Cache:      redisClient,
		Intent:     intent.NewClassifier(),
		Proxy:      proxy.NewClient(),
	}

	mux := http.NewServeMux()

	mux.HandleFunc("/chat", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		meta, ok := config.ParseRequestMetadata(r)
		if !ok {
			slog.Warn("Rejected request: Missing or invalid X-Pisces-Session-ID")
			http.Error(w, "Invalid X-Pisces-Session-ID header. Must be a valid ULID.", http.StatusBadRequest)
			return
		}

		var req ChatRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid JSON", http.StatusBadRequest)
			return
		}

		ctx := r.Context()
		response := p.Execute(ctx, req.Message, meta.SessionID, meta.Flags)

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"response": response})
	})

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("OK")) })
	mux.HandleFunc("/", handleRoot)

	srv := &http.Server{
		Addr:         ":8080",
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 30 * time.Second,
	}

	if err := srv.ListenAndServe(); err != nil {
		slog.Error("Server crashed", "error", err)
	}
}

func getEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}

func handleRoot(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Write([]byte("Pisces Gateway Online"))
}
