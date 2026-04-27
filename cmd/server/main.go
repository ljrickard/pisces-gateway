package main

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	"pisces-gateway/internal/cache"
	"pisces-gateway/internal/config"
	"pisces-gateway/internal/pipeline"
	"pisces-gateway/internal/proxy"
)

// ChatRequest represents the incoming JSON body from the user
type ChatRequest struct {
	Message string `json:"message"`
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

	// 2. Load Environment Variables
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

	// 3. Fail-Fast Downstream Health Check
	slog.Info("🔌 Connecting to Redis...", "addr", redisAddr)
	redisCache, err := cache.NewRedisClient(redisAddr)
	if err != nil {
		slog.Error("❌ Failed to connect to Redis", "error", err)
		os.Exit(1)
	}
	slog.Info("✅ Redis connected.")

	slog.Info("🔗 Connecting to downstream Frasier Bot...", "url", frasierURL)
	frasierClient, err := proxy.NewFrasierClient(frasierURL)
	if err != nil {
		slog.Error("❌ Downstream Frasier Bot is unhealthy or unreachable", "error", err)
		os.Exit(1)
	}
	slog.Info("✅ Downstream Frasier Bot is ALIVE.")

	// Inject dependencies into the Gateway Pipeline
	p := &pipeline.Pipeline{
		Cache:      redisCache,
		FrasierBot: frasierClient,
	}

	// 5. Setup HTTP Router
	mux := http.NewServeMux()

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("Pisces Gateway is Healthy"))
	})

	// Gateway's own health check (for K8s readiness probes)
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("Gateway OK"))
	})

	// The Main Chat Endpoint
	mux.HandleFunc("/chat", func(w http.ResponseWriter, r *http.Request) {
		slog.Info("Handling Request")
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		// Parse Headers for infrastructure flags & Session ID
		metadata, valid := config.ParseRequestMetadata(r)
		if !valid {
			http.Error(w, "Missing or invalid X-Pisces-Session-ID header", http.StatusBadRequest)
			return
		}

		// Decode the User's Message
		var req ChatRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			slog.Warn("Malformed request body", "error", err)
			http.Error(w, "Bad Request", http.StatusBadRequest)
			return
		}

		// Execute the Gateway Pipeline (Cache Check -> Bot Request -> Save Cache)
		answer := p.Execute(r.Context(), req.Message, metadata.SessionID, metadata.Flags)

		// Return the final answer to the user
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{
			"response": answer,
		})
	})

	// 6. Start the Server
	slog.Info("🚀 Pisces Gateway listening on :8080")
	if err := http.ListenAndServe(":8080", mux); err != nil {
		slog.Error("❌ Gateway server crashed", "error", err)
		os.Exit(1)
	}
}
