package main

import (
	"encoding/json"
	"log"
	"net/http"
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
	Message string `json:"message"`
}

func main() {
	log.Println("🐟 Starting Pisces API Gateway...")

	// 1. Initialize Concrete Dependencies
	redis := cache.NewRedisClient()
	norm := normalize.NewService()
	rewriter := rewrite.NewClient()
	classifier := intent.NewClassifier()
	prox := proxy.NewClient()

	// 2. Wire the Pipeline
	orchestrator := &pipeline.Pipeline{
		Normalizer: norm,
		Rewriter:   rewriter,
		Cache:      redis,
		Intent:     classifier,
		Proxy:      prox,
	}

	// 3. HTTP Routes
	mux := http.NewServeMux()

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	// 2. The GCP Load Balancer External Health Check (Catches "/")
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		// Only return 200 for the exact root path
		if r.URL.Path == "/" {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("GCP LB OK"))
			return
		}
		// Return 404 for any other random paths
		http.NotFound(w, r)
	})

	mux.HandleFunc("/chat", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var req ChatRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Bad request", http.StatusBadRequest)
			return
		}

		// Parse Feature Flags from Headers
		flags := config.ParseFlags(r)

		// Execute
		response := orchestrator.Execute(req.Message, flags)

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"response": response})
	})

	// 4. Start Server
	srv := &http.Server{
		Addr:         ":8080",
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	log.Println("🚀 Gateway listening on :8080")
	log.Fatal(srv.ListenAndServe())
}
