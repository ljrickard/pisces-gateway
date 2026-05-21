package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"os"
	"time"

	texporter "github.com/GoogleCloudPlatform/opentelemetry-operations-go/exporter/trace"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"

	"pisces-gateway/internal/api"
	"pisces-gateway/internal/cache"
	"pisces-gateway/internal/gemini"
	"pisces-gateway/internal/intent"
	"pisces-gateway/internal/pipeline"
	"pisces-gateway/internal/pregel"
	"pisces-gateway/internal/proxy"
	"pisces-gateway/internal/rewrite"

	secretmanager "cloud.google.com/go/secretmanager/apiv1"
	"cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"
)

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

	rewriter := &rewrite.Rewriter{LLM: geminiClient}
	classifier := &intent.Classifier{LLM: geminiClient}

	// 1. Instantiate the V2 Nodes with injected dependencies
	// In main.go during instantiation

	nodesV2 := &pregel.GatewayNodes{
		LLM:        geminiClient,
		QueryCache: queryCache,
		FrasierBot: frasierClient,
		Rewriter:   rewriter,
		Classifier: classifier,
		Workers: map[string]pregel.DomainWorker{
			"frasier": func(ctx context.Context, query string, cfg map[string]any, sessionID string) (string, error) {
				payload := map[string]any{"query": query, "session_id": sessionID}
				if len(cfg) > 0 {
					payload["config"] = cfg
				}
				res, err := frasierClient.ForwardChat(ctx, payload)
				if err != nil {
					return "", err
				}
				if ans, ok := res["response"].(string); ok {
					return ans, nil
				}
				return "Error parsing Frasier response", nil
			},

			"generic": func(ctx context.Context, query string, _ map[string]any, _ string) (string, error) {
				prompt := pregel.GatewayGenericPrompt + query
				return geminiClient.GenerateText(ctx, prompt, 0.0)
			},
		},
	}

	// 2. Compile the Graph ONCE at startup
	compiledGraph := pregel.NewGraph[pregel.AgentState]()

	// Phase 1: Context & Cache
	compiledGraph.AddNode("rewrite_node", nodesV2.RewriteNode)
	compiledGraph.AddNode("cache_node", nodesV2.SemanticCacheNode)

	// Phase 2: Fan-Out / Fan-In
	compiledGraph.AddNode("planner_node", nodesV2.PlannerNode)
	compiledGraph.AddNode("execution_node", nodesV2.ExecutionNode) // 🚀 Missing link added!
	compiledGraph.AddNode("synthesizer_node", nodesV2.SynthesizerNode)

	// Start here
	compiledGraph.SetEntryPoint("rewrite_node")

	apiServer := api.Server{
		PipelineV1: &pipeline.Pipeline{
			Rewriter:     rewriter,
			Intent:       classifier,
			Embedder:     geminiClient,
			Sessionstore: sessionStore,
			Querycache:   queryCache,
			FrasierBot:   frasierClient,
		},
		FrasierClient: frasierClient,
		QueryCache:    queryCache,
		NodesV2:       nodesV2,
		GraphV2:       compiledGraph,
	}

	mux := http.NewServeMux()
	apiServer.Mount(mux)

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
