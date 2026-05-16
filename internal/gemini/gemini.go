package gemini

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"strings"
	"time"

	"pisces-gateway/tracing"

	"google.golang.org/genai"
)

type RetryConfig struct {
	MaxRetries int
	BaseDelay  time.Duration
}

type Config struct {
	ProjectID      string
	Location       string
	APIKey         string
	TextModel      string
	EmbeddingModel string
	Retry          RetryConfig
}

type Client struct {
	rawClient      *genai.Client
	textModel      string
	embeddingModel string
	retryCfg       RetryConfig
}

func NewClient(ctx context.Context, cfg Config) (*Client, error) {
	clientConfig := &genai.ClientConfig{}

	if cfg.APIKey != "" {
		clientConfig.APIKey = cfg.APIKey
		clientConfig.Backend = genai.BackendGeminiAPI
	} else {
		clientConfig.Project = cfg.ProjectID
		clientConfig.Location = cfg.Location
		clientConfig.Backend = genai.BackendVertexAI
	}

	c, err := genai.NewClient(ctx, clientConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize genai client: %w", err)
	}

	if cfg.TextModel != "" {
		if _, err = c.Models.Get(ctx, cfg.TextModel, nil); err != nil {
			return nil, fmt.Errorf("connection test failed for text model %s: %w", cfg.TextModel, err)
		}
	}

	if cfg.EmbeddingModel != "" {
		if _, err = c.Models.Get(ctx, cfg.EmbeddingModel, nil); err != nil {
			return nil, fmt.Errorf("connection test failed for embedding model %s: %w", cfg.EmbeddingModel, err)
		}
	}

	if cfg.Retry.MaxRetries > 0 && cfg.Retry.BaseDelay == 0 {
		cfg.Retry.BaseDelay = 2 * time.Second
	}

	return &Client{
		rawClient:      c,
		textModel:      cfg.TextModel,
		embeddingModel: cfg.EmbeddingModel,
		retryCfg:       cfg.Retry,
	}, nil
}

func (c *Client) GenerateText(ctx context.Context, prompt string) (string, error) {
	traceID := tracing.GetTraceID(ctx)
	temperature := float32(0.2)

	slog.Debug("🤖 [Gemini LLM] Dispatching text generation request content", "model", c.textModel, "trace_id", traceID)

	resp, err := executeWithRetry(ctx, c.retryCfg, func() (*genai.GenerateContentResponse, error) {
		return c.rawClient.Models.GenerateContent(ctx, c.textModel, genai.Text(prompt), &genai.GenerateContentConfig{
			Temperature: &temperature,
		})
	})

	if err != nil {
		slog.Error("❌ [Gemini LLM] Text generation request failed out completely", "trace_id", traceID, "error", err)
		return "", fmt.Errorf("failed to generate content: %w", err)
	}

	answer := c.extractText(resp)
	if answer == "" {
		return "", fmt.Errorf("no text returned from model")
	}

	return answer, nil
}

func (c *Client) EmbedText(ctx context.Context, text string) ([]float32, error) {
	traceID := tracing.GetTraceID(ctx)
	if c.embeddingModel == "" {
		return nil, fmt.Errorf("embedding model is not configured")
	}

	outputDim := int32(768)
	embedConfig := &genai.EmbedContentConfig{
		OutputDimensionality: &outputDim,
	}

	slog.Debug("🧮 [Gemini Embedder] Requesting matrix token coordinates", "model", c.embeddingModel, "trace_id", traceID)

	resp, err := executeWithRetry(ctx, c.retryCfg, func() (*genai.EmbedContentResponse, error) {
		return c.rawClient.Models.EmbedContent(ctx, c.embeddingModel, genai.Text(text), embedConfig)
	})

	if err != nil {
		slog.Error("❌ [Gemini Embedder] Matrix token coordinate call failed", "trace_id", traceID, "error", err)
		return nil, fmt.Errorf("failed to generate embedding: %w", err)
	}

	if resp == nil || len(resp.Embeddings) == 0 {
		return nil, fmt.Errorf("no embeddings returned")
	}

	return resp.Embeddings[0].Values, nil
}

func executeWithRetry[T any](ctx context.Context, cfg RetryConfig, fn func() (T, error)) (T, error) {
	var zero T
	traceID := tracing.GetTraceID(ctx)

	for attempt := 0; attempt <= cfg.MaxRetries; attempt++ {
		resp, err := fn()
		if err == nil {
			return resp, nil
		}

		errStr := err.Error()
		is429 := strings.Contains(errStr, "429") ||
			strings.Contains(errStr, "RESOURCE_EXHAUSTED") ||
			strings.Contains(errStr, "resource exhausted")

		if !is429 || attempt == cfg.MaxRetries {
			return zero, err
		}

		delay := cfg.BaseDelay * (1 << uint(attempt))
		jitter := time.Duration(rand.Int63n(int64(delay) / 4))
		wait := delay + jitter

		slog.Warn("⚠️ Google Gemini API rate limited (429), initiating transient retry backoff schedule",
			"attempt", attempt+1,
			"max_retries", cfg.MaxRetries,
			"backoff_wait", wait,
			"trace_id", traceID,
		)

		select {
		case <-ctx.Done():
			return zero, ctx.Err()
		case <-time.After(wait):
		}
	}

	return zero, fmt.Errorf("max retries exceeded")
}

func (c *Client) extractText(resp *genai.GenerateContentResponse) string {
	if resp == nil || len(resp.Candidates) == 0 {
		return ""
	}
	candidate := resp.Candidates[0]
	if candidate.Content == nil || len(candidate.Content.Parts) == 0 {
		return ""
	}
	var parts []string
	for _, part := range candidate.Content.Parts {
		if part.Text != "" {
			parts = append(parts, part.Text)
		}
	}
	return strings.Join(parts, "\n")
}
