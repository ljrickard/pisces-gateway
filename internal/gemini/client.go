package gemini

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"strings"
	"time"

	"google.golang.org/genai"
)

// RetryConfig holds parameters for exponential backoff
type RetryConfig struct {
	MaxRetries int
	BaseDelay  time.Duration
}

// Config holds the necessary parameters for the Vertex AI backend
type Config struct {
	ProjectID string
	Location  string
	Model     string
	Retry     RetryConfig // Nested retry configuration
}

// Client is our encapsulated GenAI wrapper
type Client struct {
	rawClient *genai.Client
	modelName string
	retryCfg  RetryConfig
}

// NewClient creates a new GenAI client and verifies connectivity to Vertex AI
func NewClient(ctx context.Context, cfg Config) (*Client, error) {
	c, err := genai.NewClient(ctx, &genai.ClientConfig{
		Project:  cfg.ProjectID,
		Location: cfg.Location,
		Backend:  genai.BackendVertexAI,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to initialize genai client: %w", err)
	}

	_, err = c.Models.Get(ctx, cfg.Model, nil)
	if err != nil {
		return nil, fmt.Errorf("gemini connection test failed for model %s: %w", cfg.Model, err)
	}

	// Set a sensible default base delay if retries are requested but delay is omitted
	if cfg.Retry.MaxRetries > 0 && cfg.Retry.BaseDelay == 0 {
		cfg.Retry.BaseDelay = 2 * time.Second
	}

	// Return our wrapper
	return &Client{
		rawClient: c,
		modelName: cfg.Model,
		retryCfg:  cfg.Retry,
	}, nil
}

// GenerateText abstracts the SDK and applies retry logic automatically
func (c *Client) GenerateText(ctx context.Context, prompt string) (string, error) {
	temperature := float32(0.2)

	// Wrap the actual SDK call in our retry logic
	resp, err := c.callWithRetry(ctx, func() (*genai.GenerateContentResponse, error) {
		return c.rawClient.Models.GenerateContent(ctx, c.modelName, genai.Text(prompt), &genai.GenerateContentConfig{
			Temperature: &temperature,
		})
	})

	if err != nil {
		return "", fmt.Errorf("failed to generate content: %w", err)
	}

	answer := c.extractText(resp)
	if answer == "" {
		return "", fmt.Errorf("no text returned from model")
	}

	return answer, nil
}

// callWithRetry executes the provided function with exponential backoff and jitter
func (c *Client) callWithRetry(ctx context.Context, fn func() (*genai.GenerateContentResponse, error)) (*genai.GenerateContentResponse, error) {
	maxRetries := c.retryCfg.MaxRetries
	baseDelay := c.retryCfg.BaseDelay

	for attempt := 0; attempt <= maxRetries; attempt++ {
		resp, err := fn()
		if err == nil {
			return resp, nil // Success
		}

		errStr := err.Error()
		is429 := strings.Contains(errStr, "429") ||
			strings.Contains(errStr, "RESOURCE_EXHAUSTED") ||
			strings.Contains(errStr, "resource exhausted") ||
			strings.Contains(errStr, "Resource has been exhausted")

		// If it's not a rate limit error, or we've hit our max retries, fail immediately
		if !is429 || attempt == maxRetries {
			return nil, err
		}

		// Calculate exponential backoff with jitter
		delay := baseDelay * (1 << uint(attempt))
		jitter := time.Duration(rand.Int63n(int64(delay) / 4))
		wait := delay + jitter

		// Always log the retry event for tracking and monitoring
		log.Printf("⚠️ Rate limited (429) | err=[%v], retry %d/%d in %v...", errStr, attempt+1, maxRetries, wait)

		// Wait for the delay or until the context is canceled
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(wait):
		}
	}

	return nil, fmt.Errorf("max retries exceeded")
}

// extractText safely parses the deeply nested Gemini response object
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
