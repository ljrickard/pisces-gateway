package gemma

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// Config holds the connection details for your internal vLLM server
type Config struct {
	BaseURL string // e.g., "http://vllm-service.default.svc.cluster.local:80/v1"
	Model   string // e.g., "google/gemma-4-26B-A4B-it"
}

type Client struct {
	baseURL    string
	model      string
	httpClient *http.Client
}

func NewClient(cfg Config) *Client {
	return &Client{
		baseURL: cfg.BaseURL,
		model:   cfg.Model,
		httpClient: &http.Client{
			Timeout: 60 * time.Second, // Give local models time to generate
		},
	}
}

// ==========================================
// 1. GenerateText (Satisfies llm.Client)
// ==========================================

// OpenAI Chat Spec Structs
type chatRequest struct {
	Model       string    `json:"model"`
	Messages    []message `json:"messages"`
	Temperature float32   `json:"temperature"`
}

type message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type chatResponse struct {
	Choices []struct {
		Message message `json:"message"`
	} `json:"choices"`
}

func (c *Client) GenerateText(ctx context.Context, prompt string) (string, error) {
	reqBody := chatRequest{
		Model:       c.model,
		Temperature: 0.2, // Keep it low for routing/rewriting tasks
		Messages: []message{
			{Role: "user", Content: prompt},
		},
	}

	payload, err := json.Marshal(reqBody)
	if err != nil {
		return "", fmt.Errorf("failed to encode request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/chat/completions", bytes.NewReader(payload))
	if err != nil {
		return "", fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("network error reaching vLLM: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("vLLM returned status %d: %s", resp.StatusCode, string(bodyBytes))
	}

	var result chatResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", fmt.Errorf("failed to decode vLLM response: %w", err)
	}

	if len(result.Choices) == 0 {
		return "", fmt.Errorf("vLLM returned no choices")
	}

	return result.Choices[0].Message.Content, nil
}

// ==========================================
// 2. EmbedText (Satisfies llm.Client)
// ==========================================

// OpenAI Embed Spec Structs
type embedRequest struct {
	Model string `json:"model"`
	Input string `json:"input"`
}

type embedResponse struct {
	Data []struct {
		Embedding []float32 `json:"embedding"`
	} `json:"data"`
}

func (c *Client) EmbedText(ctx context.Context, text string) ([]float32, error) {
	reqBody := embedRequest{
		Model: c.model,
		Input: text,
	}

	payload, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to encode embed request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/embeddings", bytes.NewReader(payload))
	if err != nil {
		return nil, fmt.Errorf("failed to create embed request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("network error reaching vLLM: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("vLLM returned status %d: %s", resp.StatusCode, string(bodyBytes))
	}

	var result embedResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode vLLM embed response: %w", err)
	}

	if len(result.Data) == 0 {
		return nil, fmt.Errorf("vLLM returned no embedding data")
	}

	return result.Data[0].Embedding, nil
}
