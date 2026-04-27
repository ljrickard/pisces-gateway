package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// FrasierClient handles communication with the downstream bot
type FrasierClient struct {
	BaseURL    string
	HTTPClient *http.Client
}

func NewFrasierClient(url string) (*FrasierClient, error) {
	client := &http.Client{Timeout: 5 * time.Second}

	// Perform the health check before returning
	resp, err := client.Get(fmt.Sprintf("%s/health", url))
	if err != nil {
		return nil, fmt.Errorf("network error reaching bot: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("bot returned non-200 status: %d", resp.StatusCode)
	}

	return &FrasierClient{
		BaseURL:    url,
		HTTPClient: client, // We can reuse this client for the proxy
	}, nil
}

// ForwardChat sends the request to the Bot and returns the JSON response
func (c *FrasierClient) ForwardChat(ctx context.Context, payload any) (map[string]any, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, "POST", c.BaseURL+"/chat", bytes.NewBuffer(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to reach frasier bot: %w", err)
	}
	defer resp.Body.Close()

	var result map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	return result, nil
}
