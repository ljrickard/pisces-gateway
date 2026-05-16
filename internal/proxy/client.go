package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"pisces-gateway/tracing"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

type FrasierClient struct {
	BaseURL    string
	HTTPClient *http.Client
}

func NewFrasierClient(url string) (*FrasierClient, error) {
	client := &http.Client{
		Transport: otelhttp.NewTransport(http.DefaultTransport),
		Timeout:   65 * time.Second,
	}

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
		HTTPClient: client,
	}, nil
}

func (c *FrasierClient) ForwardChat(ctx context.Context, payload any) (map[string]any, error) {
	traceID := tracing.GetTraceID(ctx)
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, "POST", c.BaseURL+"/chat", bytes.NewBuffer(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req = req.WithContext(ctx)

	slog.Debug("📞 [Proxy Outbound] Forwarding network payload across distributed cluster boundary", "url", c.BaseURL+"/chat", "trace_id", traceID)

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		slog.Error("❌ [Proxy Outbound] Failed to bridge network call into downstream microservice", "trace_id", traceID, "error", err)
		return nil, fmt.Errorf("failed to reach frasier bot: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		slog.Error("❌ [Proxy Outbound] Downstream cluster responded with unhealthy status code", "status_code", resp.StatusCode, "trace_id", traceID)
		return nil, fmt.Errorf("downstream service returned HTTP %d", resp.StatusCode)
	}

	var result map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		slog.Error("❌ [Proxy Outbound] Failed to parse downstream response JSON payload", "trace_id", traceID, "error", err)
		return nil, err
	}

	return result, nil
}
