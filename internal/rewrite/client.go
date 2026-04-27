package rewrite

import (
	"context"
	"log/slog"
)

type Client struct{}

func NewClient() *Client { return &Client{} }

func (c *Client) Resolve(ctx context.Context, query string, history []string) string {
	if len(history) == 0 {
		return query
	}

	slog.Debug("🧠 Rewriter Analyzing Context",
		"query", query,
		"history_depth", len(history),
	)

	// For now, this is your placeholder logic.
	// Once we add Gemini, we will log the actual LLM latency here too.
	return query
}
