package proxy

import (
	"context"
	"pisces-gateway/internal/config"
)

type Client struct{}

func NewClient() *Client { return &Client{} }

func (c *Client) Forward(ctx context.Context, backend string, query string, flags config.FeatureState) string {
	return "Forwarded" // Placeholder
}
