package rewrite

import "log"

type Client struct{}

func NewClient() *Client { return &Client{} }

func (c *Client) Resolve(query string) string {
	log.Println("✍️  Rewriting query (Mock LLM Call)...")
	return query
}
