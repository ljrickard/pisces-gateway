package proxy

import (
	"log"
	"pisces-gateway/internal/config"
)

type Client struct{}

func NewClient() *Client { return &Client{} }

func (c *Client) Forward(targetUrl string, query string, flags config.FeatureState) string {
	log.Printf("🌐 Proxying request to backend: %s", targetUrl)
	if flags.DebugLog {
		log.Println("🐞 Debug mode enabled: Attaching extra telemetry headers")
	}
	return "I am listening. (Mock Frasier Response)"
}
