package pregel

import (
	"context"

	"pisces-gateway/internal/cache"
	"pisces-gateway/internal/intent"
	"pisces-gateway/internal/llm"
	"pisces-gateway/internal/proxy"
	"pisces-gateway/internal/rewrite"
)

const (
	defaultTemperature = 0.0
)

type DomainWorker func(ctx context.Context, query string, config map[string]any, sessionID string) (string, error)

type GatewayNodes struct {
	LLM        llm.Client
	QueryCache *cache.QueryCache
	FrasierBot *proxy.FrasierClient
	Rewriter   *rewrite.Rewriter
	Classifier *intent.Classifier
	Workers    map[string]DomainWorker
}
