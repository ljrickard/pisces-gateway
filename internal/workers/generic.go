package workers

import (
	"context"

	"pisces-gateway/internal/gemini"
	"pisces-gateway/internal/pregel"
)

// NewGenericWorker processes queries falling outside the specialized Frasier bot domain knowledge graph.
func NewGenericWorker(client *gemini.Client) pregel.DomainWorker {
	return func(ctx context.Context, query string, _ map[string]any, _ string) (string, error) {
		prompt := pregel.GatewayGenericPrompt + query
		return client.GenerateText(ctx, prompt, 0.0)
	}
}
