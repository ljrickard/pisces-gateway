package workers

import (
	"context"
	"fmt"

	"pisces-gateway/internal/pregel"
	"pisces-gateway/internal/proxy"
)

// NewFrasierWorker acts as the proxy bridge to the downstream Frasier RAG microservice.
func NewFrasierWorker(client *proxy.FrasierClient) pregel.DomainWorker {
	return func(ctx context.Context, query string, cfg map[string]any, sessionID string) (string, error) {
		payload := map[string]any{"query": query, "session_id": sessionID}
		if len(cfg) > 0 {
			payload["config"] = cfg
		}

		res, err := client.ForwardChat(ctx, payload)
		if err != nil {
			return "", err
		}

		// TODO: need to resolve this one!
		if ans, ok := res["answer"].(string); ok && ans != "" {
			return ans, nil
		}
		if ans, ok := res["response"].(string); ok && ans != "" {
			return ans, nil
		}

		return "", fmt.Errorf("downstream response missing valid 'answer' or 'response' keys")
	}
}
