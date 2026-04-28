package embeddings

import "context"

// Embedder defines the contract for any semantic embedding backend
type Embedder interface {
	EmbedText(ctx context.Context, text string) ([]float32, error)
}
