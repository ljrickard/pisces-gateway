package llm

import "context"

// Client defines the standard capabilities required by the Pisces Gateway.
// Any struct that implements these two methods can be plugged into the pipeline!
type Client interface {
	GenerateText(ctx context.Context, prompt string, temperature float32) (string, error)
	EmbedText(ctx context.Context, text string) ([]float32, error)
}
