package intent

import "context"

type Classifier struct{}

func NewClassifier() *Classifier { return &Classifier{} }

func (c *Classifier) Determine(ctx context.Context, query string) string {
	return "http://frasier-bot-svc" // Placeholder
}
