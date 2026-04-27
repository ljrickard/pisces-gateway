package intent

import "log"

type Classifier struct{}

func NewClassifier() *Classifier { return &Classifier{} }

func (c *Classifier) Determine(query string) string {
	log.Println("🧭 Classifying intent...")
	return "http://frasier-rag-svc.default.svc.cluster.local:80" // Mock routing decision
}
