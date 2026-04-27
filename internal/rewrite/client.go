package rewrite

import (
	"context"
	"fmt"
	"strings"

	"pisces-gateway/internal/gemini"
)

// The Struct is now clean and injected with your custom client
type GeminiRewriter struct {
	LLM *gemini.Client
}

func (r *GeminiRewriter) Resolve(ctx context.Context, query string, history []string) string {
	if len(history) == 0 {
		return query
	}

	prompt := fmt.Sprintf(`
		You are a query reformulation assistant. 
		Given the following conversation history and a new user query, rewrite the query to be a standalone question that resolves all pronouns.
		
		History:
		%s
		
		New Query: %s
		
		Rewritten Query:`, strings.Join(history, "\n"), query)

	// We use the wrapper method, keeping the genai types hidden!
	rewritten, err := r.LLM.GenerateText(ctx, prompt)
	if err != nil || rewritten == "" {
		return query // Graceful fallback
	}

	return strings.TrimSpace(rewritten)
}
