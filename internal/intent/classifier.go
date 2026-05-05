package intent

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"pisces-gateway/internal/gemini"
	"pisces-gateway/internal/llm"
)

type Classifier struct {
	// Changed from *gemini.Client to llm.Client
	LLM llm.Client
}

// You can use this constructor, or just initialize it directly in main.go
// like you did with the Rewriter!
func NewClassifier(llm *gemini.Client) *Classifier {
	return &Classifier{LLM: llm}
}

func (c *Classifier) Determine(ctx context.Context, query string) string {
	// 1. Craft a strict, zero-shot classification prompt
	prompt := fmt.Sprintf(`
		You are an intent classification router for an API gateway.
		Your job is to determine if a user's query is related to the TV show "Frasier" (including its characters, actors, plots, or quotes) or if it is a general/unrelated query.

		Respond with EXACTLY ONE WORD from the following two options:
		- frasier
		- generic

		Query: "%s"
		Intent:`, query)

	// 2. Call the Gemini wrapper
	response, err := c.LLM.GenerateText(ctx, prompt)
	if err != nil {
		// Fail gracefully: If the LLM errors out, default to "frasier"
		// so the user still has a chance of getting their question answered.
		slog.Error("⚠️ Classifier LLM failed, defaulting to frasier domain", "error", err)
		return "frasier"
	}

	// 3. Normalize the output
	// Gemini sometimes adds newlines or capitalization even when told not to.
	intent := strings.ToLower(strings.TrimSpace(response))

	// 4. Validate the output
	if intent == "frasier" || intent == "generic" {
		return intent
	}

	// Catch-all: If the LLM rambled instead of giving one word, default safely.
	slog.Warn("⚠️ Classifier returned unexpected intent format", "raw_response", response)
	return "unknown"
}
