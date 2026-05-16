package intent

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"pisces-gateway/internal/gemini"
	"pisces-gateway/internal/llm"
	"pisces-gateway/tracing"
)

type Classifier struct {
	LLM llm.Client
}

func NewClassifier(llm *gemini.Client) *Classifier {
	return &Classifier{LLM: llm}
}

func (c *Classifier) Determine(ctx context.Context, query string) string {
	traceID := tracing.GetTraceID(ctx)
	slog.Debug("🧭 [Intent] Requesting zero-shot domain routing classification", "trace_id", traceID)

	prompt := fmt.Sprintf(`
		You are an intent classification router for an API gateway.
		Your job is to determine if a user's query is related to the TV show "Frasier" (including its characters, actors, plots, or quotes) or if it is a general/unrelated query.

		Respond with EXACTLY ONE WORD from the following two options:
		- frasier
		- generic

		Query: "%s"
		Intent:`, query)

	response, err := c.LLM.GenerateText(ctx, prompt)
	if err != nil {
		slog.Error("⚠️ Classifier LLM failed, defaulting safely to frasier domain", "trace_id", traceID, "error", err)
		return "frasier"
	}

	intent := strings.ToLower(strings.TrimSpace(response))

	if intent == "frasier" || intent == "generic" {
		return intent
	}

	slog.Warn("⚠️ Classifier returned unexpected intent token format", "raw_response", response, "trace_id", traceID)
	return "unknown"
}
