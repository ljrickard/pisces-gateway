package rewrite

import (
	"context"
	"fmt"
	"log/slog"
	"pisces-gateway/internal/llm"
	"pisces-gateway/tracing"
	"strings"
)

type Rewriter struct {
	LLM llm.Client
}

func (r *Rewriter) Resolve(ctx context.Context, query string, history []string) string {
	traceID := tracing.GetTraceID(ctx)
	if len(history) == 0 {
		return query
	}

	slog.Debug("✍️ [Query Rewriter] Running reformulation LLM context setup", "trace_id", traceID)

	prompt := fmt.Sprintf(`
		You are a query reformulation assistant. 
		Given the following conversation history and a new user query, rewrite the query to be a standalone question that resolves all pronouns.
		
		History:
		%s
		
		New Query: %s
		
		Rewritten Query:`, strings.Join(history, "\n"), query)

	rewritten, err := r.LLM.GenerateText(ctx, prompt)
	if err != nil || rewritten == "" {
		slog.Error("⚠️ [Query Rewriter] Refomulation failed, entering fallback path", "trace_id", traceID, "error", err)
		return query
	}

	return strings.TrimSpace(rewritten)
}
