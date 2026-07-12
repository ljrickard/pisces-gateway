package pregel

import (
	"context"
	"log/slog"
)

func (n *GatewayNodes) CheckCache(ctx context.Context, state *AgentState) (string, error) {
	if state.StatusStream != nil {
		state.StatusStream("Checking semantic cache...")
	}
	slog.Info("[Node] Executing CheckCache", "loop", state.LoopCount)
	state.LoopCount++

	if state.Flags.SkipCache {
		slog.Info("SkipCache flag active. Bypassing semantic cache.")
		return "planner_node", nil
	}

	// 1. Generate Embeddings for the rewritten query
	slog.Info("Generating embeddings for cache lookup...")
	vector, err := n.LLM.EmbedText(ctx, state.Query)
	if err != nil {
		slog.Warn("Embedding failed, skipping cache lookup", "error", err)
		return "planner_node", nil // Fail-open: if embedding fails, just route to the bot
	}

	cachedAnswer, isHit := n.QueryCache.GetCache(ctx, vector, state.Flags.SimilarityThreshold)

	if isHit && cachedAnswer != "" {
		slog.Info("⚡ Semantic Cache Hit! Short-circuiting graph.")
		state.FinalAnswer = cachedAnswer
		state.IsCacheHit = true
		return "END", nil
	}

	slog.Info("Semantic Cache Miss. Proceeding to router.")
	return "planner_node", nil
}
