package pregel

import (
	"context"
	"log/slog"
)

func (n *GatewayNodes) ResolveContext(ctx context.Context, state *AgentState) (string, error) {
	if state.StatusStream != nil {
		state.StatusStream("Analyzing conversation context...")
	}
	slog.Info("[Node] Executing RewriteNode", "loop", state.LoopCount)
	state.LoopCount++

	if state.Flags.NoSession || len(state.History) == 0 {
		slog.Debug("Skipping rewrite: NoSession flag active or history is empty")
		return "planner_node", nil
	}

	// 2. Delegate to the existing domain package!
	slog.Info("Delegating query to rewrite package...")
	state.Query = n.Rewriter.Resolve(ctx, state.Query, state.History)

	return "planner_node", nil
}
