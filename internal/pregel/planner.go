package pregel

import (
	"context"
	"encoding/json"
	"log/slog"
	"pisces-gateway/utils"
)

func (n *GatewayNodes) Plan(ctx context.Context, state *AgentState) (string, error) {
	if state.StatusStream != nil {
		state.StatusStream("Decomposing query and planning execution...")
	}
	slog.Info("[Node] Executing Plan", "loop", state.LoopCount)
	state.LoopCount++

	prompt := GatewayPlannerPrompt + state.Query

	response, err := n.LLM.GenerateText(ctx, prompt, defaultTemperature)
	if err != nil {
		slog.Error("Planner LLM failed, falling back", "error", err)
		state.Tasks = []SubTask{{Query: state.Query, Domain: "generic"}}
		return "execution_node", nil
	}

	cleanJSON := utils.CleanJSON(response)

	slog.Info("Raw Planner JSON Output", "json", cleanJSON)

	var tasks []SubTask
	if err := json.Unmarshal([]byte(cleanJSON), &tasks); err != nil {
		slog.Error("Failed to parse planner JSON", "error", err, "raw", cleanJSON)
		state.Tasks = []SubTask{{Query: state.Query, Domain: "generic"}}
		return "execution_node", nil
	}

	state.Tasks = tasks
	slog.Info("Query successfully decomposed", "task_count", len(state.Tasks))
	return "execution_node", nil
}
