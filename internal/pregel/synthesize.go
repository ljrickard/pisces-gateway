package pregel

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
)

func (n *GatewayNodes) SynthesizerNode(ctx context.Context, state *AgentState) (string, error) {
	if state.StatusStream != nil {
		state.StatusStream("Synthesizing final response...")
	}
	slog.Info("[Node] Executing SynthesizerNode", "loop", state.LoopCount)
	state.LoopCount++

	// 1. Compile the raw facts gathered by the parallel execution nodes
	var rawContext strings.Builder
	rawContext.WriteString("\n\n--- RAW GATHERED CONTEXT ---\n")
	for _, task := range state.Tasks {
		rawContext.WriteString(fmt.Sprintf("[%s Domain]: %s\n", task.Domain, task.Answer))
	}

	slog.Info("Compiled raw context for synthesizer", "context", rawContext.String())

	// 2. Build the final prompt
	prompt := GatewaySynthesizerPrompt + state.Query + rawContext.String()

	// 3. Generate the final answer!
	// We use a slightly higher temperature (0.7) here so the model writes natural, fluid paragraphs.
	answer, err := n.LLM.GenerateText(ctx, prompt, 0.7)
	if err != nil {
		slog.Error("Synthesizer LLM failed", "error", err)
		state.FinalAnswer = "I gathered the information but had trouble formatting it for you."
		return "END", nil
	}

	// 4. Save to the clipboard and exit the graph
	state.FinalAnswer = answer
	slog.Info("Synthesis complete. Final answer generated.")

	return "END", nil
}
