package pregel

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"

	"pisces-gateway/internal/cache"
	"pisces-gateway/internal/intent"
	"pisces-gateway/internal/llm"
	"pisces-gateway/internal/proxy"
	"pisces-gateway/internal/rewrite"
	"pisces-gateway/utils"
)

const (
	defaultTemperature = 0.0
)

type DomainWorker func(ctx context.Context, query string, config map[string]any, sessionID string) (string, error)

type GatewayNodes struct {
	LLM        llm.Client
	QueryCache *cache.QueryCache
	FrasierBot *proxy.FrasierClient
	Rewriter   *rewrite.Rewriter
	Classifier *intent.Classifier
	Workers    map[string]DomainWorker
}

// RewriteNode is now a method, so it has full access to n.LLM
func (n *GatewayNodes) RewriteNode(ctx context.Context, state *AgentState) (string, error) {
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

func (n *GatewayNodes) PlannerNode(ctx context.Context, state *AgentState) (string, error) {
	if state.StatusStream != nil {
		state.StatusStream("Decomposing query and planning execution...")
	}
	slog.Info("[Node] Executing PlannerNode", "loop", state.LoopCount)
	state.LoopCount++

	prompt := GatewayPlannerPrompt + state.Query

	response, err := n.LLM.GenerateText(ctx, prompt, defaultTemperature)
	if err != nil {
		slog.Error("Planner LLM failed, falling back", "error", err)
		state.Tasks = []SubTask{{Query: state.Query, Domain: "frasier"}}
		return "execution_node", nil
	}

	cleanJSON := utils.CleanJSON(response)

	var tasks []SubTask
	if err := json.Unmarshal([]byte(cleanJSON), &tasks); err != nil {
		slog.Error("Failed to parse planner JSON", "error", err, "raw", cleanJSON)
		state.Tasks = []SubTask{{Query: state.Query, Domain: "frasier"}}
		return "execution_node", nil
	}

	state.Tasks = tasks
	slog.Info("Query successfully decomposed", "task_count", len(state.Tasks))
	return "execution_node", nil
}

func (n *GatewayNodes) GenerateNode(ctx context.Context, state *AgentState) (string, error) {
	if state.StatusStream != nil {
		state.StatusStream("Generating general response...")
	}
	slog.Info("[Node] Executing GenerateNode", "loop", state.LoopCount)
	state.LoopCount++

	// Clean constant from prompts.go!
	prompt := GatewayGenericPrompt + state.Query

	answer, err := n.LLM.GenerateText(ctx, prompt, defaultTemperature)
	if err != nil {
		slog.Error("Gateway LLM generation failed", "error", err)
		state.FinalAnswer = "I'm sorry, my internal systems are having trouble thinking right now."
		return "END", nil
	}

	state.FinalAnswer = answer
	return "END", nil
}

func (n *GatewayNodes) CallBotNode(ctx context.Context, state *AgentState) (string, error) {
	if state.StatusStream != nil {
		state.StatusStream("Forwarding to Frasier domain...")
	}
	slog.Info("[Node] Executing CallBotNode", "loop", state.LoopCount)
	state.LoopCount++

	slog.Info("Forwarding enriched query to downstream Frasier Bot...")

	payload := map[string]any{
		"query":      state.Query,
		"stream":     state.IsStream,
		"session_id": state.SessionID,
	}

	if len(state.ReqConfig) > 0 {
		payload["config"] = state.ReqConfig
	}

	// 🌊 STREAMING PATH
	if state.IsStream {
		streamBody, err := n.FrasierBot.ForwardChatStream(ctx, payload)
		if err != nil {
			slog.Error("Downstream stream failed", "error", err)
			return "END", err
		}
		// Attach the open socket to the state clipboard and exit the graph!
		state.StreamBody = streamBody
		return "END", nil
	}

	// 🧱 BLOCKING PATH
	result, err := n.FrasierBot.ForwardChat(ctx, payload)
	if err != nil {
		slog.Error("Downstream blocking call failed", "error", err)
		state.FinalAnswer = "Sorry, I am having trouble reaching the Frasier domain right now."
		return "END", nil
	}

	if ans, ok := result["response"].(string); ok {
		state.FinalAnswer = ans
	}

	return "END", nil
}

func (n *GatewayNodes) SemanticCacheNode(ctx context.Context, state *AgentState) (string, error) {
	if state.StatusStream != nil {
		state.StatusStream("Checking semantic cache...")
	}
	slog.Info("[Node] Executing SemanticCacheNode", "loop", state.LoopCount)
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

	// 2. Check Redis for a Semantic Match
	// Assuming your cache returns the cached answer and a boolean/error
	cachedAnswer, isHit := n.QueryCache.GetCache(ctx, vector, state.Flags.SimilarityThreshold)

	if isHit && cachedAnswer != "" {
		slog.Info("⚡ Semantic Cache Hit! Short-circuiting graph.")
		state.FinalAnswer = cachedAnswer
		return "END", nil // Skip the router and downstream bots entirely!
	}

	slog.Info("Semantic Cache Miss. Proceeding to router.")
	return "planner_node", nil
}

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

func (n *GatewayNodes) ExecutionNode(ctx context.Context, state *AgentState) (string, error) {
	if state.StatusStream != nil {
		state.StatusStream("Executing tasks in parallel...")
	}
	slog.Info("[Node] Executing ExecutionNode (Fan-Out)", "tasks", len(state.Tasks))
	state.LoopCount++

	var wg sync.WaitGroup
	var mu sync.Mutex

	for i := range state.Tasks {
		wg.Add(1)

		go func(index int) {
			defer wg.Done()
			task := state.Tasks[index]

			// 🚀 Look up the specific worker for this domain
			worker, exists := n.Workers[task.Domain]
			if !exists {
				slog.Warn("Unknown domain, falling back to generic worker", "domain", task.Domain)
				worker = n.Workers["generic"] // Fallback safety
			}

			// Execute the worker blindly!
			answer, err := worker(ctx, task.Query, state.ReqConfig, state.SessionID)
			if err != nil {
				answer = "I'm sorry, I couldn't reach the " + task.Domain + " services right now."
			}

			mu.Lock()
			state.Tasks[index].Answer = answer
			mu.Unlock()

		}(i)
	}

	wg.Wait()
	slog.Info("Parallel execution complete. Fanning in...")
	return "synthesizer_node", nil
}
