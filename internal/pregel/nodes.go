package pregel

import (
	"context"
	"log/slog"

	"pisces-gateway/internal/cache"
	"pisces-gateway/internal/intent"
	"pisces-gateway/internal/llm"
	"pisces-gateway/internal/proxy"
	"pisces-gateway/internal/rewrite"
)

const (
	defaultTemperature = 0.0
)

type GatewayNodes struct {
	LLM        llm.Client
	QueryCache *cache.QueryCache
	FrasierBot *proxy.FrasierClient
	Rewriter   *rewrite.Rewriter
	Classifier *intent.Classifier
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
		return "router_node", nil
	}

	// 2. Delegate to the existing domain package!
	slog.Info("Delegating query to rewrite package...")
	state.Query = n.Rewriter.Resolve(ctx, state.Query, state.History)

	return "router_node", nil
}
func (n *GatewayNodes) RouterNode(ctx context.Context, state *AgentState) (string, error) {
	if state.StatusStream != nil {
		state.StatusStream("Classifying query intent...")
	}
	slog.Info("[Node] Executing RouterNode", "loop", state.LoopCount)
	state.LoopCount++

	// 1. Delegate intent classification to the existing domain package
	slog.Info("Delegating query to intent classifier...")

	// Assuming your classifier takes context and the query, returning a string
	domain := n.Classifier.Determine(ctx, state.Query)

	// Fallback to frasier if the classifier panics or returns empty
	if domain == "" {
		slog.Warn("Classifier returned empty domain, defaulting to 'frasier'")
		domain = "frasier"
	}

	state.Domain = domain
	slog.Info("Intent classification complete", "domain", state.Domain)

	// 2. Route based on the classified domain
	if state.Domain == "generic" {
		slog.Info("Routing -> generic (Skipping RAG)")
		return "generate_node", nil
	}

	slog.Info("Routing -> frasier (Executing RAG)")
	return "call_bot_node", nil
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
		"config":     state.RAGConfig,
		"session_id": state.SessionID,
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

// GenerateNode uses the Gateway's LLM to answer generic/off-topic questions
// without wasting the downstream Frasier Bot's database compute.
func (n *GatewayNodes) GenerateNode(ctx context.Context, state *AgentState) (string, error) {
	if state.StatusStream != nil {
		state.StatusStream("Generating general response...")
	}

	slog.Info("[Node] Executing GenerateNode for generic query", "loop", state.LoopCount)
	state.LoopCount++

	// Create a zero-shot prompt for the Gateway's internal LLM
	prompt := "You are a helpful AI gateway for a Frasier fan application. The user asked an off-topic question. Answer politely and accurately in 1-2 sentences: " + state.Query

	answer, err := n.LLM.GenerateText(ctx, prompt, defaultTemperature)
	if err != nil {
		slog.Error("Gateway LLM generation failed", "error", err)
		state.FinalAnswer = "I'm sorry, my internal systems are having trouble thinking right now."
		return "END", nil
	}

	state.FinalAnswer = answer
	slog.Info("Generation complete.")

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
		return "router_node", nil
	}

	// 1. Generate Embeddings for the rewritten query
	slog.Info("Generating embeddings for cache lookup...")
	vector, err := n.LLM.EmbedText(ctx, state.Query)
	if err != nil {
		slog.Warn("Embedding failed, skipping cache lookup", "error", err)
		return "router_node", nil // Fail-open: if embedding fails, just route to the bot
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
	return "router_node", nil
}
