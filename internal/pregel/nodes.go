package pregel

import (
	"context"
	"log/slog"

	// Import your existing internal packages
	"pisces-gateway/internal/cache"
	"pisces-gateway/internal/intent"
	"pisces-gateway/internal/llm"
	"pisces-gateway/internal/proxy"
	"pisces-gateway/internal/rewrite"
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

	// 1. Check Configs
	if state.Config.NoSession || len(state.History) == 0 {
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

	// Construct the payload exactly as the proxy client expects it
	payload := map[string]any{
		"query":  state.Query,
		"stream": state.IsStream,
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

// SearchNode executes a vector search to retrieve relevant transcripts.
func (n *GatewayNodes) SearchNode(ctx context.Context, state *AgentState) (string, error) {
	slog.Info("[Node] Executing SearchNode", "loop", state.LoopCount)
	state.LoopCount++

	slog.Info("Executing pgvector search for query embeddings...")

	// MOCK DATABASE RESULTS: In the future, this calls your pgvector database or n.QueryCache.
	state.SearchContexts = []string{
		"Transcript 1: Maris left Niles because he stood up for himself.",
		"Transcript 2: Niles was devastated when Maris served him papers.",
	}

	slog.Info("Search complete.", "retrieved_contexts", len(state.SearchContexts))

	return "generate_node", nil
}

// GenerateNode takes the query and contexts and streams the final answer.
func (n *GatewayNodes) GenerateNode(ctx context.Context, state *AgentState) (string, error) {
	slog.Info("[Node] Executing GenerateNode", "loop", state.LoopCount)
	state.LoopCount++

	slog.Info("Compiling context and generating final answer...")

	// MOCK LLM CALL: In the future, this calls n.LLM.GenerateAnswer
	if len(state.SearchContexts) > 0 {
		state.FinalAnswer = "Based on my extensive knowledge of Frasier... Maris left Niles after he finally stood up to her over the couples therapy debacle. (Mocked Answer)"
	} else {
		state.FinalAnswer = "I'm sorry, I couldn't find any relevant transcripts for that query. (Mocked Answer)"
	}

	slog.Info("Generation complete.")

	// Route to END to terminate the engine loop
	return "END", nil
}

func (n *GatewayNodes) SemanticCacheNode(ctx context.Context, state *AgentState) (string, error) {
	if state.StatusStream != nil {
		state.StatusStream("Checking semantic cache...")
	}
	slog.Info("[Node] Executing SemanticCacheNode", "loop", state.LoopCount)
	state.LoopCount++

	if state.Config.SkipCache {
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
	cachedAnswer, isHit := n.QueryCache.GetCache(ctx, vector, state.Config.SimilarityThreshold)

	if isHit && cachedAnswer != "" {
		slog.Info("⚡ Semantic Cache Hit! Short-circuiting graph.")
		state.FinalAnswer = cachedAnswer
		return "END", nil // Skip the router and downstream bots entirely!
	}

	slog.Info("Semantic Cache Miss. Proceeding to router.")
	return "router_node", nil
}
