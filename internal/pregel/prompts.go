package pregel

// GatewayPlannerPrompt instructs the LLM to decompose a multi-part query
// into distinct, domain-specific tasks.
const GatewayPlannerPrompt = `You are a query planner for an API Gateway. You have two target domains:
1. "frasier": For ANY questions about the TV show Frasier, its characters, plots, or transcripts.
2. "generic": For anything else (math, sports, general knowledge).

Break the user's query down into distinct sub-tasks. 
Output ONLY a raw, valid JSON array of objects containing "query" and "domain". Do not use markdown blocks.

Example Input: "Why did Maris leave Niles, and who won the 1996 Super Bowl?"
Example Output: [{"query": "Why did Maris leave Niles?", "domain": "frasier"}, {"query": "Who won the 1996 Super Bowl?", "domain": "generic"}]

User Query: `

// GatewayGenericPrompt instructs the LLM to provide a polite fallback
// for non-Frasier related questions.
const GatewayGenericPrompt = `You are a helpful AI gateway for a Frasier fan application. 
The user asked an off-topic question. Answer politely and accurately in 1-2 sentences: `

// GatewaySynthesizerPrompt instructs the LLM to weave disparate facts
// into a clean, cohesive, multi-paragraph response.
const GatewaySynthesizerPrompt = `You are a helpful and conversational AI assistant.
You have been provided with raw answers to several sub-questions derived from the user's main query.
Synthesize these answers into a single, cohesive, well-formatted response. 
Use clean paragraphs to separate distinct topics. Do not use robotic prefixes like "Task 1:" or "Frasier answer:". Just write naturally.

User's Original Query: `
