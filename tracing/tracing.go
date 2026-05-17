package tracing

import (
	"context"

	"go.opentelemetry.io/otel/trace"
)

func GetTraceID(ctx context.Context) string {
	spanCtx := trace.SpanContextFromContext(ctx)
	if spanCtx.HasTraceID() {
		return spanCtx.TraceID().String()
	}
	return ""
}

func SendGatewayStatus(callback func(string), spanName string) {
	if callback == nil {
		return
	}

	messages := map[string]string{
		"Redis.GetSession":          "Loading conversation history",
		"Gemini.RewriteQuery":       "Optimizing query formulation parameters",
		"Vertex.EmbedQuery":         "Scanning semantic cache registers",
		"Gemini.ClassifyIntent":     "Classifying conversation domain intent routing",
		"HTTP.FrasierBotCallStream": "Establishing secure stream route to Frasier Bot service",
	}

	msg, exists := messages[spanName]
	if !exists {
		msg = spanName
	}

	callback(msg)
}
