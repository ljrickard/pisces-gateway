package tracing

import (
	"context"

	"go.opentelemetry.io/otel/trace"
)

// GetTraceID extracts the string representation of the current Trace ID
func GetTraceID(ctx context.Context) string {
	spanCtx := trace.SpanContextFromContext(ctx)
	if spanCtx.HasTraceID() {
		return spanCtx.TraceID().String()
	}
	return ""
}

// SendGatewayStatus systematically matches an OpenTelemetry span descriptor name
// with a beautifully formatted text update statement string for the stream callback.
func SendGatewayStatus(callback func(string), spanName string) {
	if callback == nil {
		return
	}

	messages := map[string]string{
		"Redis.GetSession":          "Loading conversation history",
		"Gemini.RewriteQuery":       "Optimizing query formulation parameters",
		"Vertex.EmbedQuery":         "Scanning semantic cache registers", // Matches embed generation span name
		"Gemini.ClassifyIntent":     "Classifying conversation domain intent routing",
		"HTTP.FrasierBotCallStream": "Establishing secure stream route to Frasier Bot service",
	}

	msg, exists := messages[spanName]
	if !exists {
		msg = spanName // Fallback safely to the span name if unmatched
	}

	callback(msg)
}
