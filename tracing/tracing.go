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
