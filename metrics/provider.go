package metrics

import (
	"context"
	"net/http"
	"time"
)

// Provider defines an abstract metrics provider that can record events,
// metrics, and traces. This allows swapping between different backends
// (New Relic, Datadog, Prometheus, no-op, etc.).
type Provider interface {
	// StartTrace starts a new trace
	StartTrace(name string) Trace

	// RecordEvent records a custom event with key-value attributes
	RecordEvent(eventName string, attributes map[string]interface{})

	// RecordCount records a count metric
	RecordCount(metricName string, count uint64)

	// RecordDuration records a duration metric
	RecordDuration(metricName string, duration time.Duration)
}

// Trace represents an active trace that can contain multiple spans and attributes.
type Trace interface {
	// StartSpan starts a new span within the trace
	StartSpan(name string) Span

	// AddAttribute adds a key-value attribute to the trace
	AddAttribute(key string, value interface{})

	// OnError records an error on the trace
	OnError(err error)

	// SetRequest sets HTTP request information on the trace
	SetRequest(r Request)

	// SetResponse sets the HTTP response writer for the trace
	SetResponse(w http.ResponseWriter) http.ResponseWriter

	// End completes the trace
	End()
}

// Request contains HTTP request information for tracing
type Request struct {
	Header    http.Header
	URL       interface{} // *url.URL
	Method    string
	Transport string
}

// Span represents a timed span within a trace for tracing individual operations.
type Span interface {
	// AddAttribute adds a key-value attribute to the span
	AddAttribute(key string, value interface{})

	// End completes the span
	End()
}

// traceContextKey is the context key for storing the current trace
type traceContextKey struct{}

// TraceKey is the context key for Trace
var TraceKey = traceContextKey{}

// NewContext returns a new context with the trace attached
func NewContext(ctx context.Context, trace Trace) context.Context {
	return context.WithValue(ctx, TraceKey, trace)
}

// TraceFromContext retrieves the trace from context, if present
func TraceFromContext(ctx context.Context) Trace {
	trace, _ := ctx.Value(TraceKey).(Trace)
	return trace
}
