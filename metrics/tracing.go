package metrics

import (
	"context"
	"fmt"
	"net/http"
)

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

// Span represents a timed span within a trace for tracing individual operations.
type Span interface {
	// AddAttribute adds a key-value attribute to the span
	AddAttribute(key string, value interface{})

	// End completes the span
	End()
}

// TraceMethodCall traces a method call with a given struct/package and method names
func TraceMethodCall(ctx context.Context, structOrPackageName, methodName string) *MethodTracer {
	trace := TraceFromContext(ctx)
	if trace == nil {
		return nil
	}

	span := trace.StartSpan(fmt.Sprintf("%s %s", structOrPackageName, methodName))

	return &MethodTracer{
		trace: trace,
		span:  span,
	}
}

// MethodTracer collects analytics for a given method call within an existing
// trace.
type MethodTracer struct {
	trace Trace
	span  Span
}

// AddAttribute adds a key-value pair metadata to the method trace
func (t *MethodTracer) AddAttribute(key string, value interface{}) {
	if t == nil {
		return
	}

	t.span.AddAttribute(key, value)
}

// AddAttributes adds a set of key-value pair metadata to the method trace
func (t *MethodTracer) AddAttributes(attributes map[string]interface{}) {
	if t == nil {
		return
	}

	for key, value := range attributes {
		t.span.AddAttribute(key, value)
	}
}

// OnError observes an error within a method trace
func (t *MethodTracer) OnError(err error) {
	if t == nil {
		return
	}

	if err == nil {
		return
	}

	t.trace.OnError(err)
}

// End completes the trace for the method call.
func (t *MethodTracer) End() {
	if t == nil {
		return
	}

	t.span.End()
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
