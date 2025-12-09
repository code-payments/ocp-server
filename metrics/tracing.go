package metrics

import (
	"context"
	"fmt"
)

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
