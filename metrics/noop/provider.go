package noop

import (
	"net/http"
	"time"

	"github.com/code-payments/ocp-server/metrics"
)

// Provider is a no-op metrics provider that discards all metrics.
// Use this when metrics collection is disabled.
type Provider struct{}

// NewProvider creates a new no-op metrics provider
func NewProvider() *Provider {
	return &Provider{}
}

// StartTrace returns a no-op trace
func (p *Provider) StartTrace(name string) metrics.Trace {
	return &Trace{}
}

// RecordEvent is a no-op
func (p *Provider) RecordEvent(eventName string, attributes map[string]interface{}) {}

// RecordCount is a no-op
func (p *Provider) RecordCount(metricName string, count uint64) {}

// RecordDuration is a no-op
func (p *Provider) RecordDuration(metricName string, duration time.Duration) {}

// Trace is a no-op trace
type Trace struct{}

// StartSpan returns a no-op span
func (t *Trace) StartSpan(name string) metrics.Span {
	return &Span{}
}

// AddAttribute is a no-op
func (t *Trace) AddAttribute(key string, value interface{}) {}

// OnError is a no-op
func (t *Trace) OnError(err error) {}

// SetRequest is a no-op
func (t *Trace) SetRequest(r metrics.Request) {}

// SetResponse returns the writer unchanged
func (t *Trace) SetResponse(w http.ResponseWriter) http.ResponseWriter {
	return w
}

// End is a no-op
func (t *Trace) End() {}

// Span is a no-op span
type Span struct{}

// AddAttribute is a no-op
func (s *Span) AddAttribute(key string, value interface{}) {}

// End is a no-op
func (s *Span) End() {}
