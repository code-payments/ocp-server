package metrics

import (
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

// Request contains HTTP request information for tracing
type Request struct {
	Header    http.Header
	URL       interface{} // *url.URL
	Method    string
	Transport string
}
