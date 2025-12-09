package newrelic

import (
	"net/http"
	"net/url"
	"time"

	"github.com/newrelic/go-agent/v3/newrelic"

	"github.com/code-payments/ocp-server/metrics"
)

// Provider wraps a New Relic application to implement the metrics.Provider interface
type Provider struct {
	app *newrelic.Application
}

// NewProvider creates a new New Relic metrics provider
func NewProvider(app *newrelic.Application) *Provider {
	return &Provider{app: app}
}

// Application returns the underlying New Relic application for cases where
// direct access is needed (e.g., log integration)
func (p *Provider) Application() *newrelic.Application {
	return p.app
}

// StartTrace starts a new trace
func (p *Provider) StartTrace(name string) metrics.Trace {
	return &Trace{txn: p.app.StartTransaction(name)}
}

// RecordEvent records a custom event with key-value attributes
func (p *Provider) RecordEvent(eventName string, attributes map[string]interface{}) {
	p.app.RecordCustomEvent(eventName, attributes)
}

// RecordCount records a count metric
func (p *Provider) RecordCount(metricName string, count uint64) {
	p.app.RecordCustomMetric(metricName, float64(count))
}

// RecordDuration records a duration metric
func (p *Provider) RecordDuration(metricName string, duration time.Duration) {
	p.app.RecordCustomMetric(metricName, float64(duration/time.Millisecond))
}

// Trace wraps a New Relic transaction
type Trace struct {
	txn *newrelic.Transaction
}

// StartSpan starts a new span within the trace
func (t *Trace) StartSpan(name string) metrics.Span {
	return &Span{seg: t.txn.StartSegment(name)}
}

// AddAttribute adds a key-value attribute to the trace
func (t *Trace) AddAttribute(key string, value interface{}) {
	t.txn.AddAttribute(key, value)
}

// OnError records an error on the trace
func (t *Trace) OnError(err error) {
	t.txn.NoticeError(err)
}

// SetRequest sets HTTP request information on the trace
func (t *Trace) SetRequest(r metrics.Request) {
	var u *url.URL
	if r.URL != nil {
		u = r.URL.(*url.URL)
	}

	transport := newrelic.TransportHTTP
	switch r.Transport {
	case "HTTP":
		transport = newrelic.TransportHTTP
	case "HTTPS":
		transport = newrelic.TransportHTTPS
	}

	t.txn.SetWebRequest(newrelic.WebRequest{
		Header:    r.Header,
		URL:       u,
		Method:    r.Method,
		Transport: transport,
	})
}

// SetResponse sets the HTTP response writer for the trace
func (t *Trace) SetResponse(w http.ResponseWriter) http.ResponseWriter {
	return t.txn.SetWebResponse(w)
}

// End completes the trace
func (t *Trace) End() {
	t.txn.End()
}

// Unwrap returns the underlying New Relic transaction for advanced use cases
func (t *Trace) Unwrap() *newrelic.Transaction {
	return t.txn
}

// Span wraps a New Relic segment
type Span struct {
	seg *newrelic.Segment
}

// AddAttribute adds a key-value attribute to the span
func (s *Span) AddAttribute(key string, value interface{}) {
	s.seg.AddAttribute(key, value)
}

// End completes the span
func (s *Span) End() {
	s.seg.End()
}
