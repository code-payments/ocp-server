package metrics

import (
	"context"
	"time"
)

// RecordCount records a count metric
func RecordCount(ctx context.Context, metricName string, count uint64) {
	provider, ok := ctx.Value(ProviderContextKey).(Provider)
	if ok && provider != nil {
		provider.RecordCount(metricName, count)
	}
}

// RecordDuration records a duration metric
func RecordDuration(ctx context.Context, metricName string, duration time.Duration) {
	provider, ok := ctx.Value(ProviderContextKey).(Provider)
	if ok && provider != nil {
		provider.RecordDuration(metricName, duration)
	}
}
