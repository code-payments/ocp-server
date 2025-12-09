package metrics

import (
	"context"
)

// RecordEvent records a new event with a name and set of key-value pairs
func RecordEvent(ctx context.Context, eventName string, kvPairs map[string]interface{}) {
	provider, ok := ctx.Value(ProviderContextKey).(Provider)
	if ok && provider != nil {
		provider.RecordEvent(eventName, kvPairs)
	}
}
