package transaction

import (
	"context"
	"time"

	"github.com/code-payments/ocp-server/grpc/client"
	"github.com/code-payments/ocp-server/metrics"
	"github.com/code-payments/ocp-server/ocp/data/intent"
)

const (
	userIntentCreatedEventName            = "UserIntentCreated"
	submitIntentLatencyBreakdownEventName = "SubmitIntentLatencyBreakdown"
	criticalSubmitIntentFailure           = "CriticalSubmitIntentFailure"
)

func recordUserIntentCreatedEvent(ctx context.Context, intentRecord *intent.Record) {
	metrics.RecordEvent(ctx, userIntentCreatedEventName, map[string]interface{}{
		"id":   intentRecord.IntentId,
		"type": intentRecord.IntentType.String(),
		"mint": intentRecord.MintAccount,
	})
}

func recordSubmitIntentLatencyBreakdownEvent(ctx context.Context, section string, latency time.Duration, actionCount int, intentType string) {
	latencyInMs := latency / time.Millisecond
	metrics.RecordEvent(ctx, submitIntentLatencyBreakdownEventName, map[string]interface{}{
		"section":      section,
		"latency_ms":   int(latencyInMs),
		"action_count": actionCount,
		"intent_type":  intentType,
	})
}

func recordCriticalSubmitIntentFailure(ctx context.Context, intentRecord *intent.Record, err error) {
	kvs := map[string]interface{}{
		"error": err.Error(),
	}

	userAgent, err := client.GetUserAgent(ctx)
	if err == nil {
		kvs["user_agent"] = userAgent.String()
	}

	if intentRecord != nil {
		if len(intentRecord.IntentId) > 0 {
			kvs["intent_id"] = intentRecord.IntentId
		}
		if len(intentRecord.InitiatorOwnerAccount) > 0 {
			kvs["user_public_key"] = intentRecord.InitiatorOwnerAccount
		}
	}

	metrics.RecordEvent(ctx, criticalSubmitIntentFailure, kvs)
}
