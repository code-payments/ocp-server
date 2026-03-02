package account

import (
	"context"
	"time"

	"github.com/code-payments/ocp-server/metrics"
)

const (
	giftCardWorkerEventName = "GiftCardWorkerPollingCheck"
)

func (p *runtime) metricsGaugeWorker(ctx context.Context) error {
	delay := time.Second

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
			start := time.Now()

			p.recordBackupQueueStatusPollingEvent(ctx)

			delay = time.Second - time.Since(start)
		}
	}
}

func (p *runtime) recordBackupQueueStatusPollingEvent(ctx context.Context) {
	count, err := p.data.GetAccountInfoCountRequiringAutoReturnCheck(ctx)
	if err == nil {
		metrics.RecordEvent(ctx, giftCardWorkerEventName, map[string]interface{}{
			"queue_size": count,
		})
	}
}
