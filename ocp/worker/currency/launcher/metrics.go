package launcher

import (
	"context"
	"time"

	"github.com/code-payments/ocp-server/metrics"
	"github.com/code-payments/ocp-server/ocp/data/currency"
)

const (
	currencyCountEventName = "CurrencyCountPollingCheck"
)

func (p *runtime) metricsGaugeWorker(ctx context.Context) error {
	delay := time.Second

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
			start := time.Now()

			for _, state := range []currency.MetadataState{
				currency.MetadataStateUnknown,
				currency.MetadataStateWaitingForInitialPurchase,
				currency.MetadataStateFundingAuthority,
				currency.MetadataStateExecutingInitialPurchase,
				currency.MetadataStateCompletingInitialization,
				currency.MetadataStateFinalValidation,
				currency.MetadataStateAvailable,
				currency.MetadataStateAbandoning,
			} {
				count, err := p.data.GetCurrencyMetadataCountByState(ctx, state)
				if err != nil {
					continue
				}
				recordCurrencyCountEvent(ctx, state, count)
			}

			delay = time.Second - time.Since(start)
		}
	}
}

func recordCurrencyCountEvent(ctx context.Context, state currency.MetadataState, count uint64) {
	metrics.RecordEvent(ctx, currencyCountEventName, map[string]interface{}{
		"count": count,
		"state": state.String(),
	})
}
