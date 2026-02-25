package launcher

import (
	"context"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/code-payments/ocp-server/database/query"
	"github.com/code-payments/ocp-server/metrics"
	"github.com/code-payments/ocp-server/ocp/data/currency"
	"github.com/code-payments/ocp-server/retry"
)

func (p *runtime) worker(runtimeCtx context.Context, state currency.MetadataState, interval time.Duration) error {
	var cursor query.Cursor
	delay := interval

	err := retry.Loop(
		func() (err error) {
			time.Sleep(delay)

			provider := runtimeCtx.Value(metrics.ProviderContextKey).(metrics.Provider)
			trace := provider.StartTrace("currency_launcher_runtime__handle_" + state.String())
			defer trace.End()
			tracedCtx := metrics.NewContext(runtimeCtx, trace)

			items, err := p.data.GetAllCurrencyMetadataByState(
				tracedCtx,
				state,
				query.WithLimit(p.conf.batchSize.Get(runtimeCtx)),
				query.WithCursor(cursor),
			)
			if err == currency.ErrNotFound {
				cursor = query.EmptyCursor
				return nil
			} else if err != nil {
				cursor = query.EmptyCursor
				return err
			}

			var wg sync.WaitGroup
			for _, item := range items {
				wg.Add(1)

				go func(record *currency.MetadataRecord) {
					defer wg.Done()

					err := p.handle(tracedCtx, record)
					if err != nil {
						trace.OnError(err)
					}
				}(item)
			}
			wg.Wait()

			if len(items) > 0 {
				cursor = query.ToCursor(items[len(items)-1].Id)
			} else {
				cursor = query.EmptyCursor
			}

			return nil
		},
		retry.NonRetriableErrors(context.Canceled),
	)

	return err
}

func (p *runtime) handle(ctx context.Context, record *currency.MetadataRecord) error {
	log := p.log.With(
		zap.String("method", "handle"),
		zap.String("state", record.State.String()),
		zap.String("mint", record.Mint),
	)

	var err error
	switch record.State {
	case currency.MetadataStateUnknown:
		err = p.handleStateUnknown(ctx, record)
	case currency.MetadataStateFundingAuthority:
		err = p.handleStateFundingAuthority(ctx, record)
	case currency.MetadataStateInitializing:
		err = p.handleStateInitializing(ctx, record)
	case currency.MetadataStateFinalValidation:
		err = p.handleStateFinalValidation(ctx, record)
	}
	if err != nil {
		log.With(zap.Error(err)).Warn("failure processing currency for launch")
		return err
	}
	return nil
}

func (p *runtime) handleStateUnknown(ctx context.Context, record *currency.MetadataRecord) error {
	// todo: Implement unknown state handling logic
	return nil
}

func (p *runtime) handleStateFundingAuthority(ctx context.Context, record *currency.MetadataRecord) error {
	// todo: Implement funding authority state handling logic
	return nil
}

func (p *runtime) handleStateInitializing(ctx context.Context, record *currency.MetadataRecord) error {
	// todo: Implement initializing state handling logic
	return nil
}

func (p *runtime) handleStateFinalValidation(ctx context.Context, record *currency.MetadataRecord) error {
	// todo: Implement final validation state handling logic
	return nil
}
