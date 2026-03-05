package reserve

import (
	"context"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/code-payments/ocp-server/metrics"
	"github.com/code-payments/ocp-server/ocp/common"
	ocp_data "github.com/code-payments/ocp-server/ocp/data"
	"github.com/code-payments/ocp-server/ocp/data/currency"
	"github.com/code-payments/ocp-server/ocp/worker"
)

type reserveRuntime struct {
	log  *zap.Logger
	data ocp_data.Provider

	mintsMu sync.RWMutex
	mints   []*common.Account
}

func New(log *zap.Logger, data ocp_data.Provider) worker.Runtime {
	return &reserveRuntime{
		log:  log,
		data: data,
	}
}

func (p *reserveRuntime) Start(runtimeCtx context.Context, interval time.Duration) error {
	for {
		start := time.Now()

		func() {
			p.log.Debug("updating historical reserves")

			provider := runtimeCtx.Value(metrics.ProviderContextKey).(metrics.Provider)
			trace := provider.StartTrace("currency_reserve_runtime")
			defer trace.End()
			tracedCtx := metrics.NewContext(runtimeCtx, trace)

			p.UpdateAllHistoricalLaunchpadCurrencyReserves(tracedCtx)
		}()

		delay := max(interval-time.Since(start), 0)
		select {
		case <-runtimeCtx.Done():
			return runtimeCtx.Err()
		case <-time.After(delay):
		}
	}
}

func (p *reserveRuntime) UpdateAllHistoricalLaunchpadCurrencyReserves(ctx context.Context) {
	now := time.Now()

	liveReserveStatesByMint, err := p.data.GetAllLiveCurrencyReserves(ctx)
	if err != nil {
		p.log.With(zap.Error(err)).Warn("failed getting all live reserve states")
		return
	}

	for mint, reserveRecord := range liveReserveStatesByMint {
		log := p.log.With(zap.String("mint", mint))

		err = p.data.PutHistoricalCurrencyReserve(ctx, &currency.ReserveRecord{
			Mint:              mint,
			SupplyFromBonding: reserveRecord.SupplyFromBonding,
			Time:              now,
		})
		if err != nil {
			log.With(zap.Error(err)).Warn("failed to put historical currency reserve")
			continue
		}

		recordReserveStateEvent(ctx, mint, reserveRecord.SupplyFromBonding)
	}
}
