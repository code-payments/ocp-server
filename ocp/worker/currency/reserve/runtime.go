package reserve

import (
	"context"
	"time"

	"go.uber.org/zap"

	"github.com/code-payments/ocp-server/metrics"
	"github.com/code-payments/ocp-server/ocp/common"
	currency_util "github.com/code-payments/ocp-server/ocp/currency"
	ocp_data "github.com/code-payments/ocp-server/ocp/data"
	"github.com/code-payments/ocp-server/ocp/data/currency"
	currency_reserve "github.com/code-payments/ocp-server/ocp/data/currency/reserve"
	"github.com/code-payments/ocp-server/ocp/worker"
)

type reserveRuntime struct {
	log          *zap.Logger
	conf         *conf
	data         ocp_data.Provider
	reserveStore currency_reserve.Store
}

func New(log *zap.Logger, data ocp_data.Provider, reserveStore currency_reserve.Store, configProvider ConfigProvider) worker.Runtime {
	return &reserveRuntime{
		log:          log,
		conf:         configProvider(),
		data:         data,
		reserveStore: reserveStore,
	}
}

func (p *reserveRuntime) Start(runtimeCtx context.Context, interval time.Duration) error {
	for {
		start := time.Now()

		func() {
			p.log.Debug("updating reserves")

			provider := runtimeCtx.Value(metrics.ProviderContextKey).(metrics.Provider)
			trace := provider.StartTrace("currency_reserve_runtime")
			defer trace.End()
			tracedCtx := metrics.NewContext(runtimeCtx, trace)

			p.UpdateAllLaunchpadCurrencyReserves(tracedCtx)
		}()

		delay := max(interval-time.Since(start), 0)
		select {
		case <-runtimeCtx.Done():
			return runtimeCtx.Err()
		case <-time.After(delay):
		}
	}
}

func (p *reserveRuntime) UpdateAllLaunchpadCurrencyReserves(ctx context.Context) {
	staleThreshold := p.conf.staleThreshold.Get(ctx)

	liveReserveStatesByMint, err := p.reserveStore.GetAllLiveReserves(ctx)
	if err != nil {
		p.log.With(zap.Error(err)).Warn("failed getting all live reserve states")
		return
	}

	for mint, liveReserveRecord := range liveReserveStatesByMint {
		log := p.log.With(zap.String("mint", mint))

		now := time.Now()
		if now.Sub(liveReserveRecord.Time) >= staleThreshold {
			newReserveRecord, err := p.refreshLiveReserveState(ctx, log, mint)
			if err == nil {
				liveReserveRecord = newReserveRecord
			}
		}

		err = p.reserveStore.PutHistoricalReserve(ctx, &currency.ReserveRecord{
			Mint:              mint,
			SupplyFromBonding: liveReserveRecord.SupplyFromBonding,
			Time:              now,
		})
		if err != nil {
			log.With(zap.Error(err)).Warn("failed to put historical currency reserve")
			continue
		}

		recordReserveStateEvent(ctx, mint, liveReserveRecord.SupplyFromBonding)
	}
}

func (p *reserveRuntime) refreshLiveReserveState(ctx context.Context, log *zap.Logger, mint string) (*currency.ReserveRecord, error) {
	mintAccount, err := common.NewAccountFromPublicKeyString(mint)
	if err != nil {
		log.With(zap.Error(err)).Warn("invalid mint public key")
		return nil, err
	}

	circulatingSupply, slot, _, err := currency_util.GetLaunchpadCurrencyCirculatingSupply(ctx, p.data, mintAccount)
	if err != nil {
		log.With(zap.Error(err)).Warn("failed to get circulating supply from blockchain")
		return nil, err
	}

	record := &currency.ReserveRecord{
		Mint:              mint,
		SupplyFromBonding: circulatingSupply,
		Slot:              slot,
		Time:              time.Now(),
	}
	err = p.reserveStore.PutLiveReserve(ctx, record)
	if err != nil && err != currency.ErrStaleReserveState {
		log.With(zap.Error(err)).Warn("failed to update live reserve state")
		return nil, err
	}
	return record, nil
}
