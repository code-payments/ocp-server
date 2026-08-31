package holder

import (
	"context"
	"time"

	"go.uber.org/zap"

	"github.com/code-payments/ocp-server/metrics"
	"github.com/code-payments/ocp-server/ocp/common"
	ocp_data "github.com/code-payments/ocp-server/ocp/data"
	"github.com/code-payments/ocp-server/ocp/data/currency"
	currency_holder "github.com/code-payments/ocp-server/ocp/data/currency/holder"
	"github.com/code-payments/ocp-server/ocp/data/currency/reserve"
	"github.com/code-payments/ocp-server/ocp/worker"
	"github.com/code-payments/ocp-server/solana/currencycreator"
)

var (
	minHoldingValue = common.ToCoreMintQuarks(10) // $10
)

type holderRuntime struct {
	log          *zap.Logger
	data         ocp_data.Provider
	reserveStore reserve.Store
	holderStore  currency_holder.Store
}

func New(log *zap.Logger, data ocp_data.Provider, reserveStore reserve.Store, holderStore currency_holder.Store) worker.Runtime {
	return &holderRuntime{
		log:          log,
		data:         data,
		reserveStore: reserveStore,
		holderStore:  holderStore,
	}
}

func (p *holderRuntime) Start(runtimeCtx context.Context, interval time.Duration) error {
	for {
		start := time.Now()

		func() {
			p.log.Debug("updating holder counts")

			provider := runtimeCtx.Value(metrics.ProviderContextKey).(metrics.Provider)
			trace := provider.StartTrace("currency_holder_runtime")
			defer trace.End()
			tracedCtx := metrics.NewContext(runtimeCtx, trace)

			p.UpdateAllLaunchpadCurrencyHolderCounts(tracedCtx)
		}()

		delay := max(interval-time.Since(start), 0)
		select {
		case <-runtimeCtx.Done():
			return runtimeCtx.Err()
		case <-time.After(delay):
		}
	}
}

func (p *holderRuntime) UpdateAllLaunchpadCurrencyHolderCounts(ctx context.Context) {
	liveReserveRecordsByMint, err := p.reserveStore.GetAllLiveReserves(ctx)
	if err != nil {
		p.log.With(zap.Error(err)).Warn("failed getting all available currencies")
		return
	}

	for mint, reserveRecord := range liveReserveRecordsByMint {
		log := p.log.With(zap.String("mint", mint))

		holderCount, err := p.countHoldersForMint(ctx, mint, reserveRecord.SupplyFromBonding)
		if err != nil {
			log.With(zap.Error(err)).Warn("failed counting holders for mint")
			continue
		}

		now := time.Now()

		err = p.holderStore.PutLiveHolderCount(ctx, &currency.HolderCountRecord{
			Mint:        mint,
			HolderCount: holderCount,
			Time:        now,
		})
		if err != nil && err != currency.ErrStaleHolderState {
			log.With(zap.Error(err)).Warn("failed updating live holder count")
			continue
		}

		err = p.holderStore.PutHistoricalHolderCount(ctx, &currency.HolderCountRecord{
			Mint:        mint,
			HolderCount: holderCount,
			Time:        now,
		})
		if err != nil {
			log.With(zap.Error(err)).Warn("failed creating historical holder count")
			continue
		}
	}
}

func (p *holderRuntime) countHoldersForMint(ctx context.Context, mint string, currentSupply uint64) (uint64, error) {
	minHoldings := currencycreator.EstimateValueExchange(&currencycreator.EstimateValueExchangeArgs{
		CurrentSupplyInQuarks: currentSupply,
		ValueInQuarks:         minHoldingValue,
		ValueMintDecimals:     uint8(common.CoreMintDecimals),
	})
	if minHoldings == 0 {
		return 0, nil
	}

	return p.data.CountLockedBalancesByMint(ctx, mint, int64(minHoldings))
}
