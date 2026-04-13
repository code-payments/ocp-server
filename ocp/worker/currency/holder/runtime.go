package holder

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/zap"

	commonpb "github.com/code-payments/ocp-protobuf-api/generated/go/common/v1"

	"github.com/code-payments/ocp-server/metrics"
	"github.com/code-payments/ocp-server/ocp/balance"
	"github.com/code-payments/ocp-server/ocp/common"
	ocp_data "github.com/code-payments/ocp-server/ocp/data"
	"github.com/code-payments/ocp-server/ocp/data/account"
	"github.com/code-payments/ocp-server/ocp/data/currency"
	"github.com/code-payments/ocp-server/ocp/worker"
	"github.com/code-payments/ocp-server/solana/currencycreator"
)

const (
	historicalUpdateTimeInterval = time.Hour
)

var (
	minHoldingValue = common.ToCoreMintQuarks(1) / 4 // $0.25
)

type holderRuntime struct {
	log  *zap.Logger
	data ocp_data.Provider
}

func New(log *zap.Logger, data ocp_data.Provider) worker.Runtime {
	return &holderRuntime{
		log:  log,
		data: data,
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
	liveReserveRecordsByMint, err := p.data.GetAllLiveCurrencyReserves(ctx)
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

		err = p.data.PutLiveCurrencyHolderCount(ctx, &currency.HolderCountRecord{
			Mint:        mint,
			HolderCount: holderCount,
			Time:        now,
		})
		if err != nil && err != currency.ErrStaleHolderState {
			log.With(zap.Error(err)).Warn("failed updating live holder count")
			continue
		}

		var shouldCreateHistoricalRecord bool
		historicalRecord, err := p.data.GetCurrencyHolderCountAtTime(ctx, mint, now)
		switch err {
		case nil:
			shouldCreateHistoricalRecord = time.Since(historicalRecord.Time) >= historicalUpdateTimeInterval
		case currency.ErrNotFound:
			shouldCreateHistoricalRecord = true
		default:
			log.With(zap.Error(err)).Warn("failed getting historical record")
			continue
		}

		if shouldCreateHistoricalRecord {
			err = p.data.PutHistoricalCurrencyHolderCount(ctx, &currency.HolderCountRecord{
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

	accountRecords, err := p.data.GetAccountInfosByMintAndType(ctx, mint, commonpb.AccountType_PRIMARY)
	if err == account.ErrAccountInfoNotFound {
		return 0, nil
	} else if err != nil {
		return 0, errors.Wrap(err, "error getting primary accounts")
	}

	if len(accountRecords) == 0 {
		return 0, nil
	}

	tokenAccounts := make([]*common.Account, len(accountRecords))
	for i, record := range accountRecords {
		tokenAccounts[i], err = common.NewAccountFromPublicKeyString(record.TokenAccount)
		if err != nil {
			return 0, errors.Wrap(err, "invalid token account public key")
		}
	}

	balances, err := balance.BatchCalculateFromCacheWithTokenAccounts(ctx, p.data, tokenAccounts...)
	if err != nil {
		return 0, errors.Wrap(err, "error calculating balances batch")
	}

	var count uint64
	for _, bal := range balances {
		if bal >= minHoldings {
			count++
		}
	}

	return count, nil
}
