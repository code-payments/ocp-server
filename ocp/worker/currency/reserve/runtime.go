package currency

import (
	"context"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/code-payments/ocp-server/metrics"
	"github.com/code-payments/ocp-server/ocp/common"
	currency_util "github.com/code-payments/ocp-server/ocp/currency"
	ocp_data "github.com/code-payments/ocp-server/ocp/data"
	"github.com/code-payments/ocp-server/ocp/data/currency"
	"github.com/code-payments/ocp-server/ocp/worker"
	"github.com/code-payments/ocp-server/retry"
	"github.com/code-payments/ocp-server/retry/backoff"
)

type reserveRuntime struct {
	log  *zap.Logger
	data ocp_data.Provider

	mintsMu sync.RWMutex
	mints   []*common.Account
}

func NewReserveRuntime(log *zap.Logger, data ocp_data.Provider) worker.Runtime {
	return &reserveRuntime{
		log:  log,
		data: data,
	}
}

func (p *reserveRuntime) Start(runtimeCtx context.Context, interval time.Duration) error {
	go p.pollMints(runtimeCtx, interval/3)

	for {
		_, err := retry.Retry(
			func() error {
				p.log.Debug("updating reserves")

				provider := runtimeCtx.Value(metrics.ProviderContextKey).(metrics.Provider)
				trace := provider.StartTrace("currency_reserve_runtime")
				defer trace.End()
				tracedCtx := metrics.NewContext(runtimeCtx, trace)

				err := p.UpdateAllLaunchpadCurrencyReserves(tracedCtx)
				if err != nil {
					trace.OnError(err)
					p.log.With(zap.Error(err)).Warn("failed to process current reserve data")
				}

				return err
			},
			retry.NonRetriableErrors(context.Canceled),
			retry.BackoffWithJitter(backoff.BinaryExponential(time.Second), interval, 0.1),
		)
		if err != nil {
			if err != context.Canceled {
				// Should not happen since only non-retriable error is context.Canceled
				p.log.With(zap.Error(err)).Warn("unexpected error when processing current reserve data")
			}

			return err
		}

		select {
		case <-runtimeCtx.Done():
			return runtimeCtx.Err()
		case <-time.After(interval):
		}
	}
}

func (p *reserveRuntime) pollMints(ctx context.Context, interval time.Duration) {
	// Initial fetch before the first reserve update
	p.refreshMints(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(interval):
			p.refreshMints(ctx)
		}
	}
}

func (p *reserveRuntime) refreshMints(ctx context.Context) {
	mintStrings, err := p.data.GetAllCurrencyMints(ctx)
	if err != nil {
		p.log.With(zap.Error(err)).Warn("failed to refresh currency mints")
		return
	}

	var mints []*common.Account
	for _, mint := range mintStrings {
		account, err := common.NewAccountFromPublicKeyString(mint)
		if err != nil {
			p.log.With(zap.Error(err), zap.String("mint", mint)).Warn("invalid mint public key")
			continue
		}

		if common.IsCoreMint(account) {
			continue
		}

		mints = append(mints, account)
	}

	p.mintsMu.Lock()
	p.mints = mints
	p.mintsMu.Unlock()
}

func (p *reserveRuntime) getMints() []*common.Account {
	p.mintsMu.RLock()
	defer p.mintsMu.RUnlock()

	return p.mints
}

func (p *reserveRuntime) UpdateAllLaunchpadCurrencyReserves(ctx context.Context) error {
	mints := p.getMints()

	var wg sync.WaitGroup
	wg.Add(len(mints))
	for _, mint := range mints {
		go func(mint *common.Account) {
			defer wg.Done()

			log := p.log.With(zap.String("mint", mint.PublicKey().ToBase58()))

			circulatingSupply, ts, err := currency_util.GetLaunchpadCurrencyCirculatingSupply(ctx, p.data, mint)
			if err != nil {
				log.With(zap.Error(err)).Warn("failed to get circulating supply")
				return
			}

			err = p.data.PutCurrencyReserve(ctx, &currency.ReserveRecord{
				Mint:              mint.PublicKey().ToBase58(),
				SupplyFromBonding: circulatingSupply,
				Time:              ts,
			})
			if err != nil {
				log.With(zap.Error(err)).Warn("failed to put currency reserve")
				return
			}
		}(mint)
	}
	wg.Wait()

	return nil
}
