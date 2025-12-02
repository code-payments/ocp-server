package async

import (
	"context"
	"time"

	"github.com/newrelic/go-agent/v3/newrelic"
	"go.uber.org/zap"

	"github.com/code-payments/ocp-server/metrics"
	"github.com/code-payments/ocp-server/ocp/common"
	ocp_data "github.com/code-payments/ocp-server/ocp/data"
	"github.com/code-payments/ocp-server/ocp/data/currency"
	"github.com/code-payments/ocp-server/ocp/worker"
	"github.com/code-payments/ocp-server/retry"
	"github.com/code-payments/ocp-server/retry/backoff"
	"github.com/code-payments/ocp-server/solana"
	"github.com/code-payments/ocp-server/solana/currencycreator"
	"github.com/code-payments/ocp-server/solana/token"
)

type reserveRuntime struct {
	log  *zap.Logger
	data ocp_data.Provider
}

func NewReserveRuntime(log *zap.Logger, data ocp_data.Provider) worker.Runtime {
	return &reserveRuntime{
		log:  log,
		data: data,
	}
}

func (p *reserveRuntime) Start(runtimeCtx context.Context, interval time.Duration) error {
	for {
		_, err := retry.Retry(
			func() error {
				p.log.Debug("updating exchange rates")

				nr := runtimeCtx.Value(metrics.NewRelicContextKey).(*newrelic.Application)
				m := nr.StartTransaction("currency_reserve_runtime")
				defer m.End()
				tracedCtx := newrelic.NewContext(runtimeCtx, m)

				err := p.UpdateAllLaunchpadCurrencyReserves(tracedCtx)
				if err != nil {
					m.NoticeError(err)
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

// todo: Don't hardcode Jeffy and other Flipcash currencies
func (p *reserveRuntime) UpdateAllLaunchpadCurrencyReserves(ctx context.Context) error {
	err1 := func() error {
		jeffyMintAccount, _ := common.NewAccountFromPublicKeyString("todo")
		jeffyVaultAccount, _ := common.NewAccountFromPublicKeyString("todo")
		coreMintVaultAccount, _ := common.NewAccountFromPublicKeyString("todo")

		var tokenAccount token.Account
		ai, err := p.data.GetBlockchainAccountInfo(ctx, jeffyVaultAccount.PublicKey().ToBase58(), solana.CommitmentFinalized)
		if err != nil {
			return err
		}
		tokenAccount.Unmarshal(ai.Data)
		jeffyVaultBalance := tokenAccount.Amount

		ai, err = p.data.GetBlockchainAccountInfo(ctx, coreMintVaultAccount.PublicKey().ToBase58(), solana.CommitmentFinalized)
		if err != nil {
			return err
		}
		tokenAccount.Unmarshal(ai.Data)
		coreMintVaultBalance := tokenAccount.Amount

		return p.data.PutCurrencyReserve(ctx, &currency.ReserveRecord{
			Mint:              jeffyMintAccount.PublicKey().ToBase58(),
			SupplyFromBonding: currencycreator.DefaultMintMaxQuarkSupply - jeffyVaultBalance,
			CoreMintLocked:    coreMintVaultBalance,
			Time:              time.Now(),
		})
	}()

	if err1 != nil {
		return err1
	}

	return nil
}
