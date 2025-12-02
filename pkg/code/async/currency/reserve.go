package async

import (
	"context"
	"time"

	"github.com/newrelic/go-agent/v3/newrelic"
	"go.uber.org/zap"

	"github.com/code-payments/ocp-server/pkg/code/async"
	"github.com/code-payments/ocp-server/pkg/code/common"
	code_data "github.com/code-payments/ocp-server/pkg/code/data"
	"github.com/code-payments/ocp-server/pkg/code/data/currency"
	"github.com/code-payments/ocp-server/pkg/metrics"
	"github.com/code-payments/ocp-server/pkg/retry"
	"github.com/code-payments/ocp-server/pkg/retry/backoff"
	"github.com/code-payments/ocp-server/pkg/solana"
	"github.com/code-payments/ocp-server/pkg/solana/currencycreator"
	"github.com/code-payments/ocp-server/pkg/solana/token"
)

type reserveService struct {
	log  *zap.Logger
	data code_data.Provider
}

func NewReserveService(log *zap.Logger, data code_data.Provider) async.Service {
	return &reserveService{
		log:  log,
		data: data,
	}
}

func (p *reserveService) Start(serviceCtx context.Context, interval time.Duration) error {
	for {
		_, err := retry.Retry(
			func() error {
				p.log.Debug("updating exchange rates")

				nr := serviceCtx.Value(metrics.NewRelicContextKey).(*newrelic.Application)
				m := nr.StartTransaction("async__currency_reserve_service")
				defer m.End()
				tracedCtx := newrelic.NewContext(serviceCtx, m)

				err := p.UpdateAllLaunchpadCurrencyReserves(tracedCtx)
				if err != nil {
					m.NoticeError(err)
					p.log.With(zap.Error(err)).Warn("failed to process current rate data")
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
		case <-serviceCtx.Done():
			return serviceCtx.Err()
		case <-time.After(interval):
		}
	}
}

// todo: Don't hardcode Jeffy and other Flipcash currencies
func (p *reserveService) UpdateAllLaunchpadCurrencyReserves(ctx context.Context) error {
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
