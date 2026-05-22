package account

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"go.uber.org/zap"

	"github.com/code-payments/ocp-server/metrics"
	"github.com/code-payments/ocp-server/ocp/data/account"
	"github.com/code-payments/ocp-server/retry"
)

// balanceBackfillBatchSize is the number of accounts whose balance is
// initialized per backfill pass.
const balanceBackfillBatchSize = 100

// balanceBackfillWorker initializes the stored balance of legacy account info
// rows that predate the balance column, computing each from full history. Once
// every account has a balance, the dual-write keeps them current and reads can
// be switched over to the stored field.
func (p *runtime) balanceBackfillWorker(runtimeCtx context.Context, interval time.Duration) error {
	delay := interval

	return retry.Loop(
		func() (err error) {
			time.Sleep(delay)

			provider := runtimeCtx.Value(metrics.ProviderContextKey).(metrics.Provider)
			trace := provider.StartTrace("account_runtime__handle_balance_backfill")
			defer trace.End()
			tracedCtx := metrics.NewContext(runtimeCtx, trace)

			records, err := p.data.GetAccountInfosRequiringBalanceInitialization(tracedCtx, balanceBackfillBatchSize)
			if err == account.ErrAccountInfoNotFound {
				// Nothing left to backfill.
				return nil
			} else if err != nil {
				trace.OnError(err)
				return err
			}

			for _, record := range records {
				err := p.backfillAccountBalance(tracedCtx, record.TokenAccount)
				if err != nil {
					// Log and continue; one bad account must not stall the backfill.
					trace.OnError(err)
					p.log.With(
						zap.Error(err),
						zap.String("token_account", record.TokenAccount),
					).Warn("failure initializing account balance")
				}
			}

			return nil
		},
		retry.NonRetriableErrors(context.Canceled),
	)
}

// backfillAccountBalance computes a token account's balance from its full
// history and stores it. It is race-safe against the concurrent balance
// dual-write: the account row is locked before the balance is computed, so a
// concurrent mutation either commits first (and is included in the computed
// total) or blocks until initialization completes (and then applies its delta
// on top).
func (p *runtime) backfillAccountBalance(ctx context.Context, tokenAccount string) error {
	return p.data.ExecuteInTx(ctx, sql.LevelDefault, func(ctx context.Context) error {
		stored, err := p.data.GetAccountBalanceForUpdate(ctx, tokenAccount)
		if err != nil {
			return err
		}
		if stored != nil {
			// Already initialized by the dual-write or a prior pass.
			return nil
		}

		netBalance, err := p.data.GetNetBalanceFromActions(ctx, tokenAccount)
		if err != nil {
			return err
		}
		depositedAmount, err := p.data.GetTotalExternalDepositedAmountInQuarks(ctx, tokenAccount)
		if err != nil {
			return err
		}

		total := netBalance + int64(depositedAmount)
		if total < 0 {
			return fmt.Errorf("computed negative balance %d for %s", total, tokenAccount)
		}

		return p.data.InitializeAccountBalance(ctx, tokenAccount, uint64(total))
	})
}
