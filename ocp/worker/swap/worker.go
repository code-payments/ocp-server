package swap

import (
	"context"
	"sync"
	"time"

	"github.com/newrelic/go-agent/v3/newrelic"
	"github.com/pkg/errors"
	"go.uber.org/zap"

	"github.com/code-payments/ocp-server/database/query"
	"github.com/code-payments/ocp-server/metrics"
	"github.com/code-payments/ocp-server/ocp/data/intent"
	"github.com/code-payments/ocp-server/ocp/data/swap"
	"github.com/code-payments/ocp-server/retry"
	"github.com/code-payments/ocp-server/solana"
)

func (p *runtime) worker(runtimeCtx context.Context, state swap.State, interval time.Duration) error {
	var cursor query.Cursor
	delay := interval

	err := retry.Loop(
		func() (err error) {
			time.Sleep(delay)

			nr := runtimeCtx.Value(metrics.NewRelicContextKey).(*newrelic.Application)
			m := nr.StartTransaction("swap_runtime__handle_" + state.String())
			defer m.End()
			tracedCtx := newrelic.NewContext(runtimeCtx, m)

			items, err := p.data.GetAllSwapsByState(
				tracedCtx,
				state,
				query.WithLimit(p.conf.batchSize.Get(runtimeCtx)),
				query.WithCursor(cursor),
			)
			if err != nil {
				cursor = query.EmptyCursor
				return err
			}

			var wg sync.WaitGroup
			for _, item := range items {
				wg.Add(1)

				go func(record *swap.Record) {
					defer wg.Done()

					err := p.handle(tracedCtx, record)
					if err != nil {
						m.NoticeError(err)
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

func (p *runtime) handle(ctx context.Context, record *swap.Record) error {
	log := p.log.With(
		zap.String("method", "handle"),
		zap.String("state", record.State.String()),
		zap.String("swap_id", record.SwapId),
		zap.String("owner", record.Owner),
	)

	var err error
	switch record.State {
	case swap.StateCreated:
		err = p.handleStateCreated(ctx, record)
	case swap.StateFunding:
		err = p.handleStateFunding(ctx, record)
	case swap.StateFunded:
		err = p.handleStateFunded(ctx, record)
	case swap.StateSubmitting:
		err = p.handleStateSubmitting(ctx, record)
	case swap.StateCancelling:
		err = p.handleStateCancelling(ctx, record)
	}
	if err != nil {
		log.With(zap.Error(err)).Warn("failure processing swap")
		return err
	}
	return nil
}

func (p *runtime) handleStateCreated(ctx context.Context, record *swap.Record) error {
	if err := p.validateSwapState(record, swap.StateCreated); err != nil {
		return err
	}

	// Cancel the swap if the client hasn't submitted the intent to fund the swap
	// within a reasonable amount of time
	if time.Since(record.CreatedAt) > p.conf.clientTimeoutToFund.Get(ctx) {
		return p.markSwapCancelled(ctx, record)
	}

	return nil
}

func (p *runtime) handleStateFunding(ctx context.Context, record *swap.Record) error {
	if err := p.validateSwapState(record, swap.StateFunding); err != nil {
		return err
	}

	// Wait for the funding intent to be confirmed before to transition the swap
	// to a funded state
	intentRecord, err := p.data.GetIntent(ctx, record.FundingId)
	if err != nil {
		return errors.Wrap(err, "error getting funding intent record")
	}
	switch intentRecord.State {
	case intent.StateConfirmed:
		return p.markSwapFunded(ctx, record)
	case intent.StateFailed:
		// todo: Should never happen, but maybe cancel the swap?
		return errors.New("funding intent failed")
	default:
		return nil
	}
}

func (p *runtime) handleStateFunded(ctx context.Context, record *swap.Record) error {
	if err := p.validateSwapState(record, swap.StateFunded); err != nil {
		return err
	}

	intentRecord, err := p.data.GetIntent(ctx, record.FundingId)
	if err != nil {
		return err
	}

	// Cancel the swap if the client hasn't signed the swap transaction within a
	// reasonable amount of time. The funds for the swap will be deposited back
	// into the source VM.
	if time.Since(intentRecord.CreatedAt) > p.conf.clientTimeoutToSwap.Get(ctx) {
		txn, err := p.makeCancellationTransaction(ctx, record)
		if err != nil {
			return err
		}

		return p.markSwapCancelling(ctx, record, txn)
	}

	return nil
}

func (p *runtime) handleStateSubmitting(ctx context.Context, record *swap.Record) error {
	if err := p.validateSwapState(record, swap.StateSubmitting); err != nil {
		return err
	}

	// Monitor for a finalized swap transaction

	finalizedTxn, err := p.data.GetBlockchainTransaction(ctx, *record.TransactionSignature, solana.CommitmentFinalized)
	if err != nil && err != solana.ErrSignatureNotFound {
		return errors.Wrap(err, "error getting finalized transaction")
	}

	if finalizedTxn != nil {
		if finalizedTxn.Err != nil || finalizedTxn.Meta.Err != nil {
			// todo: Recovery flow to put back source funds into the source VM
			return p.markSwapFailed(ctx, record)
		} else {
			quarksBought, err := p.updateBalancesForFinalizedSwap(ctx, record)
			if err != nil {
				return errors.Wrap(err, "error updating balances")
			}

			err = p.markSwapFinalized(ctx, record)
			if err != nil {
				return errors.Wrap(err, "error marking swap as finalized")
			}

			recordSwapFinalizedEvent(ctx, record, quarksBought)

			go p.notifySwapFinalized(ctx, record)

			return nil
		}
	}

	// Otherwise, continually retry submitting the transaction

	return p.submitTransaction(ctx, record)
}

func (p *runtime) handleStateCancelling(ctx context.Context, record *swap.Record) error {
	if err := p.validateSwapState(record, swap.StateCancelling); err != nil {
		return err
	}

	// Monitor for a finalized cancellation transaction

	finalizedTxn, err := p.data.GetBlockchainTransaction(ctx, *record.TransactionSignature, solana.CommitmentFinalized)
	if err != nil && err != solana.ErrSignatureNotFound {
		return errors.Wrap(err, "error getting finalized transaction")
	}

	if finalizedTxn != nil {
		if finalizedTxn.Err != nil || finalizedTxn.Meta.Err != nil {
			// todo: Try again?
			return p.markSwapCancelled(ctx, record)
		} else {
			err = p.updateBalancesForCancelledSwap(ctx, record)
			if err != nil {
				return errors.Wrap(err, "error updating balances")
			}

			return p.markSwapCancelled(ctx, record)
		}
	}

	// Otherwise, continually retry submitting the transaction

	return p.submitTransaction(ctx, record)
}
