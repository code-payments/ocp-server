package swap

import (
	"context"
	"sync"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/zap"

	"github.com/code-payments/ocp-server/coinbase"
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

			provider := runtimeCtx.Value(metrics.ProviderContextKey).(metrics.Provider)
			trace := provider.StartTrace("swap_runtime__handle_" + state.String())
			defer trace.End()
			tracedCtx := metrics.NewContext(runtimeCtx, trace)

			items, err := p.data.GetAllSwapsByState(
				tracedCtx,
				state,
				query.WithLimit(p.conf.batchSize.Get(runtimeCtx)),
				query.WithCursor(cursor),
			)
			if err == swap.ErrNotFound {
				cursor = query.EmptyCursor
				return nil
			} else if err != nil {
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
		return p.markSwapCancelled(ctx, record, nil)
	}

	return nil
}

func (p *runtime) handleStateFunding(ctx context.Context, record *swap.Record) error {
	log := p.log.With(
		zap.String("method", "handleStateFunding"),
		zap.String("swap_id", record.SwapId),
		zap.String("funding_id", record.FundingId),
		zap.String("owner", record.Owner),
	)

	if err := p.validateSwapState(record, swap.StateFunding); err != nil {
		return err
	}

	switch record.FundingSource {
	case swap.FundingSourceSubmitIntent:
		// Wait for the funding intent to be confirmed before transitioning the swap
		// to a funded state
		intentRecord, err := p.data.GetIntent(ctx, record.FundingId)
		if err != nil {
			return errors.Wrap(err, "error getting funding intent record")
		}
		switch intentRecord.State {
		case intent.StateConfirmed:
			return p.markSwapFunded(ctx, record)
		case intent.StateFailed:
			return p.markSwapCancelled(ctx, record, nil)
		default:
			return nil
		}
	case swap.FundingSourceExternalWallet:
		// Wait for the external wallet funding transaction to be finalized before
		// transitioning the swap to a funded state
		finalizedTxn, err := p.data.GetBlockchainTransaction(ctx, record.FundingId, solana.CommitmentFinalized)
		if err != nil && err != solana.ErrSignatureNotFound {
			return errors.Wrap(err, "error getting finalized funding transaction")
		}

		if finalizedTxn != nil {
			if finalizedTxn.Err != nil || finalizedTxn.Meta.Err != nil {
				return p.markSwapCancelled(ctx, record, nil)
			}
			return p.markSwapFunded(ctx, record)
		}

		// Cancel the swap if the external wallet funding transaction hasn't been
		// finalized within a reasonable amount of time
		if time.Since(record.CreatedAt) > p.conf.externalWalletFinalizationTimeout.Get(ctx) {
			return p.markSwapCancelled(ctx, record, nil)
		}

		return nil
	case swap.FundingSourceCoinbaseOnramp:
		// Look up the Coinbase order. The funding ID is the Coinbase order ID,
		// and the order's TxHash holds the on-chain settlement signature once
		// Coinbase has broadcast the transaction.
		order, err := p.coinbaseClient.GetOrder(ctx, record.FundingId)
		if err != nil {
			return errors.Wrap(err, "error getting coinbase order")
		}

		switch order.Status {
		case coinbase.OrderStatusProcessing, coinbase.OrderStatusCompleted:
			if order.TxHash != "" {
				finalizedTxn, err := p.data.GetBlockchainTransaction(ctx, order.TxHash, solana.CommitmentFinalized)
				if err != nil && err != solana.ErrSignatureNotFound {
					return errors.Wrap(err, "error getting finalized coinbase funding transaction")
				}

				if finalizedTxn != nil {
					if finalizedTxn.Err != nil || finalizedTxn.Meta.Err != nil {
						return p.markSwapCancelled(ctx, record, nil)
					}
					return p.markSwapFunded(ctx, record)
				}
			}

			if time.Since(record.CreatedAt) > 2*p.conf.coinbaseOnrampOrderTimeout.Get(ctx) {
				log.With(
					zap.String("txn", order.TxHash),
					zap.String("order_status", string(order.Status)),
				).Info("funding transaction for coinbase order is not finalizing")
			}
		case coinbase.OrderStatusFailed:
			return p.markSwapCancelled(ctx, record, nil)
		default:
			// Cancel the swap if the Coinbase onramp order hasn't been completed
			// within a reasonable amount of time. Timeout should be greater than
			// that enforced on client to avoid lost funds.
			if time.Since(record.CreatedAt) > p.conf.coinbaseOnrampOrderTimeout.Get(ctx) {
				return p.markSwapCancelled(ctx, record, nil)
			}
		}

		return nil
	default:
		return errors.New("unsupported funding source")
	}
}

func (p *runtime) handleStateFunded(ctx context.Context, record *swap.Record) error {
	if err := p.validateSwapState(record, swap.StateFunded); err != nil {
		return err
	}

	var isValid bool
	var err error
	switch record.FundingSource {
	case swap.FundingSourceSubmitIntent:
		isValid, err = p.validateIntentFunding(ctx, record)
		if err != nil {
			return errors.Wrap(err, "error validating intent funding")
		}
	case swap.FundingSourceExternalWallet:
		isValid, err = p.validateExternalWalletFunding(ctx, record)
		if err != nil {
			return errors.Wrap(err, "error validating external wallet funding")
		}
	case swap.FundingSourceCoinbaseOnramp:
		isValid, err = p.validateCoinbaseOnrampFunding(ctx, record)
		if err != nil {
			return errors.Wrap(err, "error validating coinbase onramp funding")
		}
	default:
		return errors.New("unsupported funding source")
	}

	if !isValid {
		// todo: Return funds if the amount was wrong
		return p.markSwapCancelled(ctx, record, nil)
	}

	err = p.ensureSwapDestinationIsInitialized(ctx, record)
	if err != nil {
		return errors.Wrap(err, "error ensuring swap destination is initialized")
	}

	return p.markSwapSubmitting(ctx, record)
}

func (p *runtime) handleStateSubmitting(ctx context.Context, record *swap.Record) error {
	if err := p.validateSwapState(record, swap.StateSubmitting); err != nil {
		return err
	}

	// todo: Need to wait for authority funding, which should only happen after swap
	//       funding is validated. However, having it in this handler is a bit awkward.
	//       Refactor swap state management to address.
	ok, err := p.validateDestinationCurrencyReadyForSwap(ctx, record)
	if err != nil {
		return errors.Wrap(err, "error ensuring destination currency is ready to be initialized")
	} else if !ok {
		return nil
	}

	// Monitor for a finalized swap transaction

	finalizedTxn, err := p.data.GetBlockchainTransaction(ctx, record.TransactionSignature, solana.CommitmentFinalized)
	if err != nil && err != solana.ErrSignatureNotFound {
		return errors.Wrap(err, "error getting finalized transaction")
	}

	if finalizedTxn != nil {
		if finalizedTxn.Err != nil || finalizedTxn.Meta.Err != nil {
			return p.autoRecoverSwapFunds(ctx, record)
		} else {
			tokenBalances, err := p.data.GetBlockchainTransactionTokenBalances(ctx, record.TransactionSignature)
			if err != nil {
				return errors.Wrap(err, "error getting transaction token balances")
			}

			err = p.updateLiveReserveStateForFinalizedSwap(ctx, record, tokenBalances)
			if err != nil {
				return errors.Wrap(err, "error updating live reserve state")
			}

			quarksBought, isMintInit, err := p.maybeUpdateBalancesForFinalizedSwap(ctx, record, tokenBalances)
			if err != nil {
				return errors.Wrap(err, "error updating balances")
			}

			err = p.markSwapFinalized(ctx, record)
			if err != nil {
				return errors.Wrap(err, "error marking swap as finalized")
			}

			recordSwapFinalizedEvent(ctx, record, quarksBought, isMintInit)

			go p.notifySwapFinalized(ctx, record, isMintInit)

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

	finalizedTxn, err := p.data.GetBlockchainTransaction(ctx, record.TransactionSignature, solana.CommitmentFinalized)
	if err != nil && err != solana.ErrSignatureNotFound {
		return errors.Wrap(err, "error getting finalized cancel transaction")
	}

	if finalizedTxn != nil {
		if finalizedTxn.Err != nil || finalizedTxn.Meta.Err != nil {
			return p.markSwapFailed(ctx, record)
		}
		return p.markSwapCancelled(ctx, record, finalizedTxn)
	}

	if err := p.ensureSwapSourceIsInitialized(ctx, record, true); err != nil {
		return errors.Wrap(err, "error ensuring swap source is initialized")
	}

	return p.submitTransaction(ctx, record)
}
