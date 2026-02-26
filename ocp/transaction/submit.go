package transaction

import (
	"context"
	"time"

	"github.com/mr-tron/base58"
	"github.com/pkg/errors"

	ocp_data "github.com/code-payments/ocp-server/ocp/data"
	"github.com/code-payments/ocp-server/retry"
	"github.com/code-payments/ocp-server/retry/backoff"
	"github.com/code-payments/ocp-server/solana"
)

var (
	ErrTransactionFailed = errors.New("transaction failed")
)

// SubmitAndWaitForFinalization submits a signed transaction and polls until it
// reaches finalized commitment. An error is returned if the transaction fails
// on chain or the context is cancelled.
func SubmitAndWaitForFinalization(ctx context.Context, data ocp_data.BlockchainData, txn *solana.Transaction) error {
	_, err := data.SubmitBlockchainTransaction(ctx, txn)
	if err != nil {
		return errors.Wrap(err, "error submitting transaction")
	}

	sig := base58.Encode(txn.Signature())

	_, err = retry.Retry(
		func() error {
			finalizedTxn, err := data.GetBlockchainTransaction(ctx, sig, solana.CommitmentFinalized)
			if err == solana.ErrSignatureNotFound {
				return err
			}
			if err != nil {
				return errors.Wrap(err, "error getting finalized transaction")
			}

			if finalizedTxn.Err != nil || (finalizedTxn.Meta != nil && finalizedTxn.Meta.Err != nil) {
				return ErrTransactionFailed
			}

			return nil
		},
		retry.NonRetriableErrors(context.Canceled, ErrTransactionFailed),
		retry.Backoff(backoff.Constant(time.Second), time.Second),
		retry.Limit(60),
	)
	return err
}
