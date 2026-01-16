package swap

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"fmt"
	"slices"
	"time"

	"github.com/mr-tron/base58"
	"github.com/pkg/errors"

	"github.com/code-payments/ocp-server/ocp/common"
	currency_util "github.com/code-payments/ocp-server/ocp/currency"
	"github.com/code-payments/ocp-server/ocp/data/deposit"
	"github.com/code-payments/ocp-server/ocp/data/intent"
	"github.com/code-payments/ocp-server/ocp/data/nonce"
	"github.com/code-payments/ocp-server/ocp/data/swap"
	"github.com/code-payments/ocp-server/ocp/data/transaction"
	transaction_util "github.com/code-payments/ocp-server/ocp/transaction"
	"github.com/code-payments/ocp-server/solana"
)

func (p *runtime) validateSwapState(record *swap.Record, states ...swap.State) error {
	if slices.Contains(states, record.State) {
		return nil
	}
	return errors.New("invalid swap state")
}

func (p *runtime) markSwapFunded(ctx context.Context, record *swap.Record) error {
	err := p.validateSwapState(record, swap.StateFunding)
	if err != nil {
		return err
	}

	record.State = swap.StateFunded
	return p.data.SaveSwap(ctx, record)
}

func (p *runtime) markSwapSubmitting(ctx context.Context, record *swap.Record) error {
	err := p.validateSwapState(record, swap.StateFunded)
	if err != nil {
		return err
	}

	record.State = swap.StateSubmitting
	return p.data.SaveSwap(ctx, record)
}

func (p *runtime) markSwapFinalized(ctx context.Context, record *swap.Record) error {
	return p.data.ExecuteInTx(ctx, sql.LevelDefault, func(ctx context.Context) error {
		err := p.validateSwapState(record, swap.StateSubmitting)
		if err != nil {
			return err
		}

		err = p.markNonceReleasedDueToSubmittedTransaction(ctx, record)
		if err != nil {
			return err
		}

		record.TransactionBlob = nil
		record.State = swap.StateFinalized
		return p.data.SaveSwap(ctx, record)
	})
}

func (p *runtime) markSwapFailed(ctx context.Context, record *swap.Record) error {
	return p.data.ExecuteInTx(ctx, sql.LevelDefault, func(ctx context.Context) error {
		err := p.validateSwapState(record, swap.StateSubmitting)
		if err != nil {
			return err
		}

		err = p.markNonceReleasedDueToSubmittedTransaction(ctx, record)
		if err != nil {
			return err
		}

		record.TransactionBlob = nil
		record.State = swap.StateFailed
		return p.data.SaveSwap(ctx, record)
	})
}

func (p *runtime) markSwapCancelled(ctx context.Context, record *swap.Record) error {
	return p.data.ExecuteInTx(ctx, sql.LevelDefault, func(ctx context.Context) error {
		err := p.validateSwapState(record, swap.StateCreated, swap.StateFunding, swap.StateFunded)
		if err != nil {
			return err
		}

		switch record.State {
		case swap.StateCreated, swap.StateFunding:
			err = p.markNonceAvailableDueToCancelledSwap(ctx, record)
			if err != nil {
				return err
			}
		}

		record.TransactionBlob = nil
		record.State = swap.StateCancelled
		return p.data.SaveSwap(ctx, record)
	})
}

func (p *runtime) submitTransaction(ctx context.Context, record *swap.Record) error {
	err := p.validateSwapState(record, swap.StateSubmitting, swap.StateCancelling)
	if err != nil {
		return err
	}

	var txn solana.Transaction
	err = txn.Unmarshal(record.TransactionBlob)
	if err != nil {
		return errors.Wrap(err, "error unmarshalling transaction")
	}

	if base58.Encode(txn.Signature()) != record.TransactionSignature {
		return errors.New("unexpected transaction signature")
	}

	_, err = p.data.SubmitBlockchainTransaction(ctx, &txn)
	if err != nil {
		return errors.Wrap(err, "error submitting transaction")
	}
	return nil
}

func (p *runtime) updateBalancesForFinalizedSwap(ctx context.Context, record *swap.Record) (uint64, error) {
	owner, err := common.NewAccountFromPublicKeyString(record.Owner)
	if err != nil {
		return 0, err
	}

	toMint, err := common.NewAccountFromPublicKeyString(record.ToMint)
	if err != nil {
		return 0, err
	}

	destinationVmConfig, err := common.GetVmConfigForMint(ctx, p.data, toMint)
	if err != nil {
		return 0, err
	}

	ownerDestinationTimelockVault, err := owner.ToTimelockVault(destinationVmConfig)
	if err != nil {
		return 0, err
	}

	tokenBalances, err := p.data.GetBlockchainTransactionTokenBalances(ctx, record.TransactionSignature)
	if err != nil {
		return 0, err
	}

	deltaQuarksIntoOmnibus, err := transaction_util.GetDeltaQuarksFromTokenBalances(destinationVmConfig.Omnibus, tokenBalances)
	if err != nil {
		return 0, err
	}
	if deltaQuarksIntoOmnibus <= 0 {
		return 0, errors.New("delta quarks into destination vm omnibus is not positive")
	}

	usdMarketValue, _, err := currency_util.CalculateUsdMarketValue(ctx, p.data, toMint, uint64(deltaQuarksIntoOmnibus), time.Now())
	if err != nil {
		return 0, err
	}

	err = p.data.ExecuteInTx(ctx, sql.LevelDefault, func(ctx context.Context) error {
		// For transaction history
		intentRecord := &intent.Record{
			IntentId:   getSwapDepositIntentID(record.TransactionSignature, ownerDestinationTimelockVault),
			IntentType: intent.ExternalDeposit,

			MintAccount: toMint.PublicKey().ToBase58(),

			InitiatorOwnerAccount: owner.PublicKey().ToBase58(),

			ExternalDepositMetadata: &intent.ExternalDepositMetadata{
				DestinationTokenAccount: ownerDestinationTimelockVault.PublicKey().ToBase58(),
				Quantity:                uint64(deltaQuarksIntoOmnibus),
				UsdMarketValue:          usdMarketValue,
			},

			State:     intent.StateConfirmed,
			CreatedAt: time.Now(),
		}
		err = p.data.SaveIntent(ctx, intentRecord)
		if err != nil {
			return err
		}

		// For tracking in cached balances
		externalDepositRecord := &deposit.Record{
			Signature:      record.TransactionSignature,
			Destination:    ownerDestinationTimelockVault.PublicKey().ToBase58(),
			Amount:         uint64(deltaQuarksIntoOmnibus),
			UsdMarketValue: usdMarketValue,

			Slot:              tokenBalances.Slot,
			ConfirmationState: transaction.ConfirmationFinalized,

			CreatedAt: time.Now(),
		}
		return p.data.SaveExternalDeposit(ctx, externalDepositRecord)
	})
	if err != nil {
		return 0, err
	}
	return uint64(deltaQuarksIntoOmnibus), nil
}

func (p *runtime) notifySwapFinalized(ctx context.Context, swapRecord *swap.Record) error {
	owner, err := common.NewAccountFromPublicKeyString(swapRecord.Owner)
	if err != nil {
		return err
	}

	fromMint, err := common.NewAccountFromPublicKeyString(swapRecord.FromMint)
	if err != nil {
		return err
	}

	toMint, err := common.NewAccountFromPublicKeyString(swapRecord.ToMint)
	if err != nil {
		return err
	}

	isBuy := !common.IsCoreMint(toMint)

	targetMint := toMint
	if !isBuy {
		targetMint = fromMint
	}

	targetCurrencyMetadataRecord, err := p.data.GetCurrencyMetadata(ctx, targetMint.PublicKey().ToBase58())
	if err != nil {
		return nil
	}

	fundingIntentRecord, err := p.data.GetIntent(ctx, swapRecord.FundingId)
	if err != nil {
		return err
	}

	valueReceived := fundingIntentRecord.SendPublicPaymentMetadata.NativeAmount
	if !common.IsCoreMint(fromMint) {
		valueReceived = 0.99 * valueReceived
	}

	return p.integration.OnSwapFinalized(ctx, owner, isBuy, targetCurrencyMetadataRecord.Name, fundingIntentRecord.SendPublicPaymentMetadata.ExchangeCurrency, valueReceived)
}

func (p *runtime) markNonceReleasedDueToSubmittedTransaction(ctx context.Context, record *swap.Record) error {
	err := p.validateSwapState(record, swap.StateSubmitting, swap.StateCancelling)
	if err != nil {
		return err
	}

	nonceRecord, err := p.data.GetNonce(ctx, record.Nonce)
	if err != nil {
		return err
	}

	if record.TransactionSignature != nonceRecord.Signature {
		return errors.New("unexpected nonce signature")
	}

	if record.Blockhash != nonceRecord.Blockhash {
		return errors.New("unexpected nonce blockhash")
	}

	if nonceRecord.State != nonce.StateReserved {
		return errors.New("unexpected nonce state")
	}

	nonceRecord.State = nonce.StateReleased
	return p.data.SaveNonce(ctx, nonceRecord)
}

func (p *runtime) markNonceAvailableDueToCancelledSwap(ctx context.Context, record *swap.Record) error {
	err := p.validateSwapState(record, swap.StateCreated)
	if err != nil {
		return err
	}

	nonceRecord, err := p.data.GetNonce(ctx, record.Nonce)
	if err != nil {
		return err
	}

	if record.TransactionSignature != nonceRecord.Signature {
		return errors.New("unexpected nonce signature")
	}

	if record.Blockhash != nonceRecord.Blockhash {
		return errors.New("unexpected nonce blockhash")
	}

	if nonceRecord.State != nonce.StateReserved {
		return errors.New("unexpected nonce state")
	}

	nonceRecord.State = nonce.StateAvailable
	nonceRecord.Signature = ""
	return p.data.SaveNonce(ctx, nonceRecord)
}

func (p *runtime) validateIntentFunding(ctx context.Context, record *swap.Record) (bool, error) {
	if record.FundingSource != swap.FundingSourceSubmitIntent {
		return false, errors.New("invalid funding source")
	}

	owner, err := common.NewAccountFromPublicKeyString(record.Owner)
	if err != nil {
		return false, errors.Wrap(err, "error parsing owner")
	}

	fromMint, err := common.NewAccountFromPublicKeyString(record.FromMint)
	if err != nil {
		return false, errors.Wrap(err, "error parsing from mint")
	}

	sourceVmConfig, err := common.GetVmConfigForMint(ctx, p.data, fromMint)
	if err != nil {
		return false, errors.Wrap(err, "error getting vm config for source mint")
	}

	swapAta, err := owner.ToVmSwapAta(sourceVmConfig)
	if err != nil {
		return false, errors.Wrap(err, "error getting swap ata")
	}

	intentRecord, err := p.data.GetIntent(ctx, record.FundingId)
	if err != nil {
		return false, errors.Wrap(err, "error getting intent")
	}

	if intentRecord.IntentType != intent.SendPublicPayment {
		return false, nil
	}
	if intentRecord.SendPublicPaymentMetadata.Quantity < record.Amount {
		return false, nil
	}
	if intentRecord.SendPublicPaymentMetadata.DestinationTokenAccount != swapAta.PublicKey().ToBase58() {
		return false, nil
	}
	return true, nil
}

func (p *runtime) validateExternalWalletFunding(ctx context.Context, record *swap.Record) (bool, error) {
	if record.FundingSource != swap.FundingSourceExternalWallet {
		return false, errors.New("invalid funding source")
	}

	owner, err := common.NewAccountFromPublicKeyString(record.Owner)
	if err != nil {
		return false, errors.Wrap(err, "error parsing owner")
	}

	fromMint, err := common.NewAccountFromPublicKeyString(record.FromMint)
	if err != nil {
		return false, errors.Wrap(err, "error parsing from mint")
	}

	sourceVmConfig, err := common.GetVmConfigForMint(ctx, p.data, fromMint)
	if err != nil {
		return false, errors.Wrap(err, "error getting vm config for source mint")
	}

	swapAta, err := owner.ToVmSwapAta(sourceVmConfig)
	if err != nil {
		return false, errors.Wrap(err, "error getting swap ata")
	}

	tokenBalances, err := p.data.GetBlockchainTransactionTokenBalances(ctx, record.FundingId)
	if err != nil {
		return false, errors.Wrap(err, "error getting token balances")
	}

	deltaQuarks, err := transaction_util.GetDeltaQuarksFromTokenBalances(swapAta, tokenBalances)
	if err != nil {
		return false, errors.Wrap(err, "error getting delta quarks from token balances")
	}

	if deltaQuarks < int64(record.Amount) {
		return false, nil
	}
	return true, nil
}

func getSwapDepositIntentID(signature string, destination *common.Account) string {
	combined := fmt.Sprintf("%s-%s", signature, destination.PublicKey().ToBase58())
	hashed := sha256.Sum256([]byte(combined))
	return base58.Encode(hashed[:])
}
