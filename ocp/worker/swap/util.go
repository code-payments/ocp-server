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

	currency_lib "github.com/code-payments/ocp-server/currency"
	"github.com/code-payments/ocp-server/ocp/common"
	currency_util "github.com/code-payments/ocp-server/ocp/currency"
	"github.com/code-payments/ocp-server/ocp/data/currency"
	"github.com/code-payments/ocp-server/ocp/data/deposit"
	"github.com/code-payments/ocp-server/ocp/data/intent"
	"github.com/code-payments/ocp-server/ocp/data/nonce"
	"github.com/code-payments/ocp-server/ocp/data/swap"
	"github.com/code-payments/ocp-server/ocp/data/transaction"
	transaction_util "github.com/code-payments/ocp-server/ocp/transaction"
	vm_util "github.com/code-payments/ocp-server/ocp/vm"
	"github.com/code-payments/ocp-server/solana"
	compute_budget "github.com/code-payments/ocp-server/solana/computebudget"
	"github.com/code-payments/ocp-server/solana/currencycreator"
	"github.com/code-payments/ocp-server/solana/system"
	"github.com/code-payments/ocp-server/solana/token"
	"github.com/code-payments/ocp-server/solana/vm"
)

func (p *runtime) validateSwapState(record *swap.Record, states ...swap.State) error {
	if slices.Contains(states, record.State) {
		return nil
	}
	return errors.New("invalid swap state")
}

func (p *runtime) validateCurrencyMetadataState(record *currency.MetadataRecord, states ...currency.MetadataState) error {
	if slices.Contains(states, record.State) {
		return nil
	}
	return errors.New("invalid currency state")
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

func (p *runtime) markSwapFinalized(ctx context.Context, swapRecord *swap.Record) error {
	toMint, err := common.NewAccountFromPublicKeyString(swapRecord.ToMint)
	if err != nil {
		return err
	}

	var destinationCurrencyMetadataRecord *currency.MetadataRecord
	if swapRecord.Kind == swap.KindReserve && !common.IsCoreMint(toMint) {
		destinationCurrencyMetadataRecord, err = p.data.GetCurrencyMetadata(ctx, swapRecord.ToMint)
		if err != nil {
			return err
		}
		err = p.validateCurrencyMetadataState(destinationCurrencyMetadataRecord, currency.MetadataStateExecutingInitialPurchase, currency.MetadataStateAvailable)
		if err != nil {
			return err
		}
	}

	return p.data.ExecuteInTx(ctx, sql.LevelDefault, func(ctx context.Context) error {
		err := p.validateSwapState(swapRecord, swap.StateSubmitting)
		if err != nil {
			return err
		}

		err = p.markNonceReleasedDueToSubmittedTransaction(ctx, swapRecord)
		if err != nil {
			return err
		}

		if swapRecord.Kind == swap.KindReserve && !common.IsCoreMint(toMint) {
			if destinationCurrencyMetadataRecord.State == currency.MetadataStateExecutingInitialPurchase {
				destinationCurrencyMetadataRecord.State = currency.MetadataStateCompletingInitialization
				err = p.data.SaveCurrencyMetadata(ctx, destinationCurrencyMetadataRecord)
				if err != nil {
					return err
				}
			}
		}

		swapRecord.TransactionBlob = nil
		swapRecord.State = swap.StateFinalized
		return p.data.SaveSwap(ctx, swapRecord)
	})
}

func (p *runtime) markSwapFailed(ctx context.Context, swapRecord *swap.Record) error {
	toMint, err := common.NewAccountFromPublicKeyString(swapRecord.ToMint)
	if err != nil {
		return err
	}

	var destinationCurrencyMetadataRecord *currency.MetadataRecord
	if swapRecord.Kind == swap.KindReserve && !common.IsCoreMint(toMint) {
		destinationCurrencyMetadataRecord, err = p.data.GetCurrencyMetadata(ctx, swapRecord.ToMint)
		if err != nil {
			return err
		}
		err = p.validateCurrencyMetadataState(
			destinationCurrencyMetadataRecord,
			currency.MetadataStatePrePurchaseSetup,
			currency.MetadataStateExecutingInitialPurchase,
			currency.MetadataStateAvailable,
		)
		if err != nil {
			return err
		}
	}

	return p.data.ExecuteInTx(ctx, sql.LevelDefault, func(ctx context.Context) error {
		err := p.validateSwapState(swapRecord, swap.StateSubmitting, swap.StateCancelling)
		if err != nil {
			return err
		}

		err = p.markNonceReleasedDueToSubmittedTransaction(ctx, swapRecord)
		if err != nil {
			return err
		}

		if swapRecord.Kind == swap.KindReserve && !common.IsCoreMint(toMint) {
			switch destinationCurrencyMetadataRecord.State {
			case currency.MetadataStatePrePurchaseSetup, currency.MetadataStateExecutingInitialPurchase:
				destinationCurrencyMetadataRecord.State = currency.MetadataStateAbandoning
				err = p.data.SaveCurrencyMetadata(ctx, destinationCurrencyMetadataRecord)
				if err != nil {
					return err
				}
			}
		}

		swapRecord.TransactionBlob = nil
		swapRecord.State = swap.StateFailed
		return p.data.SaveSwap(ctx, swapRecord)
	})
}

func (p *runtime) autoRecoverSwapFunds(ctx context.Context, record *swap.Record) error {
	if err := p.ensureSwapSourceIsInitialized(ctx, record, false); err != nil {
		return errors.Wrap(err, "error ensuring swap source is initialized")
	}

	selectedNonce, err := p.solanaNoncePool.GetNonce(ctx)
	if err != nil {
		return errors.Wrap(err, "error allocating nonce for cancel swap")
	}
	defer selectedNonce.ReleaseIfNotReserved(ctx)

	cancelTxn, err := p.buildCancelSwapTransaction(ctx, record, selectedNonce)
	if err != nil {
		return errors.Wrap(err, "error building cancel swap transaction")
	}

	return p.markSwapCancelling(
		ctx,
		record,
		selectedNonce,
		cancelTxn,
	)
}

func (p *runtime) buildCancelSwapTransaction(
	ctx context.Context,
	record *swap.Record,
	selectedNonce *transaction_util.Nonce,
) (*solana.Transaction, error) {
	owner, err := common.NewAccountFromPublicKeyString(record.Owner)
	if err != nil {
		return nil, errors.Wrap(err, "error parsing owner")
	}

	fromMint, err := common.NewAccountFromPublicKeyString(record.FromMint)
	if err != nil {
		return nil, errors.Wrap(err, "error parsing from mint")
	}

	sourceVmConfig, err := common.GetVmConfigForMint(ctx, p.data, fromMint)
	if err != nil {
		return nil, errors.Wrap(err, "error getting vm config for source mint")
	}

	sourceTimelockAccounts, err := owner.GetTimelockAccounts(sourceVmConfig)
	if err != nil {
		return nil, errors.Wrap(err, "error getting source timelock accounts")
	}

	memoryAccount, memoryIndex, err := vm_util.GetVirtualTimelockAccountLocationInMemory(ctx, p.data, sourceTimelockAccounts.Vault, false)
	if err != nil {
		return nil, errors.Wrap(err, "error getting source vta memory location")
	}

	subsidizer := common.GetSubsidizer()

	cancelInstr := vm.NewCancelSwapInstruction(
		&vm.CancelSwapInstructionAccounts{
			VmAuthority: sourceVmConfig.Authority.PublicKey().ToBytes(),
			Vm:          sourceVmConfig.Vm.PublicKey().ToBytes(),
			VmMemory:    memoryAccount.PublicKey().ToBytes(),
			Swapper:     owner.PublicKey().ToBytes(),
			SwapPda:     sourceTimelockAccounts.VmSwapAccounts.Pda.PublicKey().ToBytes(),
			SwapAta:     sourceTimelockAccounts.VmSwapAccounts.Ata.PublicKey().ToBytes(),
			VmOmnibus:   sourceVmConfig.Omnibus.PublicKey().ToBytes(),
		},
		&vm.CancelSwapInstructionArgs{
			AccountIndex: memoryIndex,
			Amount:       record.SwapAmount + record.FeeAmount,
			Bump:         sourceTimelockAccounts.VmSwapAccounts.PdaBump,
		},
	)

	closeInstr := vm.NewCloseSwapAccountIfEmptyInstruction(
		&vm.CloseSwapAccountIfEmptyInstructionAccounts{
			VmAuthority: sourceVmConfig.Authority.PublicKey().ToBytes(),
			Vm:          sourceVmConfig.Vm.PublicKey().ToBytes(),
			Swapper:     owner.PublicKey().ToBytes(),
			SwapPda:     sourceTimelockAccounts.VmSwapAccounts.Pda.PublicKey().ToBytes(),
			SwapAta:     sourceTimelockAccounts.VmSwapAccounts.Ata.PublicKey().ToBytes(),
			Destination: subsidizer.PublicKey().ToBytes(),
		},
		&vm.CloseSwapAccountIfEmptyInstructionArgs{
			Bump: sourceTimelockAccounts.VmSwapAccounts.PdaBump,
		},
	)

	instructions := []solana.Instruction{
		system.AdvanceNonce(selectedNonce.Account.PublicKey().ToBytes(), subsidizer.PublicKey().ToBytes()),
		compute_budget.SetComputeUnitLimit(80_000),
		compute_budget.SetComputeUnitPrice(10_000),
		cancelInstr,
		closeInstr,
	}

	txn := solana.NewLegacyTransaction(subsidizer.PublicKey().ToBytes(), instructions...)
	txn.SetBlockhash(selectedNonce.Blockhash)

	if err := txn.Sign(
		subsidizer.PrivateKey().ToBytes(),
		sourceVmConfig.Authority.PrivateKey().ToBytes(),
	); err != nil {
		return nil, errors.Wrap(err, "error signing cancel swap transaction")
	}

	return &txn, nil
}

func (p *runtime) markSwapCancelling(
	ctx context.Context,
	swapRecord *swap.Record,
	cancelNonce *transaction_util.Nonce,
	cancelTxn *solana.Transaction,
) error {
	return p.data.ExecuteInTx(ctx, sql.LevelDefault, func(ctx context.Context) error {
		err := p.validateSwapState(swapRecord, swap.StateSubmitting)
		if err != nil {
			return err
		}

		err = p.markNonceReleasedDueToSubmittedTransaction(ctx, swapRecord)
		if err != nil {
			return err
		}

		cancelTransactionSignature := base58.Encode(cancelTxn.Signature())

		err = cancelNonce.MarkReservedWithSignature(ctx, cancelTransactionSignature)
		if err != nil {
			return err
		}

		swapRecord.Nonce = cancelNonce.Account.PublicKey().ToBase58()
		swapRecord.Blockhash = base58.Encode(cancelNonce.Blockhash[:])
		swapRecord.TransactionSignature = cancelTransactionSignature
		swapRecord.TransactionBlob = cancelTxn.Marshal()
		swapRecord.State = swap.StateCancelling
		return p.data.SaveSwap(ctx, swapRecord)
	})
}

func (p *runtime) markSwapCancelled(ctx context.Context, swapRecord *swap.Record, cancellationTxn *solana.ConfirmedTransaction) error {
	toMint, err := common.NewAccountFromPublicKeyString(swapRecord.ToMint)
	if err != nil {
		return err
	}

	var destinationCurrencyMetadataRecord *currency.MetadataRecord
	if swapRecord.Kind == swap.KindReserve && !common.IsCoreMint(toMint) {
		destinationCurrencyMetadataRecord, err = p.data.GetCurrencyMetadata(ctx, swapRecord.ToMint)
		if err != nil {
			return err
		}
		err = p.validateCurrencyMetadataState(
			destinationCurrencyMetadataRecord,
			currency.MetadataStatePrePurchaseSetup,
			currency.MetadataStateExecutingInitialPurchase,
			currency.MetadataStateAvailable,
		)
		if err != nil {
			return err
		}
	}

	var refundIntentRecord *intent.Record
	var refundDepositRecord *deposit.Record
	if swapRecord.State == swap.StateCancelling {
		refundIntentRecord, refundDepositRecord, err = p.buildRefundRecordsForCancelledSwap(ctx, swapRecord, cancellationTxn)
		if err != nil {
			return err
		}
	}

	return p.data.ExecuteInTx(ctx, sql.LevelDefault, func(ctx context.Context) error {
		err := p.validateSwapState(swapRecord, swap.StateCreated, swap.StateFunding, swap.StateFunded, swap.StateCancelling)
		if err != nil {
			return err
		}

		switch swapRecord.State {
		case swap.StateCreated, swap.StateFunding, swap.StateFunded:
			err = p.markNonceAvailableDueToCancelledSwap(ctx, swapRecord)
			if err != nil {
				return err
			}
		case swap.StateCancelling:
			err = p.markNonceReleasedDueToSubmittedTransaction(ctx, swapRecord)
			if err != nil {
				return err
			}

			err = p.data.SaveIntent(ctx, refundIntentRecord)
			if err != nil {
				return err
			}

			err = p.data.SaveExternalDeposit(ctx, refundDepositRecord)
			if err != nil {
				return err
			}
		}

		if swapRecord.Kind == swap.KindReserve && !common.IsCoreMint(toMint) {
			switch destinationCurrencyMetadataRecord.State {
			case currency.MetadataStatePrePurchaseSetup, currency.MetadataStateExecutingInitialPurchase:
				destinationCurrencyMetadataRecord.State = currency.MetadataStateAbandoning
				err = p.data.SaveCurrencyMetadata(ctx, destinationCurrencyMetadataRecord)
				if err != nil {
					return err
				}
			}
		}

		swapRecord.TransactionBlob = nil
		swapRecord.State = swap.StateCancelled
		return p.data.SaveSwap(ctx, swapRecord)
	})
}

func (p *runtime) buildRefundRecordsForCancelledSwap(ctx context.Context, swapRecord *swap.Record, cancellationTxn *solana.ConfirmedTransaction) (*intent.Record, *deposit.Record, error) {
	if cancellationTxn == nil {
		return nil, nil, errors.New("cancellation transaction is required for refund records")
	}

	owner, err := common.NewAccountFromPublicKeyString(swapRecord.Owner)
	if err != nil {
		return nil, nil, err
	}

	fromMint, err := common.NewAccountFromPublicKeyString(swapRecord.FromMint)
	if err != nil {
		return nil, nil, err
	}

	sourceVmConfig, err := common.GetVmConfigForMint(ctx, p.data, fromMint)
	if err != nil {
		return nil, nil, err
	}

	ownerSourceTimelockVault, err := owner.ToTimelockVault(sourceVmConfig)
	if err != nil {
		return nil, nil, err
	}

	quantity := swapRecord.SwapAmount + swapRecord.FeeAmount

	var exchangeCurrency currency_lib.Code
	var nativeAmount, usdMarketValue float64
	var isReturned bool
	switch swapRecord.FundingSource {
	case swap.FundingSourceSubmitIntent:
		fundingIntentRecord, err := p.data.GetIntent(ctx, swapRecord.FundingId)
		if err != nil {
			return nil, nil, err
		}
		if fundingIntentRecord.IntentType != intent.SendPublicPayment {
			return nil, nil, errors.New("unexpected intent type")
		}
		exchangeCurrency = fundingIntentRecord.SendPublicPaymentMetadata.ExchangeCurrency
		nativeAmount = fundingIntentRecord.SendPublicPaymentMetadata.NativeAmount
		usdMarketValue = fundingIntentRecord.SendPublicPaymentMetadata.UsdMarketValue
		isReturned = true
	case swap.FundingSourceExternalWallet, swap.FundingSourceCoinbaseOnramp:
		if !common.IsCoreMint(fromMint) {
			return nil, nil, errors.New("unexpected source mint")
		}
		exchangeCurrency = currency_lib.USD
		usdMarketValue, err = currency_util.CalculateUsdMarketValueFromTokenAmount(ctx, p.data, p.exchangeRateStore, p.reserveStore, common.CoreMintAccount, quantity, time.Now())
		if err != nil {
			return nil, nil, err
		}
		nativeAmount = usdMarketValue
	default:
		return nil, nil, errors.New("unsupported funding source")
	}

	exchangeRate := currency_util.CalculateExchangeRate(fromMint, quantity, nativeAmount)

	intentRecord := &intent.Record{
		IntentId:   getSwapDepositIntentID(swapRecord.TransactionSignature, ownerSourceTimelockVault),
		IntentType: intent.ExternalDeposit,

		MintAccount: fromMint.PublicKey().ToBase58(),

		InitiatorOwnerAccount: owner.PublicKey().ToBase58(),

		ExternalDepositMetadata: &intent.ExternalDepositMetadata{
			DestinationTokenAccount: ownerSourceTimelockVault.PublicKey().ToBase58(),
			Quantity:                quantity,
			ExchangeCurrency:        exchangeCurrency,
			ExchangeRate:            exchangeRate,
			NativeAmount:            nativeAmount,
			UsdMarketValue:          usdMarketValue,
			IsReturned:              isReturned,
		},

		State:     intent.StateConfirmed,
		CreatedAt: time.Now(),
	}

	depositRecord := &deposit.Record{
		Signature:   swapRecord.TransactionSignature,
		Destination: ownerSourceTimelockVault.PublicKey().ToBase58(),
		Amount:      quantity,

		Slot:              cancellationTxn.Slot,
		ConfirmationState: transaction.ConfirmationFinalized,

		CreatedAt: time.Now(),
	}

	return intentRecord, depositRecord, nil
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

func (p *runtime) maybeUpdateBalancesForFinalizedSwap(ctx context.Context, swapRecord *swap.Record, tokenBalances *solana.TransactionTokenBalances) (uint64, bool, error) {
	switch swapRecord.Kind {
	case swap.KindReserve:
		return p.maybeUpdateBalancesForFinalizedReserveSwap(ctx, swapRecord, tokenBalances)
	case swap.KindStablecoin:
		return p.getStablecoinSwapQuarksBought(swapRecord, tokenBalances)
	default:
		return 0, false, errors.New("unsupported swap kind")
	}
}

func (p *runtime) maybeUpdateBalancesForFinalizedReserveSwap(ctx context.Context, swapRecord *swap.Record, tokenBalances *solana.TransactionTokenBalances) (uint64, bool, error) {
	if swapRecord.Kind != swap.KindReserve {
		return 0, false, errors.New("swap is not a reserve swap")
	}

	owner, err := common.NewAccountFromPublicKeyString(swapRecord.Owner)
	if err != nil {
		return 0, false, err
	}

	fromMint, err := common.NewAccountFromPublicKeyString(swapRecord.FromMint)
	if err != nil {
		return 0, false, err
	}

	toMint, err := common.NewAccountFromPublicKeyString(swapRecord.ToMint)
	if err != nil {
		return 0, false, err
	}

	if !common.IsCoreMintUsdStableCoin() {
		return 0, false, errors.New("core mint is not a usd stable coin")
	}

	var destinationCurrencyMetadataRecord *currency.MetadataRecord
	if !common.IsCoreMint(toMint) {
		destinationCurrencyMetadataRecord, err = p.data.GetCurrencyMetadata(ctx, swapRecord.ToMint)
		if err != nil {
			return 0, false, err
		}
		if destinationCurrencyMetadataRecord.State != currency.MetadataStateAvailable {
			currencyAccounts, err := common.GetLaunchpadCurrencyAccounts(destinationCurrencyMetadataRecord)
			if err != nil {
				return 0, false, err
			}

			deltaQuarksIntoVault, err := transaction_util.GetDeltaQuarksFromTokenBalances(currencyAccounts.VaultMint, tokenBalances)
			if err != nil {
				return 0, false, err
			}

			var quarksBought uint64
			if common.IsCoreMint(fromMint) {
				// The currency was initialized within the swap transaction, so
				// the delta includes seeding the vault with the full token supply
				if deltaQuarksIntoVault <= 0 {
					return 0, false, errors.New("delta quarks into pool vault is not positive")
				}
				quarksBought = currencycreator.DefaultMintMaxQuarkSupply - uint64(deltaQuarksIntoVault)
			} else {
				// The currency was initialized prior to the swap transaction
				if deltaQuarksIntoVault >= 0 {
					return 0, false, errors.New("delta quarks out of pool vault is not negative")
				}
				quarksBought = uint64(-deltaQuarksIntoVault)
			}

			// This swap is initializing the VM and the funds will be deposited
			// after memory accounts become available. Balances should only be
			// reflected after finalized deposit into a VTA.
			return quarksBought, true, nil
		}
	}

	destinationVmConfig, err := common.GetVmConfigForMint(ctx, p.data, toMint)
	if err != nil {
		return 0, false, err
	}

	ownerDestinationTimelockVault, err := owner.ToTimelockVault(destinationVmConfig)
	if err != nil {
		return 0, false, err
	}

	deltaQuarksIntoOmnibus, err := transaction_util.GetDeltaQuarksFromTokenBalances(destinationVmConfig.Omnibus, tokenBalances)
	if err != nil {
		return 0, false, err
	}
	if deltaQuarksIntoOmnibus <= 0 {
		return 0, false, errors.New("delta quarks into destination vm omnibus is not positive")
	}

	var exchangeCurrency currency_lib.Code
	var nativeAmountWithoutFees float64
	var usdMarketValueWithoutFees float64
	switch swapRecord.FundingSource {
	case swap.FundingSourceSubmitIntent:
		fundingIntentRecord, err := p.data.GetIntent(ctx, swapRecord.FundingId)
		if err != nil {
			return 0, false, err
		}

		if fundingIntentRecord.IntentType != intent.SendPublicPayment {
			return 0, false, errors.New("unexpected intent type")
		}

		exchangeCurrency = fundingIntentRecord.SendPublicPaymentMetadata.ExchangeCurrency
		nativeAmountWithoutFees = fundingIntentRecord.SendPublicPaymentMetadata.NativeAmount
		usdMarketValueWithoutFees = fundingIntentRecord.SendPublicPaymentMetadata.UsdMarketValue

		if common.IsCoreMint(fromMint) {
			// A buy with the core mint is funded for the swap and the buy fee,
			// but only the swap amount bought tokens. The funding payment's own
			// value is left as is, since it reflects what the user spent.
			nativeAmountWithoutFees = currency_util.DiscountValueForBuyFee(nativeAmountWithoutFees, swapRecord.SwapAmount, swapRecord.FeeAmount)
			usdMarketValueWithoutFees = currency_util.DiscountValueForBuyFee(usdMarketValueWithoutFees, swapRecord.SwapAmount, swapRecord.FeeAmount)
		} else {
			coreMintQuarksFromSell := uint64(deltaQuarksIntoOmnibus)
			if !common.IsCoreMint(toMint) {
				destinationCurrencyAccounts, err := common.GetLaunchpadCurrencyAccounts(destinationCurrencyMetadataRecord)
				if err != nil {
					return 0, false, err
				}

				coreMintQuarksIntoDestinationPool, err := transaction_util.GetDeltaQuarksFromTokenBalances(destinationCurrencyAccounts.VaultBase, tokenBalances)
				if err != nil {
					return 0, false, err
				}
				if coreMintQuarksIntoDestinationPool <= 0 {
					return 0, false, errors.New("delta quarks into destination pool base vault is not positive")
				}

				coreMintQuarksFromSell = uint64(coreMintQuarksIntoDestinationPool)
			}

			usdMarketValue, err := currency_util.CalculateUsdMarketValueFromTokenAmount(ctx, p.data, p.exchangeRateStore, p.reserveStore, common.CoreMintAccount, coreMintQuarksFromSell, time.Now())
			if err != nil {
				return 0, false, err
			}

			usdMarketValueWithoutFees = currency_util.GrossUpSellFee(usdMarketValue)

			// A sell settles into the core mint, so its native value is reported in USD. A
			// cross-currency swap keeps the client's original exchange currency and
			// native amount (discounted by the sell fee below); only its USD market
			// value is reconciled to the realized core mint.
			if common.IsCoreMint(toMint) {
				exchangeCurrency = currency_lib.USD
				nativeAmountWithoutFees = usdMarketValueWithoutFees
			}

			// Reconcile the source funding payment's cost basis to the core mint actually
			// realized by the sell.
			fundingIntentRecord.SendPublicPaymentMetadata.UsdMarketValue = usdMarketValueWithoutFees
			err = p.data.SaveIntent(ctx, fundingIntentRecord)
			if err != nil {
				return 0, false, err
			}
		}
	case swap.FundingSourceExternalWallet, swap.FundingSourceCoinbaseOnramp:
		if !common.IsCoreMint(fromMint) {
			return 0, false, errors.New("unexpected source mint")
		}

		exchangeCurrency = currency_lib.USD
		usdMarketValueWithoutFees, err = currency_util.CalculateUsdMarketValueFromTokenAmount(ctx, p.data, p.exchangeRateStore, p.reserveStore, common.CoreMintAccount, swapRecord.SwapAmount, time.Now())
		if err != nil {
			return 0, false, err
		}
		nativeAmountWithoutFees = usdMarketValueWithoutFees
	default:
		return 0, false, errors.New("unsupported funding source")
	}

	nativeAmount := nativeAmountWithoutFees
	usdMarketValue := usdMarketValueWithoutFees
	if !common.IsCoreMint(fromMint) {
		nativeAmount = currency_util.ApplySellFee(nativeAmountWithoutFees)
		usdMarketValue = currency_util.ApplySellFee(usdMarketValueWithoutFees)
	}

	exchangeRate := currency_util.CalculateExchangeRate(toMint, uint64(deltaQuarksIntoOmnibus), nativeAmount)

	err = p.data.ExecuteInTx(ctx, sql.LevelDefault, func(ctx context.Context) error {
		// For transaction history
		intentRecord := &intent.Record{
			IntentId:   getSwapDepositIntentID(swapRecord.TransactionSignature, ownerDestinationTimelockVault),
			IntentType: intent.ExternalDeposit,

			MintAccount: toMint.PublicKey().ToBase58(),

			InitiatorOwnerAccount: owner.PublicKey().ToBase58(),

			ExternalDepositMetadata: &intent.ExternalDepositMetadata{
				DestinationTokenAccount: ownerDestinationTimelockVault.PublicKey().ToBase58(),
				Quantity:                uint64(deltaQuarksIntoOmnibus),
				ExchangeCurrency:        exchangeCurrency,
				ExchangeRate:            exchangeRate,
				NativeAmount:            nativeAmount,
				UsdMarketValue:          usdMarketValue,
				IsSwapBuy:               true,
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
			Signature:   swapRecord.TransactionSignature,
			Destination: ownerDestinationTimelockVault.PublicKey().ToBase58(),
			Amount:      uint64(deltaQuarksIntoOmnibus),

			Slot:              tokenBalances.Slot,
			ConfirmationState: transaction.ConfirmationFinalized,

			CreatedAt: time.Now(),
		}
		return p.data.SaveExternalDeposit(ctx, externalDepositRecord)
	})
	if err != nil {
		return 0, false, err
	}
	return uint64(deltaQuarksIntoOmnibus), false, nil
}

func (p *runtime) getStablecoinSwapQuarksBought(swapRecord *swap.Record, tokenBalances *solana.TransactionTokenBalances) (uint64, bool, error) {
	if swapRecord.Kind != swap.KindStablecoin {
		return 0, false, errors.New("swap is not a stablecoin swap")
	}

	destinationOwner, err := common.NewAccountFromPublicKeyString(swapRecord.DestinationOwner)
	if err != nil {
		return 0, false, errors.Wrap(err, "error parsing destination owner")
	}

	toMint, err := common.NewAccountFromPublicKeyString(swapRecord.ToMint)
	if err != nil {
		return 0, false, errors.Wrap(err, "error parsing destination mint")
	}

	destinationAtaPubkey, err := token.GetAssociatedAccount(destinationOwner.PublicKey().ToBytes(), toMint.PublicKey().ToBytes())
	if err != nil {
		return 0, false, errors.Wrap(err, "error deriving destination ata")
	}

	destinationAta, err := common.NewAccountFromPublicKeyBytes(destinationAtaPubkey)
	if err != nil {
		return 0, false, err
	}

	deltaQuarks, err := transaction_util.GetDeltaQuarksFromTokenBalances(destinationAta, tokenBalances)
	if err != nil {
		return 0, false, errors.Wrap(err, "error getting delta quarks into destination ata")
	}
	if deltaQuarks <= 0 {
		return 0, false, errors.New("delta quarks into destination ata is not positive")
	}

	return uint64(deltaQuarks), false, nil
}

func (p *runtime) notifySwapFinalized(ctx context.Context, swapRecord *swap.Record, isMintInit bool) error {
	if swapRecord.Kind != swap.KindReserve {
		return nil
	}

	if isMintInit {
		return nil
	}

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

	var currencyCode currency_lib.Code
	var nativeAmount float64
	switch swapRecord.FundingSource {
	case swap.FundingSourceSubmitIntent:
		fundingIntentRecord, err := p.data.GetIntent(ctx, swapRecord.FundingId)
		if err != nil {
			return err
		}

		if fundingIntentRecord.IntentType != intent.SendPublicPayment {
			return errors.New("unexpected intent type")
		}

		currencyCode = fundingIntentRecord.SendPublicPaymentMetadata.ExchangeCurrency
		nativeAmount = fundingIntentRecord.SendPublicPaymentMetadata.NativeAmount

		if common.IsCoreMint(fromMint) {
			// A buy with the core mint is funded for the swap and the buy fee,
			// but only the swap amount bought tokens
			nativeAmount = currency_util.DiscountValueForBuyFee(nativeAmount, swapRecord.SwapAmount, swapRecord.FeeAmount)
		}
	case swap.FundingSourceExternalWallet, swap.FundingSourceCoinbaseOnramp:
		if !common.IsCoreMint(fromMint) {
			return errors.New("unexpected source mint")
		}

		if !common.IsCoreMintUsdStableCoin() {
			return errors.New("core mint is not a usd stable coin")
		}

		currencyCode = currency_lib.USD
		nativeAmount = float64(swapRecord.SwapAmount) / float64(common.GetMintQuarksPerUnit(fromMint))
	default:
		return errors.New("unsupported funding source")
	}

	valueReceived := nativeAmount
	if !common.IsCoreMint(fromMint) {
		valueReceived = currency_util.ApplySellFee(valueReceived)
	}

	return p.integration.OnSwapFinalized(ctx, owner, isBuy, targetMint, targetCurrencyMetadataRecord.Name, currencyCode, valueReceived)
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
	err := p.validateSwapState(record, swap.StateCreated, swap.StateFunding, swap.StateFunded)
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
	if intentRecord.SendPublicPaymentMetadata.Quantity < record.SwapAmount+record.FeeAmount {
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

	if deltaQuarks < int64(record.SwapAmount+record.FeeAmount) {
		return false, nil
	}
	return true, nil
}

func (p *runtime) validateCoinbaseOnrampFunding(ctx context.Context, record *swap.Record) (bool, error) {
	if record.FundingSource != swap.FundingSourceCoinbaseOnramp {
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

	order, err := p.coinbaseClient.GetOrder(ctx, record.FundingId)
	if err != nil {
		return false, errors.Wrap(err, "error getting coinbase order")
	}
	if order.TxHash == "" {
		return false, errors.New("coinbase order has no on-chain transaction")
	}

	tokenBalances, err := p.data.GetBlockchainTransactionTokenBalances(ctx, order.TxHash)
	if err != nil {
		return false, errors.Wrap(err, "error getting token balances")
	}

	deltaQuarks, err := transaction_util.GetDeltaQuarksFromTokenBalances(swapAta, tokenBalances)
	if err != nil {
		return false, errors.Wrap(err, "error getting delta quarks from token balances")
	}

	if deltaQuarks < int64(record.SwapAmount+record.FeeAmount) {
		return false, nil
	}
	return true, nil
}

func (p *runtime) ensureSwapDestinationIsInitialized(ctx context.Context, record *swap.Record) error {
	if record.Kind != swap.KindReserve {
		return nil
	}

	toMint, err := common.NewAccountFromPublicKeyString(record.ToMint)
	if err != nil {
		return err
	}

	if !common.IsCoreMint(toMint) {
		destinationCurrencyMetadataRecord, err := p.data.GetCurrencyMetadata(ctx, record.ToMint)
		if err != nil {
			return err
		}
		if destinationCurrencyMetadataRecord.State != currency.MetadataStateAvailable {
			// This swap is initializing the VM and the funds will be deposited
			// after memory accounts become available.
			return nil
		}
	}

	owner, err := common.NewAccountFromPublicKeyString(record.Owner)
	if err != nil {
		return err
	}

	destinationVmConfig, err := common.GetVmConfigForMint(ctx, p.data, toMint)
	if err != nil {
		return err
	}

	destinationTimelockVault, err := owner.ToTimelockVault(destinationVmConfig)
	if err != nil {
		return err
	}

	return vm_util.EnsureVirtualTimelockAccountIsInitialized(ctx, p.data, destinationTimelockVault, true)
}

func (p *runtime) ensureSwapSourceIsInitialized(ctx context.Context, record *swap.Record, waitForInitialization bool) error {
	owner, err := common.NewAccountFromPublicKeyString(record.Owner)
	if err != nil {
		return err
	}

	fromMint, err := common.NewAccountFromPublicKeyString(record.FromMint)
	if err != nil {
		return err
	}

	sourceVmConfig, err := common.GetVmConfigForMint(ctx, p.data, fromMint)
	if err != nil {
		return err
	}

	sourceTimelockVault, err := owner.ToTimelockVault(sourceVmConfig)
	if err != nil {
		return err
	}

	return vm_util.EnsureVirtualTimelockAccountIsInitialized(ctx, p.data, sourceTimelockVault, waitForInitialization)
}

func (p *runtime) validateDestinationCurrencyReadyForSwap(ctx context.Context, swapRecord *swap.Record) (bool, error) {
	if swapRecord.Kind != swap.KindReserve {
		return true, nil
	}

	toMint, err := common.NewAccountFromPublicKeyString(swapRecord.ToMint)
	if err != nil {
		return false, err
	}

	if common.IsCoreMint(toMint) {
		return true, nil
	}

	destinationCurrencyMetadataRecord, err := p.data.GetCurrencyMetadata(ctx, swapRecord.ToMint)
	if err != nil {
		return false, err
	}

	err = p.validateCurrencyMetadataState(destinationCurrencyMetadataRecord, currency.MetadataStateExecutingInitialPurchase, currency.MetadataStateAvailable)
	if err == nil {
		return true, nil
	}
	return false, nil
}

func (p *runtime) updateLiveReserveStateForFinalizedSwap(ctx context.Context, swapRecord *swap.Record, tokenBalances *solana.TransactionTokenBalances) error {
	if swapRecord.Kind != swap.KindReserve {
		return nil
	}

	fromMint, err := common.NewAccountFromPublicKeyString(swapRecord.FromMint)
	if err != nil {
		return err
	}

	toMint, err := common.NewAccountFromPublicKeyString(swapRecord.ToMint)
	if err != nil {
		return err
	}

	var currencyMints []*common.Account
	if !common.IsCoreMint(fromMint) {
		currencyMints = append(currencyMints, fromMint)
	}
	if !common.IsCoreMint(toMint) {
		currencyMints = append(currencyMints, toMint)
	}

	for _, mint := range currencyMints {
		metadataRecord, err := p.data.GetCurrencyMetadata(ctx, mint.PublicKey().ToBase58())
		if err != nil {
			return err
		}

		vaultMint, err := common.NewAccountFromPublicKeyString(metadataRecord.VaultMint)
		if err != nil {
			return err
		}

		postBalance, ok, err := transaction_util.GetPostQuarksFromTokenBalances(vaultMint, tokenBalances)
		if err != nil {
			return err
		}
		if !ok {
			continue
		}

		err = p.reserveStore.PutLiveReserve(ctx, &currency.ReserveRecord{
			Mint:              mint.PublicKey().ToBase58(),
			SupplyFromBonding: currencycreator.DefaultMintMaxQuarkSupply - postBalance,
			Slot:              tokenBalances.Slot,
			Time:              time.Now(),
		})
		if err == currency.ErrStaleReserveState {
			continue
		}
		if err != nil {
			return err
		}
	}

	return nil
}

func getSwapDepositIntentID(signature string, destination *common.Account) string {
	combined := fmt.Sprintf("%s-%s", signature, destination.PublicKey().ToBase58())
	hashed := sha256.Sum256([]byte(combined))
	return base58.Encode(hashed[:])
}
