package swap

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"fmt"
	"math/big"
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
	"github.com/code-payments/ocp-server/solana/currencycreator"
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
	destinationCurrencyMetadataRecord, err := p.data.GetCurrencyMetadata(ctx, swapRecord.ToMint)
	if err != nil {
		return err
	}
	err = p.validateCurrencyMetadataState(destinationCurrencyMetadataRecord, currency.MetadataStateExecutingInitialPurchase, currency.MetadataStateAvailable)
	if err != nil {
		return err
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

		if destinationCurrencyMetadataRecord.State == currency.MetadataStateExecutingInitialPurchase {
			destinationCurrencyMetadataRecord.State = currency.MetadataStateCompletingInitialization
			err = p.data.SaveCurrencyMetadata(ctx, destinationCurrencyMetadataRecord)
			if err != nil {
				return err
			}
		}

		swapRecord.TransactionBlob = nil
		swapRecord.State = swap.StateFinalized
		return p.data.SaveSwap(ctx, swapRecord)
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
		case swap.StateCreated, swap.StateFunding, swap.StateFunded:
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

func (p *runtime) maybeUpdateBalancesForFinalizedSwap(ctx context.Context, swapRecord *swap.Record, tokenBalances *solana.TransactionTokenBalances) (uint64, bool, error) {
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
	if !common.IsCoreMint(fromMint) && !common.IsCoreMint(toMint) {
		return 0, false, errors.New("core mint must be involved in swap")
	}

	destinationCurrencyMetadataRecord, err := p.data.GetCurrencyMetadata(ctx, swapRecord.ToMint)
	if err != nil {
		return 0, false, err
	}
	if destinationCurrencyMetadataRecord.State != currency.MetadataStateAvailable {
		currencyAccounts, err := common.GetLaunchpadCurrencyAccounts(destinationCurrencyMetadataRecord)
		if err != nil {
			return 0, false, err
		}

		deltaQuarksOutOfVault, err := transaction_util.GetDeltaQuarksFromTokenBalances(currencyAccounts.VaultMint, tokenBalances)
		if err != nil {
			return 0, false, nil
		}

		if deltaQuarksOutOfVault >= 0 {
			return 0, false, errors.New("delta quarks into destination vm omnibus is not negative")
		}

		// This swap is initializing the VM and the funds will be deposited
		// after memory accounts become available. Balances should only be
		// reflected after finalized deposit into a VTA.
		return uint64(-deltaQuarksOutOfVault), true, nil
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

		if common.IsCoreMint(toMint) {
			usdMarketValue, err := currency_util.CalculateUsdMarketValueFromTokenAmount(ctx, p.data, common.CoreMintAccount, uint64(deltaQuarksIntoOmnibus), time.Now())
			if err != nil {
				return 0, false, err
			}

			usdMarketValueWithoutFees, _ = new(big.Float).Quo(
				big.NewFloat(usdMarketValue).SetPrec(128),
				big.NewFloat(0.99).SetPrec(128),
			).Float64()

			exchangeCurrency = currency_lib.USD
			nativeAmountWithoutFees = usdMarketValueWithoutFees

			// Update funding intent record with actual USD market value for
			// consistent USD cost basis
			fundingIntentRecord.SendPublicPaymentMetadata.UsdMarketValue = usdMarketValueWithoutFees
			err = p.data.SaveIntent(ctx, fundingIntentRecord)
			if err != nil {
				return 0, false, err
			}
		}
	case swap.FundingSourceExternalWallet:
		if !common.IsCoreMint(fromMint) {
			return 0, false, errors.New("unexpected source mint")
		}

		exchangeCurrency = currency_lib.USD
		usdMarketValueWithoutFees, err = currency_util.CalculateUsdMarketValueFromTokenAmount(ctx, p.data, common.CoreMintAccount, swapRecord.Amount, time.Now())
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
		nativeAmount, _ = new(big.Float).Mul(
			big.NewFloat(0.99).SetPrec(128),
			big.NewFloat(nativeAmountWithoutFees).SetPrec(128),
		).Float64()
		usdMarketValue, _ = new(big.Float).Mul(
			big.NewFloat(0.99).SetPrec(128),
			big.NewFloat(usdMarketValueWithoutFees).SetPrec(128),
		).Float64()
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

func (p *runtime) notifySwapFinalized(ctx context.Context, swapRecord *swap.Record, isMintInit bool) error {
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
	case swap.FundingSourceExternalWallet:
		if !common.IsCoreMint(fromMint) {
			return errors.New("unexpected source mint")
		}

		if !common.IsCoreMintUsdStableCoin() {
			return errors.New("core mint is not a usd stable coin")
		}

		currencyCode = currency_lib.USD
		nativeAmount = float64(swapRecord.Amount) / float64(common.GetMintQuarksPerUnit(fromMint))
	default:
		return errors.New("unsupported funding source")
	}

	valueReceived := nativeAmount
	if !common.IsCoreMint(fromMint) {
		valueReceived, _ = new(big.Float).Mul(
			big.NewFloat(0.99).SetPrec(128),
			big.NewFloat(valueReceived).SetPrec(128),
		).Float64()
	}

	return p.integration.OnSwapFinalized(ctx, owner, isBuy, targetMint, targetCurrencyMetadataRecord.Name, currencyCode, valueReceived, isMintInit)
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

func (p *runtime) ensureSwapDestinationIsInitialized(ctx context.Context, record *swap.Record) error {
	toMint, err := common.NewAccountFromPublicKeyString(record.ToMint)
	if err != nil {
		return err
	}

	destinationCurrencyMetadataRecord, err := p.data.GetCurrencyMetadata(ctx, record.ToMint)
	if err != nil {
		return err
	}
	if destinationCurrencyMetadataRecord.State != currency.MetadataStateAvailable {
		// This swap is initializing the VM and the funds will be deposited
		// after memory accounts become available.
		return nil
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

func (p *runtime) updateLiveReserveStateForFinalizedSwap(ctx context.Context, swapRecord *swap.Record, tokenBalances *solana.TransactionTokenBalances) error {
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

		err = p.data.PutLiveCurrencyReserve(ctx, &currency.ReserveRecord{
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
