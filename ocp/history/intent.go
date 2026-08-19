package history

import (
	"context"

	"github.com/pkg/errors"

	transactionpb "github.com/code-payments/ocp-protobuf-api/generated/go/transaction/v1"

	"github.com/code-payments/ocp-server/ocp/common"
	ocp_data "github.com/code-payments/ocp-server/ocp/data"
	"github.com/code-payments/ocp-server/ocp/data/account"
	"github.com/code-payments/ocp-server/ocp/data/action"
	"github.com/code-payments/ocp-server/ocp/data/history"
	"github.com/code-payments/ocp-server/ocp/data/intent"
	"github.com/code-payments/ocp-server/pointer"
)

// BuildRecordsForIntent builds the transaction history records for an intent.
// An intent that isn't a history-visible money movement (eg. account opens, or
// payments whose history is owned by another flow) yields no records.
//
// Records are created in a completed state, since the sequencer guarantees a
// submitted intent is eventually fulfilled. The exception is a gift card
// issuance, which is pending until the gift card is claimed, voided, or
// auto-returned.
//
// withdrawalFeeQuarks is the fee the server charges to create a withdrawal's
// destination account, in core mint quarks as a USD value, which is what the
// charge on the intent was validated against.
func BuildRecordsForIntent(ctx context.Context, data ocp_data.DatabaseData, intentRecord *intent.Record, protoMetadata *transactionpb.Metadata, actionRecords []*action.Record, withdrawalFeeQuarks uint64) ([]*history.Record, error) {
	switch intentRecord.IntentType {
	case intent.SendPublicPayment:
		return buildRecordsForSendPublicPaymentIntent(ctx, data, intentRecord, protoMetadata, actionRecords, withdrawalFeeQuarks)
	case intent.ReceivePaymentsPublicly:
		return buildRecordsForReceivePaymentsPubliclyIntent(ctx, data, intentRecord)
	default:
		// todo: Support remaining money movement intent types
		return nil, nil
	}
}

// BuildRecordForExternalDeposit builds the transaction history record for an
// external deposit intent. External deposit intents are created by workers
// rather than SubmitIntent, so they don't flow through BuildRecordsForIntent,
// and the transaction signature is provided by the observing worker because
// the intent doesn't store it.
//
// The record is referenced by the deposit's transaction signature, which maps
// it directly to the deposit record and the transaction on chain.
func BuildRecordForExternalDeposit(intentRecord *intent.Record, signature string) *history.Record {
	metadata := intentRecord.ExternalDepositMetadata

	// Deposits initiated by a swap (buys and refunds) are not external money
	// movements. Their history is owned by the swap flow.
	//
	// todo: Support the swap flow
	if metadata.IsSwapBuy || metadata.IsReturned {
		return nil
	}

	// The funds are already on chain by the time the deposit is observed, so
	// the record is immediately complete
	return &history.Record{
		ReferenceId:      signature,
		ReferenceType:    history.SignatureReference,
		Type:             history.Deposited,
		OwnerAccount:     intentRecord.InitiatorOwnerAccount,
		ExchangeCurrency: metadata.ExchangeCurrency,
		NativeAmount:     metadata.NativeAmount,
		MintAccount:      intentRecord.MintAccount,
		Quantity:         metadata.Quantity,
		AppMetadata:      intentRecord.AppMetadata,
		State:            history.StateCompleted,
		CreatedAt:        intentRecord.CreatedAt,
	}
}

// ApplyStateTransitionsForIntent applies the state transitions an intent
// triggers on other flows' history records.
func ApplyStateTransitionsForIntent(ctx context.Context, data ocp_data.DatabaseData, intentRecord *intent.Record) error {
	switch intentRecord.IntentType {
	case intent.ReceivePaymentsPublicly:
		metadata := intentRecord.ReceivePaymentsPubliclyMetadata

		// A gift card claim completes the issuer's IndirectlySent record
		if metadata.IsIndirectSend && !metadata.IsReturned && !metadata.IsIssuerVoidingGiftCard {
			return MarkGiftCardIssuanceAsClaimed(ctx, data, metadata.Source)
		}
	}
	return nil
}

func buildRecordsForSendPublicPaymentIntent(ctx context.Context, data ocp_data.DatabaseData, intentRecord *intent.Record, protoMetadata *transactionpb.Metadata, actionRecords []*action.Record, withdrawalFeeQuarks uint64) ([]*history.Record, error) {
	metadata := intentRecord.SendPublicPaymentMetadata

	// Swap funding payments, including withdrawals executed as a stablecoin
	// swap, are not payments of their own. Their history is owned by the swap
	// flow.
	//
	// todo: Support the stablecoin swap flow
	if metadata.IsSwapSell {
		return nil, nil
	}

	if metadata.IsWithdrawal {
		return buildRecordsForWithdrawalIntent(ctx, data, intentRecord, protoMetadata, actionRecords, withdrawalFeeQuarks)
	}

	// A gift card issuance is an indirect send to a not-yet-known counterparty.
	// The record is pending until the gift card is claimed, voided, or
	// auto-returned.
	if metadata.IsIndirectSend {
		return []*history.Record{{
			ReferenceId:      intentRecord.IntentId,
			ReferenceType:    history.IntentReference,
			Type:             history.IndirectlySent,
			OwnerAccount:     intentRecord.InitiatorOwnerAccount,
			ExchangeCurrency: metadata.ExchangeCurrency,
			NativeAmount:     metadata.NativeAmount,
			MintAccount:      intentRecord.MintAccount,
			Quantity:         metadata.Quantity,
			GiftCardVault:    pointer.String(metadata.DestinationTokenAccount),
			AppMetadata:      intentRecord.AppMetadata,
			State:            history.StatePending,
			CreatedAt:        intentRecord.CreatedAt,
		}}, nil
	}

	sent := &history.Record{
		ReferenceId:      intentRecord.IntentId,
		ReferenceType:    history.IntentReference,
		Type:             history.DirectlySent,
		OwnerAccount:     intentRecord.InitiatorOwnerAccount,
		ExchangeCurrency: metadata.ExchangeCurrency,
		NativeAmount:     metadata.NativeAmount,
		MintAccount:      intentRecord.MintAccount,
		Quantity:         metadata.Quantity,
		AppMetadata:      intentRecord.AppMetadata,
		State:            history.StateCompleted,
		CreatedAt:        intentRecord.CreatedAt,
	}
	if len(metadata.DestinationOwnerAccount) > 0 {
		sent.CounterpartyOwnerAccount = pointer.String(metadata.DestinationOwnerAccount)
	}

	// A record is one owner's view of one event, so a payment to self gets a
	// single record, and one without a resolvable destination owner has no
	// receiving side to record.
	if len(metadata.DestinationOwnerAccount) == 0 || metadata.DestinationOwnerAccount == intentRecord.InitiatorOwnerAccount {
		return []*history.Record{sent}, nil
	}

	received := &history.Record{
		ReferenceId:              intentRecord.IntentId,
		ReferenceType:            history.IntentReference,
		Type:                     history.DirectlyReceived,
		OwnerAccount:             metadata.DestinationOwnerAccount,
		CounterpartyOwnerAccount: pointer.String(intentRecord.InitiatorOwnerAccount),
		ExchangeCurrency:         metadata.ExchangeCurrency,
		NativeAmount:             metadata.NativeAmount,
		MintAccount:              intentRecord.MintAccount,
		Quantity:                 metadata.Quantity,
		AppMetadata:              intentRecord.AppMetadata,
		State:                    history.StateCompleted,
		CreatedAt:                intentRecord.CreatedAt,
	}

	return []*history.Record{sent, received}, nil
}

func buildRecordsForWithdrawalIntent(ctx context.Context, data ocp_data.DatabaseData, intentRecord *intent.Record, protoMetadata *transactionpb.Metadata, actionRecords []*action.Record, withdrawalFeeQuarks uint64) ([]*history.Record, error) {
	metadata := intentRecord.SendPublicPaymentMetadata

	// The withdrawn quantity and value are gross, with the destination
	// receiving the quantity less any fees, so fees are broken out. The fee is
	// a fixed USD value, so its native value is that USD value at the client's
	// verified fiat exchange rate. The value comes from the fee the server
	// charges rather than the quarks the action moved, because those quarks are
	// only that USD value when the intent's mint is the core mint: a launchpad
	// currency pays the same fee in its own quarks.
	var fees []history.Fee
	for _, actionRecord := range actionRecords {
		if actionRecord.FeeType == nil {
			continue
		}

		if *actionRecord.FeeType != transactionpb.FeePaymentAction_CREATE_ON_SEND_WITHDRAWAL {
			return nil, errors.Errorf("unhandled fee type %s", *actionRecord.FeeType)
		}

		clientExchangeData := protoMetadata.GetSendPublicPayment().GetClientExchangeData()
		if clientExchangeData == nil {
			return nil, errors.New("client exchange data is required for fee payments")
		}

		feeUsdValue := float64(withdrawalFeeQuarks) / float64(common.CoreMintQuarksPerUnit)
		fees = append(fees, history.Fee{
			Type:         history.WithdrawalAccountCreationFee,
			NativeAmount: clientExchangeData.CoreMintFiatExchangeRate.ExchangeRate.ExchangeRate * feeUsdValue,
		})
	}

	withdrawn := &history.Record{
		ReferenceId:      intentRecord.IntentId,
		ReferenceType:    history.IntentReference,
		Type:             history.Withdrawn,
		OwnerAccount:     intentRecord.InitiatorOwnerAccount,
		ExchangeCurrency: metadata.ExchangeCurrency,
		NativeAmount:     metadata.NativeAmount,
		Fees:             fees,
		MintAccount:      intentRecord.MintAccount,
		Quantity:         metadata.Quantity,
		AppMetadata:      intentRecord.AppMetadata,
		State:            history.StateCompleted,
		CreatedAt:        intentRecord.CreatedAt,
	}
	if len(metadata.DestinationOwnerAccount) > 0 {
		withdrawn.CounterpartyOwnerAccount = pointer.String(metadata.DestinationOwnerAccount)
	}

	// A Code->Code withdrawal lands in another owner's primary account, which
	// that owner sees as a deposit
	destinationAccountInfoRecord, err := data.GetAccountInfoByTokenAddress(ctx, metadata.DestinationTokenAccount)
	if err == account.ErrAccountInfoNotFound {
		return []*history.Record{withdrawn}, nil
	} else if err != nil {
		return nil, err
	}
	if destinationAccountInfoRecord.OwnerAccount == intentRecord.InitiatorOwnerAccount {
		return []*history.Record{withdrawn}, nil
	}

	deposited := &history.Record{
		ReferenceId:              intentRecord.IntentId,
		ReferenceType:            history.IntentReference,
		Type:                     history.Deposited,
		OwnerAccount:             destinationAccountInfoRecord.OwnerAccount,
		CounterpartyOwnerAccount: pointer.String(intentRecord.InitiatorOwnerAccount),
		ExchangeCurrency:         metadata.ExchangeCurrency,
		NativeAmount:             metadata.NativeAmount,
		MintAccount:              intentRecord.MintAccount,
		Quantity:                 metadata.Quantity,
		AppMetadata:              intentRecord.AppMetadata,
		State:                    history.StateCompleted,
		CreatedAt:                intentRecord.CreatedAt,
	}

	return []*history.Record{withdrawn, deposited}, nil
}

func buildRecordsForReceivePaymentsPubliclyIntent(ctx context.Context, data ocp_data.DatabaseData, intentRecord *intent.Record) ([]*history.Record, error) {
	metadata := intentRecord.ReceivePaymentsPubliclyMetadata

	// Voids and auto-returns are server-initiated intents that are reflected
	// as state transitions on the issuer's IndirectlySent record.
	if !metadata.IsIndirectSend || metadata.IsReturned || metadata.IsIssuerVoidingGiftCard {
		return nil, nil
	}

	// The issuer is the claim's counterparty, and is only discoverable through
	// the intent that issued the gift card.
	giftCardIssuedIntentRecord, err := data.GetOriginalGiftCardIssuedIntent(ctx, metadata.Source)
	if err != nil {
		return nil, err
	}

	return []*history.Record{{
		ReferenceId:              intentRecord.IntentId,
		ReferenceType:            history.IntentReference,
		Type:                     history.IndirectlyReceived,
		OwnerAccount:             intentRecord.InitiatorOwnerAccount,
		CounterpartyOwnerAccount: pointer.String(giftCardIssuedIntentRecord.InitiatorOwnerAccount),
		ExchangeCurrency:         metadata.OriginalExchangeCurrency,
		NativeAmount:             metadata.OriginalNativeAmount,
		MintAccount:              intentRecord.MintAccount,
		Quantity:                 metadata.Quantity,
		GiftCardVault:            pointer.String(metadata.Source),
		AppMetadata:              intentRecord.AppMetadata,
		State:                    history.StateCompleted,
		CreatedAt:                intentRecord.CreatedAt,
	}}, nil
}
