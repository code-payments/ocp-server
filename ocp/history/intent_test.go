package history

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	commonpb "github.com/code-payments/ocp-protobuf-api/generated/go/common/v1"
	currencypb "github.com/code-payments/ocp-protobuf-api/generated/go/currency/v1"
	transactionpb "github.com/code-payments/ocp-protobuf-api/generated/go/transaction/v1"

	"github.com/code-payments/ocp-server/ocp/common"
	ocp_data "github.com/code-payments/ocp-server/ocp/data"
	"github.com/code-payments/ocp-server/ocp/data/account"
	"github.com/code-payments/ocp-server/ocp/data/action"
	"github.com/code-payments/ocp-server/ocp/data/history"
	"github.com/code-payments/ocp-server/ocp/data/intent"
	"github.com/code-payments/ocp-server/pointer"
	"github.com/code-payments/ocp-server/solana/currencycreator"
)

// testWithdrawalFeeQuarks mirrors the transaction server's default withdrawal
// fee, in core mint quarks as a USD value: $0.50.
var testWithdrawalFeeQuarks = common.CoreMintQuarksPerUnit / 2

func TestBuildRecordsForIntent_DirectPayment(t *testing.T) {
	ctx := context.Background()
	data := ocp_data.NewTestDataProvider()

	intentRecord := newSendPublicPaymentIntentRecord()

	records, err := BuildRecordsForIntent(ctx, data, intentRecord, nil, nil, testWithdrawalFeeQuarks)
	require.NoError(t, err)
	require.Len(t, records, 2)

	sent := records[0]
	assert.Equal(t, intentRecord.IntentId, sent.ReferenceId)
	assert.Equal(t, history.DirectlySent, sent.Type)
	assert.Equal(t, intentRecord.InitiatorOwnerAccount, sent.OwnerAccount)
	require.NotNil(t, sent.CounterpartyOwnerAccount)
	assert.Equal(t, intentRecord.SendPublicPaymentMetadata.DestinationOwnerAccount, *sent.CounterpartyOwnerAccount)

	received := records[1]
	assert.Equal(t, intentRecord.IntentId, received.ReferenceId)
	assert.Equal(t, history.DirectlyReceived, received.Type)
	assert.Equal(t, intentRecord.SendPublicPaymentMetadata.DestinationOwnerAccount, received.OwnerAccount)
	require.NotNil(t, received.CounterpartyOwnerAccount)
	assert.Equal(t, intentRecord.InitiatorOwnerAccount, *received.CounterpartyOwnerAccount)

	for _, record := range records {
		assert.Equal(t, intentRecord.SendPublicPaymentMetadata.ExchangeCurrency, record.ExchangeCurrency)
		assert.Equal(t, intentRecord.SendPublicPaymentMetadata.NativeAmount, record.NativeAmount)
		assert.Equal(t, intentRecord.MintAccount, record.MintAccount)
		assert.Equal(t, intentRecord.SendPublicPaymentMetadata.Quantity, record.Quantity)
		assert.Equal(t, intentRecord.AppMetadata, record.AppMetadata)
		assert.Nil(t, record.GiftCardVault)
		assert.Equal(t, history.StateCompleted, record.State)
		assert.Equal(t, intentRecord.CreatedAt, record.CreatedAt)
		assert.NoError(t, record.Validate())
	}
}

func TestBuildRecordsForIntent_SelfPayment(t *testing.T) {
	ctx := context.Background()
	data := ocp_data.NewTestDataProvider()

	intentRecord := newSendPublicPaymentIntentRecord()
	intentRecord.SendPublicPaymentMetadata.DestinationOwnerAccount = intentRecord.InitiatorOwnerAccount

	records, err := BuildRecordsForIntent(ctx, data, intentRecord, nil, nil, testWithdrawalFeeQuarks)
	require.NoError(t, err)
	require.Len(t, records, 1)
	assert.Equal(t, history.DirectlySent, records[0].Type)
	assert.Equal(t, intentRecord.InitiatorOwnerAccount, records[0].OwnerAccount)
	assert.NoError(t, records[0].Validate())
}

func TestBuildRecordsForIntent_GiftCardIssuance(t *testing.T) {
	ctx := context.Background()
	data := ocp_data.NewTestDataProvider()

	intentRecord := newGiftCardIssuedIntentRecord()

	records, err := BuildRecordsForIntent(ctx, data, intentRecord, nil, nil, testWithdrawalFeeQuarks)
	require.NoError(t, err)
	require.Len(t, records, 1)

	issued := records[0]
	assert.Equal(t, intentRecord.IntentId, issued.ReferenceId)
	assert.Equal(t, history.IndirectlySent, issued.Type)
	assert.Equal(t, intentRecord.InitiatorOwnerAccount, issued.OwnerAccount)
	assert.Nil(t, issued.CounterpartyOwnerAccount)
	assert.Equal(t, intentRecord.SendPublicPaymentMetadata.ExchangeCurrency, issued.ExchangeCurrency)
	assert.Equal(t, intentRecord.SendPublicPaymentMetadata.NativeAmount, issued.NativeAmount)
	assert.Equal(t, intentRecord.MintAccount, issued.MintAccount)
	assert.Equal(t, intentRecord.SendPublicPaymentMetadata.Quantity, issued.Quantity)
	require.NotNil(t, issued.GiftCardVault)
	assert.Equal(t, intentRecord.SendPublicPaymentMetadata.DestinationTokenAccount, *issued.GiftCardVault)
	assert.Equal(t, history.StatePending, issued.State)
	assert.NoError(t, issued.Validate())
}

func TestBuildRecordsForIntent_GiftCardClaim(t *testing.T) {
	ctx := context.Background()
	data := ocp_data.NewTestDataProvider()

	issuedIntentRecord := newGiftCardIssuedIntentRecord()
	require.NoError(t, data.SaveIntent(ctx, issuedIntentRecord))

	intentRecord := newGiftCardClaimedIntentRecord(issuedIntentRecord)

	records, err := BuildRecordsForIntent(ctx, data, intentRecord, nil, nil, testWithdrawalFeeQuarks)
	require.NoError(t, err)
	require.Len(t, records, 1)

	claimed := records[0]
	assert.Equal(t, intentRecord.IntentId, claimed.ReferenceId)
	assert.Equal(t, history.IndirectlyReceived, claimed.Type)
	assert.Equal(t, intentRecord.InitiatorOwnerAccount, claimed.OwnerAccount)
	require.NotNil(t, claimed.CounterpartyOwnerAccount)
	assert.Equal(t, issuedIntentRecord.InitiatorOwnerAccount, *claimed.CounterpartyOwnerAccount)
	assert.Equal(t, intentRecord.ReceivePaymentsPubliclyMetadata.OriginalExchangeCurrency, claimed.ExchangeCurrency)
	assert.Equal(t, intentRecord.ReceivePaymentsPubliclyMetadata.OriginalNativeAmount, claimed.NativeAmount)
	assert.Equal(t, intentRecord.MintAccount, claimed.MintAccount)
	assert.Equal(t, intentRecord.ReceivePaymentsPubliclyMetadata.Quantity, claimed.Quantity)
	require.NotNil(t, claimed.GiftCardVault)
	assert.Equal(t, intentRecord.ReceivePaymentsPubliclyMetadata.Source, *claimed.GiftCardVault)
	assert.Equal(t, history.StateCompleted, claimed.State)
	assert.NoError(t, claimed.Validate())
}

func TestBuildRecordsForIntent_GiftCardVoidAndAutoReturn(t *testing.T) {
	ctx := context.Background()
	data := ocp_data.NewTestDataProvider()

	issuedIntentRecord := newGiftCardIssuedIntentRecord()

	for _, mutate := range []func(*intent.Record){
		func(r *intent.Record) { r.ReceivePaymentsPubliclyMetadata.IsReturned = true },
		func(r *intent.Record) { r.ReceivePaymentsPubliclyMetadata.IsIssuerVoidingGiftCard = true },
	} {
		intentRecord := newGiftCardClaimedIntentRecord(issuedIntentRecord)
		mutate(intentRecord)

		records, err := BuildRecordsForIntent(ctx, data, intentRecord, nil, nil, testWithdrawalFeeQuarks)
		require.NoError(t, err)
		assert.Empty(t, records)
	}
}

func TestBuildRecordForExternalDeposit(t *testing.T) {
	intentRecord := newExternalDepositIntentRecord()

	record := BuildRecordForExternalDeposit(intentRecord, "deposit_signature")
	require.NotNil(t, record)

	assert.Equal(t, "deposit_signature", record.ReferenceId)
	assert.Equal(t, history.Deposited, record.Type)
	assert.Equal(t, intentRecord.InitiatorOwnerAccount, record.OwnerAccount)
	assert.Nil(t, record.CounterpartyOwnerAccount)
	assert.Equal(t, intentRecord.ExternalDepositMetadata.ExchangeCurrency, record.ExchangeCurrency)
	assert.Equal(t, intentRecord.ExternalDepositMetadata.NativeAmount, record.NativeAmount)
	assert.Equal(t, intentRecord.MintAccount, record.MintAccount)
	assert.Equal(t, intentRecord.ExternalDepositMetadata.Quantity, record.Quantity)
	assert.Nil(t, record.GiftCardVault)
	assert.Equal(t, history.StateCompleted, record.State)
	assert.Equal(t, intentRecord.CreatedAt, record.CreatedAt)
	assert.NoError(t, record.Validate())
}

func TestBuildRecordForExternalDeposit_DepositsOwnedBySwapFlow(t *testing.T) {
	for _, mutate := range []func(*intent.Record){
		func(r *intent.Record) { r.ExternalDepositMetadata.IsSwapBuy = true },
		func(r *intent.Record) { r.ExternalDepositMetadata.IsReturned = true },
	} {
		intentRecord := newExternalDepositIntentRecord()
		mutate(intentRecord)
		assert.Nil(t, BuildRecordForExternalDeposit(intentRecord, "deposit_signature"))
	}
}

func TestBuildRecordsForIntent_ExternalWithdrawal(t *testing.T) {
	ctx := context.Background()
	data := ocp_data.NewTestDataProvider()

	intentRecord := newSendPublicPaymentIntentRecord()
	intentRecord.SendPublicPaymentMetadata.IsWithdrawal = true

	records, err := BuildRecordsForIntent(ctx, data, intentRecord, nil, nil, testWithdrawalFeeQuarks)
	require.NoError(t, err)
	require.Len(t, records, 1)

	withdrawn := records[0]
	assert.Equal(t, intentRecord.IntentId, withdrawn.ReferenceId)
	assert.Equal(t, history.Withdrawn, withdrawn.Type)
	assert.Equal(t, intentRecord.InitiatorOwnerAccount, withdrawn.OwnerAccount)
	require.NotNil(t, withdrawn.CounterpartyOwnerAccount)
	assert.Equal(t, intentRecord.SendPublicPaymentMetadata.DestinationOwnerAccount, *withdrawn.CounterpartyOwnerAccount)
	assert.Equal(t, intentRecord.SendPublicPaymentMetadata.ExchangeCurrency, withdrawn.ExchangeCurrency)
	assert.Equal(t, intentRecord.SendPublicPaymentMetadata.NativeAmount, withdrawn.NativeAmount)
	assert.Empty(t, withdrawn.Fees)
	assert.Equal(t, intentRecord.MintAccount, withdrawn.MintAccount)
	assert.Equal(t, intentRecord.SendPublicPaymentMetadata.Quantity, withdrawn.Quantity)
	assert.Equal(t, history.StateCompleted, withdrawn.State)
	assert.NoError(t, withdrawn.Validate())
}

func TestBuildRecordsForIntent_ExternalWithdrawalWithFee(t *testing.T) {
	ctx := context.Background()
	data := ocp_data.NewTestDataProvider()

	intentRecord := newSendPublicPaymentIntentRecord()
	intentRecord.SendPublicPaymentMetadata.IsWithdrawal = true

	// The $0.50 fee at a verified 2.0 fiat exchange rate is worth 1.00 EUR
	protoMetadata := &transactionpb.Metadata{
		Type: &transactionpb.Metadata_SendPublicPayment{
			SendPublicPayment: &transactionpb.SendPublicPaymentMetadata{
				ExchangeData: &transactionpb.SendPublicPaymentMetadata_ClientExchangeData{
					ClientExchangeData: &transactionpb.VerifiedExchangeData{
						CoreMintFiatExchangeRate: &currencypb.VerifiedCoreMintFiatExchangeRate{
							ExchangeRate: &currencypb.CoreMintFiatExchangeRate{
								CurrencyCode: "eur",
								ExchangeRate: 2.0,
							},
						},
					},
				},
			},
		},
	}

	// The fee is the same $0.50 whatever mint it was charged in, so the quarks
	// the action moved are only that USD value for the core mint. A launchpad
	// currency pays the same fee in its own quarks, at its own price.
	for _, chargedFeeQuarks := range []uint64{
		common.CoreMintQuarksPerUnit / 2,
		5_000 * currencycreator.DefaultMintQuarksPerUnit,
	} {
		feeType := transactionpb.FeePaymentAction_CREATE_ON_SEND_WITHDRAWAL
		actionRecords := []*action.Record{
			{
				Intent:     intentRecord.IntentId,
				IntentType: intentRecord.IntentType,
				ActionId:   0,
				ActionType: action.NoPrivacyTransfer,
				Source:     "source_token_account",
				Quantity:   pointer.Uint64(intentRecord.SendPublicPaymentMetadata.Quantity),
			},
			{
				Intent:     intentRecord.IntentId,
				IntentType: intentRecord.IntentType,
				ActionId:   1,
				ActionType: action.NoPrivacyTransfer,
				Source:     "source_token_account",
				Quantity:   pointer.Uint64(chargedFeeQuarks),
				FeeType:    &feeType,
			},
		}

		records, err := BuildRecordsForIntent(ctx, data, intentRecord, protoMetadata, actionRecords, testWithdrawalFeeQuarks)
		require.NoError(t, err)
		require.Len(t, records, 1)

		require.Len(t, records[0].Fees, 1)
		assert.Equal(t, history.WithdrawalAccountCreationFee, records[0].Fees[0].Type)
		assert.InDelta(t, 1.0, records[0].Fees[0].NativeAmount, 0.0001)
		assert.NoError(t, records[0].Validate())

		// The verified exchange data is required when a fee is paid
		_, err = BuildRecordsForIntent(ctx, data, intentRecord, nil, actionRecords, testWithdrawalFeeQuarks)
		assert.Error(t, err)
	}
}

func TestBuildRecordsForIntent_ExternalWithdrawalWithUnhandledFee(t *testing.T) {
	ctx := context.Background()
	data := ocp_data.NewTestDataProvider()

	intentRecord := newSendPublicPaymentIntentRecord()
	intentRecord.SendPublicPaymentMetadata.IsWithdrawal = true

	// A fee this package doesn't price would otherwise be dropped, leaving the
	// record understating what the withdrawal cost
	feeType := transactionpb.FeePaymentAction_UNKNOWN
	actionRecords := []*action.Record{
		{
			Intent:     intentRecord.IntentId,
			IntentType: intentRecord.IntentType,
			ActionId:   0,
			ActionType: action.NoPrivacyTransfer,
			Source:     "source_token_account",
			Quantity:   pointer.Uint64(testWithdrawalFeeQuarks),
			FeeType:    &feeType,
		},
	}

	_, err := BuildRecordsForIntent(ctx, data, intentRecord, nil, actionRecords, testWithdrawalFeeQuarks)
	assert.Error(t, err)
}

func TestBuildRecordsForIntent_CodeToCodeWithdrawal(t *testing.T) {
	ctx := context.Background()
	data := ocp_data.NewTestDataProvider()

	intentRecord := newSendPublicPaymentIntentRecord()
	intentRecord.SendPublicPaymentMetadata.IsWithdrawal = true

	require.NoError(t, data.CreateAccountInfo(ctx, &account.Record{
		OwnerAccount:     intentRecord.SendPublicPaymentMetadata.DestinationOwnerAccount,
		AuthorityAccount: intentRecord.SendPublicPaymentMetadata.DestinationOwnerAccount,
		TokenAccount:     intentRecord.SendPublicPaymentMetadata.DestinationTokenAccount,
		MintAccount:      intentRecord.MintAccount,
		AccountType:      commonpb.AccountType_PRIMARY,
	}))

	records, err := BuildRecordsForIntent(ctx, data, intentRecord, nil, nil, testWithdrawalFeeQuarks)
	require.NoError(t, err)
	require.Len(t, records, 2)

	assert.Equal(t, history.Withdrawn, records[0].Type)

	// The receiving side of a Code->Code withdrawal is a deposit
	deposited := records[1]
	assert.Equal(t, intentRecord.IntentId, deposited.ReferenceId)
	assert.Equal(t, history.Deposited, deposited.Type)
	assert.Equal(t, intentRecord.SendPublicPaymentMetadata.DestinationOwnerAccount, deposited.OwnerAccount)
	require.NotNil(t, deposited.CounterpartyOwnerAccount)
	assert.Equal(t, intentRecord.InitiatorOwnerAccount, *deposited.CounterpartyOwnerAccount)
	assert.Equal(t, intentRecord.SendPublicPaymentMetadata.Quantity, deposited.Quantity)
	assert.Equal(t, history.StateCompleted, deposited.State)
	assert.NoError(t, deposited.Validate())
}

func TestBuildRecordsForIntent_PaymentsOwnedByOtherFlows(t *testing.T) {
	ctx := context.Background()
	data := ocp_data.NewTestDataProvider()

	for _, mutate := range []func(*intent.Record){
		func(r *intent.Record) { r.SendPublicPaymentMetadata.IsSwapSell = true },
		func(r *intent.Record) {
			r.SendPublicPaymentMetadata.IsWithdrawal = true
			r.SendPublicPaymentMetadata.IsSwapSell = true
		},
	} {
		intentRecord := newSendPublicPaymentIntentRecord()
		mutate(intentRecord)

		records, err := BuildRecordsForIntent(ctx, data, intentRecord, nil, nil, testWithdrawalFeeQuarks)
		require.NoError(t, err)
		assert.Empty(t, records)
	}
}

func TestBuildRecordsForIntent_UnsupportedIntentTypes(t *testing.T) {
	ctx := context.Background()
	data := ocp_data.NewTestDataProvider()

	intentRecord := &intent.Record{
		IntentId:              "open_accounts_intent",
		IntentType:            intent.OpenAccounts,
		MintAccount:           "mint",
		InitiatorOwnerAccount: "owner",
		OpenAccountsMetadata:  &intent.OpenAccountsMetadata{},
		State:                 intent.StatePending,
		CreatedAt:             time.Now(),
	}

	records, err := BuildRecordsForIntent(ctx, data, intentRecord, nil, nil, testWithdrawalFeeQuarks)
	require.NoError(t, err)
	assert.Empty(t, records)
}

func TestApplyStateTransitionsForIntent_GiftCardClaim(t *testing.T) {
	ctx := context.Background()
	data := ocp_data.NewTestDataProvider()

	issuedIntentRecord := newGiftCardIssuedIntentRecord()
	require.NoError(t, data.SaveIntent(ctx, issuedIntentRecord))
	saveRecordsForIntent(t, data, issuedIntentRecord)

	claimIntentRecord := newGiftCardClaimedIntentRecord(issuedIntentRecord)
	saveRecordsForIntent(t, data, claimIntentRecord)

	require.NoError(t, ApplyStateTransitionsForIntent(ctx, data, claimIntentRecord))

	issuanceRecords, err := data.GetAllTransactionHistoryByReference(ctx, history.IntentReference, issuedIntentRecord.IntentId)
	require.NoError(t, err)
	require.Len(t, issuanceRecords, 1)
	assert.Equal(t, history.IndirectlySent, issuanceRecords[0].Type)
	assert.Equal(t, history.StateCompleted, issuanceRecords[0].State)
}

func TestApplyStateTransitionsForIntent_NoTransitions(t *testing.T) {
	ctx := context.Background()
	data := ocp_data.NewTestDataProvider()

	intentRecord := newSendPublicPaymentIntentRecord()
	assert.NoError(t, ApplyStateTransitionsForIntent(ctx, data, intentRecord))
}

func saveRecordsForIntent(t *testing.T, data ocp_data.Provider, intentRecord *intent.Record) {
	ctx := context.Background()

	records, err := BuildRecordsForIntent(ctx, data, intentRecord, nil, nil, testWithdrawalFeeQuarks)
	require.NoError(t, err)
	for _, record := range records {
		require.NoError(t, data.SaveTransactionHistory(ctx, record))
	}
}

func newSendPublicPaymentIntentRecord() *intent.Record {
	return &intent.Record{
		IntentId:              "send_public_payment_intent",
		IntentType:            intent.SendPublicPayment,
		MintAccount:           "mint",
		InitiatorOwnerAccount: "sender_owner",
		SendPublicPaymentMetadata: &intent.SendPublicPaymentMetadata{
			DestinationOwnerAccount: "receiver_owner",
			DestinationTokenAccount: "receiver_token_account",
			Quantity:                100_000,
			ExchangeCurrency:        "usd",
			ExchangeRate:            1.0,
			NativeAmount:            10.0,
			UsdMarketValue:          10.0,
		},
		AppMetadata: []byte("app_metadata"),
		State:       intent.StatePending,
		CreatedAt:   time.Now(),
	}
}

func newExternalDepositIntentRecord() *intent.Record {
	return &intent.Record{
		IntentId:              "external_deposit_intent",
		IntentType:            intent.ExternalDeposit,
		MintAccount:           "mint",
		InitiatorOwnerAccount: "depositor_owner",
		ExternalDepositMetadata: &intent.ExternalDepositMetadata{
			DestinationTokenAccount: "depositor_token_account",
			Quantity:                100_000,
			ExchangeCurrency:        "usd",
			ExchangeRate:            1.0,
			NativeAmount:            10.0,
			UsdMarketValue:          10.0,
		},
		State:     intent.StateConfirmed,
		CreatedAt: time.Now(),
	}
}

func newGiftCardIssuedIntentRecord() *intent.Record {
	intentRecord := newSendPublicPaymentIntentRecord()
	intentRecord.IntentId = "gift_card_issued_intent"
	intentRecord.SendPublicPaymentMetadata.DestinationOwnerAccount = ""
	intentRecord.SendPublicPaymentMetadata.DestinationTokenAccount = "gift_card_vault"
	intentRecord.SendPublicPaymentMetadata.IsIndirectSend = true
	return intentRecord
}

func newGiftCardClaimedIntentRecord(issuedIntentRecord *intent.Record) *intent.Record {
	return &intent.Record{
		IntentId:              "gift_card_claimed_intent",
		IntentType:            intent.ReceivePaymentsPublicly,
		MintAccount:           issuedIntentRecord.MintAccount,
		InitiatorOwnerAccount: "claimer_owner",
		ReceivePaymentsPubliclyMetadata: &intent.ReceivePaymentsPubliclyMetadata{
			Source:   issuedIntentRecord.SendPublicPaymentMetadata.DestinationTokenAccount,
			Quantity: issuedIntentRecord.SendPublicPaymentMetadata.Quantity,

			IsIndirectSend: true,

			OriginalExchangeCurrency: issuedIntentRecord.SendPublicPaymentMetadata.ExchangeCurrency,
			OriginalExchangeRate:     issuedIntentRecord.SendPublicPaymentMetadata.ExchangeRate,
			OriginalNativeAmount:     issuedIntentRecord.SendPublicPaymentMetadata.NativeAmount,

			UsdMarketValue: issuedIntentRecord.SendPublicPaymentMetadata.UsdMarketValue,
		},
		AppMetadata: []byte("claim_app_metadata"),
		State:       intent.StatePending,
		CreatedAt:   time.Now(),
	}
}
