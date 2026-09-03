package balance

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	transactionpb "github.com/code-payments/ocp-protobuf-api/generated/go/transaction/v1"

	currency_lib "github.com/code-payments/ocp-server/currency"
	"github.com/code-payments/ocp-server/ocp/config"
	"github.com/code-payments/ocp-server/ocp/data/action"
	"github.com/code-payments/ocp-server/ocp/data/balance"
	"github.com/code-payments/ocp-server/ocp/data/intent"
	"github.com/code-payments/ocp-server/pointer"
	"github.com/code-payments/ocp-server/testutil"
)

const testWithdrawalFeeQuarks = config.CoreMintQuarksPerUnit / 4 // $0.25

func TestDeltasForSubmittedIntent_SendPublicPayment(t *testing.T) {
	intentRecord := newDeltaTestSendPublicPaymentIntent(t, 150_000, 1.5)
	actionRecords := []*action.Record{
		newDeltaTestTransferAction(intentRecord, 0, "source", "destination", 150_000),
	}

	deltas, err := DeltasForSubmittedIntent(intentRecord, actionRecords, testWithdrawalFeeQuarks)
	require.NoError(t, err)
	assert.Equal(t, []*balance.Delta{
		{TokenAccount: "destination", Kind: balance.DeltaCredit, Quarks: 150_000, UsdCostBasis: 1_500_000},
		{TokenAccount: "source", Kind: balance.DeltaDebit, Quarks: 150_000, UsdCostBasis: 1_500_000},
	}, deltas)
}

func TestDeltasForSubmittedIntent_WithdrawalWithFee(t *testing.T) {
	intentRecord := newDeltaTestSendPublicPaymentIntent(t, 150_000, 1.5)
	intentRecord.SendPublicPaymentMetadata.IsWithdrawal = true

	feeAction := newDeltaTestTransferAction(intentRecord, 0, "source", "fee_collector", testWithdrawalFeeQuarks)
	feeType := transactionpb.FeePaymentAction_CREATE_ON_SEND_WITHDRAWAL
	feeAction.FeeType = &feeType
	actionRecords := []*action.Record{
		feeAction,
		newDeltaTestTransferAction(intentRecord, 1, "source", "destination", 125_000),
	}

	deltas, err := DeltasForSubmittedIntent(intentRecord, actionRecords, testWithdrawalFeeQuarks)
	require.NoError(t, err)
	assert.Equal(t, []*balance.Delta{
		{TokenAccount: "destination", Kind: balance.DeltaCredit, Quarks: 125_000, UsdCostBasis: 1_250_000},
		{TokenAccount: "fee_collector", Kind: balance.DeltaCredit, Quarks: testWithdrawalFeeQuarks, UsdCostBasis: 250_000},
		{TokenAccount: "source", Kind: balance.DeltaDebit, Quarks: testWithdrawalFeeQuarks, UsdCostBasis: 250_000},
		{TokenAccount: "source", Kind: balance.DeltaDebit, Quarks: 125_000, UsdCostBasis: 1_250_000},
	}, deltas)
}

func TestDeltasForSubmittedIntent_GiftCardIssuanceAndClaim(t *testing.T) {
	// Issuance: a transfer to the gift card plus a deferred auto-return that
	// contributes nothing until its quantity is set
	issueIntent := newDeltaTestSendPublicPaymentIntent(t, 100_000, 1.0)
	issueIntent.SendPublicPaymentMetadata.IsIndirectSend = true
	autoReturn := newDeltaTestTransferAction(issueIntent, 1, "gift_card", "source", 0)
	autoReturn.ActionType = action.NoPrivacyWithdraw
	autoReturn.Quantity = nil
	autoReturn.State = action.StateUnknown
	actionRecords := []*action.Record{
		newDeltaTestTransferAction(issueIntent, 0, "source", "gift_card", 100_000),
		autoReturn,
	}

	deltas, err := DeltasForSubmittedIntent(issueIntent, actionRecords, testWithdrawalFeeQuarks)
	require.NoError(t, err)
	assert.Equal(t, []*balance.Delta{
		{TokenAccount: "gift_card", Kind: balance.DeltaCredit, Quarks: 100_000, UsdCostBasis: 1_000_000},
		{TokenAccount: "source", Kind: balance.DeltaDebit, Quarks: 100_000, UsdCostBasis: 1_000_000},
	}, deltas)

	// Claim: a withdrawal drains the gift card and closes it
	claimIntent := &intent.Record{
		IntentId:              testutil.NewRandomAccount(t).PublicKey().ToBase58(),
		IntentType:            intent.ReceivePaymentsPublicly,
		MintAccount:           "mint",
		InitiatorOwnerAccount: "claimer",
		ReceivePaymentsPubliclyMetadata: &intent.ReceivePaymentsPubliclyMetadata{
			Source:                   "gift_card",
			Quantity:                 100_000,
			IsIndirectSend:           true,
			OriginalExchangeCurrency: currency_lib.USD,
			OriginalExchangeRate:     1.0,
			OriginalNativeAmount:     1.0,
			UsdMarketValue:           1.0,
		},
		State: intent.StatePending,
	}
	claim := newDeltaTestTransferAction(claimIntent, 0, "gift_card", "claimer_primary", 100_000)
	claim.ActionType = action.NoPrivacyWithdraw

	deltas, err = DeltasForSubmittedIntent(claimIntent, []*action.Record{claim}, testWithdrawalFeeQuarks)
	require.NoError(t, err)
	assert.Equal(t, []*balance.Delta{
		{TokenAccount: "claimer_primary", Kind: balance.DeltaCredit, Quarks: 100_000, UsdCostBasis: 1_000_000},
		{TokenAccount: "gift_card", Kind: balance.DeltaDrain, Quarks: 100_000, UsdCostBasis: 1_000_000},
	}, deltas)
}

func TestDeltasForSubmittedIntent_OpenAccounts(t *testing.T) {
	intentRecord := &intent.Record{
		IntentId:              testutil.NewRandomAccount(t).PublicKey().ToBase58(),
		IntentType:            intent.OpenAccounts,
		MintAccount:           "mint",
		InitiatorOwnerAccount: "owner",
		OpenAccountsMetadata:  &intent.OpenAccountsMetadata{},
		State:                 intent.StatePending,
	}
	actionRecords := []*action.Record{{
		Intent:     intentRecord.IntentId,
		IntentType: intentRecord.IntentType,
		ActionId:   0,
		ActionType: action.OpenAccount,
		Source:     "primary",
		State:      action.StatePending,
	}}

	deltas, err := DeltasForSubmittedIntent(intentRecord, actionRecords, testWithdrawalFeeQuarks)
	require.NoError(t, err)
	assert.Empty(t, deltas)
}

func TestDeltasForSubmittedIntent_Unsupported(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*intent.Record, []*action.Record) (*intent.Record, []*action.Record)
	}{
		{
			name: "revoked intent",
			mutate: func(i *intent.Record, a []*action.Record) (*intent.Record, []*action.Record) {
				i.State = intent.StateRevoked
				return i, a
			},
		},
		{
			name: "revoked action",
			mutate: func(i *intent.Record, a []*action.Record) (*intent.Record, []*action.Record) {
				a[0].State = action.StateRevoked
				return i, a
			},
		},
		{
			name: "public distribution",
			mutate: func(i *intent.Record, a []*action.Record) (*intent.Record, []*action.Record) {
				i.IntentType = intent.PublicDistribution
				return i, a
			},
		},
		{
			name: "external deposit",
			mutate: func(i *intent.Record, a []*action.Record) (*intent.Record, []*action.Record) {
				i.IntentType = intent.ExternalDeposit
				i.ExternalDepositMetadata = &intent.ExternalDepositMetadata{UsdMarketValue: 1.5}
				return i, a
			},
		},
		{
			name: "more than one payment",
			mutate: func(i *intent.Record, a []*action.Record) (*intent.Record, []*action.Record) {
				return i, append(a, newDeltaTestTransferAction(i, 1, "source", "other", 1))
			},
		},
		{
			name: "more than one fee",
			mutate: func(i *intent.Record, a []*action.Record) (*intent.Record, []*action.Record) {
				feeType := transactionpb.FeePaymentAction_CREATE_ON_SEND_WITHDRAWAL
				fee1 := newDeltaTestTransferAction(i, 1, "source", "fee_collector", 1)
				fee1.FeeType = &feeType
				fee2 := newDeltaTestTransferAction(i, 2, "source", "fee_collector", 1)
				fee2.FeeType = &feeType
				return i, append(a, fee1, fee2)
			},
		},
		{
			name: "unknown fee type",
			mutate: func(i *intent.Record, a []*action.Record) (*intent.Record, []*action.Record) {
				feeType := transactionpb.FeePaymentAction_FeeType(99)
				fee := newDeltaTestTransferAction(i, 1, "source", "fee_collector", 1)
				fee.FeeType = &feeType
				return i, append(a, fee)
			},
		},
		{
			name: "action from another intent",
			mutate: func(i *intent.Record, a []*action.Record) (*intent.Record, []*action.Record) {
				a[0].Intent = "other"
				return i, a
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			intentRecord := newDeltaTestSendPublicPaymentIntent(t, 150_000, 1.5)
			actionRecords := []*action.Record{
				newDeltaTestTransferAction(intentRecord, 0, "source", "destination", 150_000),
			}
			intentRecord, actionRecords = tc.mutate(intentRecord, actionRecords)

			_, err := DeltasForSubmittedIntent(intentRecord, actionRecords, testWithdrawalFeeQuarks)
			assert.Error(t, err)
		})
	}
}

func TestDeltasForExternalDeposit(t *testing.T) {
	intentRecord := &intent.Record{
		IntentId:              testutil.NewRandomAccount(t).PublicKey().ToBase58(),
		IntentType:            intent.ExternalDeposit,
		MintAccount:           "mint",
		InitiatorOwnerAccount: "owner",
		ExternalDepositMetadata: &intent.ExternalDepositMetadata{
			DestinationTokenAccount: "destination",
			Quantity:                150_000,
			ExchangeCurrency:        currency_lib.USD,
			ExchangeRate:            1.0,
			NativeAmount:            1.5,
			UsdMarketValue:          1.5,
		},
		State: intent.StateConfirmed,
	}

	deltas, err := DeltasForExternalDeposit(intentRecord)
	require.NoError(t, err)
	assert.Equal(t, []*balance.Delta{
		{TokenAccount: "destination", Kind: balance.DeltaCredit, Quarks: 150_000, UsdCostBasis: 1_500_000},
	}, deltas)

	// Only confirmed deposits are committed by workers
	for _, state := range []intent.State{intent.StateUnknown, intent.StatePending, intent.StateFailed, intent.StateRevoked} {
		intentRecord.State = state
		_, err = DeltasForExternalDeposit(intentRecord)
		assert.ErrorIs(t, err, ErrUnsupportedBalanceChange)
	}

	_, err = DeltasForExternalDeposit(newDeltaTestSendPublicPaymentIntent(t, 1, 1.0))
	assert.ErrorIs(t, err, ErrUnsupportedBalanceChange)
}

func TestDeltasForGiftCardAutoReturn(t *testing.T) {
	newFixtures := func() (*intent.Record, *action.Record) {
		intentRecord := &intent.Record{
			IntentId:              testutil.NewRandomAccount(t).PublicKey().ToBase58(),
			IntentType:            intent.ReceivePaymentsPublicly,
			MintAccount:           "mint",
			InitiatorOwnerAccount: "issuer",
			ReceivePaymentsPubliclyMetadata: &intent.ReceivePaymentsPubliclyMetadata{
				Source:                   "gift_card",
				Quantity:                 100_000,
				IsIndirectSend:           true,
				IsReturned:               true,
				OriginalExchangeCurrency: currency_lib.USD,
				OriginalExchangeRate:     1.0,
				OriginalNativeAmount:     1.0,
				UsdMarketValue:           1.0,
			},
			State: intent.StateConfirmed,
		}
		actionRecord := &action.Record{
			Intent:      "issued_intent",
			IntentType:  intent.SendPublicPayment,
			ActionId:    1,
			ActionType:  action.NoPrivacyWithdraw,
			Source:      "gift_card",
			Destination: pointer.String("issuer_primary"),
			Quantity:    pointer.Uint64(100_000),
			State:       action.StatePending,
		}
		return intentRecord, actionRecord
	}

	intentRecord, actionRecord := newFixtures()
	deltas, err := DeltasForGiftCardAutoReturn(intentRecord, actionRecord)
	require.NoError(t, err)
	assert.Equal(t, []*balance.Delta{
		{TokenAccount: "gift_card", Kind: balance.DeltaDrain, Quarks: 100_000, UsdCostBasis: 1_000_000},
		{TokenAccount: "issuer_primary", Kind: balance.DeltaCredit, Quarks: 100_000, UsdCostBasis: 1_000_000},
	}, deltas)

	// Voiding by the issuer is the same movement
	intentRecord, actionRecord = newFixtures()
	intentRecord.ReceivePaymentsPubliclyMetadata.IsReturned = false
	intentRecord.ReceivePaymentsPubliclyMetadata.IsIssuerVoidingGiftCard = true
	_, err = DeltasForGiftCardAutoReturn(intentRecord, actionRecord)
	require.NoError(t, err)

	for _, tc := range []struct {
		name   string
		mutate func(*intent.Record, *action.Record)
	}{
		{"claim rather than return", func(i *intent.Record, a *action.Record) {
			i.ReceivePaymentsPubliclyMetadata.IsReturned = false
		}},
		{"not a gift card", func(i *intent.Record, a *action.Record) {
			i.ReceivePaymentsPubliclyMetadata.IsIndirectSend = false
		}},
		{"deferred action", func(i *intent.Record, a *action.Record) {
			a.Quantity = nil
			a.State = action.StateUnknown
		}},
		{"revoked action", func(i *intent.Record, a *action.Record) {
			a.State = action.StateRevoked
		}},
		{"not a withdraw", func(i *intent.Record, a *action.Record) {
			a.ActionType = action.NoPrivacyTransfer
		}},
		{"wrong source", func(i *intent.Record, a *action.Record) {
			a.Source = "other_gift_card"
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			intentRecord, actionRecord := newFixtures()
			tc.mutate(intentRecord, actionRecord)
			_, err := DeltasForGiftCardAutoReturn(intentRecord, actionRecord)
			assert.Error(t, err)
		})
	}
}

func TestDeltasForSwapSellReconciliation(t *testing.T) {
	newFixtures := func(previousUsd, updatedUsd float64) (*intent.Record, *intent.Record, []*action.Record) {
		previous := newDeltaTestSendPublicPaymentIntent(t, 150_000, previousUsd)
		previous.SendPublicPaymentMetadata.IsSwapSell = true
		previous.State = intent.StateConfirmed

		updatedClone := previous.Clone()
		updated := &updatedClone
		updated.SendPublicPaymentMetadata.UsdMarketValue = updatedUsd

		actionRecords := []*action.Record{
			newDeltaTestTransferAction(previous, 0, "source", "swap", 150_000),
		}
		return previous, updated, actionRecords
	}

	// Realized more than estimated: more basis leaves the source
	previous, updated, actionRecords := newFixtures(1.5, 1.75)
	deltas, err := DeltasForSwapSellReconciliation(previous, updated, actionRecords)
	require.NoError(t, err)
	assert.Equal(t, []*balance.Delta{
		{TokenAccount: "source", Kind: balance.DeltaAdjustUsdCostBasis, UsdCostBasis: -250_000},
	}, deltas)

	// Realized less than estimated: basis is returned to the source
	previous, updated, actionRecords = newFixtures(1.5, 1.25)
	deltas, err = DeltasForSwapSellReconciliation(previous, updated, actionRecords)
	require.NoError(t, err)
	assert.Equal(t, []*balance.Delta{
		{TokenAccount: "source", Kind: balance.DeltaAdjustUsdCostBasis, UsdCostBasis: 250_000},
	}, deltas)

	// No change is a no-op
	previous, updated, actionRecords = newFixtures(1.5, 1.5)
	deltas, err = DeltasForSwapSellReconciliation(previous, updated, actionRecords)
	require.NoError(t, err)
	assert.Empty(t, deltas)

	for _, tc := range []struct {
		name   string
		mutate func(previous, updated *intent.Record, a []*action.Record) []*action.Record
	}{
		{"not a swap sell", func(p, u *intent.Record, a []*action.Record) []*action.Record {
			u.SendPublicPaymentMetadata.IsSwapSell = false
			return a
		}},
		{"different intents", func(p, u *intent.Record, a []*action.Record) []*action.Record {
			u.IntentId = "other"
			return a
		}},
		{"revoked", func(p, u *intent.Record, a []*action.Record) []*action.Record {
			u.State = intent.StateRevoked
			return a
		}},
		{"no funding action", func(p, u *intent.Record, a []*action.Record) []*action.Record {
			return nil
		}},
		{"pays a fee", func(p, u *intent.Record, a []*action.Record) []*action.Record {
			feeType := transactionpb.FeePaymentAction_CREATE_ON_SEND_WITHDRAWAL
			fee := newDeltaTestTransferAction(u, 1, "source", "fee_collector", 1)
			fee.FeeType = &feeType
			return append(a, fee)
		}},
		{"more than one payment", func(p, u *intent.Record, a []*action.Record) []*action.Record {
			return append(a, newDeltaTestTransferAction(u, 1, "source", "other", 1))
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			previous, updated, actionRecords := newFixtures(1.5, 1.75)
			actionRecords = tc.mutate(previous, updated, actionRecords)
			_, err := DeltasForSwapSellReconciliation(previous, updated, actionRecords)
			assert.Error(t, err)
		})
	}
}

func newDeltaTestSendPublicPaymentIntent(t *testing.T, quantity uint64, usd float64) *intent.Record {
	return &intent.Record{
		IntentId:              testutil.NewRandomAccount(t).PublicKey().ToBase58(),
		IntentType:            intent.SendPublicPayment,
		MintAccount:           "mint",
		InitiatorOwnerAccount: "owner",
		SendPublicPaymentMetadata: &intent.SendPublicPaymentMetadata{
			DestinationOwnerAccount: "destination_owner",
			DestinationTokenAccount: "destination",
			Quantity:                quantity,
			ExchangeCurrency:        currency_lib.USD,
			ExchangeRate:            1.0,
			NativeAmount:            usd,
			UsdMarketValue:          usd,
		},
		State: intent.StatePending,
	}
}

func newDeltaTestTransferAction(intentRecord *intent.Record, actionId uint32, source, destination string, quantity uint64) *action.Record {
	return &action.Record{
		Intent:      intentRecord.IntentId,
		IntentType:  intentRecord.IntentType,
		ActionId:    actionId,
		ActionType:  action.NoPrivacyTransfer,
		Source:      source,
		Destination: pointer.String(destination),
		Quantity:    pointer.Uint64(quantity),
		State:       action.StatePending,
	}
}
