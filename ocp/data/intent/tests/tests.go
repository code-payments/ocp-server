package tests

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/code-payments/ocp-server/currency"
	"github.com/code-payments/ocp-server/database/query"
	"github.com/code-payments/ocp-server/ocp/data/intent"
)

func RunTests(t *testing.T, s intent.Store, teardown func()) {
	for _, tf := range []func(t *testing.T, s intent.Store){
		testOpenAccountsRoundTrip,
		testExternalDepositRoundTrip,
		testSendPublicPaymentRoundTrip,
		testReceivePaymentsPubliclyRoundTrip,
		testPublicDistributionRoundTrip,
		testSendPublicMultiPaymentRoundTrip,
		testUpdateHappyPath,
		testUpdateStaleRecord,
		testGetOriginalGiftCardIssuedIntent,
		testGetGiftCardClaimedIntent,
		testGetTransactedAmountForAntiMoneyLaundering,
		testGetByOwner,
		testGetByOwnerWithSendPublicMultiPayment,
		testGetUsdCostBasis,
		testGetUsdCostBasisBatch,
	} {
		tf(t, s)
		teardown()
	}
}

func testOpenAccountsRoundTrip(t *testing.T, s intent.Store) {
	t.Run("testOpenAccountsRoundTrip", func(t *testing.T) {
		ctx := context.Background()

		actual, err := s.Get(ctx, "test_intent_id")
		require.Error(t, err)
		assert.Equal(t, intent.ErrIntentNotFound, err)
		assert.Nil(t, actual)

		expected := intent.Record{
			IntentId:              "test_intent_id",
			IntentType:            intent.OpenAccounts,
			MintAccount:           "test_mint",
			InitiatorOwnerAccount: "test_owner",
			OpenAccountsMetadata:  &intent.OpenAccountsMetadata{},
			State:                 intent.StateUnknown,
			CreatedAt:             time.Now(),
		}
		cloned := expected.Clone()
		err = s.Save(ctx, &expected)
		require.NoError(t, err)
		assert.EqualValues(t, 1, expected.Id)
		assert.EqualValues(t, 1, expected.Version)

		actual, err = s.Get(ctx, "test_intent_id")
		require.NoError(t, err)
		assert.Equal(t, cloned.IntentId, actual.IntentId)
		assert.Equal(t, cloned.IntentType, actual.IntentType)
		assert.Equal(t, cloned.MintAccount, actual.MintAccount)
		assert.Equal(t, cloned.InitiatorOwnerAccount, actual.InitiatorOwnerAccount)
		require.NotNil(t, actual.OpenAccountsMetadata)
		assert.Equal(t, cloned.State, actual.State)
		assert.Equal(t, cloned.CreatedAt.Unix(), actual.CreatedAt.Unix())
		assert.EqualValues(t, 1, actual.Id)
		assert.EqualValues(t, 1, actual.Version)
	})
}

func testExternalDepositRoundTrip(t *testing.T, s intent.Store) {
	t.Run("testExternalDepositRoundTrip", func(t *testing.T) {
		ctx := context.Background()

		actual, err := s.Get(ctx, "test_intent_id")
		require.Error(t, err)
		assert.Equal(t, intent.ErrIntentNotFound, err)
		assert.Nil(t, actual)

		expected := intent.Record{
			IntentId:              "test_intent_id",
			IntentType:            intent.ExternalDeposit,
			MintAccount:           "test_mint",
			InitiatorOwnerAccount: "test_owner",
			ExternalDepositMetadata: &intent.ExternalDepositMetadata{
				DestinationTokenAccount: "test_destination_token",
				Quantity:                12345,

				ExchangeCurrency: currency.CAD,
				ExchangeRate:     0.00073,
				NativeAmount:     0.00073 * 12345,
				UsdMarketValue:   0.00042,

				IsSwapBuy: true,
			},
			State:     intent.StateUnknown,
			CreatedAt: time.Now(),
		}
		cloned := expected.Clone()
		err = s.Save(ctx, &expected)
		require.NoError(t, err)
		assert.EqualValues(t, 1, expected.Id)
		assert.EqualValues(t, 1, expected.Version)

		actual, err = s.Get(ctx, "test_intent_id")
		require.NoError(t, err)
		assert.Equal(t, cloned.IntentId, actual.IntentId)
		assert.Equal(t, cloned.IntentType, actual.IntentType)
		assert.Equal(t, cloned.MintAccount, actual.MintAccount)
		assert.Equal(t, cloned.InitiatorOwnerAccount, actual.InitiatorOwnerAccount)
		require.NotNil(t, actual.ExternalDepositMetadata)
		assert.Equal(t, cloned.ExternalDepositMetadata.DestinationTokenAccount, actual.ExternalDepositMetadata.DestinationTokenAccount)
		assert.Equal(t, cloned.ExternalDepositMetadata.Quantity, actual.ExternalDepositMetadata.Quantity)
		assert.Equal(t, cloned.ExternalDepositMetadata.ExchangeCurrency, actual.ExternalDepositMetadata.ExchangeCurrency)
		assert.Equal(t, cloned.ExternalDepositMetadata.ExchangeRate, actual.ExternalDepositMetadata.ExchangeRate)
		assert.Equal(t, cloned.ExternalDepositMetadata.NativeAmount, actual.ExternalDepositMetadata.NativeAmount)
		assert.Equal(t, cloned.ExternalDepositMetadata.UsdMarketValue, actual.ExternalDepositMetadata.UsdMarketValue)
		assert.Equal(t, cloned.ExternalDepositMetadata.IsSwapBuy, actual.ExternalDepositMetadata.IsSwapBuy)
		assert.Equal(t, cloned.State, actual.State)
		assert.Equal(t, cloned.CreatedAt.Unix(), actual.CreatedAt.Unix())
		assert.EqualValues(t, 1, actual.Id)
		assert.EqualValues(t, 1, actual.Version)
	})
}

func testSendPublicPaymentRoundTrip(t *testing.T, s intent.Store) {
	t.Run("testSendPublicPaymentRoundTrip", func(t *testing.T) {
		ctx := context.Background()

		actual, err := s.Get(ctx, "test_intent_id")
		require.Error(t, err)
		assert.Equal(t, intent.ErrIntentNotFound, err)
		assert.Nil(t, actual)

		expected := intent.Record{
			IntentId:              "test_intent_id",
			IntentType:            intent.SendPublicPayment,
			MintAccount:           "test_mint",
			InitiatorOwnerAccount: "test_owner",
			SendPublicPaymentMetadata: &intent.SendPublicPaymentMetadata{
				DestinationOwnerAccount: "test_destination_owner",
				DestinationTokenAccount: "test_destination_token",
				Quantity:                12345,

				ExchangeCurrency: currency.CAD,
				ExchangeRate:     0.00073,
				NativeAmount:     0.00073 * 12345,
				UsdMarketValue:   0.00042,

				IsWithdrawal:   true,
				IsIndirectSend: true,
				IsSwapSell:     true,
			},
			AppMetadata: []byte("test_app_metadata"),
			State:       intent.StateUnknown,
			CreatedAt:   time.Now(),
		}
		cloned := expected.Clone()
		err = s.Save(ctx, &expected)
		require.NoError(t, err)
		assert.EqualValues(t, 1, expected.Id)
		assert.EqualValues(t, 1, expected.Version)

		actual, err = s.Get(ctx, "test_intent_id")
		require.NoError(t, err)
		assert.Equal(t, cloned.IntentId, actual.IntentId)
		assert.Equal(t, cloned.IntentType, actual.IntentType)
		assert.Equal(t, cloned.MintAccount, actual.MintAccount)
		assert.Equal(t, cloned.InitiatorOwnerAccount, actual.InitiatorOwnerAccount)
		require.NotNil(t, actual.SendPublicPaymentMetadata)
		assert.Equal(t, cloned.SendPublicPaymentMetadata.DestinationOwnerAccount, actual.SendPublicPaymentMetadata.DestinationOwnerAccount)
		assert.Equal(t, cloned.SendPublicPaymentMetadata.DestinationTokenAccount, actual.SendPublicPaymentMetadata.DestinationTokenAccount)
		assert.Equal(t, cloned.SendPublicPaymentMetadata.Quantity, actual.SendPublicPaymentMetadata.Quantity)
		assert.Equal(t, cloned.SendPublicPaymentMetadata.ExchangeCurrency, actual.SendPublicPaymentMetadata.ExchangeCurrency)
		assert.Equal(t, cloned.SendPublicPaymentMetadata.ExchangeRate, actual.SendPublicPaymentMetadata.ExchangeRate)
		assert.Equal(t, cloned.SendPublicPaymentMetadata.NativeAmount, actual.SendPublicPaymentMetadata.NativeAmount)
		assert.Equal(t, cloned.SendPublicPaymentMetadata.UsdMarketValue, actual.SendPublicPaymentMetadata.UsdMarketValue)
		assert.Equal(t, cloned.SendPublicPaymentMetadata.IsWithdrawal, actual.SendPublicPaymentMetadata.IsWithdrawal)
		assert.Equal(t, cloned.SendPublicPaymentMetadata.IsIndirectSend, actual.SendPublicPaymentMetadata.IsIndirectSend)
		assert.Equal(t, cloned.SendPublicPaymentMetadata.IsSwapSell, actual.SendPublicPaymentMetadata.IsSwapSell)
		assert.Equal(t, cloned.AppMetadata, actual.AppMetadata)
		assert.Equal(t, cloned.State, actual.State)
		assert.Equal(t, cloned.CreatedAt.Unix(), actual.CreatedAt.Unix())
		assert.EqualValues(t, 1, actual.Id)
		assert.EqualValues(t, 1, actual.Version)
	})
}

func testReceivePaymentsPubliclyRoundTrip(t *testing.T, s intent.Store) {
	t.Run("testReceivePaymentsPubliclyRoundTrip", func(t *testing.T) {
		ctx := context.Background()

		actual, err := s.Get(ctx, "test_intent_id")
		require.Error(t, err)
		assert.Equal(t, intent.ErrIntentNotFound, err)
		assert.Nil(t, actual)

		expected := intent.Record{
			IntentId:              "test_intent_id",
			IntentType:            intent.ReceivePaymentsPublicly,
			MintAccount:           "test_mint",
			InitiatorOwnerAccount: "test_owner",
			ReceivePaymentsPubliclyMetadata: &intent.ReceivePaymentsPubliclyMetadata{
				Source:                   "test_source",
				Quantity:                 12345,
				IsIndirectSend:           true,
				IsReturned:               true,
				IsIssuerVoidingGiftCard:  true,
				OriginalExchangeCurrency: "usd",
				OriginalExchangeRate:     0.1,
				OriginalNativeAmount:     1234.5,
				UsdMarketValue:           999.99,
			},
			State:     intent.StateUnknown,
			CreatedAt: time.Now(),
		}
		cloned := expected.Clone()
		err = s.Save(ctx, &expected)
		require.NoError(t, err)
		assert.EqualValues(t, 1, expected.Id)
		assert.EqualValues(t, 1, expected.Version)

		actual, err = s.Get(ctx, "test_intent_id")
		require.NoError(t, err)
		assert.Equal(t, cloned.IntentId, actual.IntentId)
		assert.Equal(t, cloned.IntentType, actual.IntentType)
		assert.Equal(t, cloned.MintAccount, actual.MintAccount)
		assert.Equal(t, cloned.InitiatorOwnerAccount, actual.InitiatorOwnerAccount)
		require.NotNil(t, actual.ReceivePaymentsPubliclyMetadata)
		assert.Equal(t, cloned.ReceivePaymentsPubliclyMetadata.Source, actual.ReceivePaymentsPubliclyMetadata.Source)
		assert.Equal(t, cloned.ReceivePaymentsPubliclyMetadata.Quantity, actual.ReceivePaymentsPubliclyMetadata.Quantity)
		assert.Equal(t, cloned.ReceivePaymentsPubliclyMetadata.IsIndirectSend, actual.ReceivePaymentsPubliclyMetadata.IsIndirectSend)
		assert.Equal(t, cloned.ReceivePaymentsPubliclyMetadata.IsReturned, actual.ReceivePaymentsPubliclyMetadata.IsReturned)
		assert.Equal(t, cloned.ReceivePaymentsPubliclyMetadata.IsIssuerVoidingGiftCard, actual.ReceivePaymentsPubliclyMetadata.IsIssuerVoidingGiftCard)
		assert.Equal(t, cloned.ReceivePaymentsPubliclyMetadata.OriginalExchangeCurrency, actual.ReceivePaymentsPubliclyMetadata.OriginalExchangeCurrency)
		assert.Equal(t, cloned.ReceivePaymentsPubliclyMetadata.OriginalExchangeRate, actual.ReceivePaymentsPubliclyMetadata.OriginalExchangeRate)
		assert.Equal(t, cloned.ReceivePaymentsPubliclyMetadata.OriginalNativeAmount, actual.ReceivePaymentsPubliclyMetadata.OriginalNativeAmount)
		assert.Equal(t, cloned.ReceivePaymentsPubliclyMetadata.UsdMarketValue, actual.ReceivePaymentsPubliclyMetadata.UsdMarketValue)
		assert.Equal(t, cloned.State, actual.State)
		assert.Equal(t, cloned.CreatedAt.Unix(), actual.CreatedAt.Unix())
		assert.EqualValues(t, 1, actual.Id)
		assert.EqualValues(t, 1, actual.Version)
	})
}

func testPublicDistributionRoundTrip(t *testing.T, s intent.Store) {
	t.Run("testPublicDistributionRoundTrip", func(t *testing.T) {
		ctx := context.Background()

		actual, err := s.Get(ctx, "test_intent_id")
		require.Error(t, err)
		assert.Equal(t, intent.ErrIntentNotFound, err)
		assert.Nil(t, actual)

		expected := intent.Record{
			IntentId:              "test_intent_id",
			IntentType:            intent.PublicDistribution,
			MintAccount:           "test_mint",
			InitiatorOwnerAccount: "test_owner",
			PublicDistributionMetadata: &intent.PublicDistributionMetadata{
				Source: "test_source",
				Distributions: []*intent.Distribution{
					{
						DestinationOwnerAccount: "test_owner_2",
						DestinationTokenAccount: "test_destination_2",
						Quantity:                12300,
					},
					{
						DestinationOwnerAccount: "test_owner_3",
						DestinationTokenAccount: "test_destination_3",
						Quantity:                45,
					},
				},
				Quantity:       12345,
				UsdMarketValue: 999.99,
			},
			State:     intent.StateUnknown,
			CreatedAt: time.Now(),
		}
		cloned := expected.Clone()
		err = s.Save(ctx, &expected)
		require.NoError(t, err)
		assert.EqualValues(t, 1, expected.Id)
		assert.EqualValues(t, 1, expected.Version)
		require.Len(t, expected.PublicDistributionMetadata.Distributions, 2)
		for i, expectedDistribution := range cloned.PublicDistributionMetadata.Distributions {
			assert.Equal(t, expectedDistribution.DestinationOwnerAccount, expected.PublicDistributionMetadata.Distributions[i].DestinationOwnerAccount)
			assert.Equal(t, expectedDistribution.DestinationTokenAccount, expected.PublicDistributionMetadata.Distributions[i].DestinationTokenAccount)
			assert.Equal(t, expectedDistribution.Quantity, expected.PublicDistributionMetadata.Distributions[i].Quantity)
		}

		actual, err = s.Get(ctx, "test_intent_id")
		require.NoError(t, err)
		assert.Equal(t, cloned.IntentId, actual.IntentId)
		assert.Equal(t, cloned.IntentType, actual.IntentType)
		assert.Equal(t, cloned.MintAccount, actual.MintAccount)
		assert.Equal(t, cloned.InitiatorOwnerAccount, actual.InitiatorOwnerAccount)
		require.NotNil(t, actual.PublicDistributionMetadata)
		assert.Equal(t, cloned.PublicDistributionMetadata.Source, actual.PublicDistributionMetadata.Source)
		require.Len(t, actual.PublicDistributionMetadata.Distributions, len(cloned.PublicDistributionMetadata.Distributions))
		for i, expectedDistribution := range cloned.PublicDistributionMetadata.Distributions {
			assert.Equal(t, expectedDistribution.DestinationOwnerAccount, actual.PublicDistributionMetadata.Distributions[i].DestinationOwnerAccount)
			assert.Equal(t, expectedDistribution.DestinationTokenAccount, actual.PublicDistributionMetadata.Distributions[i].DestinationTokenAccount)
			assert.Equal(t, expectedDistribution.Quantity, actual.PublicDistributionMetadata.Distributions[i].Quantity)
		}
		assert.Equal(t, cloned.PublicDistributionMetadata.Quantity, actual.PublicDistributionMetadata.Quantity)
		assert.Equal(t, cloned.PublicDistributionMetadata.UsdMarketValue, actual.PublicDistributionMetadata.UsdMarketValue)
		assert.Equal(t, cloned.State, actual.State)
		assert.Equal(t, cloned.CreatedAt.Unix(), actual.CreatedAt.Unix())
		assert.EqualValues(t, 1, actual.Id)
		assert.EqualValues(t, 1, actual.Version)
	})
}

func testSendPublicMultiPaymentRoundTrip(t *testing.T, s intent.Store) {
	t.Run("testSendPublicMultiPaymentRoundTrip", func(t *testing.T) {
		ctx := context.Background()

		actual, err := s.Get(ctx, "test_intent_id")
		require.Error(t, err)
		assert.Equal(t, intent.ErrIntentNotFound, err)
		assert.Nil(t, actual)

		expected := intent.Record{
			IntentId:              "test_intent_id",
			IntentType:            intent.SendPublicMultiPayment,
			MintAccount:           "test_mint",
			InitiatorOwnerAccount: "test_owner",
			SendPublicMultiPaymentMetadata: &intent.SendPublicMultiPaymentMetadata{
				Source: "test_source",
				Payments: []*intent.Payment{
					{
						DestinationOwnerAccount: "test_owner_2",
						DestinationTokenAccount: "test_destination_2",
						Quantity:                12300,
						NativeAmount:            1.23,
						UsdMarketValue:          0.123,
					},
					{
						DestinationOwnerAccount: "test_owner_3",
						DestinationTokenAccount: "test_destination_3",
						Quantity:                45,
						NativeAmount:            0.0045,
						UsdMarketValue:          0.00045,
					},
				},
				Quantity: 12345,

				ExchangeCurrency: currency.CAD,
				ExchangeRate:     0.0001,
				NativeAmount:     1.2345,
				UsdMarketValue:   0.12345,
			},
			AppMetadata: []byte("app_metadata"),
			State:       intent.StateUnknown,
			CreatedAt:   time.Now(),
		}
		cloned := expected.Clone()
		err = s.Save(ctx, &expected)
		require.NoError(t, err)
		assert.EqualValues(t, 1, expected.Id)
		assert.EqualValues(t, 1, expected.Version)

		actual, err = s.Get(ctx, "test_intent_id")
		require.NoError(t, err)
		assert.Equal(t, cloned.IntentId, actual.IntentId)
		assert.Equal(t, cloned.IntentType, actual.IntentType)
		assert.Equal(t, cloned.MintAccount, actual.MintAccount)
		assert.Equal(t, cloned.InitiatorOwnerAccount, actual.InitiatorOwnerAccount)
		require.NotNil(t, actual.SendPublicMultiPaymentMetadata)
		assert.Equal(t, cloned.SendPublicMultiPaymentMetadata.Source, actual.SendPublicMultiPaymentMetadata.Source)
		require.Len(t, actual.SendPublicMultiPaymentMetadata.Payments, len(cloned.SendPublicMultiPaymentMetadata.Payments))
		for i, expectedPayment := range cloned.SendPublicMultiPaymentMetadata.Payments {
			actualPayment := actual.SendPublicMultiPaymentMetadata.Payments[i]
			assert.Equal(t, expectedPayment.DestinationOwnerAccount, actualPayment.DestinationOwnerAccount)
			assert.Equal(t, expectedPayment.DestinationTokenAccount, actualPayment.DestinationTokenAccount)
			assert.Equal(t, expectedPayment.Quantity, actualPayment.Quantity)
			assert.Equal(t, expectedPayment.NativeAmount, actualPayment.NativeAmount)
			assert.Equal(t, expectedPayment.UsdMarketValue, actualPayment.UsdMarketValue)
		}
		assert.Equal(t, cloned.SendPublicMultiPaymentMetadata.Quantity, actual.SendPublicMultiPaymentMetadata.Quantity)
		assert.Equal(t, cloned.SendPublicMultiPaymentMetadata.ExchangeCurrency, actual.SendPublicMultiPaymentMetadata.ExchangeCurrency)
		assert.Equal(t, cloned.SendPublicMultiPaymentMetadata.ExchangeRate, actual.SendPublicMultiPaymentMetadata.ExchangeRate)
		assert.Equal(t, cloned.SendPublicMultiPaymentMetadata.NativeAmount, actual.SendPublicMultiPaymentMetadata.NativeAmount)
		assert.Equal(t, cloned.SendPublicMultiPaymentMetadata.UsdMarketValue, actual.SendPublicMultiPaymentMetadata.UsdMarketValue)
		assert.Equal(t, cloned.AppMetadata, actual.AppMetadata)
		assert.Equal(t, cloned.State, actual.State)
		assert.Equal(t, cloned.CreatedAt.Unix(), actual.CreatedAt.Unix())
		assert.EqualValues(t, 1, actual.Id)
		assert.EqualValues(t, 1, actual.Version)

		// Duplicate destinations are rejected
		duplicate := cloned.Clone()
		duplicate.IntentId = "test_intent_id_2"
		duplicate.SendPublicMultiPaymentMetadata.Payments[1].DestinationTokenAccount = "test_destination_2"
		assert.Error(t, s.Save(ctx, &duplicate))

		// Quantity must be the sum of payment quantities
		mismatched := cloned.Clone()
		mismatched.IntentId = "test_intent_id_3"
		mismatched.SendPublicMultiPaymentMetadata.Quantity++
		assert.Error(t, s.Save(ctx, &mismatched))
	})
}

func testUpdateHappyPath(t *testing.T, s intent.Store) {
	t.Run("testUpdateHappyPath", func(t *testing.T) {
		ctx := context.Background()

		expected := intent.Record{
			IntentId:              "test_intent_id",
			IntentType:            intent.PublicDistribution,
			MintAccount:           "test_mint",
			InitiatorOwnerAccount: "test_owner",
			PublicDistributionMetadata: &intent.PublicDistributionMetadata{
				Source: "test_source",
				Distributions: []*intent.Distribution{
					{
						DestinationOwnerAccount: "test_owner_2",
						DestinationTokenAccount: "test_destination_2",
						Quantity:                12300,
					},
					{
						DestinationOwnerAccount: "test_owner_3",
						DestinationTokenAccount: "test_destination_3",
						Quantity:                45,
					},
				},
				Quantity:       12345,
				UsdMarketValue: 999.99,
			},
			State:     intent.StateUnknown,
			CreatedAt: time.Now(),
		}
		err := s.Save(ctx, &expected)
		require.NoError(t, err)
		assert.EqualValues(t, 1, expected.Id)
		assert.EqualValues(t, 1, expected.Version)

		expected.State = intent.StatePending

		err = s.Save(ctx, &expected)
		require.NoError(t, err)
		assert.EqualValues(t, 1, expected.Id)
		assert.EqualValues(t, 2, expected.Version)
		require.Len(t, expected.PublicDistributionMetadata.Distributions, 2)

		actual, err := s.Get(ctx, "test_intent_id")
		require.NoError(t, err)
		assert.Equal(t, intent.StatePending, actual.State)
		assert.EqualValues(t, 1, actual.Id)
		assert.EqualValues(t, 2, actual.Version)
		require.Len(t, actual.PublicDistributionMetadata.Distributions, 2)
	})
}

func testUpdateStaleRecord(t *testing.T, s intent.Store) {
	t.Run("testUpdateStaleRecord", func(t *testing.T) {
		ctx := context.Background()

		expected := intent.Record{
			IntentId:              "test_intent_id",
			IntentType:            intent.OpenAccounts,
			MintAccount:           "test_mint",
			InitiatorOwnerAccount: "test_owner",
			OpenAccountsMetadata:  &intent.OpenAccountsMetadata{},
			State:                 intent.StateUnknown,
			CreatedAt:             time.Now(),
		}
		err := s.Save(ctx, &expected)
		require.NoError(t, err)
		assert.EqualValues(t, 1, expected.Id)
		assert.EqualValues(t, 1, expected.Version)

		stale := expected.Clone()
		expected.State = intent.StatePending
		stale.Version -= 1

		err = s.Save(ctx, &stale)
		assert.Equal(t, intent.ErrStaleVersion, err)
		assert.EqualValues(t, 1, stale.Id)
		assert.EqualValues(t, 0, stale.Version)

		actual, err := s.Get(ctx, "test_intent_id")
		require.NoError(t, err)
		assert.Equal(t, intent.StateUnknown, actual.State)
		assert.EqualValues(t, 1, actual.Id)
		assert.EqualValues(t, 1, actual.Version)
	})
}

func testGetOriginalGiftCardIssuedIntent(t *testing.T, s intent.Store) {
	t.Run("testGetOriginalGiftCardIssuedIntent", func(t *testing.T) {
		ctx := context.Background()

		records := []*intent.Record{
			{IntentId: "i1", IntentType: intent.SendPublicPayment, SendPublicPaymentMetadata: &intent.SendPublicPaymentMetadata{IsIndirectSend: false, DestinationTokenAccount: "a1", DestinationOwnerAccount: "o1", Quantity: 1, ExchangeCurrency: currency.USD, ExchangeRate: 1, NativeAmount: 1, UsdMarketValue: 1}, MintAccount: "mint", InitiatorOwnerAccount: "user", State: intent.StateConfirmed},

			{IntentId: "i2", IntentType: intent.SendPublicPayment, SendPublicPaymentMetadata: &intent.SendPublicPaymentMetadata{IsIndirectSend: true, DestinationTokenAccount: "a2", DestinationOwnerAccount: "o2", Quantity: 1, ExchangeCurrency: currency.USD, ExchangeRate: 1, NativeAmount: 1, UsdMarketValue: 1}, MintAccount: "mint", InitiatorOwnerAccount: "user", State: intent.StateConfirmed},
			{IntentId: "i3", IntentType: intent.SendPublicPayment, SendPublicPaymentMetadata: &intent.SendPublicPaymentMetadata{IsIndirectSend: false, DestinationTokenAccount: "a2", DestinationOwnerAccount: "o2", Quantity: 1, ExchangeCurrency: currency.USD, ExchangeRate: 1, NativeAmount: 1, UsdMarketValue: 1}, MintAccount: "mint", InitiatorOwnerAccount: "user", State: intent.StateConfirmed},
			{IntentId: "i4", IntentType: intent.ExternalDeposit, ExternalDepositMetadata: &intent.ExternalDepositMetadata{DestinationTokenAccount: "a2", ExchangeCurrency: currency.USD, ExchangeRate: 1, NativeAmount: 1, Quantity: 1, UsdMarketValue: 1}, MintAccount: "mint", InitiatorOwnerAccount: "user", State: intent.StateConfirmed},

			{IntentId: "i5", IntentType: intent.SendPublicPayment, SendPublicPaymentMetadata: &intent.SendPublicPaymentMetadata{IsIndirectSend: true, DestinationTokenAccount: "a3", DestinationOwnerAccount: "o3", Quantity: 1, ExchangeCurrency: currency.USD, ExchangeRate: 1, NativeAmount: 1, UsdMarketValue: 1}, MintAccount: "mint", InitiatorOwnerAccount: "user", State: intent.StateConfirmed},
			{IntentId: "i6", IntentType: intent.SendPublicPayment, SendPublicPaymentMetadata: &intent.SendPublicPaymentMetadata{IsIndirectSend: true, DestinationTokenAccount: "a3", DestinationOwnerAccount: "o3", Quantity: 1, ExchangeCurrency: currency.USD, ExchangeRate: 1, NativeAmount: 1, UsdMarketValue: 1}, MintAccount: "mint", InitiatorOwnerAccount: "user", State: intent.StateConfirmed},

			{IntentId: "i7", IntentType: intent.SendPublicPayment, SendPublicPaymentMetadata: &intent.SendPublicPaymentMetadata{IsIndirectSend: true, DestinationTokenAccount: "a4", DestinationOwnerAccount: "o4", Quantity: 1, ExchangeCurrency: currency.USD, ExchangeRate: 1, NativeAmount: 1, UsdMarketValue: 1}, MintAccount: "mint", InitiatorOwnerAccount: "user", State: intent.StatePending},
			{IntentId: "i8", IntentType: intent.SendPublicPayment, SendPublicPaymentMetadata: &intent.SendPublicPaymentMetadata{IsIndirectSend: true, DestinationTokenAccount: "a4", DestinationOwnerAccount: "o4", Quantity: 1, ExchangeCurrency: currency.USD, ExchangeRate: 1, NativeAmount: 1, UsdMarketValue: 1}, MintAccount: "mint", InitiatorOwnerAccount: "user", State: intent.StateRevoked},
		}

		for _, record := range records {
			require.NoError(t, s.Save(ctx, record))
		}

		_, err := s.GetOriginalGiftCardIssuedIntent(ctx, "unknown")
		assert.Equal(t, intent.ErrIntentNotFound, err)

		_, err = s.GetOriginalGiftCardIssuedIntent(ctx, "a1")
		assert.Equal(t, intent.ErrIntentNotFound, err)

		actual, err := s.GetOriginalGiftCardIssuedIntent(ctx, "a2")
		require.NoError(t, err)
		assert.Equal(t, "i2", actual.IntentId)

		_, err = s.GetOriginalGiftCardIssuedIntent(ctx, "a3")
		assert.Equal(t, intent.ErrMultilpeIntentsFound, err)

		actual, err = s.GetOriginalGiftCardIssuedIntent(ctx, "a4")
		require.NoError(t, err)
		assert.Equal(t, "i7", actual.IntentId)
	})
}

func testGetGiftCardClaimedIntent(t *testing.T, s intent.Store) {
	t.Run("testGetGiftCardClaimedIntent", func(t *testing.T) {
		ctx := context.Background()

		records := []*intent.Record{
			{IntentId: "i1", IntentType: intent.ReceivePaymentsPublicly, ReceivePaymentsPubliclyMetadata: &intent.ReceivePaymentsPubliclyMetadata{IsIndirectSend: false, Source: "a1", Quantity: 1, OriginalExchangeCurrency: currency.USD, OriginalExchangeRate: 1, OriginalNativeAmount: 1, UsdMarketValue: 1}, MintAccount: "mint", InitiatorOwnerAccount: "user", State: intent.StateConfirmed},

			{IntentId: "i2", IntentType: intent.ReceivePaymentsPublicly, ReceivePaymentsPubliclyMetadata: &intent.ReceivePaymentsPubliclyMetadata{IsIndirectSend: false, Source: "a2", Quantity: 1, OriginalExchangeCurrency: currency.USD, OriginalExchangeRate: 1, OriginalNativeAmount: 1, UsdMarketValue: 1}, MintAccount: "mint", InitiatorOwnerAccount: "user", State: intent.StateConfirmed},
			{IntentId: "i3", IntentType: intent.ReceivePaymentsPublicly, ReceivePaymentsPubliclyMetadata: &intent.ReceivePaymentsPubliclyMetadata{IsIndirectSend: true, Source: "a2", Quantity: 1, OriginalExchangeCurrency: currency.USD, OriginalExchangeRate: 1, OriginalNativeAmount: 1, UsdMarketValue: 1}, MintAccount: "mint", InitiatorOwnerAccount: "user", State: intent.StateConfirmed},

			{IntentId: "i4", IntentType: intent.ReceivePaymentsPublicly, ReceivePaymentsPubliclyMetadata: &intent.ReceivePaymentsPubliclyMetadata{IsIndirectSend: true, Source: "a3", Quantity: 1, OriginalExchangeCurrency: currency.USD, OriginalExchangeRate: 1, OriginalNativeAmount: 1, UsdMarketValue: 1}, MintAccount: "mint", InitiatorOwnerAccount: "user", State: intent.StateConfirmed},
			{IntentId: "i5", IntentType: intent.ReceivePaymentsPublicly, ReceivePaymentsPubliclyMetadata: &intent.ReceivePaymentsPubliclyMetadata{IsIndirectSend: true, Source: "a3", Quantity: 1, OriginalExchangeCurrency: currency.USD, OriginalExchangeRate: 1, OriginalNativeAmount: 1, UsdMarketValue: 1}, MintAccount: "mint", InitiatorOwnerAccount: "user", State: intent.StateConfirmed},

			{IntentId: "i6", IntentType: intent.ReceivePaymentsPublicly, ReceivePaymentsPubliclyMetadata: &intent.ReceivePaymentsPubliclyMetadata{IsIndirectSend: true, Source: "a4", Quantity: 1, OriginalExchangeCurrency: currency.USD, OriginalExchangeRate: 1, OriginalNativeAmount: 1, UsdMarketValue: 1}, MintAccount: "mint", InitiatorOwnerAccount: "user", State: intent.StateRevoked},
			{IntentId: "i7", IntentType: intent.ReceivePaymentsPublicly, ReceivePaymentsPubliclyMetadata: &intent.ReceivePaymentsPubliclyMetadata{IsIndirectSend: true, Source: "a4", Quantity: 1, OriginalExchangeCurrency: currency.USD, OriginalExchangeRate: 1, OriginalNativeAmount: 1, UsdMarketValue: 1}, MintAccount: "mint", InitiatorOwnerAccount: "user", State: intent.StatePending},
		}

		for _, record := range records {
			require.NoError(t, s.Save(ctx, record))
		}

		_, err := s.GetGiftCardClaimedIntent(ctx, "unknown")
		assert.Equal(t, intent.ErrIntentNotFound, err)

		_, err = s.GetGiftCardClaimedIntent(ctx, "a1")
		assert.Equal(t, intent.ErrIntentNotFound, err)

		actual, err := s.GetGiftCardClaimedIntent(ctx, "a2")
		require.NoError(t, err)
		assert.Equal(t, "i3", actual.IntentId)

		_, err = s.GetGiftCardClaimedIntent(ctx, "a3")
		assert.Equal(t, intent.ErrMultilpeIntentsFound, err)

		actual, err = s.GetGiftCardClaimedIntent(ctx, "a4")
		require.NoError(t, err)
		assert.Equal(t, "i7", actual.IntentId)
	})
}

func testGetTransactedAmountForAntiMoneyLaundering(t *testing.T, s intent.Store) {
	t.Run("testGetTransactedAmountForAntiMoneyLaundering", func(t *testing.T) {
		ctx := context.Background()

		// No intents results in zero transacted values
		quarks, usdMarketValue, err := s.GetTransactedAmountForAntiMoneyLaundering(ctx, "o1", time.Now().Add(-24*time.Hour))
		require.NoError(t, err)
		assert.EqualValues(t, 0, quarks)
		assert.EqualValues(t, 0, usdMarketValue)

		records := []*intent.Record{
			{IntentId: "t1", IntentType: intent.SendPublicPayment, InitiatorOwnerAccount: "o1", SendPublicPaymentMetadata: &intent.SendPublicPaymentMetadata{DestinationOwnerAccount: "o1", DestinationTokenAccount: "a1", Quantity: 1, ExchangeCurrency: currency.USD, ExchangeRate: 2, NativeAmount: 2, UsdMarketValue: 2}, State: intent.StateUnknown, MintAccount: "mint", CreatedAt: time.Now().Add(-1 * time.Minute)},
			{IntentId: "t2", IntentType: intent.SendPublicPayment, InitiatorOwnerAccount: "o1", SendPublicPaymentMetadata: &intent.SendPublicPaymentMetadata{DestinationOwnerAccount: "o2", DestinationTokenAccount: "a2", Quantity: 10, ExchangeCurrency: currency.USD, ExchangeRate: 2, NativeAmount: 20, UsdMarketValue: 20}, State: intent.StatePending, MintAccount: "mint", CreatedAt: time.Now().Add(-2 * time.Minute)},
			{IntentId: "t3", IntentType: intent.SendPublicPayment, InitiatorOwnerAccount: "o1", SendPublicPaymentMetadata: &intent.SendPublicPaymentMetadata{DestinationOwnerAccount: "o3", DestinationTokenAccount: "a3", Quantity: 100, ExchangeCurrency: currency.USD, ExchangeRate: 2, NativeAmount: 200, UsdMarketValue: 200}, State: intent.StateConfirmed, MintAccount: "mint", CreatedAt: time.Now().Add(-3 * time.Minute)},
			{IntentId: "t4", IntentType: intent.SendPublicPayment, InitiatorOwnerAccount: "o1", SendPublicPaymentMetadata: &intent.SendPublicPaymentMetadata{DestinationOwnerAccount: "o4", DestinationTokenAccount: "a4", Quantity: 1000, ExchangeCurrency: currency.USD, ExchangeRate: 2, NativeAmount: 2000, UsdMarketValue: 2000}, State: intent.StateFailed, MintAccount: "mint", CreatedAt: time.Now().Add(-4 * time.Minute)},
			{IntentId: "t5", IntentType: intent.SendPublicPayment, InitiatorOwnerAccount: "o1", SendPublicPaymentMetadata: &intent.SendPublicPaymentMetadata{DestinationOwnerAccount: "o5", DestinationTokenAccount: "a5", Quantity: 10000, ExchangeCurrency: currency.USD, ExchangeRate: 2, NativeAmount: 20000, UsdMarketValue: 20000}, State: intent.StateRevoked, MintAccount: "mint", CreatedAt: time.Now().Add(-5 * time.Minute)},
			{IntentId: "t6", IntentType: intent.ReceivePaymentsPublicly, InitiatorOwnerAccount: "o1", ReceivePaymentsPubliclyMetadata: &intent.ReceivePaymentsPubliclyMetadata{Source: "a6", Quantity: 100000, UsdMarketValue: 200000, OriginalExchangeCurrency: currency.USD, OriginalExchangeRate: 2, OriginalNativeAmount: 200000}, State: intent.StateConfirmed, MintAccount: "mint", CreatedAt: time.Now()},
			{IntentId: "t7", IntentType: intent.ExternalDeposit, InitiatorOwnerAccount: "o1", ExternalDepositMetadata: &intent.ExternalDepositMetadata{DestinationTokenAccount: "a7", ExchangeCurrency: currency.USD, ExchangeRate: 1000000, NativeAmount: 1000000, Quantity: 1000000, UsdMarketValue: 20000}, MintAccount: "mint", State: intent.StateConfirmed, CreatedAt: time.Now()},
			{IntentId: "t8", IntentType: intent.SendPublicPayment, InitiatorOwnerAccount: "o1", SendPublicPaymentMetadata: &intent.SendPublicPaymentMetadata{DestinationOwnerAccount: "o8", DestinationTokenAccount: "a8", Quantity: 10000000, ExchangeCurrency: currency.USD, ExchangeRate: 2, NativeAmount: 2000000, UsdMarketValue: 2000000, IsWithdrawal: true}, State: intent.StateConfirmed, MintAccount: "mint", CreatedAt: time.Now()},
			{IntentId: "t9", IntentType: intent.SendPublicMultiPayment, InitiatorOwnerAccount: "o1", SendPublicMultiPaymentMetadata: &intent.SendPublicMultiPaymentMetadata{Source: "a0", Payments: []*intent.Payment{{DestinationOwnerAccount: "o9", DestinationTokenAccount: "a9", Quantity: 40000, NativeAmount: 80000, UsdMarketValue: 80000}, {DestinationOwnerAccount: "o10", DestinationTokenAccount: "a10", Quantity: 60000, NativeAmount: 120000, UsdMarketValue: 120000}}, Quantity: 100000, ExchangeCurrency: currency.USD, ExchangeRate: 2, NativeAmount: 200000, UsdMarketValue: 200000}, State: intent.StatePending, MintAccount: "mint", CreatedAt: time.Now().Add(-1 * time.Minute)},
			{IntentId: "t10", IntentType: intent.SendPublicMultiPayment, InitiatorOwnerAccount: "o1", SendPublicMultiPaymentMetadata: &intent.SendPublicMultiPaymentMetadata{Source: "a0", Payments: []*intent.Payment{{DestinationOwnerAccount: "o9", DestinationTokenAccount: "a9", Quantity: 1, NativeAmount: 2, UsdMarketValue: 2}, {DestinationOwnerAccount: "o10", DestinationTokenAccount: "a10", Quantity: 1, NativeAmount: 2, UsdMarketValue: 2}}, Quantity: 2, ExchangeCurrency: currency.USD, ExchangeRate: 2, NativeAmount: 4, UsdMarketValue: 4}, State: intent.StateRevoked, MintAccount: "mint", CreatedAt: time.Now()},
			{IntentId: "t11", IntentType: intent.SendPublicMultiPayment, InitiatorOwnerAccount: "o2", SendPublicMultiPaymentMetadata: &intent.SendPublicMultiPaymentMetadata{Source: "a2", Payments: []*intent.Payment{{DestinationOwnerAccount: "o1", DestinationTokenAccount: "a1", Quantity: 1, NativeAmount: 2, UsdMarketValue: 2}, {DestinationOwnerAccount: "o10", DestinationTokenAccount: "a10", Quantity: 1, NativeAmount: 2, UsdMarketValue: 2}}, Quantity: 2, ExchangeCurrency: currency.USD, ExchangeRate: 2, NativeAmount: 4, UsdMarketValue: 4}, State: intent.StateConfirmed, MintAccount: "mint", CreatedAt: time.Now().Add(-48 * time.Hour)},
		}

		for _, record := range records {
			require.NoError(t, s.Save(ctx, record))
		}

		// Capture all intents for the owner
		quarks, usdMarketValue, err = s.GetTransactedAmountForAntiMoneyLaundering(ctx, "o1", time.Now().Add(-24*time.Hour))
		require.NoError(t, err)
		assert.EqualValues(t, 101111, quarks)
		assert.EqualValues(t, 202222, usdMarketValue)

		// Capture a subset of intents based on time
		quarks, usdMarketValue, err = s.GetTransactedAmountForAntiMoneyLaundering(ctx, "o1", time.Now().Add(-150*time.Second))
		require.NoError(t, err)
		assert.EqualValues(t, 100011, quarks)
		assert.EqualValues(t, 200022, usdMarketValue)

		// Capture no intents because the owner mismatches, or the owner's
		// intents are outside the time window
		quarks, usdMarketValue, err = s.GetTransactedAmountForAntiMoneyLaundering(ctx, "o2", time.Now().Add(-24*time.Hour))
		require.NoError(t, err)
		assert.EqualValues(t, 0, quarks)
		assert.EqualValues(t, 0, usdMarketValue)
	})
}

func testGetByOwner(t *testing.T, s intent.Store) {
	t.Run("testGetByOwner", func(t *testing.T) {
		ctx := context.Background()

		_, err := s.GetAllByOwner(ctx, "o1", query.EmptyCursor, 100, query.Ascending)
		assert.Equal(t, intent.ErrIntentNotFound, err)

		records := []*intent.Record{
			{IntentId: "t1", IntentType: intent.OpenAccounts, InitiatorOwnerAccount: "o1", OpenAccountsMetadata: &intent.OpenAccountsMetadata{}, MintAccount: "mint", State: intent.StateConfirmed, CreatedAt: time.Now()},
			{IntentId: "t2", IntentType: intent.SendPublicPayment, InitiatorOwnerAccount: "o1", SendPublicPaymentMetadata: &intent.SendPublicPaymentMetadata{DestinationOwnerAccount: "o1", DestinationTokenAccount: "a1", Quantity: 1, ExchangeCurrency: currency.USD, ExchangeRate: 1, NativeAmount: 1, UsdMarketValue: 1}, MintAccount: "mint", State: intent.StateConfirmed, CreatedAt: time.Now()},
			{IntentId: "t3", IntentType: intent.SendPublicPayment, InitiatorOwnerAccount: "o1", SendPublicPaymentMetadata: &intent.SendPublicPaymentMetadata{DestinationOwnerAccount: "o2", DestinationTokenAccount: "a2", Quantity: 10, ExchangeCurrency: currency.USD, ExchangeRate: 10, NativeAmount: 10, UsdMarketValue: 10}, MintAccount: "mint", State: intent.StatePending, CreatedAt: time.Now()},
			{IntentId: "t4", IntentType: intent.PublicDistribution, InitiatorOwnerAccount: "o1", PublicDistributionMetadata: &intent.PublicDistributionMetadata{Source: "p1", Distributions: []*intent.Distribution{{DestinationOwnerAccount: "o1", DestinationTokenAccount: "a1", Quantity: 5}, {DestinationOwnerAccount: "o2", DestinationTokenAccount: "a2", Quantity: 5}}, UsdMarketValue: 10, Quantity: 10}, MintAccount: "mint", State: intent.StatePending, CreatedAt: time.Now()},
			{IntentId: "t5", IntentType: intent.SendPublicPayment, InitiatorOwnerAccount: "o1", SendPublicPaymentMetadata: &intent.SendPublicPaymentMetadata{DestinationOwnerAccount: "o2", DestinationTokenAccount: "a2", Quantity: 100, ExchangeCurrency: currency.USD, ExchangeRate: 100, NativeAmount: 100, UsdMarketValue: 100}, MintAccount: "mint", State: intent.StateFailed, CreatedAt: time.Now()},
			{IntentId: "t6", IntentType: intent.SendPublicPayment, InitiatorOwnerAccount: "o1", SendPublicPaymentMetadata: &intent.SendPublicPaymentMetadata{DestinationOwnerAccount: "o2", DestinationTokenAccount: "a2", Quantity: 1000, ExchangeCurrency: currency.USD, ExchangeRate: 1000, NativeAmount: 1000, UsdMarketValue: 1000}, MintAccount: "mint", State: intent.StateUnknown, CreatedAt: time.Now()},
			{IntentId: "t7", IntentType: intent.PublicDistribution, InitiatorOwnerAccount: "o1", PublicDistributionMetadata: &intent.PublicDistributionMetadata{Source: "p2", Distributions: []*intent.Distribution{{DestinationOwnerAccount: "o2", DestinationTokenAccount: "a2", Quantity: 10}}, UsdMarketValue: 10, Quantity: 10}, MintAccount: "mint", State: intent.StatePending, CreatedAt: time.Now()},
		}
		for _, record := range records {
			require.NoError(t, s.Save(ctx, record))
		}

		expected := records
		actual, err := s.GetAllByOwner(ctx, "o1", query.EmptyCursor, 100, query.Ascending)
		require.NoError(t, err)
		require.Len(t, actual, 7)
		for i, record := range expected {
			assert.Equal(t, record.IntentId, actual[i].IntentId)
		}

		expected = records[2:]
		actual, err = s.GetAllByOwner(ctx, "o2", query.EmptyCursor, 100, query.Ascending)
		require.NoError(t, err)
		require.Len(t, actual, 5)
		for i, record := range expected {
			assert.Equal(t, record.IntentId, actual[i].IntentId)
		}

		expected = records[:5]
		actual, err = s.GetAllByOwner(ctx, "o1", query.EmptyCursor, 5, query.Ascending)
		require.NoError(t, err)
		require.Len(t, actual, 5)
		for i, record := range expected {
			assert.Equal(t, record.IntentId, actual[i].IntentId)
		}

		expected = records[2:]
		actual, err = s.GetAllByOwner(ctx, "o1", query.ToCursor(records[1].Id), 100, query.Ascending)
		require.NoError(t, err)
		require.Len(t, actual, 5)
		for i, record := range expected {
			assert.Equal(t, record.IntentId, actual[i].IntentId)
		}

		expected = records[3:]
		actual, err = s.GetAllByOwner(ctx, "o1", query.ToCursor(records[2].Id), 100, query.Ascending)
		require.NoError(t, err)
		require.Len(t, actual, 4)
		for i, record := range expected {
			assert.Equal(t, record.IntentId, actual[i].IntentId)
		}

		expected = records[5:]
		actual, err = s.GetAllByOwner(ctx, "o2", query.ToCursor(records[4].Id), 100, query.Ascending)
		require.NoError(t, err)
		require.Len(t, actual, 2)
		for i, record := range expected {
			assert.Equal(t, record.IntentId, actual[i].IntentId)
		}

		expected = records
		actual, err = s.GetAllByOwner(ctx, "o1", query.EmptyCursor, 100, query.Descending)
		require.NoError(t, err)
		require.Len(t, actual, 7)
		for i, record := range expected {
			assert.Equal(t, record.IntentId, actual[len(actual)-i-1].IntentId)
		}

		expected = records[:5]
		actual, err = s.GetAllByOwner(ctx, "o1", query.ToCursor(records[5].Id), 100, query.Descending)
		require.NoError(t, err)
		require.Len(t, actual, 5)
		for i, record := range expected {
			assert.Equal(t, record.IntentId, actual[len(actual)-i-1].IntentId)
		}
	})
}

func testGetByOwnerWithSendPublicMultiPayment(t *testing.T, s intent.Store) {
	t.Run("testGetByOwnerWithSendPublicMultiPayment", func(t *testing.T) {
		ctx := context.Background()

		records := []*intent.Record{
			{IntentId: "t1", IntentType: intent.SendPublicMultiPayment, InitiatorOwnerAccount: "o1", SendPublicMultiPaymentMetadata: &intent.SendPublicMultiPaymentMetadata{Source: "a1", Payments: []*intent.Payment{{DestinationOwnerAccount: "o2", DestinationTokenAccount: "a2", Quantity: 1, NativeAmount: 1, UsdMarketValue: 1}, {DestinationOwnerAccount: "o3", DestinationTokenAccount: "a3", Quantity: 2, NativeAmount: 2, UsdMarketValue: 2}}, Quantity: 3, ExchangeCurrency: currency.USD, ExchangeRate: 1, NativeAmount: 3, UsdMarketValue: 3}, MintAccount: "mint", State: intent.StatePending, CreatedAt: time.Now()},
			{IntentId: "t2", IntentType: intent.SendPublicPayment, InitiatorOwnerAccount: "o3", SendPublicPaymentMetadata: &intent.SendPublicPaymentMetadata{DestinationOwnerAccount: "o1", DestinationTokenAccount: "a1", Quantity: 1, ExchangeCurrency: currency.USD, ExchangeRate: 1, NativeAmount: 1, UsdMarketValue: 1}, MintAccount: "mint", State: intent.StateConfirmed, CreatedAt: time.Now()},
			{IntentId: "t3", IntentType: intent.SendPublicMultiPayment, InitiatorOwnerAccount: "o2", SendPublicMultiPaymentMetadata: &intent.SendPublicMultiPaymentMetadata{Source: "a2", Payments: []*intent.Payment{{DestinationOwnerAccount: "o3", DestinationTokenAccount: "a3", Quantity: 1, NativeAmount: 1, UsdMarketValue: 1}, {DestinationOwnerAccount: "o4", DestinationTokenAccount: "a4", Quantity: 1, NativeAmount: 1, UsdMarketValue: 1}}, Quantity: 2, ExchangeCurrency: currency.USD, ExchangeRate: 1, NativeAmount: 2, UsdMarketValue: 2}, MintAccount: "mint", State: intent.StateConfirmed, CreatedAt: time.Now()},
		}
		for _, record := range records {
			require.NoError(t, s.Save(ctx, record))
		}

		for _, tc := range []struct {
			owner    string
			expected []string
		}{
			{"o1", []string{"t1", "t2"}},
			{"o2", []string{"t1", "t3"}},
			{"o3", []string{"t1", "t2", "t3"}},
			{"o4", []string{"t3"}},
		} {
			actual, err := s.GetAllByOwner(ctx, tc.owner, query.EmptyCursor, 100, query.Ascending)
			require.NoError(t, err)
			require.Len(t, actual, len(tc.expected), tc.owner)
			for i, intentId := range tc.expected {
				assert.Equal(t, intentId, actual[i].IntentId)
			}
			for _, record := range actual {
				if record.IntentType == intent.SendPublicMultiPayment {
					require.NotNil(t, record.SendPublicMultiPaymentMetadata)
					assert.Len(t, record.SendPublicMultiPaymentMetadata.Payments, 2)
				}
			}
		}

		actual, err := s.GetAllByOwner(ctx, "o3", query.ToCursor(records[0].Id), 100, query.Ascending)
		require.NoError(t, err)
		require.Len(t, actual, 2)
		assert.Equal(t, "t2", actual[0].IntentId)
		assert.Equal(t, "t3", actual[1].IntentId)

		_, err = s.GetAllByOwner(ctx, "o5", query.EmptyCursor, 100, query.Ascending)
		assert.Equal(t, intent.ErrIntentNotFound, err)
	})
}

func testGetUsdCostBasis(t *testing.T, s intent.Store) {
	t.Run("testGetUsdCostBasis", func(t *testing.T) {
		ctx := context.Background()

		// No intents results in zero cost basis
		costBasis, err := s.GetUsdCostBasis(ctx, "o1", "mint")
		require.NoError(t, err)
		assert.EqualValues(t, 0, costBasis)

		records := []*intent.Record{
			// ExternalDeposit: o1 receives +100 USD
			{IntentId: "t1", IntentType: intent.ExternalDeposit, InitiatorOwnerAccount: "o1", ExternalDepositMetadata: &intent.ExternalDepositMetadata{DestinationTokenAccount: "a1", Quantity: 1000, ExchangeCurrency: currency.USD, ExchangeRate: 0.1, NativeAmount: 100, UsdMarketValue: 100}, MintAccount: "mint", State: intent.StateConfirmed},
			// ReceivePaymentsPublicly: o1 receives +50 USD
			{IntentId: "t2", IntentType: intent.ReceivePaymentsPublicly, InitiatorOwnerAccount: "o1", ReceivePaymentsPubliclyMetadata: &intent.ReceivePaymentsPubliclyMetadata{Source: "s1", Quantity: 500, OriginalExchangeCurrency: currency.USD, OriginalExchangeRate: 0.1, OriginalNativeAmount: 50, UsdMarketValue: 50}, MintAccount: "mint", State: intent.StateConfirmed},
			// SendPublicPayment: o1 sends -30 USD to o2
			{IntentId: "t3", IntentType: intent.SendPublicPayment, InitiatorOwnerAccount: "o1", SendPublicPaymentMetadata: &intent.SendPublicPaymentMetadata{DestinationOwnerAccount: "o2", DestinationTokenAccount: "a2", Quantity: 300, ExchangeCurrency: currency.USD, ExchangeRate: 0.1, NativeAmount: 30, UsdMarketValue: 30}, MintAccount: "mint", State: intent.StateConfirmed},
			// SendPublicPayment: o2 sends to o1, o1 receives +20 USD
			{IntentId: "t4", IntentType: intent.SendPublicPayment, InitiatorOwnerAccount: "o2", SendPublicPaymentMetadata: &intent.SendPublicPaymentMetadata{DestinationOwnerAccount: "o1", DestinationTokenAccount: "a1", Quantity: 200, ExchangeCurrency: currency.USD, ExchangeRate: 0.1, NativeAmount: 20, UsdMarketValue: 20}, MintAccount: "mint", State: intent.StateConfirmed},
			// Revoked intent should be ignored
			{IntentId: "t5", IntentType: intent.ExternalDeposit, InitiatorOwnerAccount: "o1", ExternalDepositMetadata: &intent.ExternalDepositMetadata{DestinationTokenAccount: "a1", Quantity: 10000, ExchangeCurrency: currency.USD, ExchangeRate: 0.1, NativeAmount: 1000, UsdMarketValue: 1000}, MintAccount: "mint", State: intent.StateRevoked},
			// Different mint should not be included
			{IntentId: "t6", IntentType: intent.ExternalDeposit, InitiatorOwnerAccount: "o1", ExternalDepositMetadata: &intent.ExternalDepositMetadata{DestinationTokenAccount: "a1", Quantity: 5000, ExchangeCurrency: currency.USD, ExchangeRate: 0.1, NativeAmount: 500, UsdMarketValue: 500}, MintAccount: "other_mint", State: intent.StateConfirmed},
			// SendPublicPayment to self: o1 sends and receives, net 0 for this transaction
			{IntentId: "t7", IntentType: intent.SendPublicPayment, InitiatorOwnerAccount: "o1", SendPublicPaymentMetadata: &intent.SendPublicPaymentMetadata{DestinationOwnerAccount: "o1", DestinationTokenAccount: "a1", Quantity: 100, ExchangeCurrency: currency.USD, ExchangeRate: 0.1, NativeAmount: 10, UsdMarketValue: 10}, MintAccount: "mint", State: intent.StateConfirmed},
			// SendPublicMultiPayment: o2 sends -20 USD, o1 receives +15 USD and o3 receives +5 USD
			{IntentId: "t8", IntentType: intent.SendPublicMultiPayment, InitiatorOwnerAccount: "o2", SendPublicMultiPaymentMetadata: &intent.SendPublicMultiPaymentMetadata{Source: "a2", Payments: []*intent.Payment{{DestinationOwnerAccount: "o1", DestinationTokenAccount: "a1", Quantity: 150, NativeAmount: 15, UsdMarketValue: 15}, {DestinationOwnerAccount: "o3", DestinationTokenAccount: "a3", Quantity: 50, NativeAmount: 5, UsdMarketValue: 5}}, Quantity: 200, ExchangeCurrency: currency.USD, ExchangeRate: 0.1, NativeAmount: 20, UsdMarketValue: 20}, MintAccount: "mint", State: intent.StateConfirmed},
			// Revoked SendPublicMultiPayment should be ignored
			{IntentId: "t9", IntentType: intent.SendPublicMultiPayment, InitiatorOwnerAccount: "o2", SendPublicMultiPaymentMetadata: &intent.SendPublicMultiPaymentMetadata{Source: "a2", Payments: []*intent.Payment{{DestinationOwnerAccount: "o1", DestinationTokenAccount: "a1", Quantity: 1000, NativeAmount: 100, UsdMarketValue: 100}, {DestinationOwnerAccount: "o3", DestinationTokenAccount: "a3", Quantity: 1000, NativeAmount: 100, UsdMarketValue: 100}}, Quantity: 2000, ExchangeCurrency: currency.USD, ExchangeRate: 0.1, NativeAmount: 200, UsdMarketValue: 200}, MintAccount: "mint", State: intent.StateRevoked},
			// SendPublicMultiPayment on a different mint should not be included
			{IntentId: "t10", IntentType: intent.SendPublicMultiPayment, InitiatorOwnerAccount: "o2", SendPublicMultiPaymentMetadata: &intent.SendPublicMultiPaymentMetadata{Source: "a2", Payments: []*intent.Payment{{DestinationOwnerAccount: "o1", DestinationTokenAccount: "a1", Quantity: 1000, NativeAmount: 100, UsdMarketValue: 100}, {DestinationOwnerAccount: "o3", DestinationTokenAccount: "a3", Quantity: 1000, NativeAmount: 100, UsdMarketValue: 100}}, Quantity: 2000, ExchangeCurrency: currency.USD, ExchangeRate: 0.1, NativeAmount: 200, UsdMarketValue: 200}, MintAccount: "other_mint", State: intent.StateConfirmed},
		}

		for _, record := range records {
			require.NoError(t, s.Save(ctx, record))
		}

		// o1 cost basis: +100 (deposit) +50 (receive) -30 (send to o2) +20 (receive from o2) -10 (send to self) +10 (receive from self) +15 (multi payment from o2) = 155
		costBasis, err = s.GetUsdCostBasis(ctx, "o1", "mint")
		require.NoError(t, err)
		assert.EqualValues(t, 155, costBasis)

		// o2 cost basis: +30 (receive from o1) -20 (send to o1) -20 (multi payment to o1 and o3) = -10
		costBasis, err = s.GetUsdCostBasis(ctx, "o2", "mint")
		require.NoError(t, err)
		assert.EqualValues(t, -10, costBasis)

		// o3 cost basis: +5 (multi payment from o2) = 5
		costBasis, err = s.GetUsdCostBasis(ctx, "o3", "mint")
		require.NoError(t, err)
		assert.EqualValues(t, 5, costBasis)

		// o1 cost basis for other_mint: +500 (deposit) +100 (multi payment from o2) = 600
		costBasis, err = s.GetUsdCostBasis(ctx, "o1", "other_mint")
		require.NoError(t, err)
		assert.EqualValues(t, 600, costBasis)

		// Unknown owner has zero cost basis
		costBasis, err = s.GetUsdCostBasis(ctx, "unknown", "mint")
		require.NoError(t, err)
		assert.EqualValues(t, 0, costBasis)
	})
}

func testGetUsdCostBasisBatch(t *testing.T, s intent.Store) {
	t.Run("testGetUsdCostBasisBatch", func(t *testing.T) {
		ctx := context.Background()

		// Empty batch returns empty map
		res, err := s.GetUsdCostBasisBatch(ctx, "mint")
		require.NoError(t, err)
		require.Empty(t, res)

		records := []*intent.Record{
			// ExternalDeposit: o1 receives +100 USD
			{IntentId: "t1", IntentType: intent.ExternalDeposit, InitiatorOwnerAccount: "o1", ExternalDepositMetadata: &intent.ExternalDepositMetadata{DestinationTokenAccount: "a1", Quantity: 1000, ExchangeCurrency: currency.USD, ExchangeRate: 0.1, NativeAmount: 100, UsdMarketValue: 100}, MintAccount: "mint", State: intent.StateConfirmed},
			// ReceivePaymentsPublicly: o1 receives +50 USD
			{IntentId: "t2", IntentType: intent.ReceivePaymentsPublicly, InitiatorOwnerAccount: "o1", ReceivePaymentsPubliclyMetadata: &intent.ReceivePaymentsPubliclyMetadata{Source: "s1", Quantity: 500, OriginalExchangeCurrency: currency.USD, OriginalExchangeRate: 0.1, OriginalNativeAmount: 50, UsdMarketValue: 50}, MintAccount: "mint", State: intent.StateConfirmed},
			// SendPublicPayment: o1 sends -30 USD to o2
			{IntentId: "t3", IntentType: intent.SendPublicPayment, InitiatorOwnerAccount: "o1", SendPublicPaymentMetadata: &intent.SendPublicPaymentMetadata{DestinationOwnerAccount: "o2", DestinationTokenAccount: "a2", Quantity: 300, ExchangeCurrency: currency.USD, ExchangeRate: 0.1, NativeAmount: 30, UsdMarketValue: 30}, MintAccount: "mint", State: intent.StateConfirmed},
			// SendPublicPayment: o2 sends to o1, o1 receives +20 USD
			{IntentId: "t4", IntentType: intent.SendPublicPayment, InitiatorOwnerAccount: "o2", SendPublicPaymentMetadata: &intent.SendPublicPaymentMetadata{DestinationOwnerAccount: "o1", DestinationTokenAccount: "a1", Quantity: 200, ExchangeCurrency: currency.USD, ExchangeRate: 0.1, NativeAmount: 20, UsdMarketValue: 20}, MintAccount: "mint", State: intent.StateConfirmed},
			// Revoked intent should be ignored
			{IntentId: "t5", IntentType: intent.ExternalDeposit, InitiatorOwnerAccount: "o1", ExternalDepositMetadata: &intent.ExternalDepositMetadata{DestinationTokenAccount: "a1", Quantity: 10000, ExchangeCurrency: currency.USD, ExchangeRate: 0.1, NativeAmount: 1000, UsdMarketValue: 1000}, MintAccount: "mint", State: intent.StateRevoked},
			// SendPublicPayment to self: o1 sends and receives, net 0 for this transaction
			{IntentId: "t7", IntentType: intent.SendPublicPayment, InitiatorOwnerAccount: "o1", SendPublicPaymentMetadata: &intent.SendPublicPaymentMetadata{DestinationOwnerAccount: "o1", DestinationTokenAccount: "a1", Quantity: 100, ExchangeCurrency: currency.USD, ExchangeRate: 0.1, NativeAmount: 10, UsdMarketValue: 10}, MintAccount: "mint", State: intent.StateConfirmed},
			// SendPublicMultiPayment: o2 sends -20 USD, o1 receives +15 USD and o3 receives +5 USD
			{IntentId: "t8", IntentType: intent.SendPublicMultiPayment, InitiatorOwnerAccount: "o2", SendPublicMultiPaymentMetadata: &intent.SendPublicMultiPaymentMetadata{Source: "a2", Payments: []*intent.Payment{{DestinationOwnerAccount: "o1", DestinationTokenAccount: "a1", Quantity: 150, NativeAmount: 15, UsdMarketValue: 15}, {DestinationOwnerAccount: "o3", DestinationTokenAccount: "a3", Quantity: 50, NativeAmount: 5, UsdMarketValue: 5}}, Quantity: 200, ExchangeCurrency: currency.USD, ExchangeRate: 0.1, NativeAmount: 20, UsdMarketValue: 20}, MintAccount: "mint", State: intent.StateConfirmed},
			// Revoked SendPublicMultiPayment should be ignored
			{IntentId: "t9", IntentType: intent.SendPublicMultiPayment, InitiatorOwnerAccount: "o2", SendPublicMultiPaymentMetadata: &intent.SendPublicMultiPaymentMetadata{Source: "a2", Payments: []*intent.Payment{{DestinationOwnerAccount: "o1", DestinationTokenAccount: "a1", Quantity: 1000, NativeAmount: 100, UsdMarketValue: 100}, {DestinationOwnerAccount: "o3", DestinationTokenAccount: "a3", Quantity: 1000, NativeAmount: 100, UsdMarketValue: 100}}, Quantity: 2000, ExchangeCurrency: currency.USD, ExchangeRate: 0.1, NativeAmount: 200, UsdMarketValue: 200}, MintAccount: "mint", State: intent.StateRevoked},
			// SendPublicMultiPayment on a different mint should not be included
			{IntentId: "t10", IntentType: intent.SendPublicMultiPayment, InitiatorOwnerAccount: "o2", SendPublicMultiPaymentMetadata: &intent.SendPublicMultiPaymentMetadata{Source: "a2", Payments: []*intent.Payment{{DestinationOwnerAccount: "o1", DestinationTokenAccount: "a1", Quantity: 1000, NativeAmount: 100, UsdMarketValue: 100}, {DestinationOwnerAccount: "o3", DestinationTokenAccount: "a3", Quantity: 1000, NativeAmount: 100, UsdMarketValue: 100}}, Quantity: 2000, ExchangeCurrency: currency.USD, ExchangeRate: 0.1, NativeAmount: 200, UsdMarketValue: 200}, MintAccount: "other_mint", State: intent.StateConfirmed},
		}

		for _, record := range records {
			require.NoError(t, s.Save(ctx, record))
		}

		// Batch query for o1, o2, o3, and unknown owner
		res, err = s.GetUsdCostBasisBatch(ctx, "mint", "o1", "o2", "o3", "unknown")
		require.NoError(t, err)
		require.Len(t, res, 4)

		// o1 cost basis: +100 (deposit) +50 (receive) -30 (send to o2) +20 (receive from o2) -10 (send to self) +10 (receive from self) +15 (multi payment from o2) = 155
		assert.EqualValues(t, 155, res["o1"])

		// o2 cost basis: +30 (receive from o1) -20 (send to o1) -20 (multi payment to o1 and o3) = -10
		assert.EqualValues(t, -10, res["o2"])

		// o3 cost basis: +5 (multi payment from o2) = 5
		assert.EqualValues(t, 5, res["o3"])

		// o1 cost basis for other_mint: +100 (multi payment from o2) = 100
		res, err = s.GetUsdCostBasisBatch(ctx, "other_mint", "o1")
		require.NoError(t, err)
		assert.EqualValues(t, 100, res["o1"])

		// Unknown owner has zero cost basis
		assert.EqualValues(t, 0, res["unknown"])
	})
}
