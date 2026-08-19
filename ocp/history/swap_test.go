package history

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/code-payments/ocp-server/ocp/common"
	ocp_data "github.com/code-payments/ocp-server/ocp/data"
	"github.com/code-payments/ocp-server/ocp/data/history"
	"github.com/code-payments/ocp-server/ocp/data/swap"
	"github.com/code-payments/ocp-server/pointer"
	"github.com/code-payments/ocp-server/testutil"
)

// testLaunchTerms mirrors the swap worker's default launch amounts: $10 buys
// the currency's initial supply and $10 is the fee for creating it.
var testLaunchTerms = &CurrencyLaunchTerms{
	PurchaseQuarks: 10 * common.CoreMintQuarksPerUnit,
	FeeQuarks:      10 * common.CoreMintQuarksPerUnit,
}

func TestBuildRecordForFundedReserveSwap_Buy(t *testing.T) {
	swapRecord := newReserveSwapRecord()

	record, err := BuildRecordForFundedSwap(swapRecord, "usd", 10.1, 1.0, nil)
	require.NoError(t, err)

	assert.Equal(t, swapRecord.SwapId, record.ReferenceId)
	assert.Equal(t, history.Swap, record.Type)
	assert.Equal(t, swapRecord.Owner, record.OwnerAccount)
	assert.Nil(t, record.CounterpartyOwnerAccount)
	assert.EqualValues(t, "usd", record.ExchangeCurrency)
	assert.Equal(t, 10.1, record.NativeAmount)
	assert.Equal(t, swapRecord.FromMint, record.MintAccount)
	assert.Equal(t, swapRecord.SwapAmount+swapRecord.FeeAmount, record.Quantity)
	require.NotNil(t, record.DestinationMintAccount)
	assert.Equal(t, swapRecord.ToMint, *record.DestinationMintAccount)
	assert.Nil(t, record.DestinationQuantity)
	assert.Equal(t, history.StatePending, record.State)
	assert.Equal(t, swapRecord.CreatedAt, record.CreatedAt)

	// A buy funded with the core mint pays the protocol buy fee
	require.Len(t, record.Fees, 1)
	assert.Equal(t, history.ReserveBuyFee, record.Fees[0].Type)
	assert.InDelta(t, 0.1, record.Fees[0].NativeAmount, 0.0001)

	assert.NoError(t, record.Validate())
}

func TestBuildRecordForFundedReserveSwap_LegacyBuyWithoutFee(t *testing.T) {
	swapRecord := newReserveSwapRecord()
	swapRecord.FeeAmount = 0

	record, err := BuildRecordForFundedSwap(swapRecord, "usd", 10.0, 1.0, nil)
	require.NoError(t, err)
	assert.Empty(t, record.Fees)
	assert.Equal(t, swapRecord.SwapAmount, record.Quantity)
	assert.NoError(t, record.Validate())
}

func TestBuildRecordForFundedReserveSwap_Sell(t *testing.T) {
	swapRecord := newReserveSwapRecord()
	swapRecord.FromMint = testutil.NewRandomAccount(t).PublicKey().ToBase58()
	swapRecord.ToMint = common.CoreMintAccount.PublicKey().ToBase58()
	swapRecord.FeeAmount = 0

	record, err := BuildRecordForFundedSwap(swapRecord, "usd", 10.0, 1.0, nil)
	require.NoError(t, err)

	// Selling a launchpad currency incurs the pool's sell fee
	require.Len(t, record.Fees, 1)
	assert.Equal(t, history.ReserveSellFee, record.Fees[0].Type)
	assert.InDelta(t, 0.1, record.Fees[0].NativeAmount, 0.0001)

	assert.NoError(t, record.Validate())
}

func TestBuildRecordForFundedReserveSwap_CurrencyLaunchWithCoreMint(t *testing.T) {
	// $10 buys the currency's initial supply and $10 is the launch fee
	swapRecord := newReserveSwapRecord()
	swapRecord.SwapAmount = 10 * common.CoreMintQuarksPerUnit
	swapRecord.FeeAmount = 10 * common.CoreMintQuarksPerUnit

	record, err := BuildRecordForFundedSwap(swapRecord, "usd", 20.0, 1.0, testLaunchTerms)
	require.NoError(t, err)

	// The fee creates the currency, so it is a launch fee rather than the
	// percentage a buy is charged
	require.Len(t, record.Fees, 1)
	assert.Equal(t, history.CurrencyLaunchFee, record.Fees[0].Type)
	assert.InDelta(t, 10.0, record.Fees[0].NativeAmount, 0.0001)

	assert.NoError(t, record.Validate())
}

func TestBuildRecordForFundedReserveSwap_CurrencyLaunchWithLaunchpadCurrency(t *testing.T) {
	// The same launch, paid for with a launchpad currency the creator already
	// holds rather than the core mint.
	//
	// The quark amounts are what the bonding curve actually quotes for the two
	// legs against a 100k token supply: the $10 fee leg costs slightly fewer
	// quarks than the $10 purchase leg, because selling the fee leg first moves
	// the price down for the rest. So the legs split the value evenly while
	// splitting the quarks unevenly, and only the terms give the right ratio.
	swapRecord := newReserveSwapRecord()
	swapRecord.FromMint = testutil.NewRandomAccount(t).PublicKey().ToBase58()
	swapRecord.FeeAmount = 9_164_286_143_681
	swapRecord.SwapAmount = 18_335_942_827_079 - swapRecord.FeeAmount

	record, err := BuildRecordForFundedSwap(swapRecord, "usd", 20.0, 1.0, testLaunchTerms)
	require.NoError(t, err)

	// The treasury sells the whole funding amount for protocol revenue and buys
	// a fixed value on the swapper's behalf, so the pool's sell fee is not the
	// swapper's to pay and the launch fee is all they were charged
	require.Len(t, record.Fees, 1)
	assert.Equal(t, history.CurrencyLaunchFee, record.Fees[0].Type)
	assert.InDelta(t, 10.0, record.Fees[0].NativeAmount, 0.0001)

	assert.NoError(t, record.Validate())
}

func TestBuildRecordForFundedStablecoinSwap(t *testing.T) {
	swapRecord := newStablecoinSwapRecord()

	// A $0.50 USD fee at a verified 2.0 fiat exchange rate
	swapRecord.FeeAmount = common.CoreMintQuarksPerUnit / 2

	record, err := BuildRecordForFundedSwap(swapRecord, "eur", 20.0, 2.0, nil)
	require.NoError(t, err)

	assert.Equal(t, swapRecord.SwapId, record.ReferenceId)
	assert.Equal(t, history.Withdrawn, record.Type)
	assert.Equal(t, swapRecord.Owner, record.OwnerAccount)
	require.NotNil(t, record.CounterpartyOwnerAccount)
	assert.Equal(t, swapRecord.DestinationOwner, *record.CounterpartyOwnerAccount)
	assert.EqualValues(t, "eur", record.ExchangeCurrency)
	assert.Equal(t, 20.0, record.NativeAmount)
	assert.Equal(t, swapRecord.FromMint, record.MintAccount)
	assert.Equal(t, swapRecord.SwapAmount+swapRecord.FeeAmount, record.Quantity)
	require.NotNil(t, record.DestinationMintAccount)
	assert.Equal(t, swapRecord.ToMint, *record.DestinationMintAccount)

	// The destination quantity is known upfront because the swap is 1:1
	require.NotNil(t, record.DestinationQuantity)
	assert.Equal(t, swapRecord.SwapAmount, *record.DestinationQuantity)

	// The fee is the USD fee value at the client's verified fiat exchange rate
	require.Len(t, record.Fees, 1)
	assert.Equal(t, history.WithdrawalAccountCreationFee, record.Fees[0].Type)
	assert.InDelta(t, 1.0, record.Fees[0].NativeAmount, 0.0001)

	assert.Equal(t, history.StatePending, record.State)
	assert.NoError(t, record.Validate())
}

func TestBuildRecordForFundedSwap_UnsupportedKind(t *testing.T) {
	swapRecord := newReserveSwapRecord()
	swapRecord.Kind = swap.KindUnknown

	_, err := BuildRecordForFundedSwap(swapRecord, "usd", 10.0, 1.0, nil)
	assert.Error(t, err)
}

func TestMarkSwapAsCompleted_StablecoinWithdrawal(t *testing.T) {
	ctx := context.Background()
	data := ocp_data.NewTestDataProvider()

	swapRecord := newStablecoinSwapRecord()

	record, err := BuildRecordForFundedSwap(swapRecord, "usd", 10.0, 1.0, nil)
	require.NoError(t, err)
	require.NoError(t, data.SaveTransactionHistory(ctx, record))

	require.NoError(t, MarkSwapAsCompleted(ctx, data, swapRecord.SwapId, swapRecord.SwapAmount))

	records, err := data.GetAllTransactionHistoryByReference(ctx, history.SwapReference, swapRecord.SwapId)
	require.NoError(t, err)
	require.Len(t, records, 1)
	assert.Equal(t, history.Withdrawn, records[0].Type)
	assert.Equal(t, history.StateCompleted, records[0].State)
	require.NotNil(t, records[0].DestinationQuantity)
	assert.Equal(t, swapRecord.SwapAmount, *records[0].DestinationQuantity)
}

func TestMarkSwapAsCompleted(t *testing.T) {
	ctx := context.Background()
	data := ocp_data.NewTestDataProvider()

	swapRecord := newReserveSwapRecord()
	saveFundedReserveSwapRecord(t, data, swapRecord)

	require.NoError(t, MarkSwapAsCompleted(ctx, data, swapRecord.SwapId, 420_000))

	records, err := data.GetAllTransactionHistoryByReference(ctx, history.SwapReference, swapRecord.SwapId)
	require.NoError(t, err)
	require.Len(t, records, 1)
	assert.Equal(t, history.StateCompleted, records[0].State)
	require.NotNil(t, records[0].DestinationQuantity)
	assert.EqualValues(t, 420_000, *records[0].DestinationQuantity)

	// A swap transitions exactly once
	assert.Error(t, MarkSwapAsCompleted(ctx, data, swapRecord.SwapId, 420_000))
	assert.Error(t, MarkSwapAsFailed(ctx, data, swapRecord.SwapId))
}

func TestMarkSwapAsFailed(t *testing.T) {
	ctx := context.Background()
	data := ocp_data.NewTestDataProvider()

	swapRecord := newReserveSwapRecord()
	saveFundedReserveSwapRecord(t, data, swapRecord)

	require.NoError(t, MarkSwapAsFailed(ctx, data, swapRecord.SwapId))

	records, err := data.GetAllTransactionHistoryByReference(ctx, history.SwapReference, swapRecord.SwapId)
	require.NoError(t, err)
	require.Len(t, records, 1)
	assert.Equal(t, history.StateFailed, records[0].State)
	assert.Nil(t, records[0].DestinationQuantity)

	// A swap transitions exactly once
	assert.Error(t, MarkSwapAsFailed(ctx, data, swapRecord.SwapId))
}

func TestMarkSwapTerminal_NoHistory(t *testing.T) {
	ctx := context.Background()
	data := ocp_data.NewTestDataProvider()

	// Swaps predating history integration are a no-op
	assert.NoError(t, MarkSwapAsCompleted(ctx, data, "missing_swap_id", 420_000))
	assert.NoError(t, MarkSwapAsFailed(ctx, data, "missing_swap_id"))
}

func TestMarkSwapTerminal_IgnoresOtherReferenceKinds(t *testing.T) {
	ctx := context.Background()
	data := ocp_data.NewTestDataProvider()

	// An intent whose ID happens to equal a swap's. Both are client supplied
	// public keys, so nothing stops a client from picking one for the other.
	record := &history.Record{
		ReferenceId:              "swap_id",
		ReferenceType:            history.IntentReference,
		Type:                     history.DirectlySent,
		OwnerAccount:             "owner",
		CounterpartyOwnerAccount: pointer.String("counterparty_owner"),
		ExchangeCurrency:         "usd",
		NativeAmount:             10.0,
		MintAccount:              "mint",
		Quantity:                 100_000,
		State:                    history.StateCompleted,
		CreatedAt:                time.Now(),
	}
	require.NoError(t, data.SaveTransactionHistory(ctx, record))

	// The swap has no record of its own, so it is treated as predating history
	// rather than finding the intent's and transitioning it
	require.NoError(t, MarkSwapAsCompleted(ctx, data, "swap_id", 420_000))

	actual, err := data.GetAllTransactionHistoryByReference(ctx, history.IntentReference, "swap_id")
	require.NoError(t, err)
	require.Len(t, actual, 1)
	assert.Equal(t, history.DirectlySent, actual[0].Type)
	assert.Equal(t, history.StateCompleted, actual[0].State)
	assert.Nil(t, actual[0].DestinationQuantity)
}

func saveFundedReserveSwapRecord(t *testing.T, data ocp_data.Provider, swapRecord *swap.Record) {
	ctx := context.Background()

	record, err := BuildRecordForFundedSwap(swapRecord, "usd", 10.1, 1.0, nil)
	require.NoError(t, err)
	require.NoError(t, data.SaveTransactionHistory(ctx, record))
}

func newStablecoinSwapRecord() *swap.Record {
	swapRecord := newReserveSwapRecord()
	swapRecord.SwapId = "stablecoin_swap_id"
	swapRecord.Kind = swap.KindStablecoin
	swapRecord.ToMint = "usdc_mint"
	swapRecord.DestinationOwner = "external_wallet_owner"
	return swapRecord
}

func newReserveSwapRecord() *swap.Record {
	return &swap.Record{
		SwapId:        "swap_id",
		Kind:          swap.KindReserve,
		Owner:         "swapper_owner",
		FromMint:      common.CoreMintAccount.PublicKey().ToBase58(),
		ToMint:        "launchpad_mint",
		SwapAmount:    9_900_000,
		FeeAmount:     99_000,
		FundingId:     "funding_id",
		FundingSource: swap.FundingSourceSubmitIntent,
		State:         swap.StateFunded,
		CreatedAt:     time.Now(),
	}
}
