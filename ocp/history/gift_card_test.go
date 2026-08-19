package history

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ocp_data "github.com/code-payments/ocp-server/ocp/data"
	"github.com/code-payments/ocp-server/ocp/data/history"
)

func TestMarkGiftCardIssuance_StateTransitions(t *testing.T) {
	for _, tc := range []struct {
		mark     func(context.Context, ocp_data.DatabaseData, string) error
		expected history.State
	}{
		{MarkGiftCardIssuanceAsClaimed, history.StateCompleted},
		{MarkGiftCardIssuanceAsVoided, history.StateVoided},
		{MarkGiftCardIssuanceAsReturned, history.StateReturned},
	} {
		ctx := context.Background()
		data := ocp_data.NewTestDataProvider()

		issuedIntentRecord := newGiftCardIssuedIntentRecord()
		saveRecordsForIntent(t, data, issuedIntentRecord)

		giftCardVault := issuedIntentRecord.SendPublicPaymentMetadata.DestinationTokenAccount
		require.NoError(t, tc.mark(ctx, data, giftCardVault))

		records, err := data.GetAllTransactionHistoryByGiftCardVault(ctx, giftCardVault)
		require.NoError(t, err)
		require.Len(t, records, 1)
		assert.Equal(t, history.IndirectlySent, records[0].Type)
		assert.Equal(t, tc.expected, records[0].State)

		// An issuance transitions exactly once
		assert.Error(t, tc.mark(ctx, data, giftCardVault))
	}
}

func TestMarkGiftCardIssuance_AlreadyClaimed(t *testing.T) {
	ctx := context.Background()
	data := ocp_data.NewTestDataProvider()

	issuedIntentRecord := newGiftCardIssuedIntentRecord()
	saveRecordsForIntent(t, data, issuedIntentRecord)

	giftCardVault := issuedIntentRecord.SendPublicPaymentMetadata.DestinationTokenAccount
	require.NoError(t, MarkGiftCardIssuanceAsClaimed(ctx, data, giftCardVault))

	// A claimed issuance cannot transition again
	assert.Error(t, MarkGiftCardIssuanceAsReturned(ctx, data, giftCardVault))

	records, err := data.GetAllTransactionHistoryByGiftCardVault(ctx, giftCardVault)
	require.NoError(t, err)
	require.Len(t, records, 1)
	assert.Equal(t, history.StateCompleted, records[0].State)
}

func TestMarkGiftCardIssuance_ClaimRecordUntouched(t *testing.T) {
	ctx := context.Background()
	data := ocp_data.NewTestDataProvider()

	issuedIntentRecord := newGiftCardIssuedIntentRecord()
	require.NoError(t, data.SaveIntent(ctx, issuedIntentRecord))
	saveRecordsForIntent(t, data, issuedIntentRecord)

	claimIntentRecord := newGiftCardClaimedIntentRecord(issuedIntentRecord)
	saveRecordsForIntent(t, data, claimIntentRecord)

	giftCardVault := issuedIntentRecord.SendPublicPaymentMetadata.DestinationTokenAccount
	require.NoError(t, MarkGiftCardIssuanceAsClaimed(ctx, data, giftCardVault))

	records, err := data.GetAllTransactionHistoryByGiftCardVault(ctx, giftCardVault)
	require.NoError(t, err)
	require.Len(t, records, 2)
	for _, record := range records {
		assert.Equal(t, history.StateCompleted, record.State)
	}
}

func TestMarkGiftCardIssuance_NoHistory(t *testing.T) {
	ctx := context.Background()
	data := ocp_data.NewTestDataProvider()

	// Gift cards predating history integration are a no-op
	assert.NoError(t, MarkGiftCardIssuanceAsClaimed(ctx, data, "missing_gift_card_vault"))
}
