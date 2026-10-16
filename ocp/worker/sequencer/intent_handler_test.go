package sequencer

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	currency_lib "github.com/code-payments/ocp-server/currency"
	"github.com/code-payments/ocp-server/ocp/common"
	ocp_data "github.com/code-payments/ocp-server/ocp/data"
	"github.com/code-payments/ocp-server/ocp/data/action"
	"github.com/code-payments/ocp-server/ocp/data/intent"
	"github.com/code-payments/ocp-server/pointer"
)

func TestSendPublicMultiPaymentIntentHandler(t *testing.T) {
	for _, tc := range []struct {
		name     string
		states   []action.State
		expected intent.State
	}{
		{"no payments confirmed", []action.State{action.StatePending, action.StatePending}, intent.StatePending},
		{"some payments confirmed", []action.State{action.StateConfirmed, action.StatePending}, intent.StatePending},
		{"all payments confirmed", []action.State{action.StateConfirmed, action.StateConfirmed}, intent.StateConfirmed},
		{"payment failed after another confirmed", []action.State{action.StateConfirmed, action.StateFailed}, intent.StateFailed},
		{"payment failed before another confirmed", []action.State{action.StateFailed, action.StatePending}, intent.StateFailed},
		{"later payment failed while earlier is pending", []action.State{action.StatePending, action.StateFailed}, intent.StateFailed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			data := ocp_data.NewTestDataProvider()

			intentRecord := newTestSendPublicMultiPaymentIntent(t)
			require.NoError(t, data.SaveIntent(ctx, intentRecord))

			var actionRecords []*action.Record
			for i, state := range tc.states {
				actionRecords = append(actionRecords, &action.Record{
					Intent:      intentRecord.IntentId,
					IntentType:  intentRecord.IntentType,
					ActionId:    uint32(i),
					ActionType:  action.NoPrivacyTransfer,
					Source:      intentRecord.SendPublicMultiPaymentMetadata.Source,
					Destination: pointer.String(intentRecord.SendPublicMultiPaymentMetadata.Payments[i].DestinationTokenAccount),
					Quantity:    pointer.Uint64(intentRecord.SendPublicMultiPaymentMetadata.Payments[i].Quantity),
					State:       state,
				})
			}
			require.NoError(t, data.PutAllActions(ctx, actionRecords...))

			handler := getIntentHandlers(data)[intent.SendPublicMultiPayment]
			require.NotNil(t, handler)

			// Repeated calls are idempotent, since the sequencer retries
			for range 2 {
				require.NoError(t, handler.OnActionUpdated(ctx, intentRecord.IntentId))

				updated, err := data.GetIntent(ctx, intentRecord.IntentId)
				require.NoError(t, err)
				assert.Equal(t, tc.expected, updated.State)
			}
		})
	}
}

func newTestSendPublicMultiPaymentIntent(t *testing.T) *intent.Record {
	intentId, err := common.NewRandomAccount()
	require.NoError(t, err)

	return &intent.Record{
		IntentId:              intentId.PublicKey().ToBase58(),
		IntentType:            intent.SendPublicMultiPayment,
		MintAccount:           "mint",
		InitiatorOwnerAccount: "owner",
		SendPublicMultiPaymentMetadata: &intent.SendPublicMultiPaymentMetadata{
			Source: "source",
			Payments: []*intent.Payment{
				{DestinationOwnerAccount: "destination_owner_1", DestinationTokenAccount: "destination_1", Quantity: 100, NativeAmount: 1, UsdMarketValue: 1},
				{DestinationOwnerAccount: "destination_owner_2", DestinationTokenAccount: "destination_2", Quantity: 50, NativeAmount: 0.5, UsdMarketValue: 0.5},
			},
			Quantity:         150,
			ExchangeCurrency: currency_lib.USD,
			ExchangeRate:     1,
			NativeAmount:     1.5,
			UsdMarketValue:   1.5,
		},
		State:     intent.StatePending,
		CreatedAt: time.Now(),
	}
}
