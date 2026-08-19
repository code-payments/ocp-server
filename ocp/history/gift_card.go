package history

import (
	"context"

	"github.com/pkg/errors"

	ocp_data "github.com/code-payments/ocp-server/ocp/data"
	"github.com/code-payments/ocp-server/ocp/data/history"
)

// MarkGiftCardIssuanceAsClaimed completes the issuer's IndirectlySent record
// after the gift card is claimed.
func MarkGiftCardIssuanceAsClaimed(ctx context.Context, data ocp_data.DatabaseData, giftCardVault string) error {
	return markGiftCardIssuance(ctx, data, giftCardVault, history.StateCompleted)
}

// MarkGiftCardIssuanceAsVoided transitions the issuer's IndirectlySent record
// after the issuer voids the gift card.
func MarkGiftCardIssuanceAsVoided(ctx context.Context, data ocp_data.DatabaseData, giftCardVault string) error {
	return markGiftCardIssuance(ctx, data, giftCardVault, history.StateVoided)
}

// MarkGiftCardIssuanceAsReturned transitions the issuer's IndirectlySent record
// after the gift card expires unclaimed and is auto-returned.
func MarkGiftCardIssuanceAsReturned(ctx context.Context, data ocp_data.DatabaseData, giftCardVault string) error {
	return markGiftCardIssuance(ctx, data, giftCardVault, history.StateReturned)
}

func markGiftCardIssuance(ctx context.Context, data ocp_data.DatabaseData, giftCardVault string, newState history.State) error {
	records, err := data.GetAllTransactionHistoryByGiftCardVault(ctx, giftCardVault)
	if errors.Is(err, history.ErrNotFound) {
		// The gift card predates history integration
		return nil
	}
	if err != nil {
		return err
	}

	for _, record := range records {
		if record.Type != history.IndirectlySent {
			continue
		}

		// An issuance transitions exactly once, from pending, so anything else
		// is a flow violation
		if record.State != history.StatePending {
			return errors.Errorf("gift card issuance record is %s, expected %s", record.State, history.StatePending)
		}

		record.State = newState
		return data.SaveTransactionHistory(ctx, record)
	}

	return nil
}
