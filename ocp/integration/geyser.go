package integration

import (
	"context"

	"github.com/code-payments/ocp-server/ocp/common"
)

// Swap is an integration that hooks into the Geyser worker
type Geyser interface {
	// OnDepositReceived allows for notifications for external deposits processed by Geyser
	OnDepositReceived(ctx context.Context, owner, mint *common.Account, currencyName string, usdMarketValue float64) error
}
