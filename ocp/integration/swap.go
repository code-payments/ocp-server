package integration

import (
	"context"

	"github.com/code-payments/ocp-server/currency"
	"github.com/code-payments/ocp-server/ocp/common"
)

// Swap is an integration that hooks into the swap worker
type Swap interface {
	// OnSwapSubmitted allows for notification when the swap for when a swap is submitted
	OnSwapSubmitted(ctx context.Context, owner *common.Account, fromMint, toMint *common.Account, amount uint64) error

	// OnSwapFinalized allows for notifications based on events processed by the swap worker
	OnSwapFinalized(ctx context.Context, owner *common.Account, isBuy bool, mint *common.Account, currencyName string, region currency.Code, valueReceived float64) error
}
