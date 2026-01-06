package swap

import (
	"context"

	"github.com/code-payments/ocp-server/currency"
	"github.com/code-payments/ocp-server/ocp/common"
)

// Integration allows for notifications based on events processed by the swap worker
type Integration interface {
	OnSwapFinalized(ctx context.Context, owner, fromMint, toMint *common.Account, currencyName string, region currency.Code, valueReceived float64) error
}
