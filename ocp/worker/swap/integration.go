package swap

import (
	"context"

	"github.com/code-payments/ocp-server/ocp/common"
	"github.com/code-payments/ocp-server/currency"
)

// Integration allows for notifications based on events processed by the swap worker
type Integration interface {
	OnSwapFinalized(ctx context.Context, owner, mint *common.Account, currencyName string, region currency.Code, nativeAmount float64) error
}
