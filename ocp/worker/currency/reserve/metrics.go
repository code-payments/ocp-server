package reserve

import (
	"context"

	"github.com/code-payments/ocp-server/metrics"
	"github.com/code-payments/ocp-server/ocp/common"
	"github.com/code-payments/ocp-server/solana/currencycreator"
)

const (
	reserveStateEventName = "CurrencyReserveStateObserved"
)

func recordReserveStateEvent(ctx context.Context, mint string, supply uint64) {
	if !common.IsCoreMintUsdStableCoin() {
		return
	}

	price, _ := currencycreator.EstimateCurrentPrice(supply).Float64()
	usdMarketCap := price * (float64(supply) / float64(currencycreator.DefaultMintQuarksPerUnit))
	metrics.RecordEvent(ctx, reserveStateEventName, map[string]interface{}{
		"mint":           mint,
		"supply":         supply,
		"usd_market_cap": usdMarketCap,
	})
}
