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

func recordReserveStateEvent(ctx context.Context, mint *common.Account, supply uint64) {
	if !common.IsCoreMintUsdStableCoin() {
		return
	}

	sellValueInQuarks, _ := currencycreator.EstimateSell(&currencycreator.EstimateSellArgs{
		CurrentSupplyInQuarks: supply,
		SellAmountInQuarks:    supply,
		ValueMintDecimals:     uint8(common.CoreMintDecimals),
		SellFeeBps:            0,
	})

	usdMarketValue := float64(sellValueInQuarks) / float64(common.CoreMintQuarksPerUnit)

	metrics.RecordEvent(ctx, reserveStateEventName, map[string]interface{}{
		"mint":             mint.PublicKey().ToBase58(),
		"supply":           supply,
		"usd_market_value": usdMarketValue,
	})
}
