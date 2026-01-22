package currency

import (
	"math/big"

	"github.com/code-payments/ocp-server/ocp/common"
)

// CalculateExchangeRate calculates the exchange rate for a crypto value exchange.
func CalculateExchangeRate(mintAccount *common.Account, nativeAmount float64, quarks uint64) float64 {
	quarksPerUnit := common.GetMintQuarksPerUnit(mintAccount)
	tokenUnits := new(big.Float).Quo(
		big.NewFloat(float64(quarks)).SetPrec(defaultPrecision),
		big.NewFloat(float64(quarksPerUnit)).SetPrec(defaultPrecision),
	)
	exchangeRate := new(big.Float).Quo(
		big.NewFloat(float64(nativeAmount)).SetPrec(defaultPrecision),
		tokenUnits,
	)
	res, _ := exchangeRate.Float64()
	return res
}
