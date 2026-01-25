package currency

import (
	"math/big"

	"github.com/code-payments/ocp-server/ocp/common"
)

// CalculateExchangeRate calculates the exchange rate for a crypto value exchange.
func CalculateExchangeRate(mintAccount *common.Account, quarks uint64, nativeAmount float64) float64 {
	quarksPerUnit := common.GetMintQuarksPerUnit(mintAccount)
	tokenUnitsBig := new(big.Float).Quo(
		big.NewFloat(float64(quarks)).SetPrec(defaultPrecision),
		big.NewFloat(float64(quarksPerUnit)).SetPrec(defaultPrecision),
	)
	exchangeRateBig := new(big.Float).Quo(
		big.NewFloat(float64(nativeAmount)).SetPrec(defaultPrecision),
		tokenUnitsBig,
	)
	exchangeRate, _ := exchangeRateBig.Float64()
	return exchangeRate
}
