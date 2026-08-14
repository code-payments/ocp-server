package currency

import (
	"math/big"

	"github.com/code-payments/ocp-server/solana/currencycreator"
)

const (
	// BuyFeeBps is the fee charged when buying a launchpad currency with the
	// core mint, which mirrors currencycreator.DefaultSellFeeBps.
	BuyFeeBps = currencycreator.DefaultSellFeeBps

	// FeeQuarkTolerance is the maximum difference allowed between a client
	// provided fee amount and the server computed value. It absorbs rounding
	// differences between client and server.
	FeeQuarkTolerance = 1

	bpsPerUnit = 10_000

	// sellFeeRemainder is the fraction of a swap's value that remains after
	// the launchpad sell fee is taken by the liquidity pool.
	sellFeeRemainder = 1 - currencycreator.DefaultSellFeeBps/float64(bpsPerUnit)
)

// ExpectedBuyFeeQuarks returns the fee in quarks charged on a buy of the
// provided quark amount.
func ExpectedBuyFeeQuarks(swapAmount uint64) uint64 {
	fee := new(big.Int).Mul(
		new(big.Int).SetUint64(swapAmount),
		big.NewInt(BuyFeeBps),
	)
	return fee.Div(fee, big.NewInt(bpsPerUnit)).Uint64()
}

// IsExpectedFeeQuarks returns whether a client provided fee amount matches the
// server computed value within FeeQuarkTolerance.
func IsExpectedFeeQuarks(actual, expected uint64) bool {
	if actual > expected {
		return actual-expected <= FeeQuarkTolerance
	}
	return expected-actual <= FeeQuarkTolerance
}

// DiscountValueForBuyFee scales a value quoted over the full amount funding a
// swap (swap plus fee) down to the portion that was actually swapped.
func DiscountValueForBuyFee(value float64, swapAmount, feeAmount uint64) float64 {
	if feeAmount == 0 {
		return value
	}

	swapAmountBig := new(big.Float).SetPrec(defaultPrecision).SetUint64(swapAmount)
	feeAmountBig := new(big.Float).SetPrec(defaultPrecision).SetUint64(feeAmount)
	fundedAmountBig := new(big.Float).Add(swapAmountBig, feeAmountBig)
	if fundedAmountBig.Sign() == 0 {
		return value
	}

	discounted, _ := new(big.Float).Mul(
		big.NewFloat(value).SetPrec(defaultPrecision),
		new(big.Float).Quo(swapAmountBig, fundedAmountBig),
	).Float64()
	return discounted
}

// ApplySellFee returns the value remaining after the launchpad sell fee.
func ApplySellFee(value float64) float64 {
	discounted, _ := new(big.Float).Mul(
		big.NewFloat(sellFeeRemainder).SetPrec(defaultPrecision),
		big.NewFloat(value).SetPrec(defaultPrecision),
	).Float64()
	return discounted
}

// GrossUpSellFee returns the value prior to the launchpad sell fee being taken,
// which is the inverse of ApplySellFee.
func GrossUpSellFee(value float64) float64 {
	grossedUp, _ := new(big.Float).Quo(
		big.NewFloat(value).SetPrec(defaultPrecision),
		big.NewFloat(sellFeeRemainder).SetPrec(defaultPrecision),
	).Float64()
	return grossedUp
}
