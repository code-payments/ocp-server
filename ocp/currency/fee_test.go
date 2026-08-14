package currency

import (
	"math"
	"math/big"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/code-payments/ocp-server/ocp/common"
)

func TestExpectedBuyFeeQuarks(t *testing.T) {
	for _, tc := range []struct {
		swapAmount uint64
		expected   uint64
	}{
		{swapAmount: 0, expected: 0},
		{swapAmount: 99, expected: 0},
		{swapAmount: 100, expected: 1},
		{swapAmount: 199, expected: 1},
		{swapAmount: 10 * common.CoreMintQuarksPerUnit, expected: common.CoreMintQuarksPerUnit / 10},
		{swapAmount: 12_345_678, expected: 123_456},

		// The maximum swap amount doesn't overflow
		{swapAmount: math.MaxUint64, expected: math.MaxUint64 / 100},
	} {
		assert.Equal(t, tc.expected, ExpectedBuyFeeQuarks(tc.swapAmount))
	}
}

func TestIsExpectedFeeQuarks(t *testing.T) {
	assert.True(t, IsExpectedFeeQuarks(100, 100))
	assert.True(t, IsExpectedFeeQuarks(99, 100))
	assert.True(t, IsExpectedFeeQuarks(101, 100))

	assert.False(t, IsExpectedFeeQuarks(98, 100))
	assert.False(t, IsExpectedFeeQuarks(102, 100))

	assert.True(t, IsExpectedFeeQuarks(0, 0))
	assert.True(t, IsExpectedFeeQuarks(1, 0))
	assert.False(t, IsExpectedFeeQuarks(2, 0))
}

func TestDiscountValueForBuyFee(t *testing.T) {
	// Without a fee, the value is untouched
	assert.Equal(t, 10.0, DiscountValueForBuyFee(10.0, 10*common.CoreMintQuarksPerUnit, 0))

	// A $10.10 payment buying $10.00 of a currency is valued at the swap amount
	swapAmount := 10 * common.CoreMintQuarksPerUnit
	feeAmount := ExpectedBuyFeeQuarks(swapAmount)
	fundedValue := float64(swapAmount+feeAmount) / float64(common.CoreMintQuarksPerUnit)
	assert.InDelta(t, 10.0, DiscountValueForBuyFee(fundedValue, swapAmount, feeAmount), 1e-9)

	// The discount is proportional regardless of the exchange currency
	assert.InDelta(t, 100.0, DiscountValueForBuyFee(101.0, 10_000, 100), 1e-9)
}

func TestSellFeeRoundTrip(t *testing.T) {
	for _, value := range []float64{0.01, 1.0, 12.34, 1_000_000.0} {
		assert.InDelta(t, value, GrossUpSellFee(ApplySellFee(value)), 1e-9)
	}
}

// The sell fee was previously applied with hardcoded 0.99 literals. The helpers
// must remain bit-for-bit identical so reported values don't shift.
func TestSellFeeMatchesLegacyLiterals(t *testing.T) {
	for _, value := range []float64{0.01, 1.0, 12.34, 99.99, 1_000_000.0} {
		legacyApplied, _ := new(big.Float).Mul(
			big.NewFloat(0.99).SetPrec(128),
			big.NewFloat(value).SetPrec(128),
		).Float64()
		assert.Equal(t, legacyApplied, ApplySellFee(value))

		legacyGrossedUp, _ := new(big.Float).Quo(
			big.NewFloat(value).SetPrec(128),
			big.NewFloat(0.99).SetPrec(128),
		).Float64()
		assert.Equal(t, legacyGrossedUp, GrossUpSellFee(value))
	}
}
