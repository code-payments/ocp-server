package transaction

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestExecComputeUnitLimits(t *testing.T) {
	// base 55,000 + (5 creates + 2 banks) * 1,500 = 65,500, plus 15% margin
	assert.EqualValues(t, 75_325, internalExecComputeUnitLimit(2))

	// base 55,000 + (4 creates + 2 banks) * 1,500 = 64,000, plus 15% margin
	assert.EqualValues(t, 73_600, externalExecComputeUnitLimit(2))

	// Adding a memory bank costs one derivation plus margin
	assert.EqualValues(t, 1_725, internalExecComputeUnitLimit(3)-internalExecComputeUnitLimit(2))

	// The ATA derivation cost scales with how far the bump is from 255
	canonical := externalExecWithAtaCreateComputeUnitLimit(2, 255)
	lowBump := externalExecWithAtaCreateComputeUnitLimit(2, 240)
	assert.EqualValues(t, 15*cuPerPdaDerivation*115/100, lowBump-canonical)

	// A create-on-send transaction always budgets more than a plain external
	// transfer
	assert.Greater(t, canonical, externalExecComputeUnitLimit(2))
}

func TestReserveSwapComputeUnitLimits(t *testing.T) {
	// base 80,000 + ATA create (20,000) + ATA find (1,500) = 101,500, plus
	// 15% margin
	assert.EqualValues(t, 116_725, ReserveBuySwapComputeUnitLimit(255))

	// base 80,000 + fee transfer CPI (2,600) + ATA create (20,000) + ATA find
	// (1,500) = 104,100, plus 15% margin
	assert.EqualValues(t, 119_715, ReserveBuyWithFeeSwapComputeUnitLimit(255))

	// base 90,000 + ATA create (20,000) + ATA find (1,500) = 111,500, plus
	// 15% margin
	assert.EqualValues(t, 128_225, ReserveSellSwapComputeUnitLimit(255))

	// base 130,000 + 2 ATA creates (40,000) + 2 ATA finds (3,000) = 173,000,
	// plus 15% margin
	assert.EqualValues(t, 198_950, ReserveBuySellSwapComputeUnitLimit(255, 255))
}

func TestOpenAccountComputeUnitLimit(t *testing.T) {
	// base 20,000 + 2 creates (3,000) + state/vault/unlock finds (4,500) +
	// withdraw receipt allowance (36,000) = 63,500, plus 15% margin
	assert.EqualValues(t, 73_025, openAccountComputeUnitLimit(255, 255, 255))

	// Low user bumps grow the limit by exactly the extra find iterations
	assert.EqualValues(
		t,
		15*cuPerPdaDerivation*115/100,
		openAccountComputeUnitLimit(240, 255, 255)-openAccountComputeUnitLimit(255, 255, 255),
	)
}
