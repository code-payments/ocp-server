package transaction

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestExecComputeUnitLimits(t *testing.T) {
	// base 60,000 + (5 creates + 2 banks) * 1,500 = 70,500, plus 15% margin
	assert.EqualValues(t, 81_075, internalExecComputeUnitLimit(2))

	// base 65,000 + (4 creates + 2 banks) * 1,500 = 74,000, plus 15% margin
	assert.EqualValues(t, 85_100, externalExecComputeUnitLimit(2))

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
	// base 90,000 + ATA create (15,000) + ATA find (1,500) = 106,500, plus
	// 15% margin
	assert.EqualValues(t, 122_475, ReserveBuySwapComputeUnitLimit(255))

	// base 100,000 + ATA create (15,000) + ATA find (1,500) = 116,500, plus
	// 15% margin
	assert.EqualValues(t, 133_975, ReserveSellSwapComputeUnitLimit(255))

	// base 150,000 + 2 ATA creates (30,000) + 2 ATA finds (3,000) = 183,000,
	// plus 15% margin
	assert.EqualValues(t, 210_450, ReserveBuySellSwapComputeUnitLimit(255, 255))
}

func TestOpenAccountComputeUnitLimit(t *testing.T) {
	// base 10,000 + 2 creates (3,000) + state/vault/unlock finds (4,500) +
	// withdraw receipt allowance (36,000) = 53,500, plus 15% margin
	assert.EqualValues(t, 61_525, openAccountComputeUnitLimit(255, 255, 255))

	// Low user bumps grow the limit by exactly the extra find iterations
	assert.EqualValues(
		t,
		15*cuPerPdaDerivation*115/100,
		openAccountComputeUnitLimit(240, 255, 255)-openAccountComputeUnitLimit(255, 255, 255),
	)
}
