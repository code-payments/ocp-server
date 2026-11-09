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

func TestNewCurrencyLaunchComputeUnitLimit(t *testing.T) {
	canonicalArgs := func(isTreasuryFunded bool) *NewCurrencyLaunchComputeUnitArgs {
		return &NewCurrencyLaunchComputeUnitArgs{
			MintBump:             255,
			CurrencyConfigBump:   255,
			LiquidityPoolBump:    255,
			VaultMintBump:        255,
			VaultBaseBump:        255,
			VmBump:               255,
			OmnibusBump:          255,
			MetaplexMetadataBump: 255,
			MemoryAccountBumps:   []uint8{255, 255},
			AtaBumps:             []uint8{255, 255, 255},
			IsTreasuryFunded:     isTreasuryFunded,
		}
	}

	// advance nonce (150) + initialize currency (22,400 + 2 finds) + initialize
	// pool (29,700 + 3 finds) + init VM (17,400 + 2 finds) + transfer for swap
	// with fee (8,400) + buy (36,000) + close (200) + initialize metadata
	// (67,400 + 2 finds) + 2 init memory (9,400 + 1 find each) + 3 ATA creates
	// (20,000 + 1 find each) = 281,450, plus 15% margin
	assert.EqualValues(t, 323_667, NewCurrencyLaunchComputeUnitLimit(canonicalArgs(false)))

	// Treasury funding sells (46,000) instead of closing (200) = 327,250, plus
	// 15% margin
	assert.EqualValues(t, 376_337, NewCurrencyLaunchComputeUnitLimit(canonicalArgs(true)))

	// Each lower bump grows the limit by the extra find iterations, and the
	// Metaplex metadata PDA is found twice
	for name, mutate := range map[string]func(args *NewCurrencyLaunchComputeUnitArgs){
		"mint":           func(args *NewCurrencyLaunchComputeUnitArgs) { args.MintBump = 254 },
		"currency":       func(args *NewCurrencyLaunchComputeUnitArgs) { args.CurrencyConfigBump = 254 },
		"pool":           func(args *NewCurrencyLaunchComputeUnitArgs) { args.LiquidityPoolBump = 254 },
		"vault mint":     func(args *NewCurrencyLaunchComputeUnitArgs) { args.VaultMintBump = 254 },
		"vault base":     func(args *NewCurrencyLaunchComputeUnitArgs) { args.VaultBaseBump = 254 },
		"vm":             func(args *NewCurrencyLaunchComputeUnitArgs) { args.VmBump = 254 },
		"omnibus":        func(args *NewCurrencyLaunchComputeUnitArgs) { args.OmnibusBump = 254 },
		"memory account": func(args *NewCurrencyLaunchComputeUnitArgs) { args.MemoryAccountBumps[1] = 254 },
		"ata":            func(args *NewCurrencyLaunchComputeUnitArgs) { args.AtaBumps[2] = 254 },
	} {
		args := canonicalArgs(false)
		mutate(args)
		assert.EqualValues(t, 323_667+1_725, NewCurrencyLaunchComputeUnitLimit(args), name)
	}
	args := canonicalArgs(false)
	args.MetaplexMetadataBump = 254
	assert.EqualValues(t, 323_667+3_450, NewCurrencyLaunchComputeUnitLimit(args))
}
