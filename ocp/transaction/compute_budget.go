package transaction

const (
	// todo: optimize
	baseExecComputeUnits = 55_000

	// vm check + 4 timelock/vault message derivations
	numCreateDerivationsInternalExec = 5

	// vm check + 2 message derivations + omnibus invoke_signed
	numCreateDerivationsExternalExec = 4

	// todo: optimize
	baseReserveBuySwapComputeUnits     = 80_000 + baseAtaCreateComputeUnits
	baseReserveSellSwapComputeUnits    = 90_000 + baseAtaCreateComputeUnits
	baseReserveBuySellSwapComputeUnits = 130_000 + 2*baseAtaCreateComputeUnits

	// transfer_for_swap_with_fee makes a second token transfer to the fee
	// destination on top of the one made by transfer_for_swap. Measured on
	// mainnet: the token program consumes 145 CUs for a transfer, and the CPI
	// invoke plus account serialization make up the remainder. The
	// transfer_for_swap instruction consumes 5,869 CUs against the 8,395 CUs
	// of transfer_for_swap_with_fee.
	baseReserveBuyWithFeeSwapComputeUnits = baseReserveBuySwapComputeUnits + 2_600

	// todo: optimize
	baseExternalDepositComputeUnits = 25_000
	baseCloseVmDepositComputeUnits  = 10_000

	// todo: optimize
	baseInitTimelockComputeUnits = 20_000 + withdrawReceiptFindComputeUnits

	// init_timelock: vm + memory checks
	numCreateDerivationsInitTimelock = 2

	// init_timelock derives the withdraw receipt PDA from the VM's PoH value
	// at execution time, so its bump is unknowable when the transaction is
	// built. Budget for bump 232, which covers all but ~1 in 16M account
	// creations.
	withdrawReceiptFindComputeUnits = 36_000

	// todo: optimize
	baseAtaCreateComputeUnits = 20_000

	cuPerPdaDerivation = 1_500

	computeUnitMarginPercent = 15
)

func findPdaComputeUnits(bump uint8) uint32 {
	return (256 - uint32(bump)) * cuPerPdaDerivation
}

// WithComputeUnitMargin pads a measured or modeled compute unit count by the
// standard safety margin.
func WithComputeUnitMargin(computeUnits uint32) uint32 {
	return computeUnits * (100 + computeUnitMarginPercent) / 100
}

// openAccountComputeUnitLimit computes the compute unit limit for an
// init_timelock transaction. The timelock state, vault and unlock PDAs
// remain find-derived on-chain even with stored-bump validation, since bump
// canonicality must be proven at account creation.
func openAccountComputeUnitLimit(timelockStateBump, vaultBump, unlockBump uint8) uint32 {
	computeUnits := baseInitTimelockComputeUnits +
		numCreateDerivationsInitTimelock*cuPerPdaDerivation +
		findPdaComputeUnits(timelockStateBump) +
		findPdaComputeUnits(vaultBump) +
		findPdaComputeUnits(unlockBump)
	return WithComputeUnitMargin(computeUnits)
}

func internalExecComputeUnitLimit(numMemoryBanks int) uint32 {
	return execComputeUnitLimit(baseExecComputeUnits, numCreateDerivationsInternalExec, numMemoryBanks, 0)
}

func externalExecComputeUnitLimit(numMemoryBanks int) uint32 {
	return execComputeUnitLimit(baseExecComputeUnits, numCreateDerivationsExternalExec, numMemoryBanks, 0)
}

func externalExecWithAtaCreateComputeUnitLimit(numMemoryBanks int, ataBump uint8) uint32 {
	return execComputeUnitLimit(
		baseExecComputeUnits,
		numCreateDerivationsExternalExec,
		numMemoryBanks,
		baseAtaCreateComputeUnits+findPdaComputeUnits(ataBump),
	)
}

func execComputeUnitLimit(baseComputeUnits, numCreateDerivations uint32, numMemoryBanks int, additionalComputeUnits uint32) uint32 {
	computeUnits := baseComputeUnits + additionalComputeUnits
	computeUnits += (numCreateDerivations + uint32(numMemoryBanks)) * cuPerPdaDerivation
	return WithComputeUnitMargin(computeUnits)
}

// ReserveBuySwapComputeUnitLimit computes the compute unit limit for a
// reserve buy swap transaction, whose only bump-dependent cost is creating
// the temporary core mint ATA.
func ReserveBuySwapComputeUnitLimit(temporaryAtaBump uint8) uint32 {
	return WithComputeUnitMargin(baseReserveBuySwapComputeUnits + findPdaComputeUnits(temporaryAtaBump))
}

// ReserveBuyWithFeeSwapComputeUnitLimit computes the compute unit limit for a
// reserve buy swap transaction that also collects a buy fee, whose only
// bump-dependent cost is creating the temporary core mint ATA.
func ReserveBuyWithFeeSwapComputeUnitLimit(temporaryAtaBump uint8) uint32 {
	return WithComputeUnitMargin(baseReserveBuyWithFeeSwapComputeUnits + findPdaComputeUnits(temporaryAtaBump))
}

// ReserveSellSwapComputeUnitLimit computes the compute unit limit for a
// reserve sell swap transaction, whose only bump-dependent cost is creating
// the temporary source currency ATA.
func ReserveSellSwapComputeUnitLimit(temporaryAtaBump uint8) uint32 {
	return WithComputeUnitMargin(baseReserveSellSwapComputeUnits + findPdaComputeUnits(temporaryAtaBump))
}

// ReserveBuySellSwapComputeUnitLimit computes the compute unit limit for a
// reserve buy/sell swap transaction, which creates temporary ATAs for both
// the core mint and the source currency.
func ReserveBuySellSwapComputeUnitLimit(temporaryCoreAtaBump, temporarySourceAtaBump uint8) uint32 {
	return WithComputeUnitMargin(
		baseReserveBuySellSwapComputeUnits +
			findPdaComputeUnits(temporaryCoreAtaBump) +
			findPdaComputeUnits(temporarySourceAtaBump),
	)
}

// ExternalDepositComputeUnitLimit computes the compute unit limit for a
// deposit_from_pda transaction sweeping an external deposit into the VM.
func ExternalDepositComputeUnitLimit() uint32 {
	return WithComputeUnitMargin(baseExternalDepositComputeUnits)
}

// CloseVmDepositComputeUnitLimit computes the compute unit limit for a
// close_deposit_account_if_empty transaction.
func CloseVmDepositComputeUnitLimit() uint32 {
	return WithComputeUnitMargin(baseCloseVmDepositComputeUnits)
}
