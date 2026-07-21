package transaction

// Compute unit limits for exec transactions, modeled against the VM program
// version that validates PDAs with create_program_address using stored bumps
// (a flat 1,500 CUs per derivation, independent of bump value). Deploying
// these limits against the older find_program_address-based VM program would
// under-budget VMs with low-bump PDAs.
//
// The ATA program still derives the associated token address with
// find_program_address, which walks candidate bumps from 255 down at 1,500
// CUs each, so create-on-send carries the one remaining bump-dependent term.
const (
	// todo: optimize
	baseInternalExecComputeUnits = 60_000
	baseExternalExecComputeUnits = 65_000

	// vm check + 4 timelock/vault message derivations
	numCreateDerivationsInternalExec = 5

	// vm check + 2 message derivations + omnibus invoke_signed
	numCreateDerivationsExternalExec = 4

	// todo: optimize
	baseReserveBuySwapComputeUnits     = 90_000
	baseReserveSellSwapComputeUnits    = 100_000
	baseReserveBuySellSwapComputeUnits = 150_000

	// todo: optimize
	baseExternalDepositComputeUnits = 25_000
	baseCloseVmDepositComputeUnits  = 10_000

	// todo: optimize
	baseInitTimelockComputeUnits = 10_000

	// init_timelock: vm + memory checks
	numCreateDerivationsInitTimelock = 2

	// init_timelock derives the withdraw receipt PDA from the VM's PoH value
	// at execution time, so its bump is unknowable when the transaction is
	// built. Budget for bump 232, which covers all but ~1 in 16M account
	// creations.
	withdrawReceiptFindComputeUnits = 36_000

	// todo: optimize
	baseAtaCreateComputeUnits = 15_000

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
		findPdaComputeUnits(unlockBump) +
		withdrawReceiptFindComputeUnits
	return WithComputeUnitMargin(computeUnits)
}

func internalExecComputeUnitLimit(numMemoryBanks int) uint32 {
	return execComputeUnitLimit(baseInternalExecComputeUnits, numCreateDerivationsInternalExec, numMemoryBanks, 0)
}

func externalExecComputeUnitLimit(numMemoryBanks int) uint32 {
	return execComputeUnitLimit(baseExternalExecComputeUnits, numCreateDerivationsExternalExec, numMemoryBanks, 0)
}

func externalExecWithAtaCreateComputeUnitLimit(numMemoryBanks int, ataBump uint8) uint32 {
	return execComputeUnitLimit(
		baseExternalExecComputeUnits,
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
	return WithComputeUnitMargin(baseReserveBuySwapComputeUnits + baseAtaCreateComputeUnits + findPdaComputeUnits(temporaryAtaBump))
}

// ReserveSellSwapComputeUnitLimit computes the compute unit limit for a
// reserve sell swap transaction, whose only bump-dependent cost is creating
// the temporary source currency ATA.
func ReserveSellSwapComputeUnitLimit(temporaryAtaBump uint8) uint32 {
	return WithComputeUnitMargin(baseReserveSellSwapComputeUnits + baseAtaCreateComputeUnits + findPdaComputeUnits(temporaryAtaBump))
}

// ReserveBuySellSwapComputeUnitLimit computes the compute unit limit for a
// reserve buy/sell swap transaction, which creates temporary ATAs for both
// the core mint and the source currency.
func ReserveBuySellSwapComputeUnitLimit(temporaryCoreAtaBump, temporarySourceAtaBump uint8) uint32 {
	return WithComputeUnitMargin(
		baseReserveBuySellSwapComputeUnits +
			2*baseAtaCreateComputeUnits +
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
