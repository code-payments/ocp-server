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

	// init_nonce costs 5,218 CUs on mainnet before finding the virtual
	// nonce PDA, measured across 540 transactions from 12 launches after the
	// July 2026 VM program upgrade
	baseInitNonceComputeUnits = 5_300

	// init_nonce derives the virtual nonce PDA from the VM's PoH value at
	// execution time, so its bump is unknowable when the transaction is built.
	// Finding it takes 2 iterations on average with a variance of 2. A batch
	// budgets 2 iterations per nonce plus this slack, which doubles the
	// expected iterations for a batch of 50 nonces. A batch of 50 exceeds that
	// in about 1 in 10^13 attempts, before the compute unit margin.
	initNonceFindIterationsSlack = 100

	// New currency launch instructions, measured on mainnet from the
	// create-and-buy transaction 5kqAEba8eS (slot 460,249,402) and the same
	// launch's post-purchase transactions, after the July 2026 VM program
	// upgrade. Each measurement includes the PDA finds it ran for that
	// launch's bumps, which are added back for the bumps of the currency being
	// launched, so the model over-counts by at least 1,500 CUs per find.
	initializeCurrencyComputeUnits     = 22_400 // finds the mint and currency PDAs
	initializePoolComputeUnits         = 29_700 // finds the pool and both vault PDAs
	initVmComputeUnits                 = 17_400 // finds the VM and omnibus PDAs
	initializeMetadataComputeUnits     = 67_400 // finds the Metaplex metadata PDA twice
	initMemoryComputeUnits             = 9_400  // finds the memory PDA
	transferForSwapWithFeeComputeUnits = 8_400
	buyTokensComputeUnits              = 36_000
	sellTokensComputeUnits             = 46_000 // highest across 50 buy/sell swaps was 45,737
	closeAccountComputeUnits           = 200
	advanceNonceComputeUnits           = 150
	systemTransferComputeUnits         = 150

	// VM::ResizeMemory of a memory account pre-funded for its new size, so it
	// makes no rent transfer CPI. Measured in LiteSVM against the mainnet VM
	// program binary: 5,455 CUs for every resize, independent of size and
	// bumps. An unfunded resize costs about 2,000 CUs more.
	resizeMemoryComputeUnits = 5_500

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

// InitNonceBatchComputeUnitLimit computes the compute unit limit for a
// transaction initializing a batch of virtual durable nonces in a VM's nonce
// memory account.
func InitNonceBatchComputeUnitLimit(numNonces int) uint32 {
	findIterations := 2*uint32(numNonces) + initNonceFindIterationsSlack
	return WithComputeUnitMargin(uint32(numNonces)*baseInitNonceComputeUnits + findIterations*cuPerPdaDerivation)
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

// NewCurrencyLaunchComputeUnitArgs are the bumps and counts that determine
// the compute units of a v1 transaction that launches a new currency within
// its first purchase
type NewCurrencyLaunchComputeUnitArgs struct {
	MintBump             uint8
	CurrencyConfigBump   uint8
	LiquidityPoolBump    uint8
	VaultMintBump        uint8
	VaultBaseBump        uint8
	VmBump               uint8
	OmnibusBump          uint8
	MetaplexMetadataBump uint8

	// One per VM::InitMemory instruction
	MemoryAccountBumps []uint8

	// One per AssociatedTokenAccount::CreateIdempotent instruction
	AtaBumps []uint8

	// System::Transfer rent top-ups and the VM::ResizeMemory instructions
	// they pre-fund
	NumRentTopUps    int
	NumMemoryResizes int

	// Treasury-funded launches sell the source currency and buy with the
	// treasury's core mint. Core mint funded launches buy directly and close
	// the temporary core mint ATA.
	IsTreasuryFunded bool
}

// NewCurrencyLaunchComputeUnitLimit computes the compute unit limit for a v1
// create-and-buy transaction, which initializes the currency, pool, VM,
// metadata and memory accounts alongside the first purchase, and resizes the
// memory accounts to full capacity
func NewCurrencyLaunchComputeUnitLimit(args *NewCurrencyLaunchComputeUnitArgs) uint32 {
	computeUnits := uint32(advanceNonceComputeUnits)

	computeUnits += initializeCurrencyComputeUnits +
		findPdaComputeUnits(args.MintBump) +
		findPdaComputeUnits(args.CurrencyConfigBump)

	computeUnits += initializePoolComputeUnits +
		findPdaComputeUnits(args.LiquidityPoolBump) +
		findPdaComputeUnits(args.VaultMintBump) +
		findPdaComputeUnits(args.VaultBaseBump)

	computeUnits += initVmComputeUnits +
		findPdaComputeUnits(args.VmBump) +
		findPdaComputeUnits(args.OmnibusBump)

	computeUnits += transferForSwapWithFeeComputeUnits + buyTokensComputeUnits
	if args.IsTreasuryFunded {
		computeUnits += sellTokensComputeUnits
	} else {
		computeUnits += closeAccountComputeUnits
	}

	computeUnits += initializeMetadataComputeUnits + 2*findPdaComputeUnits(args.MetaplexMetadataBump)

	for _, bump := range args.MemoryAccountBumps {
		computeUnits += initMemoryComputeUnits + findPdaComputeUnits(bump)
	}

	for _, bump := range args.AtaBumps {
		computeUnits += baseAtaCreateComputeUnits + findPdaComputeUnits(bump)
	}

	computeUnits += uint32(args.NumRentTopUps)*systemTransferComputeUnits +
		uint32(args.NumMemoryResizes)*resizeMemoryComputeUnits

	return WithComputeUnitMargin(computeUnits)
}
