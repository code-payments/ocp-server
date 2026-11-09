package transaction

import (
	"context"
	"crypto/ed25519"

	"github.com/pkg/errors"

	"github.com/code-payments/ocp-server/ocp/common"
	"github.com/code-payments/ocp-server/ocp/data/currency"
	"github.com/code-payments/ocp-server/ocp/data/swap"
	vm_metadata "github.com/code-payments/ocp-server/ocp/data/vm/metadata"
	"github.com/code-payments/ocp-server/solana"
	"github.com/code-payments/ocp-server/solana/currencycreator"
	"github.com/code-payments/ocp-server/solana/system"
	timelock_token "github.com/code-payments/ocp-server/solana/timelock/v1"
	"github.com/code-payments/ocp-server/solana/token"
	"github.com/code-payments/ocp-server/solana/vm"
)

const (
	CurrencyLaunchNoncePoolSize          = 100
	CurrencyLaunchNonceMemoryAccountName = "nonce-0"

	CurrencyLaunchTimelockAccounts          = 500
	CurrencyLaunchTimelockMemoryAccountName = "timelock-0"

	// MemoryResizeStepSize is the most a single VM::ResizeMemory instruction
	// grows a memory account by, which is the runtime's per-instruction
	// realloc limit
	MemoryResizeStepSize = 10 * 1024

	// MaxInstructionTraceLength is the runtime's limit on a transaction's
	// instruction trace: its top-level instructions plus every CPI they make
	MaxInstructionTraceLength = 64
)

// Instruction trace entries each launch instruction takes in the worst case,
// where every account it opens is new: the instruction plus the CPIs it
// makes. Measured against the mainnet programs.
const (
	advanceNonceTraceLength           = 1
	initializeCurrencyTraceLength     = 4
	initializePoolTraceLength         = 7
	initVmTraceLength                 = 4
	createAtaTraceLength              = 5
	transferForSwapWithFeeTraceLength = 3 // also when the fee is 0
	buyTokensTraceLength              = 3
	sellTokensTraceLength             = 3
	closeAccountTraceLength           = 1
	initializeMetadataTraceLength     = 6
	initMemoryTraceLength             = 2
	rentTopUpTraceLength              = 1

	// VM::ResizeMemory transfers rent with a CPI only when the memory account
	// is short of rent for its new size. The rent top-up pre-funds it.
	prefundedResizeMemoryTraceLength = 1
)

// GetInitialPurchaseTransactionVersion returns the version of a currency's
// initial purchase transaction, which its swap record holds until the swap
// reaches a terminal state. The version decides how the currency is
// initialized and what its launch is charged.
func GetInitialPurchaseTransactionVersion(swapRecord *swap.Record) (solana.MessageVersion, error) {
	var txn solana.Transaction
	err := txn.Unmarshal(swapRecord.TransactionBlob)
	if err != nil {
		return 0, errors.Wrap(err, "error unmarshalling initial purchase transaction")
	}
	return txn.Message.Version, nil
}

// CurrencyLaunch builds the instructions that launch a new currency within its
// first purchase transaction. The currency authority pays for, and signs,
// every account it initializes.
type CurrencyLaunch struct {
	Name     string
	Symbol   string
	Accounts *common.LaunchpadCurrencyAccounts

	VmConfig    *common.VmConfig
	VmBump      uint8
	OmnibusBump uint8

	MetaplexMetadata     *common.Account
	MetaplexMetadataBump uint8

	MemoryAccounts  []*CurrencyLaunchMemoryAccount
	FeeAccountOwner *common.Account
}

// NewCurrencyLaunch derives the accounts a launch initializes for the currency
func NewCurrencyLaunch(metadataRecord *currency.MetadataRecord, vmMetadataRecord *vm_metadata.Record) (*CurrencyLaunch, error) {
	accounts, err := common.GetLaunchpadCurrencyAccounts(metadataRecord)
	if err != nil {
		return nil, err
	}

	vmAccount, err := common.NewAccountFromPublicKeyString(vmMetadataRecord.Vm)
	if err != nil {
		return nil, err
	}
	omnibusAccount, err := common.NewAccountFromPublicKeyString(vmMetadataRecord.Omnibus)
	if err != nil {
		return nil, err
	}

	metaplexMetadata, metaplexMetadataBump, err := GetMetaplexMetadataAccount(accounts.Mint)
	if err != nil {
		return nil, err
	}

	memoryAccounts, err := GetCurrencyLaunchMemoryAccounts(vmAccount)
	if err != nil {
		return nil, err
	}

	return &CurrencyLaunch{
		Name:     metadataRecord.Name,
		Symbol:   metadataRecord.Symbol,
		Accounts: accounts,

		VmConfig: &common.VmConfig{
			Authority: accounts.Authority,
			Vm:        vmAccount,
			Omnibus:   omnibusAccount,
			Mint:      accounts.Mint,
		},
		VmBump:      vmMetadataRecord.VmBump,
		OmnibusBump: vmMetadataRecord.OmnibusBump,

		MetaplexMetadata:     metaplexMetadata,
		MetaplexMetadataBump: metaplexMetadataBump,

		MemoryAccounts:  memoryAccounts,
		FeeAccountOwner: common.GetSubsidizer(),
	}, nil
}

// MakeInitInstructions makes the Reserve::InitializeCurrency,
// Reserve::InitializePool and VM::InitializeVm instructions
func (l *CurrencyLaunch) MakeInitInstructions() []solana.Instruction {
	initCurrencyIxn := currencycreator.NewInitializeCurrencyInstruction(
		&currencycreator.InitializeCurrencyInstructionAccounts{
			Authority: l.Accounts.Authority.PublicKey().ToBytes(),
			Mint:      l.Accounts.Mint.PublicKey().ToBytes(),
			Currency:  l.Accounts.CurrencyConfig.PublicKey().ToBytes(),
		},
		&currencycreator.InitializeCurrencyInstructionArgs{
			Name:     l.Name,
			Symbol:   l.Symbol,
			Seed:     l.Accounts.Seed.PublicKey().ToBytes(),
			Bump:     l.Accounts.CurrencyConfigBump,
			MintBump: l.Accounts.MintBump,
		},
	)

	initPoolIxn := currencycreator.NewInitializePoolInstruction(
		&currencycreator.InitializePoolInstructionAccounts{
			Authority:   l.Accounts.Authority.PublicKey().ToBytes(),
			Currency:    l.Accounts.CurrencyConfig.PublicKey().ToBytes(),
			TargetMint:  l.Accounts.Mint.PublicKey().ToBytes(),
			BaseMint:    common.CoreMintAccount.PublicKey().ToBytes(),
			Pool:        l.Accounts.LiquidityPool.PublicKey().ToBytes(),
			VaultTarget: l.Accounts.VaultMint.PublicKey().ToBytes(),
			VaultBase:   l.Accounts.VaultBase.PublicKey().ToBytes(),
		},
		&currencycreator.InitializePoolInstructionArgs{
			SellFee:         currencycreator.DefaultSellFeeBps,
			Bump:            l.Accounts.LiquidityPoolBump,
			VaultTargetBump: l.Accounts.VaultMintBump,
			VaultBaseBump:   l.Accounts.VaultBaseBump,
		},
	)

	initVmIxn := vm.NewInitVmInstruction(
		&vm.InitVmInstructionAccounts{
			VmAuthority: l.Accounts.Authority.PublicKey().ToBytes(),
			Vm:          l.VmConfig.Vm.PublicKey().ToBytes(),
			VmOmnibus:   l.VmConfig.Omnibus.PublicKey().ToBytes(),
			Mint:        l.Accounts.Mint.PublicKey().ToBytes(),
		},
		&vm.InitVmInstructionArgs{
			LockDuration:  timelock_token.DefaultNumDaysLocked,
			VmBump:        l.VmBump,
			VmOmnibusBump: l.OmnibusBump,
		},
	)

	return []solana.Instruction{initCurrencyIxn, initPoolIxn, initVmIxn}
}

// MakeCreateAtaInstruction makes an AssociatedTokenAccount::CreateIdempotent
// instruction paid for by the currency authority, and returns the ATA and its
// bump
func (l *CurrencyLaunch) MakeCreateAtaInstruction(owner, mint *common.Account) (solana.Instruction, ed25519.PublicKey, uint8, error) {
	ixn, ata, err := token.CreateAssociatedTokenAccountIdempotent(
		l.Accounts.Authority.PublicKey().ToBytes(),
		owner.PublicKey().ToBytes(),
		mint.PublicKey().ToBytes(),
	)
	if err != nil {
		return solana.Instruction{}, nil, 0, err
	}

	_, bump, err := token.GetAssociatedAccountAndBump(owner.PublicKey().ToBytes(), mint.PublicKey().ToBytes())
	if err != nil {
		return solana.Instruction{}, nil, 0, err
	}

	return ixn, ata, bump, nil
}

// SetMemoryAccountRentTopUps sets each memory account's rent top-up to the
// rent for its full capacity beyond what VM::InitMemory pays. Rent is read
// rather than computed because the network's rent rate can change.
func (l *CurrencyLaunch) SetMemoryAccountRentTopUps(ctx context.Context, getMinimumBalanceForRentExemption func(ctx context.Context, size uint64) (uint64, error)) error {
	initRent, err := getMinimumBalanceForRentExemption(ctx, vm.MemoryAccountSize)
	if err != nil {
		return errors.Wrap(err, "error getting rent for initialized memory account")
	}

	for _, memoryAccount := range l.MemoryAccounts {
		fullRent, err := getMinimumBalanceForRentExemption(ctx, uint64(memoryAccount.Capacity()))
		if err != nil {
			return errors.Wrapf(err, "error getting rent for %s memory account", memoryAccount.Name)
		}
		if fullRent <= initRent {
			return errors.Errorf("%s memory account rent at full capacity (%d) doesn't exceed rent at initialization (%d)", memoryAccount.Name, fullRent, initRent)
		}
		memoryAccount.RentTopUpLamports = fullRent - initRent
	}
	return nil
}

// MakePostPurchaseInstructions makes the instructions that follow the
// purchase: Reserve::InitializeMetadata, VM::InitMemory for each memory
// account, the fee account's creation, a System::Transfer of each memory
// account's rent top-up, then the VM::ResizeMemory instructions growing each
// memory account to full capacity. It returns the fee account's bump.
//
// The rent top-ups pre-fund the resizes, so VM::ResizeMemory doesn't transfer
// rent with a CPI. That keeps the transaction's instruction trace within
// MaxInstructionTraceLength.
func (l *CurrencyLaunch) MakePostPurchaseInstructions() ([]solana.Instruction, uint8, error) {
	for _, memoryAccount := range l.MemoryAccounts {
		if memoryAccount.RentTopUpLamports == 0 {
			return nil, 0, errors.Errorf("%s memory account rent top-up is not set", memoryAccount.Name)
		}
	}

	ixns := []solana.Instruction{
		NewInitializeMetadataInstruction(
			l.Accounts.Authority,
			l.Accounts.Mint,
			l.Accounts.CurrencyConfig,
			l.MetaplexMetadata,
		),
	}

	for _, memoryAccount := range l.MemoryAccounts {
		ixns = append(ixns, memoryAccount.NewInitMemoryInstruction(l.Accounts.Authority, l.VmConfig.Vm))
	}

	createFeeAtaIxn, _, feeAtaBump, err := l.MakeCreateAtaInstruction(l.FeeAccountOwner, l.Accounts.Mint)
	if err != nil {
		return nil, 0, err
	}
	ixns = append(ixns, createFeeAtaIxn)

	for _, memoryAccount := range l.MemoryAccounts {
		ixns = append(ixns, memoryAccount.NewRentTopUpInstruction(l.Accounts.Authority))
	}

	for _, memoryAccount := range l.MemoryAccounts {
		ixns = append(ixns, memoryAccount.NewResizeMemoryInstructions(l.Accounts.Authority, l.VmConfig.Vm)...)
	}

	return ixns, feeAtaBump, nil
}

// InstructionTraceLength returns the worst-case instruction trace length of
// a launch transaction, where every account the transaction opens is new
func (l *CurrencyLaunch) InstructionTraceLength(isTreasuryFunded bool) int {
	length := advanceNonceTraceLength +
		initializeCurrencyTraceLength +
		initializePoolTraceLength +
		initVmTraceLength

	// The VM deposit ATA, then either the temporary core mint ATA or the
	// treasury's from mint ATA
	length += 2 * createAtaTraceLength

	length += transferForSwapWithFeeTraceLength + buyTokensTraceLength
	if isTreasuryFunded {
		length += sellTokensTraceLength
	} else {
		length += closeAccountTraceLength
	}

	length += initializeMetadataTraceLength
	for _, memoryAccount := range l.MemoryAccounts {
		length += initMemoryTraceLength +
			rentTopUpTraceLength +
			len(memoryAccount.ResizeSizes())*prefundedResizeMemoryTraceLength
	}

	// The fee ATA
	length += createAtaTraceLength

	return length
}

// ValidateInstructionTraceLength errors when a launch transaction's
// worst-case instruction trace exceeds MaxInstructionTraceLength
func (l *CurrencyLaunch) ValidateInstructionTraceLength(isTreasuryFunded bool) error {
	length := l.InstructionTraceLength(isTreasuryFunded)
	if length > MaxInstructionTraceLength {
		return errors.Errorf("launch transaction instruction trace length %d exceeds %d", length, MaxInstructionTraceLength)
	}
	return nil
}

// ComputeUnitLimit computes the compute unit limit for a launch transaction
// that creates ATAs with the given bumps
func (l *CurrencyLaunch) ComputeUnitLimit(ataBumps []uint8, isTreasuryFunded bool) uint32 {
	args := &NewCurrencyLaunchComputeUnitArgs{
		MintBump:             l.Accounts.MintBump,
		CurrencyConfigBump:   l.Accounts.CurrencyConfigBump,
		LiquidityPoolBump:    l.Accounts.LiquidityPoolBump,
		VaultMintBump:        l.Accounts.VaultMintBump,
		VaultBaseBump:        l.Accounts.VaultBaseBump,
		VmBump:               l.VmBump,
		OmnibusBump:          l.OmnibusBump,
		MetaplexMetadataBump: l.MetaplexMetadataBump,
		AtaBumps:             ataBumps,
		IsTreasuryFunded:     isTreasuryFunded,
	}
	for _, memoryAccount := range l.MemoryAccounts {
		args.MemoryAccountBumps = append(args.MemoryAccountBumps, memoryAccount.Bump)
		args.NumRentTopUps++
		args.NumMemoryResizes += len(memoryAccount.ResizeSizes())
	}
	return NewCurrencyLaunchComputeUnitLimit(args)
}

// TransactionConfig returns the v1 transaction config for a launch
// transaction
func (l *CurrencyLaunch) TransactionConfig(computeUnitLimit uint32, computeUnitPrice uint64) solana.TransactionConfig {
	return NewV1TransactionConfig(computeUnitLimit, computeUnitPrice, NewCurrencyLaunchLoadedAccountsDataSizeLimit)
}

// CurrencyLaunchMemoryAccount is a VM memory account and the arguments it's
// initialized with
type CurrencyLaunchMemoryAccount struct {
	Address     *common.Account
	Bump        uint8
	Name        string
	NumAccounts uint32
	AccountSize uint16

	RentTopUpLamports uint64
}

// GetCurrencyLaunchMemoryAccounts returns the memory accounts a new currency's
// VM is launched with, in initialization order: durable nonces, then timelocks
func GetCurrencyLaunchMemoryAccounts(vmAccount *common.Account) ([]*CurrencyLaunchMemoryAccount, error) {
	nonceMemoryAccount, err := newCurrencyLaunchMemoryAccount(
		vmAccount,
		CurrencyLaunchNonceMemoryAccountName,
		CurrencyLaunchNoncePoolSize,
		vm.VirtualAccountTypeDurableNonce,
	)
	if err != nil {
		return nil, errors.Wrap(err, "error deriving nonce memory account")
	}

	timelockMemoryAccount, err := newCurrencyLaunchMemoryAccount(
		vmAccount,
		CurrencyLaunchTimelockMemoryAccountName,
		CurrencyLaunchTimelockAccounts,
		vm.VirtualAccountTypeTimelock,
	)
	if err != nil {
		return nil, errors.Wrap(err, "error deriving timelock memory account")
	}

	return []*CurrencyLaunchMemoryAccount{nonceMemoryAccount, timelockMemoryAccount}, nil
}

func newCurrencyLaunchMemoryAccount(vmAccount *common.Account, name string, numAccounts uint32, accountType vm.VirtualAccountType) (*CurrencyLaunchMemoryAccount, error) {
	address, bump, err := vm.GetMemoryAccountAddress(&vm.GetMemoryAccountAddressArgs{
		Name: name,
		Vm:   vmAccount.PublicKey().ToBytes(),
	})
	if err != nil {
		return nil, err
	}

	account, err := common.NewAccountFromPublicKeyBytes(address)
	if err != nil {
		return nil, err
	}

	return &CurrencyLaunchMemoryAccount{
		Address:     account,
		Bump:        bump,
		Name:        name,
		NumAccounts: numAccounts,
		AccountSize: uint16(vm.GetVirtualAccountSizeInMemory(accountType)),
	}, nil
}

// Capacity returns the memory account's size at full capacity
func (m *CurrencyLaunchMemoryAccount) Capacity() uint32 {
	return MemoryCapacity(m.NumAccounts, m.AccountSize)
}

// ResizeSizes returns the account sizes passed to successive VM::ResizeMemory
// instructions to grow the memory account to its full capacity
func (m *CurrencyLaunchMemoryAccount) ResizeSizes() []uint32 {
	return MemoryResizeSizes(m.NumAccounts, m.AccountSize)
}

// NewRentTopUpInstruction makes the System::Transfer of the memory account's
// rent top-up from the VM authority
func (m *CurrencyLaunchMemoryAccount) NewRentTopUpInstruction(vmAuthority *common.Account) solana.Instruction {
	return system.Transfer(vmAuthority.PublicKey().ToBytes(), m.Address.PublicKey().ToBytes(), m.RentTopUpLamports)
}

// NewInitMemoryInstruction makes the VM::InitMemory instruction for the
// memory account, paid for by the VM authority
func (m *CurrencyLaunchMemoryAccount) NewInitMemoryInstruction(vmAuthority, vmAccount *common.Account) solana.Instruction {
	return vm.NewInitMemoryInstruction(
		&vm.InitMemoryInstructionAccounts{
			VmAuthority: vmAuthority.PublicKey().ToBytes(),
			Vm:          vmAccount.PublicKey().ToBytes(),
			VmMemory:    m.Address.PublicKey().ToBytes(),
		},
		&vm.InitMemoryInstructionArgs{
			Name:         m.Name,
			NumAccounts:  m.NumAccounts,
			AccountSize:  m.AccountSize,
			VmMemoryBump: m.Bump,
		},
	)
}

// NewResizeMemoryInstructions makes the VM::ResizeMemory instructions that
// grow the memory account to its full capacity, paid for by the VM authority
func (m *CurrencyLaunchMemoryAccount) NewResizeMemoryInstructions(vmAuthority, vmAccount *common.Account) []solana.Instruction {
	return NewResizeMemoryInstructions(vmAuthority, vmAccount, m.Address, m.ResizeSizes())
}

// MemoryResizeSizes returns the account sizes passed to successive
// VM::ResizeMemory instructions to grow a memory account holding numAccounts
// virtual accounts of accountSize bytes to its full capacity
func MemoryResizeSizes(numAccounts uint32, accountSize uint16) []uint32 {
	capacity := MemoryCapacity(numAccounts, accountSize)

	var sizes []uint32
	for size := uint32(MemoryResizeStepSize); ; size += MemoryResizeStepSize {
		if size >= capacity {
			sizes = append(sizes, capacity)
			return sizes
		}
		sizes = append(sizes, size)
	}
}

// MemoryCapacity returns the size of a memory account holding numAccounts
// virtual accounts of accountSize bytes at full capacity
func MemoryCapacity(numAccounts uint32, accountSize uint16) uint32 {
	return uint32(vm.MemoryAccountSize + vm.GetSliceAllocatorSize(int(numAccounts), int(accountSize)))
}

// NewResizeMemoryInstructions makes a VM::ResizeMemory instruction for each
// size, paid for by the VM authority
func NewResizeMemoryInstructions(vmAuthority, vmAccount, memoryAccount *common.Account, sizes []uint32) []solana.Instruction {
	ixns := make([]solana.Instruction, len(sizes))
	for i, size := range sizes {
		ixns[i] = vm.NewResizeMemoryInstruction(
			&vm.ResizeMemoryInstructionAccounts{
				VmAuthority: vmAuthority.PublicKey().ToBytes(),
				Vm:          vmAccount.PublicKey().ToBytes(),
				VmMemory:    memoryAccount.PublicKey().ToBytes(),
			},
			&vm.ResizeMemoryInstructionArgs{
				AccountSize: size,
			},
		)
	}
	return ixns
}

// GetMetaplexMetadataAccount returns the mint's Metaplex metadata account and
// its bump
func GetMetaplexMetadataAccount(mint *common.Account) (*common.Account, uint8, error) {
	address, bump, err := currencycreator.GetMetadataAddress(&currencycreator.GetMetadataAddressArgs{
		Mint: mint.PublicKey().ToBytes(),
	})
	if err != nil {
		return nil, 0, err
	}

	account, err := common.NewAccountFromPublicKeyBytes(address)
	if err != nil {
		return nil, 0, err
	}
	return account, bump, nil
}

// NewInitializeMetadataInstruction makes the Reserve::InitializeMetadata
// instruction, paid for by the currency authority
func NewInitializeMetadataInstruction(authority, mint, currencyConfig, metaplexMetadata *common.Account) solana.Instruction {
	return currencycreator.NewInitializeMetadataInstruction(
		&currencycreator.InitializeMetadataInstructionAccounts{
			Authority: authority.PublicKey().ToBytes(),
			Mint:      mint.PublicKey().ToBytes(),
			Currency:  currencyConfig.PublicKey().ToBytes(),
			Metadata:  metaplexMetadata.PublicKey().ToBytes(),
		},
		&currencycreator.InitializeMetadataInstructionArgs{},
	)
}
