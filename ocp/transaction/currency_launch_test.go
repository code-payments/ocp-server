package transaction

import (
	"context"
	"testing"

	"github.com/pkg/errors"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/code-payments/ocp-server/ocp/common"
	"github.com/code-payments/ocp-server/solana/currencycreator"
	"github.com/code-payments/ocp-server/solana/system"
	"github.com/code-payments/ocp-server/solana/vm"
	"github.com/code-payments/ocp-server/testutil"
)

func TestGetCurrencyLaunchMemoryAccounts(t *testing.T) {
	vmAccount := testutil.NewRandomAccount(t)

	memoryAccounts, err := GetCurrencyLaunchMemoryAccounts(vmAccount)
	require.NoError(t, err)
	require.Len(t, memoryAccounts, 2)

	for i, expected := range []struct {
		name        string
		numAccounts uint32
		accountType vm.VirtualAccountType
		resizeSizes []uint32
	}{
		{CurrencyLaunchNonceMemoryAccountName, CurrencyLaunchNoncePoolSize, vm.VirtualAccountTypeDurableNonce, []uint32{6_680}},
		{CurrencyLaunchTimelockMemoryAccountName, CurrencyLaunchTimelockAccounts, vm.VirtualAccountTypeTimelock, []uint32{10_240, 20_480, 30_720, 39_080}},
	} {
		memoryAccount := memoryAccounts[i]

		address, bump, err := vm.GetMemoryAccountAddress(&vm.GetMemoryAccountAddressArgs{
			Name: expected.name,
			Vm:   vmAccount.PublicKey().ToBytes(),
		})
		require.NoError(t, err)

		assert.Equal(t, []byte(address), memoryAccount.Address.PublicKey().ToBytes())
		assert.Equal(t, bump, memoryAccount.Bump)
		assert.Equal(t, expected.name, memoryAccount.Name)
		assert.Equal(t, expected.numAccounts, memoryAccount.NumAccounts)
		assert.EqualValues(t, vm.GetVirtualAccountSizeInMemory(expected.accountType), memoryAccount.AccountSize)
		assert.Equal(t, expected.resizeSizes[len(expected.resizeSizes)-1], memoryAccount.Capacity())
		assert.Equal(t, expected.resizeSizes, memoryAccount.ResizeSizes())
		assert.Zero(t, memoryAccount.RentTopUpLamports)
	}
}

func TestCurrencyLaunch_SetMemoryAccountRentTopUps(t *testing.T) {
	launch := newTestCurrencyLaunch(t)

	require.NoError(t, launch.SetMemoryAccountRentTopUps(context.Background(), testMinimumBalanceForRentExemption))
	for _, memoryAccount := range launch.MemoryAccounts {
		assert.EqualValues(t, 6_960*(memoryAccount.Capacity()-vm.MemoryAccountSize), memoryAccount.RentTopUpLamports, memoryAccount.Name)
	}

	launch = newTestCurrencyLaunch(t)
	err := launch.SetMemoryAccountRentTopUps(context.Background(), func(context.Context, uint64) (uint64, error) {
		return 0, errors.New("rpc error")
	})
	assert.Error(t, err)

	launch = newTestCurrencyLaunch(t)
	err = launch.SetMemoryAccountRentTopUps(context.Background(), func(context.Context, uint64) (uint64, error) {
		return 1, nil
	})
	assert.Error(t, err)
}

func TestCurrencyLaunch_MakePostPurchaseInstructions(t *testing.T) {
	launch := newTestCurrencyLaunch(t)

	// The rent top-ups must be set before the instructions are made
	_, _, err := launch.MakePostPurchaseInstructions()
	assert.Error(t, err)

	require.NoError(t, launch.SetMemoryAccountRentTopUps(context.Background(), testMinimumBalanceForRentExemption))
	ixns, feeAtaBump, err := launch.MakePostPurchaseInstructions()
	require.NoError(t, err)

	authority := launch.Accounts.Authority
	createFeeAtaIxn, _, expectedFeeAtaBump, err := launch.MakeCreateAtaInstruction(launch.FeeAccountOwner, launch.Accounts.Mint)
	require.NoError(t, err)
	assert.Equal(t, expectedFeeAtaBump, feeAtaBump)

	nonceMemory, timelockMemory := launch.MemoryAccounts[0], launch.MemoryAccounts[1]
	expected := []any{
		NewInitializeMetadataInstruction(authority, launch.Accounts.Mint, launch.Accounts.CurrencyConfig, launch.MetaplexMetadata),
		nonceMemory.NewInitMemoryInstruction(authority, launch.VmConfig.Vm),
		timelockMemory.NewInitMemoryInstruction(authority, launch.VmConfig.Vm),
		createFeeAtaIxn,
		system.Transfer(authority.PublicKey().ToBytes(), nonceMemory.Address.PublicKey().ToBytes(), nonceMemory.RentTopUpLamports),
		system.Transfer(authority.PublicKey().ToBytes(), timelockMemory.Address.PublicKey().ToBytes(), timelockMemory.RentTopUpLamports),
	}
	for _, ixn := range NewResizeMemoryInstructions(authority, launch.VmConfig.Vm, nonceMemory.Address, []uint32{6_680}) {
		expected = append(expected, ixn)
	}
	for _, ixn := range NewResizeMemoryInstructions(authority, launch.VmConfig.Vm, timelockMemory.Address, []uint32{10_240, 20_480, 30_720, 39_080}) {
		expected = append(expected, ixn)
	}
	require.Len(t, ixns, len(expected))
	for i := range ixns {
		assert.Equal(t, expected[i], ixns[i], i)
	}
}

func TestCurrencyLaunch_InstructionTraceLength(t *testing.T) {
	launch := newTestCurrencyLaunch(t)

	// Advance nonce (1) + initialize currency (4) + initialize pool (7) + init
	// VM (4) + 2 ATA creates (5 each) + transfer for swap with fee (3) + buy
	// (3) + close (1) + initialize metadata (6) + 2 init memory (2 each) + fee
	// ATA create (5) + 2 rent top-ups (1 each) + 5 pre-funded resizes (1 each)
	assert.Equal(t, 55, launch.InstructionTraceLength(false))
	require.NoError(t, launch.ValidateInstructionTraceLength(false))

	// Treasury funding sells (3) instead of closing (1)
	assert.Equal(t, 57, launch.InstructionTraceLength(true))
	require.NoError(t, launch.ValidateInstructionTraceLength(true))

	// Keep headroom for an optional memo, and for resizes that transfer rent
	// if the rent rate rises between building and executing the transaction
	const headroom = 3
	assert.LessOrEqual(t, launch.InstructionTraceLength(true), MaxInstructionTraceLength-headroom)

	// Growing the timelock memory account past the limit is caught
	launch.MemoryAccounts[1].NumAccounts = 1_000
	assert.Equal(t, 61, launch.InstructionTraceLength(true))
	launch.MemoryAccounts[1].NumAccounts = 2_000
	assert.Greater(t, launch.InstructionTraceLength(true), MaxInstructionTraceLength)
	assert.Error(t, launch.ValidateInstructionTraceLength(true))
}

func newTestCurrencyLaunch(t *testing.T) *CurrencyLaunch {
	vmAccount := testutil.NewRandomAccount(t)
	memoryAccounts, err := GetCurrencyLaunchMemoryAccounts(vmAccount)
	require.NoError(t, err)

	accounts := &common.LaunchpadCurrencyAccounts{
		Authority:      testutil.NewRandomAccount(t),
		Mint:           testutil.NewRandomAccount(t),
		CurrencyConfig: testutil.NewRandomAccount(t),
	}
	metaplexMetadata, metaplexMetadataBump, err := GetMetaplexMetadataAccount(accounts.Mint)
	require.NoError(t, err)

	return &CurrencyLaunch{
		Name:     "Test",
		Symbol:   "TEST",
		Accounts: accounts,
		VmConfig: &common.VmConfig{
			Authority: accounts.Authority,
			Vm:        vmAccount,
			Mint:      accounts.Mint,
		},
		MetaplexMetadata:     metaplexMetadata,
		MetaplexMetadataBump: metaplexMetadataBump,
		MemoryAccounts:       memoryAccounts,
		FeeAccountOwner:      testutil.NewRandomAccount(t),
	}
}

// testMinimumBalanceForRentExemption models the network's rent at 6,960
// lamports per byte including the 128 byte account overhead
func testMinimumBalanceForRentExemption(_ context.Context, size uint64) (uint64, error) {
	return (size + 128) * 6_960, nil
}

func TestMemoryResizeSizes(t *testing.T) {
	timelockSize := uint16(vm.GetVirtualAccountSizeInMemory(vm.VirtualAccountTypeTimelock))
	nonceSize := uint16(vm.GetVirtualAccountSizeInMemory(vm.VirtualAccountTypeDurableNonce))

	// Memory accounts launched with a larger nonce pool still resize to
	// their on-chain capacity
	assert.Equal(t, []uint32{10_240, 20_480, 30_720, 40_960, 51_200, 61_440, 66_080}, MemoryResizeSizes(1_000, nonceSize))

	// A capacity within the first step takes a single resize
	capacity := uint32(vm.MemoryAccountSize + vm.GetSliceAllocatorSize(1, int(timelockSize)))
	assert.Equal(t, []uint32{capacity}, MemoryResizeSizes(1, timelockSize))

	for _, numAccounts := range []uint32{1, 250, 1_000, 32_000} {
		sizes := MemoryResizeSizes(numAccounts, timelockSize)
		require.NotEmpty(t, sizes)

		// Each resize grows by at most a step, and the last one reaches the
		// full capacity without repeating it
		expectedCapacity := uint32(vm.MemoryAccountSize + vm.GetSliceAllocatorSize(int(numAccounts), int(timelockSize)))
		assert.Equal(t, expectedCapacity, sizes[len(sizes)-1])
		previous := uint32(0)
		for _, size := range sizes {
			assert.Greater(t, size, previous)
			assert.LessOrEqual(t, size-previous, uint32(MemoryResizeStepSize))
			previous = size
		}
	}
}

func TestGetMetaplexMetadataAccount(t *testing.T) {
	mint := testutil.NewRandomAccount(t)

	account, bump, err := GetMetaplexMetadataAccount(mint)
	require.NoError(t, err)

	address, expectedBump, err := currencycreator.GetMetadataAddress(&currencycreator.GetMetadataAddressArgs{
		Mint: mint.PublicKey().ToBytes(),
	})
	require.NoError(t, err)
	assert.Equal(t, []byte(address), account.PublicKey().ToBytes())
	assert.Equal(t, expectedBump, bump)
}
