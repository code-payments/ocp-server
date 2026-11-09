package transaction

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/code-payments/ocp-server/solana/currencycreator"
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
		{CurrencyLaunchNonceMemoryAccountName, CurrencyLaunchNoncePoolSize, vm.VirtualAccountTypeDurableNonce, []uint32{10_240, 16_580}},
		{CurrencyLaunchTimelockMemoryAccountName, CurrencyLaunchTimelockAccounts, vm.VirtualAccountTypeTimelock, []uint32{10_240, 20_480, 30_720, 40_960, 51_200, 61_440, 71_680, 78_080}},
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
		assert.Equal(t, expected.resizeSizes, memoryAccount.ResizeSizes())
	}
}

func TestMemoryResizeSizes(t *testing.T) {
	timelockSize := uint16(vm.GetVirtualAccountSizeInMemory(vm.VirtualAccountTypeTimelock))
	nonceSize := uint16(vm.GetVirtualAccountSizeInMemory(vm.VirtualAccountTypeDurableNonce))

	// Memory accounts launched before the nonce pool shrank to 250 still
	// resize to their on-chain capacity
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
