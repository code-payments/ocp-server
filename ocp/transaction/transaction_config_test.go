package transaction

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/code-payments/ocp-server/pointer"
	"github.com/code-payments/ocp-server/solana"
	"github.com/code-payments/ocp-server/solana/vm"
)

func TestPriorityFeeLamports(t *testing.T) {
	for _, tc := range []struct {
		limit    uint32
		price    uint64
		expected uint64
	}{
		{200_000, 10_000, 2_000},
		{116_725, 10_000, 1_168}, // 1,167.25 rounds up
		{1, 1, 1},
		{0, 10_000, 0},
		{200_000, 0, 0},
		{math.MaxUint32, math.MaxUint64, math.MaxUint64}, // saturates
	} {
		assert.Equal(t, tc.expected, PriorityFeeLamports(tc.limit, tc.price), "limit=%d price=%d", tc.limit, tc.price)
	}
}

func TestNewV1TransactionConfig(t *testing.T) {
	config := NewV1TransactionConfig(200_000, 10_000, CoinbaseSwapLoadedAccountsDataSizeLimit)

	require.NotNil(t, config.ComputeUnitLimit)
	assert.EqualValues(t, 200_000, *config.ComputeUnitLimit)
	require.NotNil(t, config.LoadedAccountsDataSizeLimit)
	assert.EqualValues(t, 4*1024*1024, *config.LoadedAccountsDataSizeLimit)
	require.NotNil(t, config.PriorityFeeLamports)
	assert.EqualValues(t, 2_000, *config.PriorityFeeLamports)
	assert.Nil(t, config.HeapSize)

	proto := ToProtoTransactionConfig(config)
	assert.EqualValues(t, 200_000, proto.ComputeUnitLimit)
	assert.EqualValues(t, 4*1024*1024, proto.LoadedAccountsDataSizeLimit)
	assert.EqualValues(t, 2_000, proto.PriorityFeeLamports)
	assert.EqualValues(t, 0, proto.HeapSize)

	proto = ToProtoTransactionConfig(solana.TransactionConfig{HeapSize: pointer.Uint32(64 * 1024)})
	assert.EqualValues(t, 64*1024, proto.HeapSize)
}

func TestValidateInitNonceMemoryAccounts(t *testing.T) {
	assert.NoError(t, ValidateInitNonceMemoryAccounts(250))
	assert.NoError(t, ValidateInitNonceMemoryAccounts(MaxInitNonceMemoryAccounts))
	assert.Error(t, ValidateInitNonceMemoryAccounts(MaxInitNonceMemoryAccounts+1))
}

func TestInitNonceLoadedAccountsDataSizeLimit_CoversMaxNonceMemory(t *testing.T) {
	const (
		accountOverhead = 64 // charged per loaded account

		// VM program data on mainnet as of 2026-11. A program upgrade that grows
		// it needs this updated.
		vmProgramDataSize  = 488_621
		programAccountSize = 36

		// Authority, VM, nonce memory, VM program and 50 nonce owners, plus the
		// program data account the VM program points to
		maxLoadedAccounts = 4 + 50 + 1
	)

	nonceMemorySize := vm.MemoryAccountSize + vm.GetSliceAllocatorSize(
		MaxInitNonceMemoryAccounts,
		int(vm.GetVirtualAccountSizeInMemory(vm.VirtualAccountTypeDurableNonce)),
	)

	loaded := maxLoadedAccounts*accountOverhead +
		vmProgramDataSize +
		programAccountSize +
		vm.CodeVmAccountSize +
		nonceMemorySize
	assert.LessOrEqual(t, loaded, InitNonceLoadedAccountsDataSizeLimit)
}
