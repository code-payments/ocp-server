package launcher

import (
	"bytes"
	"crypto/ed25519"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/code-payments/ocp-server/ocp/common"
	transaction_util "github.com/code-payments/ocp-server/ocp/transaction"
	"github.com/code-payments/ocp-server/solana"
	compute_budget "github.com/code-payments/ocp-server/solana/computebudget"
	"github.com/code-payments/ocp-server/solana/vm"
)

func TestInitNonceBatches(t *testing.T) {
	noneAllocated := func(int) bool { return false }

	batches := initNonceBatches(initialNoncePoolSize, noneAllocated, initNoncesPerTxn)
	require.Len(t, batches, 5)
	for i, batch := range batches {
		require.Len(t, batch, initNoncesPerTxn)
		for j, memoryIndex := range batch {
			assert.Equal(t, i*initNoncesPerTxn+j, memoryIndex)
		}
	}

	// Currencies launched with 1,000 nonces still populate fully
	assert.Len(t, initNonceBatches(1_000, noneAllocated, initNoncesPerTxn), 20)

	// A partial final batch stops at the memory account's capacity
	batches = initNonceBatches(120, noneAllocated, initNoncesPerTxn)
	require.Len(t, batches, 3)
	assert.Len(t, batches[2], 20)
	assert.Equal(t, 119, batches[2][19])

	// A retry only initializes what a failed attempt left unallocated
	allocated := func(index int) bool { return index < 100 || index%2 == 0 }
	batches = initNonceBatches(initialNoncePoolSize, allocated, initNoncesPerTxn)
	require.Len(t, batches, 3)
	for _, batch := range batches {
		assert.Len(t, batch, 25)
		for _, memoryIndex := range batch {
			assert.False(t, allocated(memoryIndex))
		}
	}

	assert.Empty(t, initNonceBatches(initialNoncePoolSize, func(int) bool { return true }, initNoncesPerTxn))
}

func TestInitialNoncePoolSize_FitsInitNonceLoadedAccountsDataSizeLimit(t *testing.T) {
	require.NoError(t, transaction_util.ValidateInitNonceMemoryAccounts(initialNoncePoolSize))
}

func TestMakeInitNonceTransaction(t *testing.T) {
	accounts := &newCurrencyAccounts{
		Authority:          newRandomAccount(t),
		Vm:                 newRandomAccount(t),
		NonceMemoryAccount: newRandomAccount(t),
	}

	memoryIndexes := make([]int, initNoncesPerTxn)
	for i := range memoryIndexes {
		memoryIndexes[i] = 200 + i
	}

	var blockhash solana.Blockhash
	blockhash[0] = 1

	txn, err := makeInitNonceTransaction(accounts, memoryIndexes, blockhash)
	require.NoError(t, err)

	assert.Equal(t, solana.MessageVersion1, txn.Message.Version)
	assert.Equal(t, blockhash, txn.Message.RecentBlockhash)
	assert.Empty(t, txn.Message.AddressTableLookups)

	config := txn.Message.Config
	require.NotNil(t, config.ComputeUnitLimit)
	require.NotNil(t, config.LoadedAccountsDataSizeLimit)
	require.NotNil(t, config.PriorityFeeLamports)
	assert.EqualValues(t, 649_750, *config.ComputeUnitLimit)
	assert.Equal(t, transaction_util.InitNonceBatchComputeUnitLimit(initNoncesPerTxn), *config.ComputeUnitLimit)
	assert.EqualValues(t, transaction_util.InitNonceLoadedAccountsDataSizeLimit, *config.LoadedAccountsDataSizeLimit)
	assert.EqualValues(t, 6_498, *config.PriorityFeeLamports)
	assert.Nil(t, config.HeapSize)

	// Authority, VM, nonce memory, VM program and one owner per nonce
	assert.Len(t, txn.Message.Accounts, 4+initNoncesPerTxn)
	assert.LessOrEqual(t, len(txn.Message.Accounts), 64)
	for _, account := range txn.Message.Accounts {
		assert.NotEqual(t, compute_budget.ProgramKey, account)
	}

	assert.EqualValues(t, 1, txn.Message.Header.NumSignatures)
	assert.EqualValues(t, accounts.Authority.PublicKey().ToBytes(), txn.Message.Accounts[0])
	require.Len(t, txn.Signatures, 1)
	message, err := txn.Message.Marshal()
	require.NoError(t, err)
	assert.True(t, ed25519.Verify(accounts.Authority.PublicKey().ToBytes(), message, txn.Signatures[0][:]))

	owners := make(map[string]struct{})
	require.Len(t, txn.Message.Instructions, initNoncesPerTxn)
	for i, ixn := range txn.Message.Instructions {
		assert.EqualValues(t, vm.PROGRAM_ADDRESS, txn.Message.Accounts[ixn.ProgramIndex])

		require.Len(t, ixn.Data, 1+vm.InitNonceInstructionArgsSize)
		assert.Equal(t, byte(vm.CodeInstructionInitNonce), ixn.Data[0])
		assert.EqualValues(t, memoryIndexes[i], binary.LittleEndian.Uint16(ixn.Data[1:]))

		require.Len(t, ixn.Accounts, 4)
		assert.EqualValues(t, accounts.Authority.PublicKey().ToBytes(), txn.Message.Accounts[ixn.Accounts[0]])
		assert.EqualValues(t, accounts.Vm.PublicKey().ToBytes(), txn.Message.Accounts[ixn.Accounts[1]])
		assert.EqualValues(t, accounts.NonceMemoryAccount.PublicKey().ToBytes(), txn.Message.Accounts[ixn.Accounts[2]])
		owners[string(txn.Message.Accounts[ixn.Accounts[3]])] = struct{}{}
	}
	assert.Len(t, owners, initNoncesPerTxn)

	marshalled, err := txn.Marshal()
	require.NoError(t, err)
	assert.LessOrEqual(t, len(marshalled), solana.MaxV1TransactionSize)

	var decoded solana.Transaction
	require.NoError(t, decoded.Unmarshal(marshalled))
	remarshalled, err := decoded.Marshal()
	require.NoError(t, err)
	assert.True(t, bytes.Equal(marshalled, remarshalled))
}

func newRandomAccount(t *testing.T) *common.Account {
	account, err := common.NewRandomAccount()
	require.NoError(t, err)
	return account
}
