package transaction

import (
	"crypto/ed25519"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	commonpb "github.com/code-payments/ocp-protobuf-api/generated/go/common/v1"
	transactionpb "github.com/code-payments/ocp-protobuf-api/generated/go/transaction/v1"

	"github.com/code-payments/ocp-server/ocp/common"
	ocp_data "github.com/code-payments/ocp-server/ocp/data"
	transaction_util "github.com/code-payments/ocp-server/ocp/transaction"
	"github.com/code-payments/ocp-server/pointer"
	"github.com/code-payments/ocp-server/solana"
	compute_budget "github.com/code-payments/ocp-server/solana/computebudget"
	"github.com/code-payments/ocp-server/solana/memo"
	"github.com/code-payments/ocp-server/solana/token"
	"github.com/code-payments/ocp-server/testutil"
)

func TestMakeStatelessSwapTransaction(t *testing.T) {
	testutil.SetupRandomSubsidizer(t, ocp_data.NewTestDataProvider())

	owner := testutil.NewRandomAccount(t)
	fromMint := testutil.NewRandomAccount(t)
	poolFeeRecipient := testutil.NewRandomAccount(t)

	createDepositAtaIxn, depositAta, err := token.CreateAssociatedTokenAccountIdempotent(
		common.GetSubsidizer().PublicKey().ToBytes(),
		testutil.NewRandomAccount(t).PublicKey().ToBytes(),
		common.CoreMintAccount.PublicKey().ToBytes(),
	)
	require.NoError(t, err)

	coinbaseAccounts := &transaction_util.CoinbaseSwapAccounts{
		Pool:                     testutil.NewRandomAccount(t).PublicKey().ToBytes(),
		InVault:                  testutil.NewRandomAccount(t).PublicKey().ToBytes(),
		OutVault:                 testutil.NewRandomAccount(t).PublicKey().ToBytes(),
		InVaultTokenAccount:      testutil.NewRandomAccount(t).PublicKey().ToBytes(),
		OutVaultTokenAccount:     testutil.NewRandomAccount(t).PublicKey().ToBytes(),
		Whitelist:                testutil.NewRandomAccount(t).PublicKey().ToBytes(),
		FeeRecipient:             poolFeeRecipient.PublicKey().ToBytes(),
		FeeRecipientTokenAccount: testutil.NewRandomAccount(t).PublicKey().ToBytes(),
	}

	var blockhash solana.Blockhash
	copy(blockhash[:], testutil.NewRandomAccount(t).PublicKey().ToBytes())

	for _, swapVersion := range []transaction_util.CoinbaseSwapVersion{transaction_util.CoinbaseSwapV1, transaction_util.CoinbaseSwapV2} {
		coinbaseSwapIxn, err := transaction_util.MakeCoinbaseSwapInstruction(
			swapVersion,
			coinbaseAccounts,
			owner.PublicKey().ToBytes(),
			fromMint.PublicKey().ToBytes(),
			common.CoreMintAccount.PublicKey().ToBytes(),
			testutil.NewRandomAccount(t).PublicKey().ToBytes(),
			depositAta,
			1_000_000,
			1_000_000,
		)
		require.NoError(t, err)

		t.Run("txn v0 swap "+swapVersion.String(), func(t *testing.T) {
			txn, serverParameters, err := makeStatelessSwapTransaction(solana.MessageVersion0, blockhash, poolFeeRecipient, createDepositAtaIxn, coinbaseSwapIxn)
			require.NoError(t, err)

			assert.Equal(t, solana.MessageVersion0, txn.Message.Version)
			assertCommonStatelessSwapServerParameters(t, serverParameters, blockhash, poolFeeRecipient)
			assert.Equal(t, "coinbase_stable_swapper_v0", serverParameters.MemoValue)
			assert.Equal(t, transaction_util.ToProtoAlts([]solana.AddressLookupTable{transaction_util.GetAltForCoreMint()}), serverParameters.Alts)
			assert.EqualValues(t, statelessSwapComputeUnitLimit, serverParameters.ComputeUnitLimit)
			assert.EqualValues(t, statelessSwapComputeUnitPrice, serverParameters.ComputeUnitPrice)
			assert.Nil(t, serverParameters.TransactionConfig)

			requireClientRebuildMatches(t, txn, serverParameters, createDepositAtaIxn, coinbaseSwapIxn)
		})

		t.Run("txn v1 swap "+swapVersion.String(), func(t *testing.T) {
			txn, serverParameters, err := makeStatelessSwapTransaction(solana.MessageVersion1, blockhash, poolFeeRecipient, createDepositAtaIxn, coinbaseSwapIxn)
			require.NoError(t, err)

			assert.Equal(t, solana.MessageVersion1, txn.Message.Version)
			assert.Empty(t, txn.Message.AddressTableLookups)
			assertCommonStatelessSwapServerParameters(t, serverParameters, blockhash, poolFeeRecipient)
			assert.Equal(t, "coinbase_stable_swapper_v1", serverParameters.MemoValue)
			assert.Empty(t, serverParameters.Alts)
			assert.Zero(t, serverParameters.ComputeUnitLimit)
			assert.Zero(t, serverParameters.ComputeUnitPrice)

			require.NotNil(t, serverParameters.TransactionConfig)
			assert.EqualValues(t, statelessSwapComputeUnitLimit, serverParameters.TransactionConfig.ComputeUnitLimit)
			assert.EqualValues(t, transaction_util.CoinbaseSwapLoadedAccountsDataSizeLimit, serverParameters.TransactionConfig.LoadedAccountsDataSizeLimit)
			assert.EqualValues(t, 1_000, serverParameters.TransactionConfig.PriorityFeeLamports)
			assert.Zero(t, serverParameters.TransactionConfig.HeapSize)

			// ComputeBudget instructions are omitted in v1
			for _, account := range txn.Message.Accounts {
				assert.NotEqual(t, compute_budget.ProgramKey, account)
			}
			assert.Len(t, txn.Message.Instructions, 3)

			marshalled, err := txn.Marshal()
			require.NoError(t, err)
			assert.LessOrEqual(t, len(marshalled), solana.MaxV1TransactionSize)

			requireClientRebuildMatches(t, txn, serverParameters, createDepositAtaIxn, coinbaseSwapIxn)
		})
	}

	t.Run("unsupported version", func(t *testing.T) {
		_, _, err := makeStatelessSwapTransaction(solana.MessageVersionLegacy, blockhash, poolFeeRecipient, createDepositAtaIxn, createDepositAtaIxn)
		assert.Error(t, err)
	})
}

func assertCommonStatelessSwapServerParameters(t *testing.T, serverParameters *transactionpb.StatelessSwapResponse_ServerParameters_CoinbaseStableSwapperServerParameter, blockhash solana.Blockhash, poolFeeRecipient *common.Account) {
	assert.Equal(t, common.GetSubsidizer().PublicKey().ToBytes(), serverParameters.Payer.Value)
	assert.Equal(t, blockhash[:], serverParameters.Blockhash.Value)
	assert.Equal(t, poolFeeRecipient.PublicKey().ToBytes(), serverParameters.PoolFeeRecipient.Value)
}

// requireClientRebuildMatches builds the transaction from the server
// parameters the way the proto documents for clients, and requires the
// message clients sign to match the server's exactly.
func requireClientRebuildMatches(
	t *testing.T,
	serverTxn solana.Transaction,
	serverParameters *transactionpb.StatelessSwapResponse_ServerParameters_CoinbaseStableSwapperServerParameter,
	createDepositAtaIxn, coinbaseSwapIxn solana.Instruction,
) {
	var ixns []solana.Instruction
	if serverParameters.TransactionConfig == nil {
		if serverParameters.ComputeUnitLimit != 0 {
			ixns = append(ixns, compute_budget.SetComputeUnitLimit(serverParameters.ComputeUnitLimit))
		}
		if serverParameters.ComputeUnitPrice != 0 {
			ixns = append(ixns, compute_budget.SetComputeUnitPrice(serverParameters.ComputeUnitPrice))
		}
	}
	if len(serverParameters.MemoValue) > 0 {
		ixns = append(ixns, memo.Instruction(serverParameters.MemoValue))
	}
	ixns = append(ixns, createDepositAtaIxn, coinbaseSwapIxn)

	var clientTxn solana.Transaction
	if serverParameters.TransactionConfig == nil {
		clientTxn = solana.NewV0Transaction(serverParameters.Payer.Value, fromProtoAlts(serverParameters.Alts), ixns)
	} else {
		// CU limit, loaded accounts data size limit and priority fee are always
		// encoded, and the heap size only when non-zero
		config := solana.TransactionConfig{
			ComputeUnitLimit:            pointer.Uint32(serverParameters.TransactionConfig.ComputeUnitLimit),
			LoadedAccountsDataSizeLimit: pointer.Uint32(serverParameters.TransactionConfig.LoadedAccountsDataSizeLimit),
			PriorityFeeLamports:         pointer.Uint64(serverParameters.TransactionConfig.PriorityFeeLamports),
		}
		if serverParameters.TransactionConfig.HeapSize != 0 {
			config.HeapSize = pointer.Uint32(serverParameters.TransactionConfig.HeapSize)
		}

		var err error
		clientTxn, err = solana.NewV1Transaction(serverParameters.Payer.Value, config, ixns...)
		require.NoError(t, err)
	}

	var blockhash solana.Blockhash
	copy(blockhash[:], serverParameters.Blockhash.Value)
	clientTxn.SetBlockhash(blockhash)

	expected, err := serverTxn.Message.Marshal()
	require.NoError(t, err)
	actual, err := clientTxn.Message.Marshal()
	require.NoError(t, err)
	require.Equal(t, expected, actual)
}

func fromProtoAlts(protos []*commonpb.SolanaAddressLookupTable) []solana.AddressLookupTable {
	alts := make([]solana.AddressLookupTable, len(protos))
	for i, proto := range protos {
		alts[i] = solana.AddressLookupTable{PublicKey: proto.Address.Value}
		for _, entry := range proto.Entries {
			alts[i].Addresses = append(alts[i].Addresses, ed25519.PublicKey(entry.Value))
		}
	}
	return alts
}
