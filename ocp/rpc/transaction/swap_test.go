package transaction

import (
	"bytes"
	"context"
	"encoding/base64"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	commonpb "github.com/code-payments/ocp-protobuf-api/generated/go/common/v1"
	transactionpb "github.com/code-payments/ocp-protobuf-api/generated/go/transaction/v1"

	"github.com/code-payments/ocp-server/ocp/common"
	transaction_util "github.com/code-payments/ocp-server/ocp/transaction"
	"github.com/code-payments/ocp-server/pointer"
	"github.com/code-payments/ocp-server/solana"
	coinbase_stable_swapper "github.com/code-payments/ocp-server/solana/coinbasestableswapper"
	compute_budget "github.com/code-payments/ocp-server/solana/computebudget"
	"github.com/code-payments/ocp-server/solana/memo"
	"github.com/code-payments/ocp-server/solana/system"
	"github.com/code-payments/ocp-server/solana/token"
	"github.com/code-payments/ocp-server/solana/vm"
	"github.com/code-payments/ocp-server/testutil"
	"github.com/code-payments/ocp-server/usdc"
)

func usdcMint(t *testing.T) *commonpb.SolanaAccountId {
	mint, err := common.NewAccountFromPublicKeyString(usdc.Mint)
	require.NoError(t, err)
	return mint.ToProto()
}

func TestStatefulSwap_TransactionVersionV1(t *testing.T) {
	env := setupTestEnv(t, withSwapsEnabled())
	owner := env.newUser(t)
	swapAuthority := env.newAccount(t)

	for _, tc := range []struct {
		name     string
		initiate *transactionpb.StatefulSwapRequest_Initiate
		assert   func(t *testing.T, respErr *transactionpb.StatefulSwapResponse_Error)
	}{
		{
			name: "reserve swaps deny v1",
			initiate: &transactionpb.StatefulSwapRequest_Initiate{
				Kind: &transactionpb.StatefulSwapRequest_Initiate_Reserve{
					Reserve: &transactionpb.StatefulSwapRequest_Initiate_ReserveSwapClientParameters{
						Id:            &commonpb.SwapId{Value: testutil.NewRandomAccount(t).PublicKey().ToBytes()},
						FromMint:      common.CoreMintAccount.ToProto(),
						ToMint:        env.newLaunchpadMint(t).ToProto(),
						SwapAmount:    common.CoreMintQuarksPerUnit,
						FundingSource: transactionpb.FundingSource_FUNDING_SOURCE_SUBMIT_INTENT,
						FundingId:     testutil.NewRandomAccount(t).PublicKey().ToBase58(),
					},
				},
			},
			assert: func(t *testing.T, respErr *transactionpb.StatefulSwapResponse_Error) {
				require.Equal(t, transactionpb.StatefulSwapResponse_Error_DENIED, respErr.Code)
				require.Len(t, respErr.ErrorDetails, 1)
				require.Equal(t, "transaction version 1 is not supported", respErr.ErrorDetails[0].GetDenied().Reason)
			},
		},
		{
			// A v1 request passes the version check and reaches verification
			// of the proof signature, which signs the wrong message here
			name: "coinbase swaps accept v1",
			initiate: &transactionpb.StatefulSwapRequest_Initiate{
				Kind: &transactionpb.StatefulSwapRequest_Initiate_Stablecoin{
					Stablecoin: &transactionpb.StatefulSwapRequest_Initiate_CoinbaseStableSwapperClientParameters{
						Id:               &commonpb.SwapId{Value: testutil.NewRandomAccount(t).PublicKey().ToBytes()},
						FromMint:         common.CoreMintAccount.ToProto(),
						ToMint:           usdcMint(t),
						SwapAmount:       common.CoreMintQuarksPerUnit,
						FundingSource:    transactionpb.FundingSource_FUNDING_SOURCE_SUBMIT_INTENT,
						FundingId:        testutil.NewRandomAccount(t).PublicKey().ToBase58(),
						DestinationOwner: testutil.NewRandomAccount(t).ToProto(),
					},
				},
			},
			assert: func(t *testing.T, respErr *transactionpb.StatefulSwapResponse_Error) {
				require.Equal(t, transactionpb.StatefulSwapResponse_Error_SIGNATURE_ERROR, respErr.Code)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(env.ctx, defaultTestStreamTimeout)
			defer cancel()

			stream, err := env.client.StatefulSwap(ctx)
			require.NoError(t, err)

			initiate := tc.initiate
			initiate.Owner = owner.ToProto()
			initiate.SwapAuthority = swapAuthority.ToProto()
			initiate.ProofSignature = env.signProto(t, owner, initiate)
			initiate.TransactionVersion = 1
			initiate.Signature = env.signProto(t, owner, initiate)

			require.NoError(t, stream.Send(&transactionpb.StatefulSwapRequest{
				Request: &transactionpb.StatefulSwapRequest_Initiate_{Initiate: initiate},
			}))

			resp, err := stream.Recv()
			require.NoError(t, err)
			require.NotNil(t, resp.GetError())
			tc.assert(t, resp.GetError())
		})
	}
}

func TestStatelessSwap_TransactionVersionV1Accepted(t *testing.T) {
	env := setupTestEnv(t, withSwapsEnabled())
	owner := env.newUser(t)

	ctx, cancel := context.WithTimeout(env.ctx, defaultTestStreamTimeout)
	defer cancel()

	stream, err := env.client.StatelessSwap(ctx)
	require.NoError(t, err)

	// A v1 request passes the version check and reaches swap validation,
	// which rejects the core mint as a source
	initiate := &transactionpb.StatelessSwapRequest_Initiate{
		Kind: &transactionpb.StatelessSwapRequest_Initiate_Stablecoin{
			Stablecoin: &transactionpb.StatelessSwapRequest_Initiate_CoinbaseStableSwapperClientParameters{
				FromMint:   common.CoreMintAccount.ToProto(),
				ToMint:     common.CoreMintAccount.ToProto(),
				SwapAmount: common.CoreMintQuarksPerUnit,
			},
		},
		Owner:              owner.ToProto(),
		TransactionVersion: 1,
	}
	initiate.Signature = env.signProto(t, owner, initiate)

	require.NoError(t, stream.Send(&transactionpb.StatelessSwapRequest{
		Request: &transactionpb.StatelessSwapRequest_Initiate_{Initiate: initiate},
	}))

	resp, err := stream.Recv()
	require.NoError(t, err)
	require.NotNil(t, resp.GetError())
	require.Equal(t, transactionpb.StatelessSwapResponse_Error_INVALID_SWAP, resp.GetError().Code)
	require.Len(t, resp.GetError().ErrorDetails, 1)
	require.Equal(t, "source mint must be usdc", resp.GetError().ErrorDetails[0].GetReasonString().Reason)
}

// First 315 bytes of the mainnet Coinbase Stable Swapper liquidity pool, which
// is 2107 bytes with the remainder zeroed. Copied from the
// coinbasestableswapper package tests.
const mainnetCoinbaseLiquidityPoolPrefix = "QiYRQLxQRIEFHqE9vluQFKO1wbEwnd22aRe9qGrV03SIsz1AV7GqT/yEzcR/f+ALaKG4KMxbBfZ5dTNFPqxxZHMfbTqNfj6St0p+" +
	"+yObz2IILMcKsoko07O+oRSDek7YwzrH9TroL1+QbHdT/t9qpcq4Kyx8OPWZm79AIUM9UlN+X6ujF0hPgzT4z8SXtrVTfBhZj7Lz" +
	"SPTgCpHoi6cjfPqXvflOJvRBAgAAAN0H70q0C5DeChX575Umuo4KwnYx+lqZmPTGnq1wMK2QSoyv1lJlvQkMgeq0VkN3NML2MHaz" +
	"cTzSusODf5c6RhACAAAAxvp6877brTo9ZfNqq8l0MbG75MLS9uDkfKYCA0UvXWE908SAij1Ps5+uycukm1pjSlLt4RVI9SSqIPLN" +
	"Ru+AcQAAAAAAAAAAAAD/"

func TestCoinbaseStableSwapperSwapHandler_TransactionVersions(t *testing.T) {
	env := setupTestEnv(t)

	prefix, err := base64.StdEncoding.DecodeString(mainnetCoinbaseLiquidityPoolPrefix)
	require.NoError(t, err)
	poolData := make([]byte, 2107)
	copy(poolData, prefix)
	pool, _, err := coinbase_stable_swapper.GetPoolAddress()
	require.NoError(t, err)
	env.data.setBlockchainAccountData(pool, poolData)

	owner := testutil.NewRandomAccount(t)
	swapAuthority := testutil.NewRandomAccount(t)
	destinationOwner := testutil.NewRandomAccount(t)
	fromMint := common.CoreMintAccount
	toMint, err := common.NewAccountFromProto(usdcMint(t))
	require.NoError(t, err)

	var blockhash solana.Blockhash
	copy(blockhash[:], testutil.NewRandomAccount(t).PublicKey().ToBytes())
	selectedNonce := &transaction_util.Nonce{
		Account:   testutil.NewRandomAccount(t),
		Blockhash: blockhash,
	}

	const swapAmount, feeAmount = 5_000_000, 500_000

	for _, transactionVersion := range []solana.MessageVersion{solana.MessageVersion0, solana.MessageVersion1} {
		for _, coinbaseSwapVersion := range []transaction_util.CoinbaseSwapVersion{transaction_util.CoinbaseSwapV1, transaction_util.CoinbaseSwapV2} {
			t.Run("txn "+transactionVersion.String()+" swap "+coinbaseSwapVersion.String(), func(t *testing.T) {
				h := NewCoinbaseStableSwapperSwapHandler(
					env.data,
					owner,
					swapAuthority,
					destinationOwner,
					fromMint,
					toMint,
					swapAmount,
					feeAmount,
					selectedNonce,
					transactionVersion,
					coinbaseSwapVersion,
				)

				txn, err := makeStatefulSwapTransaction(env.ctx, h)
				require.NoError(t, err)
				txn.SetBlockhash(selectedNonce.Blockhash)

				serverParameters := h.GetServerParameters().GetStablecoin()
				require.NotNil(t, serverParameters)

				assert.Equal(t, transactionVersion, txn.Message.Version)
				assert.Equal(t, common.GetSubsidizer().PublicKey().ToBytes(), serverParameters.Payer.Value)
				assert.Equal(t, selectedNonce.Account.PublicKey().ToBytes(), serverParameters.Nonce.Value)
				assert.Equal(t, blockhash[:], serverParameters.Blockhash.Value)
				assert.Equal(t, common.CoreMintFeesAccount.PublicKey().ToBytes(), serverParameters.FeeDestination.Value)

				switch transactionVersion {
				case solana.MessageVersion0:
					assert.Equal(t, "coinbase_stable_swapper_v0", serverParameters.MemoValue)
					assert.Equal(t, transaction_util.ToProtoAlts([]solana.AddressLookupTable{transaction_util.GetAltForCoreMint()}), serverParameters.Alts)
					assert.EqualValues(t, 125_000, serverParameters.ComputeUnitLimit)
					assert.EqualValues(t, 10_000, serverParameters.ComputeUnitPrice)
					assert.Nil(t, serverParameters.TransactionConfig)
					assert.Nil(t, h.GetTransactionConfig())
				case solana.MessageVersion1:
					assert.Equal(t, "coinbase_stable_swapper_v1", serverParameters.MemoValue)
					assert.Empty(t, serverParameters.Alts)
					assert.Zero(t, serverParameters.ComputeUnitLimit)
					assert.Zero(t, serverParameters.ComputeUnitPrice)

					require.NotNil(t, serverParameters.TransactionConfig)
					assert.EqualValues(t, 125_000, serverParameters.TransactionConfig.ComputeUnitLimit)
					assert.EqualValues(t, transaction_util.CoinbaseSwapLoadedAccountsDataSizeLimit, serverParameters.TransactionConfig.LoadedAccountsDataSizeLimit)
					assert.EqualValues(t, 1_250, serverParameters.TransactionConfig.PriorityFeeLamports)
					assert.Zero(t, serverParameters.TransactionConfig.HeapSize)

					assert.Empty(t, txn.Message.AddressTableLookups)
					for _, account := range txn.Message.Accounts {
						assert.NotEqual(t, compute_budget.ProgramKey, account)
					}
					assert.Len(t, txn.Message.Instructions, 7)

					marshalled, err := txn.Marshal()
					require.NoError(t, err)
					assert.LessOrEqual(t, len(marshalled), solana.MaxV1TransactionSize)
				}

				// The owner and swap authority are the client signers
				var clientSigners int
				for i := range int(txn.Message.Header.NumSignatures) {
					account := txn.Message.Accounts[i]
					if bytes.Equal(account, owner.PublicKey().ToBytes()) || bytes.Equal(account, swapAuthority.PublicKey().ToBytes()) {
						clientSigners++
					}
				}
				assert.Equal(t, 2, clientSigners)

				requireCoinbaseStatefulClientRebuildMatches(t, env, txn, serverParameters, owner, swapAuthority, destinationOwner, fromMint, toMint, swapAmount, feeAmount, coinbaseSwapVersion)
			})
		}
	}
}

// requireCoinbaseStatefulClientRebuildMatches builds the transaction from the
// server parameters following the instruction format documented on
// StatefulSwapResponse.ServerParameters.CoinbaseStableSwapperServerParameter,
// and requires the message clients sign to match the server's exactly.
func requireCoinbaseStatefulClientRebuildMatches(
	t *testing.T,
	env *testEnv,
	serverTxn solana.Transaction,
	params *transactionpb.StatefulSwapResponse_ServerParameters_CoinbaseStableSwapperServerParameter,
	owner, swapAuthority, destinationOwner, fromMint, toMint *common.Account,
	swapAmount, feeAmount uint64,
	coinbaseSwapVersion transaction_util.CoinbaseSwapVersion,
) {
	payer := params.Payer.Value

	vmConfig, err := common.GetVmConfigForMint(env.ctx, env.data, fromMint)
	require.NoError(t, err)
	ownerTimelockAccounts, err := owner.GetTimelockAccounts(vmConfig)
	require.NoError(t, err)

	createSwapAuthorityAtaIxn, swapAuthorityAta, err := token.CreateAssociatedTokenAccountIdempotent(payer, swapAuthority.PublicKey().ToBytes(), fromMint.PublicKey().ToBytes())
	require.NoError(t, err)
	createDestinationAtaIxn, destinationAta, err := token.CreateAssociatedTokenAccountIdempotent(payer, destinationOwner.PublicKey().ToBytes(), toMint.PublicKey().ToBytes())
	require.NoError(t, err)

	coinbaseAccounts, err := transaction_util.GetCoinbaseSwapAccounts(env.ctx, env.data, fromMint.PublicKey().ToBytes(), toMint.PublicKey().ToBytes())
	require.NoError(t, err)
	require.Equal(t, []byte(coinbaseAccounts.FeeRecipient), params.PoolFeeRecipient.Value)

	coinbaseSwapIxn, err := transaction_util.MakeCoinbaseSwapInstruction(
		coinbaseSwapVersion,
		coinbaseAccounts,
		swapAuthority.PublicKey().ToBytes(),
		fromMint.PublicKey().ToBytes(),
		toMint.PublicKey().ToBytes(),
		swapAuthorityAta,
		destinationAta,
		swapAmount,
		swapAmount,
	)
	require.NoError(t, err)

	ixns := []solana.Instruction{system.AdvanceNonce(params.Nonce.Value, payer)}
	if params.TransactionConfig == nil {
		if params.ComputeUnitLimit != 0 {
			ixns = append(ixns, compute_budget.SetComputeUnitLimit(params.ComputeUnitLimit))
		}
		if params.ComputeUnitPrice != 0 {
			ixns = append(ixns, compute_budget.SetComputeUnitPrice(params.ComputeUnitPrice))
		}
	}
	if len(params.MemoValue) > 0 {
		ixns = append(ixns, memo.Instruction(params.MemoValue))
	}
	ixns = append(
		ixns,
		createSwapAuthorityAtaIxn,
		createDestinationAtaIxn,
		vm.NewTransferForSwapWithFeeInstruction(
			&vm.TransferForSwapWithFeeInstructionAccounts{
				VmAuthority:     vmConfig.Authority.PublicKey().ToBytes(),
				Vm:              vmConfig.Vm.PublicKey().ToBytes(),
				Swapper:         owner.PublicKey().ToBytes(),
				SwapPda:         ownerTimelockAccounts.VmSwapAccounts.Pda.PublicKey().ToBytes(),
				SwapAta:         ownerTimelockAccounts.VmSwapAccounts.Ata.PublicKey().ToBytes(),
				SwapDestination: swapAuthorityAta,
				FeeDestination:  params.FeeDestination.Value,
			},
			&vm.TransferForSwapWithFeeInstructionArgs{
				SwapAmount: swapAmount,
				FeeAmount:  feeAmount,
				Bump:       ownerTimelockAccounts.VmSwapAccounts.PdaBump,
			},
		),
		coinbaseSwapIxn,
		token.CloseAccount(swapAuthorityAta, payer, swapAuthority.PublicKey().ToBytes()),
	)

	var clientTxn solana.Transaction
	if params.TransactionConfig == nil {
		clientTxn = solana.NewV0Transaction(payer, fromProtoAlts(params.Alts), ixns)
	} else {
		// CU limit, loaded accounts data size limit and priority fee are always
		// encoded, and the heap size only when non-zero
		config := solana.TransactionConfig{
			ComputeUnitLimit:            pointer.Uint32(params.TransactionConfig.ComputeUnitLimit),
			LoadedAccountsDataSizeLimit: pointer.Uint32(params.TransactionConfig.LoadedAccountsDataSizeLimit),
			PriorityFeeLamports:         pointer.Uint64(params.TransactionConfig.PriorityFeeLamports),
		}
		if params.TransactionConfig.HeapSize != 0 {
			config.HeapSize = pointer.Uint32(params.TransactionConfig.HeapSize)
		}

		clientTxn, err = solana.NewV1Transaction(payer, config, ixns...)
		require.NoError(t, err)
	}

	var blockhash solana.Blockhash
	copy(blockhash[:], params.Blockhash.Value)
	clientTxn.SetBlockhash(blockhash)

	expected, err := serverTxn.Message.Marshal()
	require.NoError(t, err)
	actual, err := clientTxn.Message.Marshal()
	require.NoError(t, err)
	require.Equal(t, expected, actual)
}
