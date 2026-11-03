package transaction

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	commonpb "github.com/code-payments/ocp-protobuf-api/generated/go/common/v1"
	transactionpb "github.com/code-payments/ocp-protobuf-api/generated/go/transaction/v1"

	"github.com/code-payments/ocp-server/ocp/common"
	"github.com/code-payments/ocp-server/ocp/data/currency"
	"github.com/code-payments/ocp-server/ocp/data/vm/ram"
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

func TestStatefulSwap_Coinbase_TransactionVersionV1(t *testing.T) {
	env := setupTestEnv(t, withSwapsEnabled())
	owner := env.newUser(t)
	swapAuthority := env.newAccount(t)

	for _, tc := range []struct {
		name     string
		initiate *transactionpb.StatefulSwapRequest_Initiate
		assert   func(t *testing.T, respErr *transactionpb.StatefulSwapResponse_Error)
	}{
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

// reserveTimelockMemory reserves VM memory for the owner's virtual timelock
// account on the mint, which reserve swaps deposit into
func (e *testEnv) reserveTimelockMemory(t *testing.T, owner, mint *common.Account) {
	vmConfig, err := common.GetVmConfigForMint(e.ctx, e.data, mint)
	require.NoError(t, err)

	vmAddress := vmConfig.Vm.PublicKey().ToBase58()
	err = e.data.InitializeVmMemory(e.ctx, &ram.Record{
		Vm:                vmAddress,
		Address:           testutil.NewRandomAccount(t).PublicKey().ToBase58(),
		Capacity:          1000,
		NumSectors:        1,
		NumPages:          1000,
		PageSize:          uint8(vm.GetVirtualAccountSizeInMemory(vm.VirtualAccountTypeTimelock)),
		StoredAccountType: vm.VirtualAccountTypeTimelock,
		CreatedAt:         time.Now(),
	})
	require.NoError(t, err)

	_, _, err = e.data.ReserveVmMemory(e.ctx, vmAddress, vm.VirtualAccountTypeTimelock, e.getVault(t, owner, mint).PublicKey().ToBase58())
	require.NoError(t, err)
}

// initiateReserveSwap sends a signed reserve StatefulSwap Initiate request and
// returns the first response
func (e *testEnv) initiateReserveSwap(
	t *testing.T,
	owner, swapAuthority *common.Account,
	clientParameters *transactionpb.StatefulSwapRequest_Initiate_ReserveSwapClientParameters,
	transactionVersion uint32,
) *transactionpb.StatefulSwapResponse {
	ctx, cancel := context.WithTimeout(e.ctx, defaultTestStreamTimeout)
	defer cancel()

	stream, err := e.client.StatefulSwap(ctx)
	require.NoError(t, err)

	initiate := &transactionpb.StatefulSwapRequest_Initiate{
		Kind: &transactionpb.StatefulSwapRequest_Initiate_Reserve{
			Reserve: clientParameters,
		},
		Owner:         owner.ToProto(),
		SwapAuthority: swapAuthority.ToProto(),
		ProofSignature: e.signProto(t, owner, &transactionpb.VerifiedSwapMetadata{
			Kind: &transactionpb.VerifiedSwapMetadata_Reserve{
				Reserve: &transactionpb.VerifiedReserveSwapMetadata{
					ClientParameters: clientParameters,
				},
			},
		}),
		TransactionVersion: transactionVersion,
	}
	initiate.Signature = e.signProto(t, owner, initiate)

	require.NoError(t, stream.Send(&transactionpb.StatefulSwapRequest{
		Request: &transactionpb.StatefulSwapRequest_Initiate_{Initiate: initiate},
	}))

	resp, err := stream.Recv()
	require.NoError(t, err)
	return resp
}

func TestStatefulSwap_ReserveExistingCurrency_TransactionVersions(t *testing.T) {
	env := setupTestEnv(t, withSwapsEnabled())

	mint := env.newLaunchpadMint(t)
	owner := env.newFundedUser(t, common.ToCoreMintQuarks(100), mint)
	env.reserveTimelockMemory(t, owner, mint)

	for _, transactionVersion := range []uint32{0, 1} {
		t.Run(fmt.Sprintf("v%d", transactionVersion), func(t *testing.T) {
			resp := env.initiateReserveSwap(t, owner, env.newAccount(t), &transactionpb.StatefulSwapRequest_Initiate_ReserveSwapClientParameters{
				Id:            &commonpb.SwapId{Value: testutil.NewRandomAccount(t).PublicKey().ToBytes()},
				FromMint:      common.CoreMintAccount.ToProto(),
				ToMint:        mint.ToProto(),
				SwapAmount:    common.CoreMintQuarksPerUnit,
				FundingSource: transactionpb.FundingSource_FUNDING_SOURCE_SUBMIT_INTENT,
				FundingId:     testutil.NewRandomAccount(t).PublicKey().ToBase58(),
			}, transactionVersion)
			require.Nil(t, resp.GetError())

			serverParameters := resp.GetServerParameters().GetReserveExistingCurrency()
			require.NotNil(t, serverParameters)
			if transactionVersion == 1 {
				require.NotNil(t, serverParameters.TransactionConfig)
				assert.NotZero(t, serverParameters.TransactionConfig.ComputeUnitLimit)
				assert.Empty(t, serverParameters.Alts)
				assert.Equal(t, "buy_v1", serverParameters.MemoValue)
			} else {
				assert.Nil(t, serverParameters.TransactionConfig)
				assert.NotEmpty(t, serverParameters.Alts)
				assert.Equal(t, "buy_v0", serverParameters.MemoValue)
			}
		})
	}
}

func TestStatefulSwap_ReserveNewCurrency_TransactionVersionV1Denied(t *testing.T) {
	env := setupTestEnv(t, withSwapsEnabled())

	owner := env.newFundedUser(t, common.ToCoreMintQuarks(100))

	mint := env.newLaunchpadMint(t)
	metadataRecord, err := env.data.GetCurrencyMetadata(env.ctx, mint.PublicKey().ToBase58())
	require.NoError(t, err)
	metadataRecord.State = currency.MetadataStateWaitingForInitialPurchase
	metadataRecord.CreatedBy = owner.PublicKey().ToBase58()
	require.NoError(t, env.data.SaveCurrencyMetadata(env.ctx, metadataRecord))

	resp := env.initiateReserveSwap(t, owner, owner, &transactionpb.StatefulSwapRequest_Initiate_ReserveSwapClientParameters{
		Id:            &commonpb.SwapId{Value: testutil.NewRandomAccount(t).PublicKey().ToBytes()},
		FromMint:      common.CoreMintAccount.ToProto(),
		ToMint:        mint.ToProto(),
		SwapAmount:    defaultNewCurrencyPurchaseQuarks,
		FeeAmount:     defaultNewCurrencyFeeQuarks,
		FundingSource: transactionpb.FundingSource_FUNDING_SOURCE_SUBMIT_INTENT,
		FundingId:     testutil.NewRandomAccount(t).PublicKey().ToBase58(),
	}, 1)

	require.NotNil(t, resp.GetError())
	require.Equal(t, transactionpb.StatefulSwapResponse_Error_DENIED, resp.GetError().Code)
	require.Len(t, resp.GetError().ErrorDetails, 1)
	require.Equal(t, "transaction version 1 is not supported for currency creation", resp.GetError().ErrorDetails[0].GetDenied().Reason)
}

func TestReserveSwapHandlers_TransactionVersions(t *testing.T) {
	env := setupTestEnv(t)

	launchpadMint1 := env.newLaunchpadMint(t)
	launchpadMint2 := env.newLaunchpadMint(t)
	swapper := env.newFundedUser(t, common.ToCoreMintQuarks(100), launchpadMint1, launchpadMint2)
	for _, mint := range []*common.Account{common.CoreMintAccount, launchpadMint1, launchpadMint2} {
		env.reserveTimelockMemory(t, swapper, mint)
	}

	launchpadAlt1, err := transaction_util.GetAltForMint(env.ctx, env.data, launchpadMint1)
	require.NoError(t, err)
	launchpadAlt2, err := transaction_util.GetAltForMint(env.ctx, env.data, launchpadMint2)
	require.NoError(t, err)

	var blockhash solana.Blockhash
	copy(blockhash[:], testutil.NewRandomAccount(t).PublicKey().ToBytes())
	selectedNonce := &transaction_util.Nonce{
		Account:   testutil.NewRandomAccount(t),
		Blockhash: blockhash,
	}

	const swapAmount = 1_000_000

	for _, tc := range []struct {
		flow           string
		newHandler     func(temporaryHolder *common.Account, transactionVersion solana.MessageVersion) SwapHandler
		expectedV0Alts []solana.AddressLookupTable
		feeDestination *common.Account
	}{
		{
			flow: "buy",
			newHandler: func(temporaryHolder *common.Account, transactionVersion solana.MessageVersion) SwapHandler {
				return NewReserveBuySwapHandler(env.data, swapper, temporaryHolder, launchpadMint1, swapAmount, 0, selectedNonce, transactionVersion)
			},
			expectedV0Alts: []solana.AddressLookupTable{launchpadAlt1},
		},
		{
			flow: "buy",
			newHandler: func(temporaryHolder *common.Account, transactionVersion solana.MessageVersion) SwapHandler {
				return NewReserveBuySwapHandler(env.data, swapper, temporaryHolder, launchpadMint1, swapAmount, 10_000, selectedNonce, transactionVersion)
			},
			expectedV0Alts: []solana.AddressLookupTable{launchpadAlt1, transaction_util.GetAltForCoreMint()},
			feeDestination: common.CoreMintFeesAccount,
		},
		{
			flow: "sell",
			newHandler: func(temporaryHolder *common.Account, transactionVersion solana.MessageVersion) SwapHandler {
				return NewReserveSellSwapHandler(env.data, swapper, temporaryHolder, launchpadMint1, swapAmount, selectedNonce, transactionVersion)
			},
			expectedV0Alts: []solana.AddressLookupTable{launchpadAlt1},
		},
		{
			flow: "buy_sell",
			newHandler: func(temporaryHolder *common.Account, transactionVersion solana.MessageVersion) SwapHandler {
				return NewReserveBuySellSwapHandler(env.data, swapper, temporaryHolder, launchpadMint1, launchpadMint2, swapAmount, selectedNonce, transactionVersion)
			},
			expectedV0Alts: []solana.AddressLookupTable{launchpadAlt1, launchpadAlt2},
		},
	} {
		name := tc.flow
		if tc.feeDestination != nil {
			name += " with fee"
		}
		t.Run(name, func(t *testing.T) {
			temporaryHolder := testutil.NewRandomAccount(t)

			//
			// v0 is the existing format clients already build
			//

			v0Handler := tc.newHandler(temporaryHolder, solana.MessageVersion0)
			v0Ixns, err := v0Handler.MakeInstructions(env.ctx)
			require.NoError(t, err)
			v0Txn, err := makeStatefulSwapTransaction(env.ctx, v0Handler)
			require.NoError(t, err)
			assert.Equal(t, solana.MessageVersion0, v0Txn.Message.Version)
			assert.Nil(t, v0Handler.GetTransactionConfig())

			v0Parameters := v0Handler.GetServerParameters().GetReserveExistingCurrency()
			require.NotNil(t, v0Parameters)
			assert.Equal(t, tc.flow+"_v0", v0Parameters.MemoValue)
			assert.Equal(t, transaction_util.ToProtoAlts(tc.expectedV0Alts), v0Parameters.Alts)
			assert.NotZero(t, v0Parameters.ComputeUnitLimit)
			assert.EqualValues(t, 10_000, v0Parameters.ComputeUnitPrice)
			assert.Nil(t, v0Parameters.TransactionConfig)

			//
			// v1
			//

			v1Handler := tc.newHandler(temporaryHolder, solana.MessageVersion1)
			v1Ixns, err := v1Handler.MakeInstructions(env.ctx)
			require.NoError(t, err)
			v1Txn, err := makeStatefulSwapTransaction(env.ctx, v1Handler)
			require.NoError(t, err)
			v1Txn.SetBlockhash(selectedNonce.Blockhash)
			assert.Equal(t, solana.MessageVersion1, v1Txn.Message.Version)

			alts, err := v1Handler.GetAlts(env.ctx)
			require.NoError(t, err)
			assert.Nil(t, alts)

			v1Parameters := v1Handler.GetServerParameters().GetReserveExistingCurrency()
			require.NotNil(t, v1Parameters)
			assert.Equal(t, common.GetSubsidizer().PublicKey().ToBytes(), v1Parameters.Payer.Value)
			assert.Equal(t, selectedNonce.Account.PublicKey().ToBytes(), v1Parameters.Nonce.Value)
			assert.Equal(t, blockhash[:], v1Parameters.Blockhash.Value)
			assert.Equal(t, tc.flow+"_v1", v1Parameters.MemoValue)
			assert.Equal(t, v0Parameters.MemoryAccount, v1Parameters.MemoryAccount)
			assert.Equal(t, v0Parameters.MemoryIndex, v1Parameters.MemoryIndex)
			assert.Equal(t, v0Parameters.FeeDestination, v1Parameters.FeeDestination)
			if tc.feeDestination != nil {
				assert.Equal(t, tc.feeDestination.PublicKey().ToBytes(), v1Parameters.FeeDestination.Value)
			}
			assert.Empty(t, v1Parameters.Alts)
			assert.Zero(t, v1Parameters.ComputeUnitLimit)
			assert.Zero(t, v1Parameters.ComputeUnitPrice)

			// The v1 config carries the same compute budget the v0 transaction
			// sets with ComputeBudget instructions
			require.NotNil(t, v1Parameters.TransactionConfig)
			assert.Equal(t, v0Parameters.ComputeUnitLimit, v1Parameters.TransactionConfig.ComputeUnitLimit)
			assert.EqualValues(t, transaction_util.ReserveSwapLoadedAccountsDataSizeLimit, v1Parameters.TransactionConfig.LoadedAccountsDataSizeLimit)
			assert.Equal(t, transaction_util.PriorityFeeLamports(v0Parameters.ComputeUnitLimit, 10_000), v1Parameters.TransactionConfig.PriorityFeeLamports)
			assert.Zero(t, v1Parameters.TransactionConfig.HeapSize)

			// Per the proto, the v1 instructions are the v0 instructions without
			// ComputeBudget, with the v1 memo
			var expectedV1Ixns []solana.Instruction
			for _, ixn := range v0Ixns {
				switch {
				case bytes.Equal(ixn.Program, compute_budget.ProgramKey):
					continue
				case bytes.Equal(ixn.Program, memo.ProgramKey):
					expectedV1Ixns = append(expectedV1Ixns, memo.Instruction(tc.flow+"_v1"))
				default:
					expectedV1Ixns = append(expectedV1Ixns, ixn)
				}
			}
			assert.Equal(t, expectedV1Ixns, v1Ixns)
			assert.Len(t, v1Ixns, len(v0Ixns)-2)

			assert.Empty(t, v1Txn.Message.AddressTableLookups)
			for _, account := range v1Txn.Message.Accounts {
				assert.NotEqual(t, compute_budget.ProgramKey, account)
			}
			assert.LessOrEqual(t, len(v1Txn.Message.Accounts), 64)
			marshalled, err := v1Txn.Marshal()
			require.NoError(t, err)
			assert.LessOrEqual(t, len(marshalled), solana.MaxV1TransactionSize)
		})
	}
}
