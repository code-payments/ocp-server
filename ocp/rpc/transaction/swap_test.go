package transaction

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	commonpb "github.com/code-payments/ocp-protobuf-api/generated/go/common/v1"
	transactionpb "github.com/code-payments/ocp-protobuf-api/generated/go/transaction/v1"

	"github.com/code-payments/ocp-server/ocp/common"
	"github.com/code-payments/ocp-server/testutil"
	"github.com/code-payments/ocp-server/usdc"
)

func usdcMint(t *testing.T) *commonpb.SolanaAccountId {
	mint, err := common.NewAccountFromPublicKeyString(usdc.Mint)
	require.NoError(t, err)
	return mint.ToProto()
}

func TestStatefulSwap_TransactionVersionV1Denied(t *testing.T) {
	env := setupTestEnv(t, withSwapsEnabled())
	owner := env.newUser(t)
	swapAuthority := env.newAccount(t)

	for _, tc := range []struct {
		name     string
		initiate *transactionpb.StatefulSwapRequest_Initiate
	}{
		{
			name: "reserve",
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
		},
		{
			name: "stablecoin",
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
			require.Equal(t, transactionpb.StatefulSwapResponse_Error_DENIED, resp.GetError().Code)
			require.Len(t, resp.GetError().ErrorDetails, 1)
			require.Equal(t, "transaction version 1 is not supported", resp.GetError().ErrorDetails[0].GetDenied().Reason)

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
