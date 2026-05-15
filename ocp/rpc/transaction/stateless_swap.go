package transaction

import (
	"bytes"
	"context"
	"crypto/ed25519"

	"github.com/mr-tron/base58/base58"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	commonpb "github.com/code-payments/ocp-protobuf-api/generated/go/common/v1"
	transactionpb "github.com/code-payments/ocp-protobuf-api/generated/go/transaction/v1"

	"github.com/code-payments/ocp-server/grpc/client"
	"github.com/code-payments/ocp-server/ocp/common"
	"github.com/code-payments/ocp-server/ocp/data/swap"
	"github.com/code-payments/ocp-server/ocp/data/timelock"
	"github.com/code-payments/ocp-server/ocp/rpc"
	transaction_util "github.com/code-payments/ocp-server/ocp/transaction"
	"github.com/code-payments/ocp-server/protoutil"
	"github.com/code-payments/ocp-server/solana"
	compute_budget "github.com/code-payments/ocp-server/solana/computebudget"
	"github.com/code-payments/ocp-server/solana/memo"
	"github.com/code-payments/ocp-server/solana/token"
	"github.com/code-payments/ocp-server/usdc"
)

// todo: Generalize this more when we have more than one swap kind similar to StatefulSwap

const (
	statelessSwapComputeUnitLimit = 80_000
	statelessSwapComputeUnitPrice = 10_000
	statelessSwapMemoValue        = "coinbase_stable_swapper_v0"
)

func (s *transactionServer) StatelessSwap(streamer transactionpb.Transaction_StatelessSwapServer) error {
	ctx, cancel := context.WithTimeout(streamer.Context(), s.conf.swapTimeout.Get(streamer.Context()))
	defer cancel()

	log := s.log.With(zap.String("method", "StatelessSwap"))
	log = client.InjectLoggingMetadata(ctx, log, rpc.UserAgentName)

	if s.conf.disableSwaps.Get(ctx) {
		return handleStatelessSwapError(streamer, status.Error(codes.Unavailable, "temporarily unavailable"))
	}

	req, err := protoutil.BoundedReceive[transactionpb.StatelessSwapRequest](ctx, streamer, s.conf.clientReceiveTimeout.Get(ctx))
	if err != nil {
		log.With(zap.Error(err)).Info("error receiving request from client")
		return handleStatelessSwapError(streamer, err)
	}

	initiateReq := req.GetInitiate()
	if initiateReq == nil {
		return handleStatelessSwapError(streamer, status.Error(codes.InvalidArgument, "StatelessSwapRequest.Initiate is nil"))
	}

	owner, err := common.NewAccountFromProto(initiateReq.Owner)
	if err != nil {
		log.With(zap.Error(err)).Warn("invalid owner account")
		return handleStatelessSwapError(streamer, err)
	}
	log = log.With(zap.String("owner", owner.PublicKey().ToBase58()))

	reqSignature := initiateReq.Signature
	initiateReq.Signature = nil
	if err := s.auth.Authenticate(ctx, owner, initiateReq, reqSignature); err != nil {
		return handleStatelessSwapError(streamer, err)
	}

	switch initiateReq.GetKind().(type) {
	case *transactionpb.StatelessSwapRequest_Initiate_Stablecoin:
		log = log.With(zap.String("kind", "stablecoin"))
		return s.handleStablecoinStatelessSwap(ctx, log, streamer, initiateReq, owner)
	default:
		return handleStatelessSwapError(streamer, status.Error(codes.InvalidArgument, "StatelessSwapRequest.Initiate.Kind is nil"))
	}
}

func (s *transactionServer) handleStablecoinStatelessSwap(
	ctx context.Context,
	log *zap.Logger,
	streamer transactionpb.Transaction_StatelessSwapServer,
	initiateReq *transactionpb.StatelessSwapRequest_Initiate,
	owner *common.Account,
) error {
	initiateStablecoinSwapReq := initiateReq.GetStablecoin()
	if initiateStablecoinSwapReq == nil {
		return handleStatelessSwapError(streamer, status.Error(codes.InvalidArgument, "StatelessSwapRequest.Initiate.Stablecoin is nil"))
	}

	fromMint, err := common.NewAccountFromProto(initiateStablecoinSwapReq.FromMint)
	if err != nil {
		log.With(zap.Error(err)).Warn("invalid source mint account")
		return handleStatelessSwapError(streamer, err)
	}
	log = log.With(zap.String("from_mint", fromMint.PublicKey().ToBase58()))

	toMint, err := common.NewAccountFromProto(initiateStablecoinSwapReq.ToMint)
	if err != nil {
		log.With(zap.Error(err)).Warn("invalid destination mint account")
		return handleStatelessSwapError(streamer, err)
	}
	log = log.With(zap.String("to_mint", toMint.PublicKey().ToBase58()))

	log = log.With(
		zap.Uint64("swap_amount", initiateStablecoinSwapReq.SwapAmount),
		zap.Bool("wait_for_finalization", initiateReq.WaitForFinalization),
	)

	//
	// Section: Validation
	//

	if fromMint.PublicKey().ToBase58() != usdc.Mint {
		return handleStatelessSwapError(streamer, NewSwapValidationError("source mint must be usdc"))
	}
	if !common.IsCoreMint(toMint) {
		return handleStatelessSwapError(streamer, NewSwapValidationError("destination mint must be the core mint"))
	}

	if initiateStablecoinSwapReq.SwapAmount == 0 {
		return handleStatelessSwapError(streamer, NewSwapValidationError("swap amount must be positive"))
	}

	// The destination is the owner's VM Deposit ATA for to_mint, so to_mint must
	// be a VM-supported mint.
	destinationVmConfig, err := common.GetVmConfigForMint(ctx, s.data, toMint)
	if err == common.ErrUnsupportedMint {
		return handleStatelessSwapError(streamer, NewSwapValidationError("invalid destination mint"))
	} else if err != nil {
		log.With(zap.Error(err)).Warn("failure getting destination vm config")
		return handleStatelessSwapError(streamer, err)
	}

	ownerDestinationTimelockVault, err := owner.ToTimelockVault(destinationVmConfig)
	if err != nil {
		log.With(zap.Error(err)).Warn("failure getting owner destination timelock vault")
		return handleStatelessSwapError(streamer, err)
	}

	destinationTimelockAccountRecord, err := s.data.GetTimelockByVault(ctx, ownerDestinationTimelockVault.PublicKey().ToBase58())
	if err == timelock.ErrTimelockNotFound {
		return handleStatelessSwapError(streamer, NewSwapValidationError("destination timelock vault account not opened"))
	} else if err != nil {
		log.With(zap.Error(err)).Warn("failure getting destination timelock record")
		return handleStatelessSwapError(streamer, err)
	}
	if !destinationTimelockAccountRecord.IsLocked() {
		return handleStatelessSwapError(streamer, NewSwapDeniedError("destination timelock account isn't locked"))
	}

	// Confirm the owner has enough balance in their from_mint ATA to cover the swap.
	ownerFromMintAta, err := owner.ToAssociatedTokenAccount(fromMint)
	if err != nil {
		log.With(zap.Error(err)).Warn("failure getting owner from_mint ata")
		return handleStatelessSwapError(streamer, err)
	}

	ownerFromMintAtaInfo, err := s.data.GetBlockchainTokenAccountInfo(
		ctx,
		ownerFromMintAta.PublicKey().ToBase58(),
		fromMint.PublicKey().ToBase58(),
		solana.CommitmentConfirmed,
	)
	if err != nil {
		log.With(zap.Error(err)).Warn("failure getting owner from_mint ata info")
		return handleStatelessSwapError(streamer, NewSwapValidationError("source ata not found or invalid"))
	}
	if ownerFromMintAtaInfo.Amount < initiateStablecoinSwapReq.SwapAmount {
		return handleStatelessSwapError(streamer, NewSwapValidationError("insufficient balance"))
	}

	destinationLiquidity, err := transaction_util.GetCoinbaseSwapDestinationLiquidity(ctx, s.data, toMint.PublicKey().ToBytes())
	if err != nil {
		log.With(zap.Error(err)).Warn("failure getting coinbase destination liquidity")
		return handleStatelessSwapError(streamer, err)
	}
	if destinationLiquidity < initiateStablecoinSwapReq.SwapAmount {
		return handleStatelessSwapError(streamer, NewSwapDeniedError("insufficient coinbase stable swapper destination liquidity"))
	}

	//
	// Section: Antispam
	//

	ownerMetadata, err := common.GetOwnerMetadata(ctx, s.data, owner)
	if err == common.ErrOwnerNotFound {
		return handleStatelessSwapError(streamer, NewSwapDeniedError("not an ocp account"))
	} else if err != nil {
		log.With(zap.Error(err)).Warn("failure getting owner metadata")
		return handleStatelessSwapError(streamer, err)
	}
	if ownerMetadata.State != common.OwnerManagementStateOcpAccount {
		return handleStatelessSwapError(streamer, NewSwapDeniedError("not an ocp account"))
	}
	if ownerMetadata.Type != common.OwnerTypeUser12Words {
		return handleStatelessSwapError(streamer, NewSwapDeniedError("not a user ocp account"))
	}

	allow, err := s.antispamGuard.AllowSwap(ctx, swap.KindStablecoin, swap.FundingSource(transactionpb.FundingSource_FUNDING_SOURCE_UNKNOWN), owner, fromMint, toMint, initiateStablecoinSwapReq.SwapAmount, 0, false)
	if err != nil {
		return handleStatelessSwapError(streamer, err)
	} else if !allow {
		return handleStatelessSwapError(streamer, NewSwapDeniedError("rate limited"))
	}

	//
	// Section: Transaction construction
	//

	coinbaseAccounts, err := transaction_util.GetCoinbaseSwapAccounts(
		ctx,
		s.data,
		fromMint.PublicKey().ToBytes(),
		toMint.PublicKey().ToBytes(),
	)
	if err != nil {
		log.With(zap.Error(err)).Warn("failure getting coinbase swap accounts")
		return handleStatelessSwapError(streamer, err)
	}

	destinationVmDepositAccounts, err := owner.GetVmDepositAccounts(destinationVmConfig)
	if err != nil {
		log.With(zap.Error(err)).Warn("failure getting owner destination vm deposit accounts")
		return handleStatelessSwapError(streamer, err)
	}

	createDepositAtaIxn, _, err := token.CreateAssociatedTokenAccountIdempotent(
		common.GetSubsidizer().PublicKey().ToBytes(),
		destinationVmDepositAccounts.Pda.PublicKey().ToBytes(),
		toMint.PublicKey().ToBytes(),
	)
	if err != nil {
		log.With(zap.Error(err)).Warn("failure building create deposit ata instruction")
		return handleStatelessSwapError(streamer, err)
	}

	coinbaseSwapIxn := transaction_util.MakeCoinbaseSwapInstruction(
		coinbaseAccounts,
		owner.PublicKey().ToBytes(),
		fromMint.PublicKey().ToBytes(),
		toMint.PublicKey().ToBytes(),
		ownerFromMintAta.PublicKey().ToBytes(),
		destinationVmDepositAccounts.Ata.PublicKey().ToBytes(),
		initiateStablecoinSwapReq.SwapAmount,
		initiateStablecoinSwapReq.SwapAmount,
	)

	alts := []solana.AddressLookupTable{transaction_util.GetAltForCoreMint()}

	ixns := []solana.Instruction{
		compute_budget.SetComputeUnitLimit(statelessSwapComputeUnitLimit),
		compute_budget.SetComputeUnitPrice(statelessSwapComputeUnitPrice),
		memo.Instruction(statelessSwapMemoValue),
		createDepositAtaIxn,
		coinbaseSwapIxn,
	}

	feeRecipient, err := common.NewAccountFromPublicKeyBytes(coinbaseAccounts.FeeRecipient)
	if err != nil {
		log.With(zap.Error(err)).Warn("invalid coinbase pool fee recipient")
		return handleStatelessSwapError(streamer, err)
	}

	txn := solana.NewV0Transaction(
		common.GetSubsidizer().PublicKey().ToBytes(),
		alts,
		ixns,
	)

	blockhash, err := s.data.GetBlockchainLatestBlockhash(ctx)
	if err != nil {
		log.With(zap.Error(err)).Warn("failure getting latest blockhash")
		return handleStatelessSwapError(streamer, err)
	}
	txn.SetBlockhash(blockhash)

	marshalledTxnMessage := txn.Message.Marshal()

	//
	// Section: Server parameters
	//

	if err := streamer.Send(&transactionpb.StatelessSwapResponse{
		Response: &transactionpb.StatelessSwapResponse_ServerParameters_{
			ServerParameters: &transactionpb.StatelessSwapResponse_ServerParameters{
				Kind: &transactionpb.StatelessSwapResponse_ServerParameters_Stablecoin{
					Stablecoin: &transactionpb.StatelessSwapResponse_ServerParameters_CoinbaseStableSwapperServerParameter{
						Payer:            common.GetSubsidizer().ToProto(),
						Blockhash:        &commonpb.Blockhash{Value: blockhash[:]},
						Alts:             transaction_util.ToProtoAlts(alts),
						ComputeUnitLimit: statelessSwapComputeUnitLimit,
						ComputeUnitPrice: statelessSwapComputeUnitPrice,
						MemoValue:        statelessSwapMemoValue,
						PoolFeeRecipient: feeRecipient.ToProto(),
					},
				},
			},
		},
	}); err != nil {
		return handleStatelessSwapError(streamer, err)
	}

	//
	// Section: Transaction signing
	//

	req, err := protoutil.BoundedReceive[transactionpb.StatelessSwapRequest](ctx, streamer, s.conf.clientReceiveTimeout.Get(ctx))
	if err != nil {
		log.With(zap.Error(err)).Info("error receiving request from client")
		return handleStatelessSwapError(streamer, err)
	}

	submitSignaturesReq := req.GetSubmitSignatures()
	if submitSignaturesReq == nil {
		return handleStatelessSwapError(streamer, status.Error(codes.InvalidArgument, "StatelessSwapRequest.SubmitSignatures is nil"))
	}

	protoSignature := submitSignaturesReq.TransactionSignatures[0]

	if !ed25519.Verify(
		owner.PublicKey().ToBytes(),
		marshalledTxnMessage,
		protoSignature.Value,
	) {
		return handleStatelessSwapStructuredError(
			streamer,
			transactionpb.StatelessSwapResponse_Error_SIGNATURE_ERROR,
			toInvalidTxnSignatureErrorDetails(0, txn, protoSignature),
		)
	}

	for i := range txn.Message.Header.NumSignatures {
		account := txn.Message.Accounts[i]
		if bytes.Equal(account, owner.PublicKey().ToBytes()) {
			copy(txn.Signatures[i][:], protoSignature.Value)
		}
	}

	err = txn.Sign(
		common.GetSubsidizer().PrivateKey().ToBytes(),
	)
	if err != nil {
		log.With(zap.Error(err)).Info("failure signing transaction")
		return handleStatelessSwapError(streamer, err)
	}

	txnSignature := base58.Encode(txn.Signature())
	log = log.With(zap.String("transaction_signature", txnSignature))

	//
	// Section: Submission
	//

	if initiateReq.WaitForFinalization {
		if err := transaction_util.SubmitAndWaitForFinalization(ctx, s.data, &txn); err != nil {
			if err == transaction_util.ErrTransactionFailed {
				log.With(zap.Error(err)).Info("stateless swap transaction failed")
				return handleStatelessSwapError(streamer, err)
			}
			log.With(zap.Error(err)).Warn("failure submitting/finalizing transaction")
			return handleStatelessSwapError(streamer, err)
		}

		return handleStatelessSwapError(streamer, streamer.Send(&transactionpb.StatelessSwapResponse{
			Response: &transactionpb.StatelessSwapResponse_Success_{
				Success: &transactionpb.StatelessSwapResponse_Success{
					Code:                 transactionpb.StatelessSwapResponse_Success_FINALIZED,
					TransactionSignature: &commonpb.Signature{Value: txn.Signature()},
				},
			},
		}))
	}

	if _, err := s.data.SubmitBlockchainTransaction(ctx, &txn); err != nil {
		log.With(zap.Error(err)).Warn("failure submitting transaction")
		return handleStatelessSwapError(streamer, err)
	}

	return handleStatelessSwapError(streamer, streamer.Send(&transactionpb.StatelessSwapResponse{
		Response: &transactionpb.StatelessSwapResponse_Success_{
			Success: &transactionpb.StatelessSwapResponse_Success{
				Code:                 transactionpb.StatelessSwapResponse_Success_SUBMITTED,
				TransactionSignature: &commonpb.Signature{Value: txn.Signature()},
			},
		},
	}))
}
