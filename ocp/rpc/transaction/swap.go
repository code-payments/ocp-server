package transaction

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"database/sql"
	"time"

	"github.com/mr-tron/base58/base58"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	commonpb "github.com/code-payments/ocp-protobuf-api/generated/go/common/v1"
	transactionpb "github.com/code-payments/ocp-protobuf-api/generated/go/transaction/v1"

	"github.com/code-payments/ocp-server/grpc/client"
	"github.com/code-payments/ocp-server/ocp/balance"
	"github.com/code-payments/ocp-server/ocp/common"
	"github.com/code-payments/ocp-server/ocp/data/currency"
	"github.com/code-payments/ocp-server/ocp/data/intent"
	"github.com/code-payments/ocp-server/ocp/data/nonce"
	"github.com/code-payments/ocp-server/ocp/data/swap"
	"github.com/code-payments/ocp-server/ocp/data/timelock"
	transaction_util "github.com/code-payments/ocp-server/ocp/transaction"
	"github.com/code-payments/ocp-server/ocp/vm"
	"github.com/code-payments/ocp-server/protoutil"
	"github.com/code-payments/ocp-server/solana"
)

func (s *transactionServer) StatefulSwap(streamer transactionpb.Transaction_StatefulSwapServer) error {
	// Bound the total RPC. Keeping the timeout higher to see where we land because
	// there's a lot of stuff happening in this method.
	ctx, cancel := context.WithTimeout(streamer.Context(), s.conf.swapTimeout.Get(streamer.Context()))
	defer cancel()

	log := s.log.With(zap.String("method", "StatefulSwap"))
	log = client.InjectLoggingMetadata(ctx, log)

	if s.conf.disableSwaps.Get(ctx) {
		return handleStatefulSwapError(streamer, status.Error(codes.Unavailable, "temporarily unavailable"))
	}

	req, err := protoutil.BoundedReceive[transactionpb.StatefulSwapRequest](ctx, streamer, s.conf.clientReceiveTimeout.Get(ctx))
	if err != nil {
		log.With(zap.Error(err)).Info("error receiving request from client")
		return handleStatefulSwapError(streamer, err)
	}

	initiateReq := req.GetInitiate()
	if initiateReq == nil {
		return handleStatefulSwapError(streamer, status.Error(codes.InvalidArgument, "StatefulSwapRequest.Initiate is nil"))
	}

	owner, err := common.NewAccountFromProto(initiateReq.Owner)
	if err != nil {
		log.With(zap.Error(err)).Warn("invalid owner account")
		return handleStatefulSwapError(streamer, err)
	}
	log = log.With(zap.String("owner", owner.PublicKey().ToBase58()))

	swapAuthority, err := common.NewAccountFromProto(initiateReq.SwapAuthority)
	if err != nil {
		log.With(zap.Error(err)).Warn("invalid swap authority")
		return handleStatefulSwapError(streamer, err)
	}
	log = log.With(zap.String("swap_authority", swapAuthority.PublicKey().ToBase58()))

	reqSignature := initiateReq.Signature
	initiateReq.Signature = nil
	if err := s.auth.Authenticate(ctx, owner, initiateReq, reqSignature); err != nil {
		return handleStatefulSwapError(streamer, err)
	}

	initiateReserveSwapReq := initiateReq.GetReserve()
	if initiateReserveSwapReq == nil {
		return handleStatefulSwapError(streamer, status.Error(codes.InvalidArgument, "StatefulSwapRequest.Initiate.Reserve is nil"))
	}

	swapId := base58.Encode(initiateReserveSwapReq.Id.Value)
	log = log.With(zap.String("swap_id", swapId))

	fromMint, err := common.NewAccountFromProto(initiateReserveSwapReq.FromMint)
	if err != nil {
		log.With(zap.Error(err)).Warn("invalid source mint account")
		return handleStatefulSwapError(streamer, err)
	}
	log = log.With(zap.String("from_mint", fromMint.PublicKey().ToBase58()))

	toMint, err := common.NewAccountFromProto(initiateReserveSwapReq.ToMint)
	if err != nil {
		log.With(zap.Error(err)).Warn("invalid destination mint account")
		return handleStatefulSwapError(streamer, err)
	}
	log = log.With(zap.String("to_mint", toMint.PublicKey().ToBase58()))

	log = log.With(
		zap.Uint64("amount", initiateReserveSwapReq.Amount),
		zap.String("funding_source", initiateReserveSwapReq.FundingSource.String()),
		zap.String("funding_id", initiateReserveSwapReq.FundingId),
	)

	//
	// Section: Verified metadata signature verification
	//

	verifiedMetadata := &transactionpb.VerifiedSwapMetadata{
		Kind: &transactionpb.VerifiedSwapMetadata_Reserve{
			Reserve: &transactionpb.VerifiedReserveSwapMetadata{
				ClientParameters: initiateReserveSwapReq,
			},
		},
	}

	metadataSignature := initiateReq.ProofSignature
	if err := s.auth.Authenticate(ctx, owner, verifiedMetadata, metadataSignature); err != nil {
		return handleStatefulSwapStructuredError(streamer, transactionpb.StatefulSwapResponse_Error_SIGNATURE_ERROR)
	}

	//
	// Section: Validation
	//

	_, err = s.data.GetSwapById(ctx, swapId)
	if err == nil {
		return handleStatefulSwapError(streamer, NewSwapDeniedError("attempt to reuse swap id"))
	} else if err != swap.ErrNotFound {
		log.With(zap.Error(err)).Warn("failure checking for existing swap record by id")
		return handleStatefulSwapError(streamer, err)
	}

	_, err = s.data.GetSwapByFundingId(ctx, initiateReserveSwapReq.FundingId)
	if err == nil {
		return handleStatefulSwapError(streamer, NewSwapDeniedError("attempt to reuse swap funding id"))
	} else if err != swap.ErrNotFound {
		log.With(zap.Error(err)).Warn("failure checking for existing swap record by funding id")
		return handleStatefulSwapError(streamer, err)
	}

	if !common.IsCoreMint(fromMint) && !common.IsCoreMint(toMint) {
		return handleStatefulSwapError(streamer, NewSwapDeniedError("swap must involve core mint"))
	}

	if bytes.Equal(fromMint.PublicKey().ToBytes(), toMint.PublicKey().ToBytes()) {
		return handleStatefulSwapError(streamer, NewSwapValidationError("must swap between two different mints"))
	}

	if initiateReserveSwapReq.Amount == 0 {
		return handleStatefulSwapError(streamer, NewSwapValidationError("amount must be positive"))
	}

	sourceVmConfig, err := common.GetVmConfigForMint(ctx, s.data, fromMint)
	if err == common.ErrUnsupportedMint {
		return handleStatefulSwapError(streamer, NewSwapValidationError("invalid source mint"))
	} else if err != nil {
		log.With(zap.Error(err)).Warn("failure getting source vm config")
		return handleStatefulSwapError(streamer, err)
	}

	ownerSourceTimelockVault, err := owner.ToTimelockVault(sourceVmConfig)
	if err != nil {
		log.With(zap.Error(err)).Warn("failure getting owner source timelock vault")
		return handleStatefulSwapError(streamer, err)
	}

	_, err = s.data.GetTimelockByVault(ctx, ownerSourceTimelockVault.PublicKey().ToBase58())
	if err == timelock.ErrTimelockNotFound {
		return handleStatefulSwapError(streamer, NewSwapValidationError("source timelock vault account not opened"))
	} else if err != nil {
		log.With(zap.Error(err)).Warn("failure getting source timelock record")
		return handleStatefulSwapError(streamer, err)
	}

	switch initiateReserveSwapReq.FundingSource {
	case transactionpb.FundingSource_FUNDING_SOURCE_SUBMIT_INTENT:
		decodedFundingId, err := base58.Decode(initiateReserveSwapReq.FundingId)
		if err != nil || len(decodedFundingId) != ed25519.PublicKeySize {
			log.With(zap.Error(err)).Warn("invalid funding id")
			return handleStatefulSwapError(streamer, NewSwapValidationError("funding id is not a public key"))
		}

		_, err = s.data.GetIntent(ctx, initiateReserveSwapReq.FundingId)
		if err == nil {
			return handleStatefulSwapError(streamer, NewSwapValidationError("funding intent already exists"))
		} else if err != intent.ErrIntentNotFound {
			log.With(zap.Error(err)).Warn("failure getting funding intent record")
			return handleStatefulSwapError(streamer, err)
		}

		balance, err := balance.CalculateFromCache(ctx, s.data, ownerSourceTimelockVault)
		if err != nil {
			log.With(zap.Error(err)).Warn("failure getting owner source timelock vault balance")
			return handleStatefulSwapError(streamer, err)
		}
		if balance < initiateReserveSwapReq.Amount {
			return handleStatefulSwapError(streamer, NewSwapValidationError("insufficient balance"))
		}
	case transactionpb.FundingSource_FUNDING_SOURCE_EXTERNAL_WALLET:
		decodedFundingId, err := base58.Decode(initiateReserveSwapReq.FundingId)
		if err != nil || len(decodedFundingId) != ed25519.SignatureSize {
			log.With(zap.Error(err)).Warn("invalid funding id")
			return handleStatefulSwapError(streamer, NewSwapValidationError("funding id is not a signature"))
		}

		if !common.IsCoreMint(fromMint) {
			return handleStatefulSwapError(streamer, NewSwapDeniedError("source mint must be core mint"))
		}
	default:
		return handleStatefulSwapError(streamer, NewSwapDeniedErrorf("funding source %s is not supported", initiateReserveSwapReq.FundingSource))
	}

	otherMint := fromMint
	if common.IsCoreMint(otherMint) {
		otherMint = toMint
	}

	var initializesMint bool
	currencyMetadataRecord, err := s.data.GetCurrencyMetadata(ctx, otherMint.PublicKey().ToBase58())
	if err == currency.ErrNotFound {
		return handleStatefulSwapError(streamer, NewSwapValidationError("mint not found"))
	} else if err != nil {
		log.With(zap.Error(err)).Warn("failure getting destination timelock record")
		return handleStatefulSwapError(streamer, err)
	}
	switch currencyMetadataRecord.State {
	case currency.MetadataStateAvailable:
		initializesMint = false
	case currency.MetadataStateWaitingForInitialPurchase:
		initializesMint = true
	default:
		return handleStatefulSwapError(streamer, NewSwapDeniedError("mint is being initialized"))
	}

	var destinationVmAuthority *common.Account
	if !initializesMint {
		if owner.PublicKey().ToBase58() == swapAuthority.PublicKey().ToBase58() {
			return handleStatefulSwapError(streamer, NewSwapValidationError("owner cannot be swap authority"))
		}

		destinationVmConfig, err := common.GetVmConfigForMint(ctx, s.data, toMint)
		if err == common.ErrUnsupportedMint {
			return handleStatefulSwapError(streamer, NewSwapValidationError("invalid destination mint"))
		} else if err != nil {
			log.With(zap.Error(err)).Warn("failure getting destination vm config")
			return handleStatefulSwapError(streamer, err)
		}
		destinationVmAuthority = destinationVmConfig.Authority

		ownerDestinationTimelockVault, err := owner.ToTimelockVault(destinationVmConfig)
		if err != nil {
			log.With(zap.Error(err)).Warn("failure getting owner destination timelock vault")
			return handleStatefulSwapError(streamer, err)
		}

		_, err = s.data.GetTimelockByVault(ctx, ownerDestinationTimelockVault.PublicKey().ToBase58())
		if err == timelock.ErrTimelockNotFound {
			return handleStatefulSwapError(streamer, NewSwapValidationError("destination timelock vault account not opened"))
		} else if err != nil {
			log.With(zap.Error(err)).Warn("failure getting destination timelock record")
			return handleStatefulSwapError(streamer, err)
		}

		err = vm.EnsureVirtualTimelockAccountIsInitialized(ctx, s.data, ownerDestinationTimelockVault, false)
		if err != nil {
			log.With(zap.Error(err)).Warn("error ensuring destination virtual timelock account is initialized")
			return handleStatefulSwapError(streamer, err)
		}
	} else {
		if owner.PublicKey().ToBase58() != swapAuthority.PublicKey().ToBase58() {
			return handleStatefulSwapError(streamer, NewSwapValidationError("owner must be swap authority"))
		}

		if owner.PublicKey().ToBase58() != currencyMetadataRecord.CreatedBy {
			return handleStatefulSwapError(streamer, NewSwapDeniedError("only the currency creator can buy initial tokens"))
		}

		if !common.IsCoreMint(fromMint) {
			return handleStatefulSwapError(streamer, NewSwapValidationError("source mint must be the core mint"))
		}

		// The VM is not supported yet, so we need to work around GetVmConfigForMint
		destinationVaultRecord, err := s.data.GetKey(ctx, currencyMetadataRecord.Authority)
		if err != nil {
			log.With(zap.Error(err)).Warn("failure getting destination vm authority vault record")
			return handleStatefulSwapError(streamer, err)
		}
		destinationVmAuthority, err = common.NewAccountFromPrivateKeyString(destinationVaultRecord.PrivateKey)
		if err != nil {
			log.With(zap.Error(err)).Warn("invalid destination vm authority private key")
			return handleStatefulSwapError(streamer, err)
		}
	}

	//
	// Section: Antispam
	//

	ownerMetadata, err := common.GetOwnerMetadata(ctx, s.data, owner)
	if err == common.ErrOwnerNotFound {
		return handleStatefulSwapError(streamer, NewSwapDeniedError("not an ocp account"))
	} else if err != nil {
		log.With(zap.Error(err)).Warn("failure getting owner metadata")
		return handleStatefulSwapError(streamer, err)
	}
	if ownerMetadata.State != common.OwnerManagementStateOcpAccount {
		return handleStatefulSwapError(streamer, NewSwapDeniedError("not an ocp account"))
	}
	if ownerMetadata.Type != common.OwnerTypeUser12Words {
		return handleStatefulSwapError(streamer, NewSwapDeniedError("not a user ocp account"))
	}

	allow, err := s.antispamGuard.AllowSwap(ctx, swap.FundingSource(initiateReserveSwapReq.FundingSource), owner, fromMint, toMint, initiateReserveSwapReq.Amount, initializesMint)
	if err != nil {
		return handleStatefulSwapError(streamer, err)
	} else if !allow {
		return handleStatefulSwapError(streamer, NewSwapDeniedError("rate limited"))
	}

	//
	// Section: Transaction construction
	//

	noncePool, err := transaction_util.SelectNoncePool(
		nonce.EnvironmentSolana,
		nonce.EnvironmentInstanceSolanaMainnet,
		nonce.PurposeClientSwap,
		s.noncePools...,
	)
	if err != nil {
		log.With(zap.Error(err)).Warn("failure selecting nonce pool")
		return handleStatefulSwapError(streamer, err)
	}
	selectedNonce, err := noncePool.GetNonce(ctx)
	if err != nil {
		log.With(zap.Error(err)).Warn("failure selecting available nonce")
		return handleStatefulSwapError(streamer, err)
	}
	defer func() {
		selectedNonce.ReleaseIfNotReserved(ctx)
	}()

	var swapHandler SwapHandler
	if initializesMint {
		swapHandler, err = NewReserveCreateAndBuySwapHandler(
			ctx,
			s.data,
			owner,
			toMint,
			initiateReserveSwapReq.Amount,
			selectedNonce,
		)
	} else if common.IsCoreMint(fromMint) {
		swapHandler = NewReserveBuySwapHandler(
			s.data,
			owner,
			swapAuthority,
			toMint,
			initiateReserveSwapReq.Amount,
			selectedNonce,
		)
	} else if common.IsCoreMint(toMint) {
		swapHandler = NewReserveSellSwapHandler(
			s.data,
			owner,
			swapAuthority,
			fromMint,
			initiateReserveSwapReq.Amount,
			selectedNonce,
		)
	} else {
		swapHandler = NewReserveBuySellSwapHandler(
			s.data,
			owner,
			swapAuthority,
			fromMint,
			toMint,
			initiateReserveSwapReq.Amount,
			selectedNonce,
		)
	}
	if err != nil {
		log.With(zap.Error(err)).Warn("failure initializing swap handler")
		return handleStatefulSwapError(streamer, err)
	}

	alts, err := swapHandler.GetAlts(ctx)
	if err != nil {
		log.With(zap.Error(err)).Warn("failure getting alt")
		return handleStatefulSwapError(streamer, err)
	}

	ixns, err := swapHandler.MakeInstructions(ctx)
	if err != nil {
		log.With(zap.Error(err)).Warn("failure making instructions")
		return handleStatefulSwapError(streamer, err)
	}

	txn := solana.NewV0Transaction(
		common.GetSubsidizer().PublicKey().ToBytes(),
		alts,
		ixns,
	)

	txn.SetBlockhash(selectedNonce.Blockhash)

	marshalledTxnMessage := txn.Message.Marshal()

	//
	// Section: Server parameters
	//

	if err := streamer.Send(&transactionpb.StatefulSwapResponse{
		Response: &transactionpb.StatefulSwapResponse_ServerParameters_{
			ServerParameters: swapHandler.GetServerParameters(),
		},
	}); err != nil {
		return handleStatefulSwapError(streamer, err)
	}

	//
	// Section: Transaction signing
	//

	req, err = protoutil.BoundedReceive[transactionpb.StatefulSwapRequest](ctx, streamer, s.conf.clientReceiveTimeout.Get(ctx))
	if err != nil {
		log.With(zap.Error(err)).Info("error receiving request from client")
		return err
	}

	submitSignaturesReq := req.GetSubmitSignatures()
	if submitSignaturesReq == nil {
		return handleStatefulSwapError(streamer, status.Error(codes.InvalidArgument, "StatefulSwapRequest.SubmitSignatures is nil"))
	}

	requiredSignatures := 2
	if bytes.Equal(owner.PublicKey().ToBytes(), swapAuthority.PublicKey().ToBytes()) {
		requiredSignatures = 1
	}
	if len(submitSignaturesReq.TransactionSignatures) != requiredSignatures {
		return handleStatefulSwapStructuredError(
			streamer,
			transactionpb.StatefulSwapResponse_Error_SIGNATURE_ERROR,
			toReasonStringErrorDetails(errors.Errorf("expected %d signatures", requiredSignatures)),
		)
	}

	for i := range txn.Message.Header.NumSignatures {
		account := txn.Message.Accounts[i]

		var isClientSignature bool
		var protoSignature *commonpb.Signature

		if bytes.Equal(account, owner.PublicKey().ToBytes()) {
			isClientSignature = true
			protoSignature = submitSignaturesReq.TransactionSignatures[0]
		} else if bytes.Equal(account, swapAuthority.PublicKey().ToBytes()) {
			isClientSignature = true
			protoSignature = submitSignaturesReq.TransactionSignatures[1]
		}

		if !isClientSignature {
			continue
		}

		if !ed25519.Verify(
			account,
			marshalledTxnMessage,
			protoSignature.Value,
		) {
			return handleStatefulSwapStructuredError(
				streamer,
				transactionpb.StatefulSwapResponse_Error_SIGNATURE_ERROR,
				toInvalidTxnSignatureErrorDetails(0, txn, protoSignature),
			)
		}

		copy(txn.Signatures[i][:], protoSignature.Value)
	}

	err = txn.Sign(
		common.GetSubsidizer().PrivateKey().ToBytes(),
		sourceVmConfig.Authority.PrivateKey().ToBytes(),
		destinationVmAuthority.PrivateKey().ToBytes(),
	)
	if err != nil {
		log.With(zap.Error(err)).Info("failure signing transaction")
		return handleStatefulSwapError(streamer, err)
	}

	marshalledTxn := txn.Marshal()

	txnSignature := base58.Encode(txn.Signature())

	//
	// Section: Swap state DB commit
	//

	var initialState swap.State
	switch initiateReserveSwapReq.FundingSource {
	case transactionpb.FundingSource_FUNDING_SOURCE_SUBMIT_INTENT:
		initialState = swap.StateCreated
	case transactionpb.FundingSource_FUNDING_SOURCE_EXTERNAL_WALLET:
		initialState = swap.StateFunding
	default:
		return handleStatefulSwapError(streamer, NewSwapDeniedErrorf("funding source %s is not supported", initiateReserveSwapReq.FundingSource))
	}

	record := &swap.Record{
		SwapId:               swapId,
		Owner:                owner.PublicKey().ToBase58(),
		FromMint:             fromMint.PublicKey().ToBase58(),
		ToMint:               toMint.PublicKey().ToBase58(),
		Amount:               initiateReserveSwapReq.Amount,
		FundingSource:        swap.FundingSource(initiateReserveSwapReq.FundingSource),
		FundingId:            initiateReserveSwapReq.FundingId,
		Nonce:                selectedNonce.Account.PublicKey().ToBase58(),
		Blockhash:            base58.Encode(selectedNonce.Blockhash[:]),
		ProofSignature:       base58.Encode(initiateReq.ProofSignature.Value),
		TransactionSignature: txnSignature,
		TransactionBlob:      marshalledTxn,
		State:                initialState,
		CreatedAt:            time.Now(),
	}

	err = s.data.ExecuteInTx(ctx, sql.LevelDefault, func(ctx context.Context) error {
		err := selectedNonce.MarkReservedWithSignature(ctx, txnSignature)
		if err != nil {
			log.With(zap.Error(err)).Warn("failure reserving nonce")
			return err
		}

		err = s.data.SaveSwap(ctx, record)
		if err != nil {
			log.With(zap.Error(err)).Warn("failure saving swap record")
			return err
		}

		if initializesMint {
			currencyMetadataRecord.State = currency.MetadataStateFundingAuthority
			err = s.data.SaveCurrencyMetadata(ctx, currencyMetadataRecord)
			if err != nil {
				log.With(zap.Error(err)).Warn("failure saving currency metadata record")
				return err
			}
		}

		return nil
	})
	if err != nil {
		return handleStatefulSwapError(streamer, err)
	}

	//
	// Section: Final RPC response
	//

	err = streamer.Send(&transactionpb.StatefulSwapResponse{
		Response: &transactionpb.StatefulSwapResponse_Success_{
			Success: &transactionpb.StatefulSwapResponse_Success{
				Code: transactionpb.StatefulSwapResponse_Success_OK,
			},
		},
	})
	return handleStatefulSwapError(streamer, err)
}

func (s *transactionServer) GetSwap(ctx context.Context, req *transactionpb.GetSwapRequest) (*transactionpb.GetSwapResponse, error) {
	log := s.log.With(zap.String("method", "GetSwap"))
	log = client.InjectLoggingMetadata(ctx, log)

	owner, err := common.NewAccountFromProto(req.Owner)
	if err != nil {
		log.With(zap.Error(err)).Warn("invalid owner account")
		return nil, status.Error(codes.Internal, "")
	}
	log = log.With(zap.String("owner", owner.PublicKey().ToBase58()))

	swapId := base58.Encode(req.Id.Value)
	log = log.With(zap.String("swap_id", swapId))

	signature := req.Signature
	req.Signature = nil
	if err := s.auth.Authenticate(ctx, owner, req, signature); err != nil {
		return nil, err
	}

	record, err := s.data.GetSwapById(ctx, swapId)
	if err == swap.ErrNotFound {
		return &transactionpb.GetSwapResponse{
			Result: transactionpb.GetSwapResponse_NOT_FOUND,
		}, nil
	} else if err != nil {
		log.With(zap.Error(err)).Warn("failure getting swap")
		return nil, status.Error(codes.Internal, "")
	}

	if record.Owner != owner.PublicKey().ToBase58() {
		return &transactionpb.GetSwapResponse{
			Result: transactionpb.GetSwapResponse_DENIED,
		}, nil
	}

	protoSwap, err := toProtoSwap(record)
	if err != nil {
		log.With(zap.Error(err)).Warn("failure converting swap to proto")
		return nil, status.Error(codes.Internal, "")
	}

	return &transactionpb.GetSwapResponse{
		Result: transactionpb.GetSwapResponse_OK,
		Swap:   protoSwap,
	}, nil
}

func (s *transactionServer) GetPendingSwaps(ctx context.Context, req *transactionpb.GetPendingSwapsRequest) (*transactionpb.GetPendingSwapsResponse, error) {
	log := s.log.With(zap.String("method", "GetPendingSwaps"))
	log = client.InjectLoggingMetadata(ctx, log)

	owner, err := common.NewAccountFromProto(req.Owner)
	if err != nil {
		log.With(zap.Error(err)).Warn("invalid owner account")
		return nil, status.Error(codes.Internal, "")
	}
	log = log.With(zap.String("owner", owner.PublicKey().ToBase58()))

	signature := req.Signature
	req.Signature = nil
	if err := s.auth.Authenticate(ctx, owner, req, signature); err != nil {
		return nil, err
	}

	if s.conf.disableSwaps.Get(ctx) {
		return &transactionpb.GetPendingSwapsResponse{
			Result: transactionpb.GetPendingSwapsResponse_OK,
		}, nil
	}

	// Swap is created, but requires client to initiate the funding
	createdSwaps, err := s.data.GetAllSwapsByOwnerAndState(ctx, owner.PublicKey().ToBase58(), swap.StateCreated)
	if err != nil && err != swap.ErrNotFound {
		log.With(zap.Error(err)).Warn("failure getting swaps in CREATED state")
		return nil, status.Error(codes.Internal, "")
	}

	allPendingSwaps := createdSwaps

	if len(allPendingSwaps) == 0 {
		return &transactionpb.GetPendingSwapsResponse{
			Result: transactionpb.GetPendingSwapsResponse_NOT_FOUND,
		}, nil
	}

	res := make([]*transactionpb.SwapMetadata, len(allPendingSwaps))
	for i, pendingSwap := range allPendingSwaps {
		log := log.With(zap.String("swap_id", pendingSwap.SwapId))

		res[i], err = toProtoSwap(pendingSwap)
		if err != nil {
			log.With(zap.Error(err)).Warn("failure converting swap to proto")
			return nil, status.Error(codes.Internal, "")
		}
	}

	return &transactionpb.GetPendingSwapsResponse{
		Result: transactionpb.GetPendingSwapsResponse_OK,
		Swaps:  res,
	}, nil
}

func toProtoSwap(record *swap.Record) (*transactionpb.SwapMetadata, error) {
	decodedSwapId, err := base58.Decode(record.SwapId)
	if err != nil {
		return nil, err
	}

	fromMint, err := common.NewAccountFromPublicKeyString(record.FromMint)
	if err != nil {
		return nil, err
	}

	toMint, err := common.NewAccountFromPublicKeyString(record.ToMint)
	if err != nil {
		return nil, err
	}

	decodedSignature, err := base58.Decode(record.ProofSignature)
	if err != nil {
		return nil, err
	}

	return &transactionpb.SwapMetadata{
		VerifiedMetadata: &transactionpb.VerifiedSwapMetadata{
			Kind: &transactionpb.VerifiedSwapMetadata_Reserve{
				Reserve: &transactionpb.VerifiedReserveSwapMetadata{
					ClientParameters: &transactionpb.StatefulSwapRequest_Initiate_ReserveSwapClientParameters{
						Id:            &commonpb.SwapId{Value: decodedSwapId},
						FromMint:      fromMint.ToProto(),
						ToMint:        toMint.ToProto(),
						Amount:        record.Amount,
						FundingSource: transactionpb.FundingSource(record.FundingSource),
						FundingId:     record.FundingId,
					},
				},
			},
		},
		State:     transactionpb.SwapMetadata_State(record.State),
		Signature: &commonpb.Signature{Value: decodedSignature},
	}, nil
}
