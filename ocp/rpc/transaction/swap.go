package transaction

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"database/sql"
	"math/big"
	"strings"
	"time"

	"github.com/mr-tron/base58/base58"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	commonpb "github.com/code-payments/ocp-protobuf-api/generated/go/common/v1"
	transactionpb "github.com/code-payments/ocp-protobuf-api/generated/go/transaction/v1"

	"github.com/code-payments/ocp-server/coinbase"
	"github.com/code-payments/ocp-server/grpc/client"
	"github.com/code-payments/ocp-server/ocp/balance"
	"github.com/code-payments/ocp-server/ocp/common"
	"github.com/code-payments/ocp-server/ocp/data/currency"
	"github.com/code-payments/ocp-server/ocp/data/intent"
	"github.com/code-payments/ocp-server/ocp/data/nonce"
	"github.com/code-payments/ocp-server/ocp/data/swap"
	"github.com/code-payments/ocp-server/ocp/data/timelock"
	"github.com/code-payments/ocp-server/ocp/rpc"
	transaction_util "github.com/code-payments/ocp-server/ocp/transaction"
	"github.com/code-payments/ocp-server/ocp/vm"
	"github.com/code-payments/ocp-server/protoutil"
	"github.com/code-payments/ocp-server/solana"
	"github.com/code-payments/ocp-server/solana/currencycreator"
	"github.com/code-payments/ocp-server/solana/token"
	"github.com/code-payments/ocp-server/usdc"
)

func (s *transactionServer) StatefulSwap(streamer transactionpb.Transaction_StatefulSwapServer) error {
	// Bound the total RPC. Keeping the timeout higher to see where we land because
	// there's a lot of stuff happening in this method.
	ctx, cancel := context.WithTimeout(streamer.Context(), s.conf.swapTimeout.Get(streamer.Context()))
	defer cancel()

	log := s.log.With(zap.String("method", "StatefulSwap"))
	log = client.InjectLoggingMetadata(ctx, log, rpc.UserAgentName)

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

	reqSignature := initiateReq.Signature
	initiateReq.Signature = nil
	if err := s.auth.Authenticate(ctx, owner, initiateReq, reqSignature); err != nil {
		return handleStatefulSwapError(streamer, err)
	}

	// todo: Refactor needed for duplication of code, but this isolates fundamentally different swap flows
	switch initiateReq.GetKind().(type) {
	case *transactionpb.StatefulSwapRequest_Initiate_Reserve:
		log = log.With(zap.String("kind", "reserve"))
		return s.handleReserveStatefulSwap(ctx, log, streamer, initiateReq, owner)
	case *transactionpb.StatefulSwapRequest_Initiate_Stablecoin:
		log = log.With(zap.String("kind", "stablecoin"))
		return s.handleStablecoinStatefulSwap(ctx, log, streamer, initiateReq, owner)
	default:
		return handleStatefulSwapError(streamer, status.Error(codes.InvalidArgument, "StatefulSwapRequest.Initiate.Kind is nil"))
	}
}

func (s *transactionServer) handleReserveStatefulSwap(
	ctx context.Context,
	log *zap.Logger,
	streamer transactionpb.Transaction_StatefulSwapServer,
	initiateReq *transactionpb.StatefulSwapRequest_Initiate,
	owner *common.Account,
) error {
	initiateReserveSwapReq := initiateReq.GetReserve()
	if initiateReserveSwapReq == nil {
		return handleStatefulSwapError(streamer, status.Error(codes.InvalidArgument, "StatefulSwapRequest.Initiate.Reserve is nil"))
	}

	swapId := base58.Encode(initiateReserveSwapReq.Id.Value)
	log = log.With(zap.String("swap_id", swapId))

	swapAuthority, err := common.NewAccountFromProto(initiateReq.SwapAuthority)
	if err != nil {
		log.With(zap.Error(err)).Warn("invalid swap authority")
		return handleStatefulSwapError(streamer, err)
	}
	log = log.With(zap.String("swap_authority", swapAuthority.PublicKey().ToBase58()))

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
		zap.Uint64("swap_amount", initiateReserveSwapReq.SwapAmount),
		zap.Uint64("fee_amount", initiateReserveSwapReq.FeeAmount),
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

	if initiateReserveSwapReq.SwapAmount == 0 {
		return handleStatefulSwapError(streamer, NewSwapValidationError("swap amount must be positive"))
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

	sourceTimelockAccountRecord, err := s.data.GetTimelockByVault(ctx, ownerSourceTimelockVault.PublicKey().ToBase58())
	if err == timelock.ErrTimelockNotFound {
		return handleStatefulSwapError(streamer, NewSwapValidationError("source timelock vault account not opened"))
	} else if err != nil {
		log.With(zap.Error(err)).Warn("failure getting source timelock record")
		return handleStatefulSwapError(streamer, err)
	}
	if !sourceTimelockAccountRecord.IsLocked() {
		return handleStatefulSwapError(streamer, NewSwapDeniedError("source timelock account isn't locked"))
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
		if balance < initiateReserveSwapReq.SwapAmount+initiateReserveSwapReq.FeeAmount {
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
	case transactionpb.FundingSource_FUNDING_SOURCE_COINBASE_ONRAMP:
		if !common.IsCoreMint(fromMint) {
			return handleStatefulSwapError(streamer, NewSwapDeniedError("source mint must be core mint"))
		}

		order, err := s.coinbaseClient.GetOrder(ctx, initiateReserveSwapReq.FundingId)
		if err == coinbase.ErrOrderNotFound {
			return handleStatefulSwapError(streamer, NewSwapValidationError("coinbase order not found"))
		} else if err != nil {
			log.With(zap.Error(err)).Warn("failure getting coinbase order")
			return handleStatefulSwapError(streamer, err)
		}
		if order.Status == coinbase.OrderStatusFailed {
			return handleStatefulSwapError(streamer, NewSwapValidationError("coinbase order is in a failed state"))
		}

		if !strings.EqualFold(order.PurchaseAmount.Currency, common.CoreMintSymbol) {
			return handleStatefulSwapError(streamer, NewSwapValidationError("coinbase order is not for the core mint"))
		}
		if order.PartnerUserRef != owner.PublicKey().ToBase58() {
			return handleStatefulSwapError(streamer, NewSwapDeniedError("coinbase order partner user ref does not match owner"))
		}
		if order.DestinationAddress != sourceTimelockAccountRecord.SwapPdaAddress {
			return handleStatefulSwapError(streamer, NewSwapValidationError("coinbase order destination address is not the owner's swap pda"))
		}

		orderQuarks, err := decimalToQuarks(order.PurchaseAmount.Value, common.CoreMintDecimals)
		if err != nil {
			log.With(zap.Error(err)).Warn("invalid coinbase order purchase amount")
			return handleStatefulSwapError(streamer, NewSwapValidationError("coinbase order purchase amount is invalid"))
		}
		if orderQuarks != initiateReserveSwapReq.SwapAmount+initiateReserveSwapReq.FeeAmount {
			return handleStatefulSwapError(streamer, NewSwapDeniedError("coinbase order purchase amount does not match swap amount"))
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

	if !initializesMint && !common.IsCoreMint(fromMint) {
		liveReserveState, err := s.mintDataProvider.GetLiveReserveState(ctx, fromMint)
		if err != nil {
			log.With(zap.Error(err)).Warn("failure getting live reserve state")
			return handleStatefulSwapError(streamer, err)
		}

		_, estimatedFees := currencycreator.EstimateSell(&currencycreator.EstimateSellArgs{
			CurrentSupplyInQuarks: liveReserveState.SupplyFromBonding,
			SellAmountInQuarks:    initiateReserveSwapReq.SwapAmount,
			ValueMintDecimals:     uint8(common.CoreMintDecimals),
			SellFeeBps:            currencyMetadataRecord.SellFeeBps,
		})
		if estimatedFees == 0 {
			return handleStatefulSwapError(streamer, NewSwapDeniedError("swap would not generate a sell fee"))
		}
	}

	var destinationVmAuthority *common.Account
	if !initializesMint {
		if owner.PublicKey().ToBase58() == swapAuthority.PublicKey().ToBase58() {
			return handleStatefulSwapError(streamer, NewSwapValidationError("owner cannot be swap authority"))
		}

		if initiateReserveSwapReq.FeeAmount != 0 {
			return handleStatefulSwapError(streamer, NewSwapValidationError("fee amount must be 0"))
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

		destinationTimelockAccountRecord, err := s.data.GetTimelockByVault(ctx, ownerDestinationTimelockVault.PublicKey().ToBase58())
		if err == timelock.ErrTimelockNotFound {
			return handleStatefulSwapError(streamer, NewSwapValidationError("destination timelock vault account not opened"))
		} else if err != nil {
			log.With(zap.Error(err)).Warn("failure getting destination timelock record")
			return handleStatefulSwapError(streamer, err)
		}
		if !destinationTimelockAccountRecord.IsLocked() {
			return handleStatefulSwapError(streamer, NewSwapDeniedError("destination timelock account isn't locked"))
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

	allow, err := s.antispamGuard.AllowSwap(ctx, swap.KindReserve, swap.FundingSource(initiateReserveSwapReq.FundingSource), owner, fromMint, toMint, initiateReserveSwapReq.SwapAmount, initiateReserveSwapReq.FeeAmount, initializesMint)
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
			initiateReserveSwapReq.SwapAmount,
			initiateReserveSwapReq.FeeAmount,
			selectedNonce,
		)
	} else if common.IsCoreMint(fromMint) {
		swapHandler = NewReserveBuySwapHandler(
			s.data,
			owner,
			swapAuthority,
			toMint,
			initiateReserveSwapReq.SwapAmount,
			selectedNonce,
		)
	} else if common.IsCoreMint(toMint) {
		swapHandler = NewReserveSellSwapHandler(
			s.data,
			owner,
			swapAuthority,
			fromMint,
			initiateReserveSwapReq.SwapAmount,
			selectedNonce,
		)
	} else {
		swapHandler = NewReserveBuySellSwapHandler(
			s.data,
			owner,
			swapAuthority,
			fromMint,
			toMint,
			initiateReserveSwapReq.SwapAmount,
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

	req, err := protoutil.BoundedReceive[transactionpb.StatefulSwapRequest](ctx, streamer, s.conf.clientReceiveTimeout.Get(ctx))
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
	case transactionpb.FundingSource_FUNDING_SOURCE_EXTERNAL_WALLET, transactionpb.FundingSource_FUNDING_SOURCE_COINBASE_ONRAMP:
		initialState = swap.StateFunding
	default:
		return handleStatefulSwapError(streamer, NewSwapDeniedErrorf("funding source %s is not supported", initiateReserveSwapReq.FundingSource))
	}

	record := &swap.Record{
		SwapId:               swapId,
		Kind:                 swap.KindReserve,
		Owner:                owner.PublicKey().ToBase58(),
		FromMint:             fromMint.PublicKey().ToBase58(),
		ToMint:               toMint.PublicKey().ToBase58(),
		SwapAmount:           initiateReserveSwapReq.SwapAmount,
		FeeAmount:            initiateReserveSwapReq.FeeAmount,
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

func (s *transactionServer) handleStablecoinStatefulSwap(
	ctx context.Context,
	log *zap.Logger,
	streamer transactionpb.Transaction_StatefulSwapServer,
	initiateReq *transactionpb.StatefulSwapRequest_Initiate,
	owner *common.Account,
) error {
	initiateStablecoinSwapReq := initiateReq.GetStablecoin()
	if initiateStablecoinSwapReq == nil {
		return handleStatefulSwapError(streamer, status.Error(codes.InvalidArgument, "StatefulSwapRequest.Initiate.Stablecoin is nil"))
	}

	swapId := base58.Encode(initiateStablecoinSwapReq.Id.Value)
	log = log.With(zap.String("swap_id", swapId))

	swapAuthority, err := common.NewAccountFromProto(initiateReq.SwapAuthority)
	if err != nil {
		log.With(zap.Error(err)).Warn("invalid swap authority")
		return handleStatefulSwapError(streamer, err)
	}
	log = log.With(zap.String("swap_authority", swapAuthority.PublicKey().ToBase58()))

	fromMint, err := common.NewAccountFromProto(initiateStablecoinSwapReq.FromMint)
	if err != nil {
		log.With(zap.Error(err)).Warn("invalid source mint account")
		return handleStatefulSwapError(streamer, err)
	}
	log = log.With(zap.String("from_mint", fromMint.PublicKey().ToBase58()))

	toMint, err := common.NewAccountFromProto(initiateStablecoinSwapReq.ToMint)
	if err != nil {
		log.With(zap.Error(err)).Warn("invalid destination mint account")
		return handleStatefulSwapError(streamer, err)
	}
	log = log.With(zap.String("to_mint", toMint.PublicKey().ToBase58()))

	destinationOwner, err := common.NewAccountFromProto(initiateStablecoinSwapReq.DestinationOwner)
	if err != nil {
		log.With(zap.Error(err)).Warn("invalid destination owner account")
		return handleStatefulSwapError(streamer, err)
	}
	log = log.With(zap.String("destination_owner", destinationOwner.PublicKey().ToBase58()))

	log = log.With(
		zap.Uint64("swap_amount", initiateStablecoinSwapReq.SwapAmount),
		zap.Uint64("fee_amount", initiateStablecoinSwapReq.FeeAmount),
		zap.String("funding_source", initiateStablecoinSwapReq.FundingSource.String()),
		zap.String("funding_id", initiateStablecoinSwapReq.FundingId),
	)

	//
	// Section: Verified metadata signature verification
	//

	verifiedMetadata := &transactionpb.VerifiedSwapMetadata{
		Kind: &transactionpb.VerifiedSwapMetadata_Stablecoin{
			Stablecoin: &transactionpb.VerifiedCoinbaseStableSwapperSwapMetadata{
				ClientParameters: initiateStablecoinSwapReq,
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

	_, err = s.data.GetSwapByFundingId(ctx, initiateStablecoinSwapReq.FundingId)
	if err == nil {
		return handleStatefulSwapError(streamer, NewSwapDeniedError("attempt to reuse swap funding id"))
	} else if err != swap.ErrNotFound {
		log.With(zap.Error(err)).Warn("failure checking for existing swap record by funding id")
		return handleStatefulSwapError(streamer, err)
	}

	if !common.IsCoreMint(fromMint) {
		return handleStatefulSwapError(streamer, NewSwapValidationError("source mint must be the core mint"))
	}
	if toMint.PublicKey().ToBase58() != usdc.Mint {
		return handleStatefulSwapError(streamer, NewSwapValidationError("destination mint must be usdc"))
	}

	if initiateStablecoinSwapReq.SwapAmount == 0 {
		return handleStatefulSwapError(streamer, NewSwapValidationError("swap amount must be positive"))
	}

	expectedFeeQuarks := uint64(s.conf.createOnSendWithdrawalUsdFee.Get(ctx) * float64(common.CoreMintQuarksPerUnit))
	if initiateStablecoinSwapReq.FeeAmount != expectedFeeQuarks {
		return handleStatefulSwapError(streamer, NewSwapDeniedErrorf("fee amount must be %d quarks", expectedFeeQuarks))
	}

	if initiateStablecoinSwapReq.FundingSource != transactionpb.FundingSource_FUNDING_SOURCE_SUBMIT_INTENT {
		return handleStatefulSwapError(streamer, NewSwapDeniedErrorf("funding source %s is not supported", initiateStablecoinSwapReq.FundingSource))
	}

	decodedFundingId, err := base58.Decode(initiateStablecoinSwapReq.FundingId)
	if err != nil || len(decodedFundingId) != ed25519.PublicKeySize {
		log.With(zap.Error(err)).Warn("invalid funding id")
		return handleStatefulSwapError(streamer, NewSwapValidationError("funding id is not a public key"))
	}

	_, err = s.data.GetIntent(ctx, initiateStablecoinSwapReq.FundingId)
	if err == nil {
		return handleStatefulSwapError(streamer, NewSwapValidationError("funding intent already exists"))
	} else if err != intent.ErrIntentNotFound {
		log.With(zap.Error(err)).Warn("failure getting funding intent record")
		return handleStatefulSwapError(streamer, err)
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

	sourceTimelockAccountRecord, err := s.data.GetTimelockByVault(ctx, ownerSourceTimelockVault.PublicKey().ToBase58())
	if err == timelock.ErrTimelockNotFound {
		return handleStatefulSwapError(streamer, NewSwapValidationError("source timelock vault account not opened"))
	} else if err != nil {
		log.With(zap.Error(err)).Warn("failure getting source timelock record")
		return handleStatefulSwapError(streamer, err)
	}
	if !sourceTimelockAccountRecord.IsLocked() {
		return handleStatefulSwapError(streamer, NewSwapDeniedError("source timelock account isn't locked"))
	}

	ownerBalance, err := balance.CalculateFromCache(ctx, s.data, ownerSourceTimelockVault)
	if err != nil {
		log.With(zap.Error(err)).Warn("failure getting owner source timelock vault balance")
		return handleStatefulSwapError(streamer, err)
	}
	if ownerBalance < initiateStablecoinSwapReq.SwapAmount+initiateStablecoinSwapReq.FeeAmount {
		return handleStatefulSwapError(streamer, NewSwapValidationError("insufficient balance"))
	}

	if owner.PublicKey().ToBase58() == swapAuthority.PublicKey().ToBase58() {
		return handleStatefulSwapError(streamer, NewSwapValidationError("owner cannot be swap authority"))
	}

	destinationOwnerAccountInfo, _, err := s.data.GetBlockchainAccountInfo(ctx, destinationOwner.PublicKey().ToBase58(), solana.CommitmentFinalized)
	switch err {
	case nil:
		if bytes.Equal(destinationOwnerAccountInfo.Owner, token.ProgramKey) {
			return handleStatefulSwapError(streamer, NewSwapValidationError("destination owner is a token account"))
		}
	case solana.ErrNoAccountInfo:
	default:
		log.With(zap.Error(err)).Warn("failure getting destination owner blockchain account info")
		return handleStatefulSwapError(streamer, err)
	}

	destinationLiquidity, err := transaction_util.GetCoinbaseSwapDestinationLiquidity(ctx, s.data, toMint.PublicKey().ToBytes())
	if err != nil {
		log.With(zap.Error(err)).Warn("failure getting coinbase destination liquidity")
		return handleStatefulSwapError(streamer, err)
	}
	if destinationLiquidity < 4*initiateStablecoinSwapReq.SwapAmount/3 {
		return handleStatefulSwapError(streamer, NewSwapDeniedError("insufficient coinbase stable swapper destination liquidity"))
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

	allow, err := s.antispamGuard.AllowSwap(ctx, swap.KindStablecoin, swap.FundingSource(initiateStablecoinSwapReq.FundingSource), owner, fromMint, toMint, initiateStablecoinSwapReq.SwapAmount, initiateStablecoinSwapReq.FeeAmount, false)
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

	swapHandler := NewCoinbaseStableSwapperSwapHandler(
		s.data,
		owner,
		swapAuthority,
		destinationOwner,
		fromMint,
		toMint,
		initiateStablecoinSwapReq.SwapAmount,
		initiateStablecoinSwapReq.FeeAmount,
		selectedNonce,
	)

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

	req, err := protoutil.BoundedReceive[transactionpb.StatefulSwapRequest](ctx, streamer, s.conf.clientReceiveTimeout.Get(ctx))
	if err != nil {
		log.With(zap.Error(err)).Info("error receiving request from client")
		return err
	}

	submitSignaturesReq := req.GetSubmitSignatures()
	if submitSignaturesReq == nil {
		return handleStatefulSwapError(streamer, status.Error(codes.InvalidArgument, "StatefulSwapRequest.SubmitSignatures is nil"))
	}

	if len(submitSignaturesReq.TransactionSignatures) != 2 {
		return handleStatefulSwapStructuredError(
			streamer,
			transactionpb.StatefulSwapResponse_Error_SIGNATURE_ERROR,
			toReasonStringErrorDetails(errors.New("expected 2 signatures")),
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

	record := &swap.Record{
		SwapId:               swapId,
		Kind:                 swap.KindStablecoin,
		Owner:                owner.PublicKey().ToBase58(),
		DestinationOwner:     destinationOwner.PublicKey().ToBase58(),
		FromMint:             fromMint.PublicKey().ToBase58(),
		ToMint:               toMint.PublicKey().ToBase58(),
		SwapAmount:           initiateStablecoinSwapReq.SwapAmount,
		FeeAmount:            initiateStablecoinSwapReq.FeeAmount,
		FundingSource:        swap.FundingSource(initiateStablecoinSwapReq.FundingSource),
		FundingId:            initiateStablecoinSwapReq.FundingId,
		Nonce:                selectedNonce.Account.PublicKey().ToBase58(),
		Blockhash:            base58.Encode(selectedNonce.Blockhash[:]),
		ProofSignature:       base58.Encode(initiateReq.ProofSignature.Value),
		TransactionSignature: txnSignature,
		TransactionBlob:      marshalledTxn,
		State:                swap.StateCreated,
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
	log = client.InjectLoggingMetadata(ctx, log, rpc.UserAgentName)

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
	log = client.InjectLoggingMetadata(ctx, log, rpc.UserAgentName)

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

	var verifiedMetadata *transactionpb.VerifiedSwapMetadata
	switch record.Kind {
	case swap.KindReserve:
		verifiedMetadata = &transactionpb.VerifiedSwapMetadata{
			Kind: &transactionpb.VerifiedSwapMetadata_Reserve{
				Reserve: &transactionpb.VerifiedReserveSwapMetadata{
					ClientParameters: &transactionpb.StatefulSwapRequest_Initiate_ReserveSwapClientParameters{
						Id:            &commonpb.SwapId{Value: decodedSwapId},
						FromMint:      fromMint.ToProto(),
						ToMint:        toMint.ToProto(),
						SwapAmount:    record.SwapAmount,
						FeeAmount:     record.FeeAmount,
						FundingSource: transactionpb.FundingSource(record.FundingSource),
						FundingId:     record.FundingId,
					},
				},
			},
		}
	case swap.KindStablecoin:
		destinationOwner, err := common.NewAccountFromPublicKeyString(record.DestinationOwner)
		if err != nil {
			return nil, err
		}
		verifiedMetadata = &transactionpb.VerifiedSwapMetadata{
			Kind: &transactionpb.VerifiedSwapMetadata_Stablecoin{
				Stablecoin: &transactionpb.VerifiedCoinbaseStableSwapperSwapMetadata{
					ClientParameters: &transactionpb.StatefulSwapRequest_Initiate_CoinbaseStableSwapperClientParameters{
						Id:               &commonpb.SwapId{Value: decodedSwapId},
						FromMint:         fromMint.ToProto(),
						ToMint:           toMint.ToProto(),
						SwapAmount:       record.SwapAmount,
						FeeAmount:        record.FeeAmount,
						FundingSource:    transactionpb.FundingSource(record.FundingSource),
						FundingId:        record.FundingId,
						DestinationOwner: destinationOwner.ToProto(),
					},
				},
			},
		}
	default:
		return nil, errors.Errorf("unsupported swap kind: %s", record.Kind)
	}

	return &transactionpb.SwapMetadata{
		VerifiedMetadata: verifiedMetadata,
		State:            transactionpb.SwapMetadata_State(record.State),
		Signature:        &commonpb.Signature{Value: decodedSignature},
	}, nil
}

func decimalToQuarks(value string, decimals int) (uint64, error) {
	rat, ok := new(big.Rat).SetString(value)
	if !ok {
		return 0, errors.Errorf("invalid decimal value: %s", value)
	}
	if rat.Sign() < 0 {
		return 0, errors.New("amount is negative")
	}
	multiplier := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(decimals)), nil)
	scaled := new(big.Rat).Mul(rat, new(big.Rat).SetInt(multiplier))
	if !scaled.IsInt() {
		return 0, errors.New("amount has more precision than mint decimals")
	}
	quarks := scaled.Num()
	if !quarks.IsUint64() {
		return 0, errors.New("amount overflows uint64")
	}
	return quarks.Uint64(), nil
}
