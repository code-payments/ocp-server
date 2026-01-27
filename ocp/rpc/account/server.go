package account

import (
	"context"
	"errors"
	"time"

	"github.com/golang/protobuf/proto"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	accountpb "github.com/code-payments/ocp-protobuf-api/generated/go/account/v1"
	commonpb "github.com/code-payments/ocp-protobuf-api/generated/go/common/v1"
	transactionpb "github.com/code-payments/ocp-protobuf-api/generated/go/transaction/v1"

	"github.com/code-payments/ocp-server/cache"
	"github.com/code-payments/ocp-server/grpc/client"
	auth_util "github.com/code-payments/ocp-server/ocp/auth"
	"github.com/code-payments/ocp-server/ocp/balance"
	"github.com/code-payments/ocp-server/ocp/common"
	ocp_data "github.com/code-payments/ocp-server/ocp/data"
	"github.com/code-payments/ocp-server/ocp/data/action"
	account_worker "github.com/code-payments/ocp-server/ocp/worker/account"
	timelock_token_v1 "github.com/code-payments/ocp-server/solana/timelock/v1"
)

var (
	giftCardCacheByOwner = cache.NewCache(10_000)
)

type balanceMetadata struct {
	realValue                  uint64                      // The real value of a token account's balance
	additionalPendingSwapValue *balance.PendingSwapBalance // Estimated additional pending value of a token account's balance from swaps
	source                     accountpb.TokenAccountInfo_BalanceSource
}

type server struct {
	log  *zap.Logger
	data ocp_data.Provider
	auth *auth_util.RPCSignatureVerifier

	accountpb.UnimplementedAccountServer
}

func NewAccountServer(log *zap.Logger, data ocp_data.Provider) accountpb.AccountServer {
	return &server{
		log:  log,
		data: data,
		auth: auth_util.NewRPCSignatureVerifier(log, data),
	}
}

func (s *server) IsOcpAccount(ctx context.Context, req *accountpb.IsOcpAccountRequest) (*accountpb.IsOcpAccountResponse, error) {
	log := s.log.With(zap.String("method", "IsOcpAccount"))
	log = client.InjectLoggingMetadata(ctx, log)

	owner, err := common.NewAccountFromProto(req.Owner)
	if err != nil {
		log.With(zap.Error(err)).Warn("invalid owner account")
		return nil, status.Error(codes.Internal, "")
	}
	log = log.With(zap.String("owner_account", owner.PublicKey().ToBase58()))

	signature := req.Signature
	req.Signature = nil
	if err := s.auth.Authenticate(ctx, owner, req, signature); err != nil {
		return nil, err
	}

	ownerMetadata, err := common.GetOwnerMetadata(ctx, s.data, owner)
	if err == common.ErrOwnerNotFound {
		return &accountpb.IsOcpAccountResponse{
			Result: accountpb.IsOcpAccountResponse_NOT_FOUND,
		}, nil
	} else if err != nil {
		log.With(zap.Error(err)).Warn("failure getting owner management state")
		return nil, status.Error(codes.Internal, "")
	}

	if ownerMetadata.Type != common.OwnerTypeUser12Words {
		return &accountpb.IsOcpAccountResponse{
			Result: accountpb.IsOcpAccountResponse_NOT_FOUND,
		}, nil
	}

	var result accountpb.IsOcpAccountResponse_Result
	switch ownerMetadata.State {
	case common.OwnerManagementStateOcpAccount:
		result = accountpb.IsOcpAccountResponse_OK
	case common.OwnerManagementStateNotFound:
		result = accountpb.IsOcpAccountResponse_NOT_FOUND
	case common.OwnerManagementStateUnlocked:
		result = accountpb.IsOcpAccountResponse_UNLOCKED_TIMELOCK_ACCOUNT
	case common.OwnerManagementStateUnknown:
		log.Warn("unknown owner management state")
		return nil, status.Error(codes.Internal, "")
	}

	return &accountpb.IsOcpAccountResponse{
		Result: result,
	}, nil
}

func (s *server) GetTokenAccountInfos(ctx context.Context, req *accountpb.GetTokenAccountInfosRequest) (*accountpb.GetTokenAccountInfosResponse, error) {
	log := s.log.With(zap.String("method", "GetTokenAccountInfos"))
	log = client.InjectLoggingMetadata(ctx, log)

	owner, err := common.NewAccountFromProto(req.Owner)
	if err != nil {
		log.With(zap.Error(err)).Warn("invalid owner account")
		return nil, status.Error(codes.Internal, "")
	}
	log = log.With(zap.String("owner_account", owner.PublicKey().ToBase58()))

	ownerSignature := req.Signature
	req.Signature = nil

	var requestingOwner *common.Account
	var requestingOwnerSignature *commonpb.Signature
	if req.RequestingOwner != nil {
		requestingOwner, err = common.NewAccountFromProto(req.RequestingOwner)
		if err != nil {
			log.With(zap.Error(err)).Warn("invalid requesting owner account")
			return nil, status.Error(codes.Internal, "")
		}
		log = log.With(zap.String("requesting_owner_account", requestingOwner.PublicKey().ToBase58()))

		requestingOwnerSignature = req.RequestingOwnerSignature
		req.RequestingOwnerSignature = nil
	}

	if err := s.auth.Authenticate(ctx, owner, req, ownerSignature); err != nil {
		return nil, err
	}
	if requestingOwner != nil {
		if err := s.auth.Authenticate(ctx, requestingOwner, req, requestingOwnerSignature); err != nil {
			return nil, err
		}
	}

	cachedResp, ok := giftCardCacheByOwner.Retrieve(owner.PublicKey().ToBase58())
	if ok {
		cachedResp := cachedResp.(*accountpb.GetTokenAccountInfosResponse)

		s.updateCachedResponse(cachedResp)

		resp, err := s.addRequestingOwnerMetadata(ctx, cachedResp, requestingOwner)
		if err != nil {
			log.With(zap.Error(err)).Warn("failure adding requesting owner metadata")
			return nil, status.Error(codes.Internal, "")
		}
		return resp, nil
	}

	// Fetch all account records
	allRecordsByMintAndType, err := common.GetLatestTokenAccountRecordsForOwner(ctx, s.data, owner)
	if err != nil {
		log.With(zap.Error(err)).Warn("failure getting latest account records")
		return nil, status.Error(codes.Internal, "")
	}

	var hasGiftCardAccount bool
	var allRecords []*common.AccountRecords
	for _, recordsByType := range allRecordsByMintAndType {
		for _, batchRecords := range recordsByType {
			for _, records := range batchRecords {
				if records.General.AccountType == commonpb.AccountType_REMOTE_SEND_GIFT_CARD {
					hasGiftCardAccount = true
				}
				allRecords = append(allRecords, records)
			}
		}
	}

	// Filter account records based on client request
	//
	// todo: this needs tests
	var filteredRecords []*common.AccountRecords
	switch req.Filter.(type) {
	case *accountpb.GetTokenAccountInfosRequest_FilterByTokenAddress,
		*accountpb.GetTokenAccountInfosRequest_FilterByAccountType,
		*accountpb.GetTokenAccountInfosRequest_FilterByMintAddress:
		if hasGiftCardAccount {
			return nil, status.Error(codes.InvalidArgument, "filter must be nil for gift card owner accounts")
		}
	}
	switch typed := req.Filter.(type) {
	case *accountpb.GetTokenAccountInfosRequest_FilterByTokenAddress:
		filterAccount, err := common.NewAccountFromProto(typed.FilterByTokenAddress)
		if err != nil {
			log.With(zap.Error(err)).Warn("invalid token address filter")
			return nil, status.Error(codes.Internal, "")
		}
		for _, records := range allRecords {
			if records.General.TokenAccount == filterAccount.PublicKey().ToBase58() {
				filteredRecords = append(filteredRecords, records)
				break
			}
		}
	case *accountpb.GetTokenAccountInfosRequest_FilterByAccountType:
		for _, records := range allRecords {
			if records.General.AccountType == typed.FilterByAccountType {
				filteredRecords = append(filteredRecords, records)
			}
		}
	case *accountpb.GetTokenAccountInfosRequest_FilterByMintAddress:
		filterAccount, err := common.NewAccountFromProto(typed.FilterByMintAddress)
		if err != nil {
			log.With(zap.Error(err)).Warn("invalid mint address filter")
			return nil, status.Error(codes.Internal, "")
		}
		for _, records := range allRecords {
			if records.General.MintAccount == filterAccount.PublicKey().ToBase58() {
				filteredRecords = append(filteredRecords, records)
			}
		}
	default:
		filteredRecords = allRecords
	}

	// Trigger a deposit sync with the blockchain for the primary account, if it exists
	for _, records := range filteredRecords {
		if records.General.AccountType == commonpb.AccountType_PRIMARY && !records.General.RequiresDepositSync {
			records.General.RequiresDepositSync = true
			err = s.data.UpdateAccountInfo(ctx, records.General)
			if err != nil {
				log.With(zap.Error(err), zap.String("token_account", records.General.TokenAccount)).Warn("failure marking primary account for deposit sync")
			}
		}
	}

	// Fetch balances
	balanceMetadataByTokenAccount, err := s.fetchBalances(ctx, owner, filteredRecords)
	if err != nil {
		log.With(zap.Error(err)).Warn("failure getting balances")
		return nil, status.Error(codes.Internal, "")
	}

	// Construct token account info
	tokenAccountInfos := make(map[string]*accountpb.TokenAccountInfo)
	for _, records := range filteredRecords {
		log := log.With(zap.String("token_account", records.General.TokenAccount))

		proto, err := s.getProtoAccountInfo(ctx, records, balanceMetadataByTokenAccount[records.General.TokenAccount])
		if err != nil {
			log.With(zap.Error(err)).Warn("failure getting proto account info")
			return nil, status.Error(codes.Internal, "")
		}

		tokenAccountInfos[records.General.TokenAccount] = proto
	}

	if len(tokenAccountInfos) == 0 {
		return &accountpb.GetTokenAccountInfosResponse{
			Result: accountpb.GetTokenAccountInfosResponse_NOT_FOUND,
		}, nil
	}

	resp := &accountpb.GetTokenAccountInfosResponse{
		Result:            accountpb.GetTokenAccountInfosResponse_OK,
		TokenAccountInfos: tokenAccountInfos,
	}

	// Is this a gift card in a terminal state that we can cache?
	if len(tokenAccountInfos) == 1 && filteredRecords[0].General.AccountType == commonpb.AccountType_REMOTE_SEND_GIFT_CARD {
		tokenAccountInfo := tokenAccountInfos[filteredRecords[0].General.TokenAccount]

		switch tokenAccountInfo.ClaimState {
		case accountpb.TokenAccountInfo_CLAIM_STATE_CLAIMED, accountpb.TokenAccountInfo_CLAIM_STATE_EXPIRED:
			giftCardCacheByOwner.Insert(owner.PublicKey().ToBase58(), resp, 1)
		}
	}

	resp, err = s.addRequestingOwnerMetadata(ctx, resp, requestingOwner)
	if err != nil {
		log.With(zap.Error(err)).Warn("failure adding requesting owner metadata")
		return nil, status.Error(codes.Internal, "")
	}
	return resp, nil
}

// fetchBalances optimally routes and batches balance calculations for a set of
// account records.
func (s *server) fetchBalances(ctx context.Context, owner *common.Account, allAccountRecords []*common.AccountRecords) (map[string]*balanceMetadata, error) {
	balanceMetadataByTokenAccount := make(map[string]*balanceMetadata)

	// Fetch cached balances
	var mangedByCodeRecords []*common.AccountRecords
	for _, accountRecords := range allAccountRecords {
		if accountRecords.IsManagedByCode(ctx) {
			mangedByCodeRecords = append(mangedByCodeRecords, accountRecords)
		} else {
			// Don't calculate a balance for now, since the caching strategy
			// is not possible.
			balanceMetadataByTokenAccount[accountRecords.General.TokenAccount] = &balanceMetadata{
				realValue: 0,
				source:    accountpb.TokenAccountInfo_BALANCE_SOURCE_UNKNOWN,
			}
		}
	}
	balancesByTokenAccount, err := balance.BatchCalculateFromCacheWithAccountRecords(ctx, s.data, mangedByCodeRecords...)
	if err != nil {
		return nil, err
	}
	for tokenAccount, quarks := range balancesByTokenAccount {
		balanceMetadataByTokenAccount[tokenAccount] = &balanceMetadata{
			realValue: quarks,
			source:    accountpb.TokenAccountInfo_BALANCE_SOURCE_CACHE,
		}
	}

	// Fetch and add any pending balances from swaps
	pendingSwapBalances, err := balance.GetDeltaQuarksFromPendingSwaps(ctx, s.data, owner.PublicKey().ToBase58())
	if err != nil {
		return nil, err
	}
	for _, accountRecords := range mangedByCodeRecords {
		if accountRecords.General.AccountType != commonpb.AccountType_PRIMARY {
			continue
		}

		pendingSwapBalance, ok := pendingSwapBalances[accountRecords.General.MintAccount]
		if !ok {
			continue
		}

		balanceMetadataByTokenAccount[accountRecords.General.TokenAccount].additionalPendingSwapValue = pendingSwapBalance
	}

	// Any accounts that aren't Timelock can be deferred to the blockchain
	//
	// todo: support batching
	for _, accountRecords := range allAccountRecords {
		if accountRecords.IsTimelock() {
			continue
		}

		tokenAccount, err := common.NewAccountFromPublicKeyString(accountRecords.General.TokenAccount)
		if err != nil {
			return nil, err
		}

		quarks, balanceSource, err := balance.CalculateFromBlockchain(ctx, s.data, tokenAccount)
		if err != nil {
			return nil, err
		}
		var protoBalanceSource accountpb.TokenAccountInfo_BalanceSource
		switch balanceSource {
		case balance.BlockchainSource:
			protoBalanceSource = accountpb.TokenAccountInfo_BALANCE_SOURCE_BLOCKCHAIN
		case balance.CacheSource:
			protoBalanceSource = accountpb.TokenAccountInfo_BALANCE_SOURCE_CACHE
		default:
			protoBalanceSource = accountpb.TokenAccountInfo_BALANCE_SOURCE_UNKNOWN
		}
		balanceMetadataByTokenAccount[tokenAccount.PublicKey().ToBase58()] = &balanceMetadata{
			realValue: quarks,
			source:    protoBalanceSource,
		}
	}

	return balanceMetadataByTokenAccount, nil
}

func (s *server) getProtoAccountInfo(ctx context.Context, records *common.AccountRecords, prefetchedBalanceMetadata *balanceMetadata) (*accountpb.TokenAccountInfo, error) {
	ownerAccount, err := common.NewAccountFromPublicKeyString(records.General.OwnerAccount)
	if err != nil {
		return nil, err
	}

	authorityAccount, err := common.NewAccountFromPublicKeyString(records.General.AuthorityAccount)
	if err != nil {
		return nil, err
	}

	tokenAccount, err := common.NewAccountFromPublicKeyString(records.General.TokenAccount)
	if err != nil {
		return nil, err
	}

	mintAccount, err := common.NewAccountFromPublicKeyString(records.General.MintAccount)
	if err != nil {
		return nil, err
	}

	// todo: We don't yet handle the closing state
	var managementState accountpb.TokenAccountInfo_ManagementState
	if !records.IsTimelock() {
		managementState = accountpb.TokenAccountInfo_MANAGEMENT_STATE_NONE
	} else {
		switch records.Timelock.VaultState {
		case timelock_token_v1.StateUnknown:
			managementState = accountpb.TokenAccountInfo_MANAGEMENT_STATE_UNKNOWN
			if records.Timelock.Block == 0 {
				// We haven't observed the timelock account at all on the blockchain,
				// but we know it's guaranteed to be created by the intents system
				// in the locked state.
				managementState = accountpb.TokenAccountInfo_MANAGEMENT_STATE_LOCKED
			}
		case timelock_token_v1.StateUnlocked:
			managementState = accountpb.TokenAccountInfo_MANAGEMENT_STATE_UNLOCKED
		case timelock_token_v1.StateWaitingForTimeout:
			managementState = accountpb.TokenAccountInfo_MANAGEMENT_STATE_UNLOCKING
		case timelock_token_v1.StateLocked:
			managementState = accountpb.TokenAccountInfo_MANAGEMENT_STATE_LOCKED
		case timelock_token_v1.StateClosed:
			managementState = accountpb.TokenAccountInfo_MANAGEMENT_STATE_CLOSED
		default:
			managementState = accountpb.TokenAccountInfo_MANAGEMENT_STATE_UNKNOWN
		}
	}

	blockchainState := accountpb.TokenAccountInfo_BLOCKCHAIN_STATE_DOES_NOT_EXIST
	if records.IsTimelock() && records.Timelock.ExistsOnBlockchain() {
		blockchainState = accountpb.TokenAccountInfo_BLOCKCHAIN_STATE_EXISTS
	}
	if !records.IsTimelock() {
		blockchainState = accountpb.TokenAccountInfo_BLOCKCHAIN_STATE_UNKNOWN
	}

	// Claimed states only apply to gift card accounts
	var claimState accountpb.TokenAccountInfo_ClaimState
	if records.General.AccountType == commonpb.AccountType_REMOTE_SEND_GIFT_CARD {
		// Check for an explicit action to claim the gift card
		_, err = s.data.GetGiftCardClaimedAction(ctx, records.General.TokenAccount)
		switch err {
		case nil:
			claimState = accountpb.TokenAccountInfo_CLAIM_STATE_CLAIMED
		case action.ErrActionNotFound:
			if common.IsManagedByCode(ctx, records.Timelock) {
				claimState = accountpb.TokenAccountInfo_CLAIM_STATE_NOT_CLAIMED
			}
		default:
			return nil, err
		}

		// Otherwise, check whether it looks like the gift card was claimed.
		if prefetchedBalanceMetadata.source == accountpb.TokenAccountInfo_BALANCE_SOURCE_CACHE && prefetchedBalanceMetadata.realValue == 0 {
			claimState = accountpb.TokenAccountInfo_CLAIM_STATE_CLAIMED
		} else if records.Timelock.IsClosed() {
			claimState = accountpb.TokenAccountInfo_CLAIM_STATE_CLAIMED
		}

		// Finally, check the status of the auto-return action. This will correct
		// any false positive claim states not generated from an explicit action.
		autoReturnActionRecord, err := s.data.GetGiftCardAutoReturnAction(ctx, records.General.TokenAccount)
		switch err {
		case nil:
			if autoReturnActionRecord.State != action.StateUnknown {
				claimState = accountpb.TokenAccountInfo_CLAIM_STATE_CLAIMED
			}
		case action.ErrActionNotFound:
		default:
			return nil, err
		}

		// Gift cards that are close to the auto-return window are marked as expired in
		// a consistent manner as SubmitIntent to avoid race conditions with the auto-return.
		if time.Since(records.General.CreatedAt) >= account_worker.GiftCardExpiry-time.Minute {
			claimState = accountpb.TokenAccountInfo_CLAIM_STATE_EXPIRED
		}

		// If the gift card account is claimed or expired, force the balance to zero.
		if claimState == accountpb.TokenAccountInfo_CLAIM_STATE_CLAIMED || claimState == accountpb.TokenAccountInfo_CLAIM_STATE_EXPIRED {
			prefetchedBalanceMetadata.source = accountpb.TokenAccountInfo_BALANCE_SOURCE_CACHE
			prefetchedBalanceMetadata.realValue = 0
		}
	}

	var originalExchangeData *transactionpb.ExchangeData
	if records.General.AccountType == commonpb.AccountType_REMOTE_SEND_GIFT_CARD {
		originalExchangeData, err = s.getOriginalGiftCardExchangeData(ctx, records)
		if err != nil {
			return nil, err
		}
	}

	estimatedTotalBalance := prefetchedBalanceMetadata.realValue
	if prefetchedBalanceMetadata.additionalPendingSwapValue != nil {
		estimatedTotalBalance += prefetchedBalanceMetadata.additionalPendingSwapValue.DeltaQuarks
	}

	// todo: USD cost basis requires tests
	var usdCostBasis float64
	if common.IsCoreMint(mintAccount) && common.IsCoreMintUsdStableCoin() {
		usdCostBasis = float64(estimatedTotalBalance) / float64(common.CoreMintQuarksPerUnit)
	} else {
		switch records.General.AccountType {
		case commonpb.AccountType_PRIMARY:
			// todo: Assumes the structure that each user has exactly one primary account per mint
			usdCostBasis, err = s.data.GetUsdCostBasis(ctx, ownerAccount.PublicKey().ToBase58(), mintAccount.PublicKey().ToBase58())
			if err != nil {
				return nil, err
			}

			if prefetchedBalanceMetadata.additionalPendingSwapValue != nil {
				usdCostBasis += prefetchedBalanceMetadata.additionalPendingSwapValue.GetUsdMarketValue()
			}
		default:
			usdCostBasis = 0 // Account type not supported
		}
	}

	return &accountpb.TokenAccountInfo{
		Address:              tokenAccount.ToProto(),
		Owner:                ownerAccount.ToProto(),
		Authority:            authorityAccount.ToProto(),
		AccountType:          records.General.AccountType,
		Index:                records.General.Index,
		BalanceSource:        prefetchedBalanceMetadata.source,
		Balance:              estimatedTotalBalance,
		UsdCostBasis:         usdCostBasis,
		ManagementState:      managementState,
		BlockchainState:      blockchainState,
		ClaimState:           claimState,
		OriginalExchangeData: originalExchangeData,
		Mint:                 mintAccount.ToProto(),
		CreatedAt:            timestamppb.New(records.General.CreatedAt),
	}, nil
}

func (s *server) getOriginalGiftCardExchangeData(ctx context.Context, records *common.AccountRecords) (*transactionpb.ExchangeData, error) {
	if records.General.AccountType != commonpb.AccountType_REMOTE_SEND_GIFT_CARD {
		return nil, errors.New("invalid account type")
	}

	intentRecord, err := s.data.GetOriginalGiftCardIssuedIntent(ctx, records.General.TokenAccount)
	if err != nil {
		return nil, err
	}

	mintAccount, err := common.NewAccountFromPublicKeyString(intentRecord.MintAccount)
	if err != nil {
		return nil, err
	}

	return &transactionpb.ExchangeData{
		Currency:     string(intentRecord.SendPublicPaymentMetadata.ExchangeCurrency),
		ExchangeRate: intentRecord.SendPublicPaymentMetadata.ExchangeRate,
		NativeAmount: intentRecord.SendPublicPaymentMetadata.NativeAmount,
		Quarks:       intentRecord.SendPublicPaymentMetadata.Quantity,
		Mint:         mintAccount.ToProto(),
	}, nil
}

func (s *server) addRequestingOwnerMetadata(ctx context.Context, resp *accountpb.GetTokenAccountInfosResponse, requestingOwner *common.Account) (*accountpb.GetTokenAccountInfosResponse, error) {
	if requestingOwner == nil {
		return resp, nil
	}

	cloned := proto.Clone(resp).(*accountpb.GetTokenAccountInfosResponse)

	for _, ai := range cloned.TokenAccountInfos {
		switch ai.AccountType {
		case commonpb.AccountType_REMOTE_SEND_GIFT_CARD:
			giftCardVaultAccount, err := common.NewAccountFromProto(ai.Address)
			if err != nil {
				return nil, err
			}

			intentRecord, err := s.data.GetOriginalGiftCardIssuedIntent(ctx, giftCardVaultAccount.PublicKey().ToBase58())
			if err != nil {
				return nil, err
			}

			if intentRecord.InitiatorOwnerAccount == requestingOwner.PublicKey().ToBase58() {
				ai.IsGiftCardIssuer = true
			}
		}
	}

	return cloned, nil
}

func (s *server) updateCachedResponse(resp *accountpb.GetTokenAccountInfosResponse) {
	for _, ai := range resp.TokenAccountInfos {
		switch ai.AccountType {
		case commonpb.AccountType_REMOTE_SEND_GIFT_CARD:
			// Transition any gift card records to expired if we elapsed the expiry window
			if time.Since(ai.CreatedAt.AsTime()) >= account_worker.GiftCardExpiry-time.Minute {
				ai.ClaimState = accountpb.TokenAccountInfo_CLAIM_STATE_EXPIRED
				ai.BalanceSource = accountpb.TokenAccountInfo_BALANCE_SOURCE_CACHE
				ai.Balance = 0
			}
		}
	}
}
