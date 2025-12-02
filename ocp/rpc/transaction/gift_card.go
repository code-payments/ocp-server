package transaction

import (
	"context"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	commonpb "github.com/code-payments/ocp-protobuf-api/generated/go/common/v1"
	transactionpb "github.com/code-payments/ocp-protobuf-api/generated/go/transaction/v1"

	"github.com/code-payments/ocp-server/grpc/client"
	"github.com/code-payments/ocp-server/ocp/balance"
	"github.com/code-payments/ocp-server/ocp/common"
	"github.com/code-payments/ocp-server/ocp/data/account"
	"github.com/code-payments/ocp-server/ocp/data/action"
	account_worker "github.com/code-payments/ocp-server/ocp/worker/account"
)

func (s *transactionServer) VoidGiftCard(ctx context.Context, req *transactionpb.VoidGiftCardRequest) (*transactionpb.VoidGiftCardResponse, error) {
	log := s.log.With(zap.String("method", "VoidGiftCard"))
	log = client.InjectLoggingMetadata(ctx, log)

	owner, err := common.NewAccountFromProto(req.Owner)
	if err != nil {
		log.With(zap.Error(err)).Warn("invalid owner account")
		return nil, status.Error(codes.Internal, "")
	}
	log = log.With(zap.String("owner_account", owner.PublicKey().ToBase58()))

	giftCardVault, err := common.NewAccountFromProto(req.GiftCardVault)
	if err != nil {
		log.With(zap.Error(err)).Warn("invalid owner account")
		return nil, status.Error(codes.Internal, "")
	}
	log = log.With(zap.String("gift_card_vault_account", giftCardVault.PublicKey().ToBase58()))

	signature := req.Signature
	req.Signature = nil
	if err := s.auth.Authenticate(ctx, owner, req, signature); err != nil {
		return nil, err
	}

	accountInfoRecord, err := s.data.GetAccountInfoByTokenAddress(ctx, giftCardVault.PublicKey().ToBase58())
	switch err {
	case nil:
		if accountInfoRecord.AccountType != commonpb.AccountType_REMOTE_SEND_GIFT_CARD {
			return &transactionpb.VoidGiftCardResponse{
				Result: transactionpb.VoidGiftCardResponse_NOT_FOUND,
			}, nil
		}
	case account.ErrAccountInfoNotFound:
		return &transactionpb.VoidGiftCardResponse{
			Result: transactionpb.VoidGiftCardResponse_NOT_FOUND,
		}, nil
	default:
		log.With(zap.Error(err)).Warn("failure getting gift card account info")
		return nil, status.Error(codes.Internal, "")
	}

	giftCardIssuedIntentRecord, err := s.data.GetOriginalGiftCardIssuedIntent(ctx, giftCardVault.PublicKey().ToBase58())
	if err != nil {
		log.With(zap.Error(err)).Warn("failure getting gift card issued intent record")
		return nil, status.Error(codes.Internal, "")
	} else if giftCardIssuedIntentRecord.InitiatorOwnerAccount != owner.PublicKey().ToBase58() {
		return &transactionpb.VoidGiftCardResponse{
			Result: transactionpb.VoidGiftCardResponse_DENIED,
		}, nil
	}

	if time.Since(accountInfoRecord.CreatedAt) >= account_worker.GiftCardExpiry {
		return &transactionpb.VoidGiftCardResponse{
			Result: transactionpb.VoidGiftCardResponse_OK,
		}, nil
	}

	globalBalanceLock, err := balance.GetOptimisticVersionLock(ctx, s.data, giftCardVault)
	if err != nil {
		log.With(zap.Error(err)).Warn("failure getting balance lock")
		return nil, status.Error(codes.Internal, "")
	}

	localAccountLock := s.getLocalAccountLock(giftCardVault)
	localAccountLock.Lock()
	defer localAccountLock.Unlock()

	claimedActionRecord, err := s.data.GetGiftCardClaimedAction(ctx, giftCardVault.PublicKey().ToBase58())
	if err == nil {
		mintAccount, err := common.NewAccountFromPublicKeyString(accountInfoRecord.MintAccount)
		if err != nil {
			log.With(zap.Error(err)).Warn("invalid mint account")
			return nil, status.Error(codes.Internal, "")
		}

		vmConfig, err := common.GetVmConfigForMint(ctx, s.data, mintAccount)
		if err != nil {
			log.With(zap.Error(err)).Warn("failure getting vm config")
			return nil, status.Error(codes.Internal, "")
		}

		ownerTimelockAccounts, err := owner.GetTimelockAccounts(vmConfig)
		if err != nil {
			log.With(zap.Error(err)).Warn("failure getting owner timelock accounts")
			return nil, status.Error(codes.Internal, "")
		}

		if *claimedActionRecord.Destination != ownerTimelockAccounts.Vault.PublicKey().ToBase58() {
			return &transactionpb.VoidGiftCardResponse{
				Result: transactionpb.VoidGiftCardResponse_CLAIMED_BY_OTHER_USER,
			}, nil
		}
		return &transactionpb.VoidGiftCardResponse{
			Result: transactionpb.VoidGiftCardResponse_OK,
		}, nil
	} else if err != action.ErrActionNotFound {
		log.With(zap.Error(err)).Warn("failure getting gift card claimed action")
		return nil, status.Error(codes.Internal, "")
	}

	err = account_worker.InitiateProcessToAutoReturnGiftCard(ctx, s.data, giftCardVault, true, globalBalanceLock)
	if err != nil {
		log.With(zap.Error(err)).Warn("failure scheduling auto-return action")
		return nil, status.Error(codes.Internal, "")
	}

	// It's ok if this fails, the auto-return worker will just process this account
	// idempotently at a later time
	account_worker.MarkAutoReturnCheckComplete(ctx, s.data, accountInfoRecord)

	return &transactionpb.VoidGiftCardResponse{
		Result: transactionpb.VoidGiftCardResponse_OK,
	}, nil
}
