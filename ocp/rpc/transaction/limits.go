package transaction

import (
	"context"
	"errors"
	"fmt"
	"math"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	transactionpb "github.com/code-payments/ocp-protobuf-api/generated/go/transaction/v1"

	currency_lib "github.com/code-payments/ocp-server/currency"
	"github.com/code-payments/ocp-server/grpc/client"
	"github.com/code-payments/ocp-server/ocp/common"
	currency_util "github.com/code-payments/ocp-server/ocp/currency"
	"github.com/code-payments/ocp-server/ocp/data/intent"
	"github.com/code-payments/ocp-server/ocp/rpc"
)

func (s *transactionServer) GetLimits(ctx context.Context, req *transactionpb.GetLimitsRequest) (*transactionpb.GetLimitsResponse, error) {
	log := s.log.With(zap.String("method", "GetLimits"))
	log = client.InjectLoggingMetadata(ctx, log, rpc.UserAgentName)

	ownerAccount, err := common.NewAccountFromProto(req.Owner)
	if err != nil {
		log.With(zap.Error(err)).Warn("invalid owner account")
		return nil, status.Error(codes.Internal, "")
	}
	log = log.With(zap.String("owner_account", ownerAccount.PublicKey().ToBase58()))

	sig := req.Signature
	req.Signature = nil
	if err := s.auth.Authenticate(ctx, ownerAccount, req, sig); err != nil {
		return nil, err
	}

	liveExchangeData, err := s.mintDataProvider.GetLiveExchangeRates(ctx)
	if err != nil {
		log.With(zap.Error(err)).Warn("failure getting current exchange rates")
		return nil, status.Error(codes.Internal, "")
	}

	usdRate, ok := liveExchangeData.Rates[string(currency_lib.USD)]
	if !ok {
		log.With(zap.Error(err)).Warn("usd rate is missing")
		return nil, status.Error(codes.Internal, "")
	}

	_, consumedUsdForPayments, err := s.data.GetTransactedAmountForAntiMoneyLaundering(ctx, ownerAccount.PublicKey().ToBase58(), req.ConsumedSince.AsTime())
	if err != nil {
		log.With(zap.Error(err)).Warn("failure calculating consumed usd payment value")
		return nil, status.Error(codes.Internal, "")
	}

	// Calculate send limits
	usdLeftForPayments := currency_util.MaxDailyUsdLimit - consumedUsdForPayments
	if usdLeftForPayments < 0 {
		usdLeftForPayments = 0
	}
	sendLimits := make(map[string]*transactionpb.SendLimit)
	for currency, sendLimit := range currency_util.SendLimits {
		otherRate, ok := liveExchangeData.Rates[string(currency)]
		if !ok {
			log.Debug(fmt.Sprintf("%s rate is missing", currency))
			continue
		}

		// How much do we have left for payments in the other currency?
		amountLeftInOtherCurrency := usdLeftForPayments * otherRate / usdRate

		// Limit to the localized max per-transaction amount
		remainingNextTransaction := math.Min(sendLimit.PerTransaction, amountLeftInOtherCurrency)

		sendLimits[string(currency)] = &transactionpb.SendLimit{
			NextTransaction:   float32(remainingNextTransaction),
			MaxPerTransaction: float32(sendLimit.PerTransaction),
			MaxPerDay:         float32(currency_util.MaxDailyUsdLimit * otherRate / usdRate),
		}
	}

	return &transactionpb.GetLimitsResponse{
		Result:               transactionpb.GetLimitsResponse_OK,
		SendLimitsByCurrency: sendLimits,
		UsdTransacted:        consumedUsdForPayments,
	}, nil
}

// validateIntentQuarks bounds every client amount on a new intent to the most
// its mint allows. Totals are checked while summing, so they can't overflow.
func validateIntentQuarks(mint *common.Account, intentRecord *intent.Record) error {
	var quantity uint64
	var parts []uint64
	switch intentRecord.IntentType {
	case intent.OpenAccounts:
		return nil
	case intent.SendPublicPayment:
		quantity = intentRecord.SendPublicPaymentMetadata.Quantity
	case intent.ReceivePaymentsPublicly:
		quantity = intentRecord.ReceivePaymentsPubliclyMetadata.Quantity
	case intent.PublicDistribution:
		quantity = intentRecord.PublicDistributionMetadata.Quantity
		for _, distribution := range intentRecord.PublicDistributionMetadata.Distributions {
			parts = append(parts, distribution.Quantity)
		}
	case intent.SendPublicMultiPayment:
		quantity = intentRecord.SendPublicMultiPaymentMetadata.Quantity
		for _, payment := range intentRecord.SendPublicMultiPaymentMetadata.Payments {
			parts = append(parts, payment.Quantity)
		}
	default:
		return errors.New("unsupported intent type")
	}

	maxQuarks := currency_util.GetMaxQuarks(mint)

	var total uint64
	for _, part := range parts {
		if part > maxQuarks || total+part > maxQuarks {
			return NewIntentValidationErrorf("quantity exceeds max of %d quarks", maxQuarks)
		}
		total += part
	}
	if quantity > maxQuarks {
		return NewIntentValidationErrorf("quantity exceeds max of %d quarks", maxQuarks)
	}
	return nil
}
