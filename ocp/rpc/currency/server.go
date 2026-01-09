package currency

import (
	"context"
	"fmt"
	"strings"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	currencypb "github.com/code-payments/ocp-protobuf-api/generated/go/currency/v1"
	"github.com/pkg/errors"

	"github.com/code-payments/ocp-server/cache"
	currency_lib "github.com/code-payments/ocp-server/currency"
	"github.com/code-payments/ocp-server/database/query"
	"github.com/code-payments/ocp-server/grpc/client"
	"github.com/code-payments/ocp-server/ocp/common"
	"github.com/code-payments/ocp-server/ocp/config"
	currency_util "github.com/code-payments/ocp-server/ocp/currency"
	ocp_data "github.com/code-payments/ocp-server/ocp/data"
	"github.com/code-payments/ocp-server/ocp/data/currency"
	"github.com/code-payments/ocp-server/solana/currencycreator"
	timelock_token "github.com/code-payments/ocp-server/solana/timelock/v1"
)

type currencyServer struct {
	log  *zap.Logger
	data ocp_data.Provider

	exchangeRateHistoryCache cache.Cache
	reserveHistoryCache      cache.Cache

	currencypb.UnimplementedCurrencyServer
}

func NewCurrencyServer(
	log *zap.Logger,
	data ocp_data.Provider,
) currencypb.CurrencyServer {
	return &currencyServer{
		log:  log,
		data: data,

		exchangeRateHistoryCache: cache.NewCache(1_000),
		reserveHistoryCache:      cache.NewCache(1_000),
	}
}

func (s *currencyServer) GetAllRates(ctx context.Context, req *currencypb.GetAllRatesRequest) (resp *currencypb.GetAllRatesResponse, err error) {
	log := s.log.With(zap.String("method", "GetAllRates"))
	log = client.InjectLoggingMetadata(ctx, log)

	var record *currency.MultiRateRecord
	if req.Timestamp != nil && req.Timestamp.AsTime().Before(time.Now().Add(-15*time.Minute)) {
		record, err = s.loadExchangeRatesForTime(ctx, req.Timestamp.AsTime())
	} else if req.Timestamp == nil || req.Timestamp.AsTime().Sub(time.Now()) < time.Hour {
		record, err = s.loadExchangeRatesLatest(ctx)
	} else {
		return nil, status.Error(codes.InvalidArgument, "timestamp too far in the future")
	}

	if err != nil {
		log.With(zap.Error(err)).Warn("failed to load latest rate")
		return nil, status.Error(codes.Internal, err.Error())
	}

	protoTime := timestamppb.New(record.Time)
	return &currencypb.GetAllRatesResponse{
		AsOf:  protoTime,
		Rates: record.Rates,
	}, nil
}

func (s *currencyServer) GetMints(ctx context.Context, req *currencypb.GetMintsRequest) (*currencypb.GetMintsResponse, error) {
	log := s.log.With(zap.String("method", "GetMints"))
	log = client.InjectLoggingMetadata(ctx, log)

	resp := &currencypb.GetMintsResponse{
		MetadataByAddress: make(map[string]*currencypb.Mint),
	}

	for _, protoMintAddress := range req.Addresses {
		mintAccount, err := common.NewAccountFromProto(protoMintAddress)
		if err != nil {
			log.With(zap.Error(err)).Warn("invalid mint address")
			return nil, status.Error(codes.Internal, "")
		}

		log := log.With(zap.String("mint", mintAccount.PublicKey().ToBase58()))

		var protoMetadata *currencypb.Mint
		switch mintAccount.PublicKey().ToBase58() {
		case common.CoreMintAccount.PublicKey().ToBase58():
			vmConfig, err := common.GetVmConfigForMint(ctx, s.data, common.CoreMintAccount)
			if err != nil {
				log.With(zap.Error(err)).Warn("failure getting vm config")
				return nil, status.Error(codes.Internal, "")
			}

			protoMetadata = &currencypb.Mint{
				Address:     protoMintAddress,
				Decimals:    uint32(common.CoreMintDecimals),
				Name:        common.CoreMintName,
				Symbol:      strings.ToUpper(string(common.CoreMintSymbol)),
				Description: config.CoreMintDescription,
				ImageUrl:    config.CoreMintImageUrl,
				VmMetadata: &currencypb.VmMetadata{
					Vm:                 vmConfig.Vm.ToProto(),
					Omnibus:            vmConfig.Omnibus.ToProto(),
					Authority:          vmConfig.Authority.ToProto(),
					LockDurationInDays: uint32(timelock_token.DefaultNumDaysLocked),
				},
				CreatedAt: timestamppb.New(time.Time{}),
			}
		default:
			metadataRecord, err := s.data.GetCurrencyMetadata(ctx, mintAccount.PublicKey().ToBase58())
			if err == currency.ErrNotFound {
				return &currencypb.GetMintsResponse{Result: currencypb.GetMintsResponse_NOT_FOUND}, nil
			} else if err != nil {
				log.With(zap.Error(err)).Warn("failed to load currency metadata record")
				return nil, status.Error(codes.Internal, "")
			}

			reserveRecord, err := s.data.GetCurrencyReserveAtTime(ctx, mintAccount.PublicKey().ToBase58(), currency_util.GetLatestExchangeRateTime())
			if err != nil {
				log.With(zap.Error(err)).Warn("failed to load currency reserve record")
				return nil, status.Error(codes.Internal, "")
			}

			vmConfig, err := common.GetVmConfigForMint(ctx, s.data, mintAccount)
			if err != nil {
				log.With(zap.Error(err)).Warn("failure getting vm config")
				return nil, status.Error(codes.Internal, "")
			}

			seed, err := common.NewAccountFromPublicKeyString(metadataRecord.Seed)
			if err != nil {
				log.With(zap.Error(err)).Warn("invalid seed")
				return nil, status.Error(codes.Internal, "")
			}
			currencyAuthorityAccount, err := common.NewAccountFromPublicKeyString(metadataRecord.Authority)
			if err != nil {
				log.With(zap.Error(err)).Warn("invalid currency authority account")
				return nil, status.Error(codes.Internal, "")
			}
			currencyConfigAccount, err := common.NewAccountFromPublicKeyString(metadataRecord.CurrencyConfig)
			if err != nil {
				log.With(zap.Error(err)).Warn("invalid currency config account")
				return nil, status.Error(codes.Internal, "")
			}
			liquidityPoolAccount, err := common.NewAccountFromPublicKeyString(metadataRecord.LiquidityPool)
			if err != nil {
				log.With(zap.Error(err)).Warn("invalid liquidity pool account")
				return nil, status.Error(codes.Internal, "")
			}
			mintVaultAccount, err := common.NewAccountFromPublicKeyString(metadataRecord.VaultMint)
			if err != nil {
				log.With(zap.Error(err)).Warn("invalid mint vault account")
				return nil, status.Error(codes.Internal, "")
			}
			coreMintVaultAccount, err := common.NewAccountFromPublicKeyString(metadataRecord.VaultCore)
			if err != nil {
				log.With(zap.Error(err)).Warn("invalid core mint vault account")
				return nil, status.Error(codes.Internal, "")
			}

			protoMetadata = &currencypb.Mint{
				Address:     protoMintAddress,
				Decimals:    uint32(metadataRecord.Decimals),
				Name:        metadataRecord.Name,
				Symbol:      metadataRecord.Symbol,
				Description: metadataRecord.Description,
				ImageUrl:    metadataRecord.ImageUrl,
				VmMetadata: &currencypb.VmMetadata{
					Vm:                 vmConfig.Vm.ToProto(),
					Omnibus:            vmConfig.Omnibus.ToProto(),
					Authority:          vmConfig.Authority.ToProto(),
					LockDurationInDays: uint32(timelock_token.DefaultNumDaysLocked),
				},
				LaunchpadMetadata: &currencypb.LaunchpadMetadata{
					CurrencyConfig:    currencyConfigAccount.ToProto(),
					LiquidityPool:     liquidityPoolAccount.ToProto(),
					Seed:              seed.ToProto(),
					Authority:         currencyAuthorityAccount.ToProto(),
					MintVault:         mintVaultAccount.ToProto(),
					CoreMintVault:     coreMintVaultAccount.ToProto(),
					SupplyFromBonding: reserveRecord.SupplyFromBonding,
					SellFeeBps:        uint32(metadataRecord.SellFeeBps),
				},
				CreatedAt: timestamppb.New(metadataRecord.CreatedAt),
			}
		}

		resp.MetadataByAddress[mintAccount.PublicKey().ToBase58()] = protoMetadata
	}
	return resp, nil
}

func (s *currencyServer) GetHistoricalMintData(ctx context.Context, req *currencypb.GetHistoricalMintDataRequest) (*currencypb.GetHistoricalMintDataResponse, error) {
	log := s.log.With(zap.String("method", "GetHistoricalMintData"))
	log = client.InjectLoggingMetadata(ctx, log)

	mintAccount, err := common.NewAccountFromProto(req.Address)
	if err != nil {
		log.With(zap.Error(err)).Warn("invalid mint address")
		return nil, status.Error(codes.Internal, "")
	}

	log = log.With(
		zap.String("mint", mintAccount.PublicKey().ToBase58()),
		zap.String("range", req.GetPredefinedRange().String()),
	)

	// Only support currency creator mints, not the core mint
	if common.IsCoreMint(mintAccount) {
		return &currencypb.GetHistoricalMintDataResponse{
			Result: currencypb.GetHistoricalMintDataResponse_NOT_FOUND,
		}, nil
	}

	// Get currency code
	currencyCode := currency_lib.Code(strings.ToLower(req.CurrencyCode))
	if currencyCode == "" {
		return &currencypb.GetHistoricalMintDataResponse{
			Result: currencypb.GetHistoricalMintDataResponse_NOT_FOUND,
		}, nil
	}

	// Verify the mint exists as a currency creator mint
	_, err = s.data.GetCurrencyMetadata(ctx, mintAccount.PublicKey().ToBase58())
	if err == currency.ErrNotFound {
		return &currencypb.GetHistoricalMintDataResponse{
			Result: currencypb.GetHistoricalMintDataResponse_NOT_FOUND,
		}, nil
	} else if err != nil {
		log.With(zap.Error(err)).Warn("failed to load currency metadata")
		return nil, status.Error(codes.Internal, "")
	}

	// Determine the time range and interval based on the predefined range.
	// endTime is GetLatestExchangeRateTime() which is used in cache keys to
	// invalidate entries when new market data is generated (every 15 minutes).
	startTime, endTime, interval := getTimeRangeForPredefinedRange(req.GetPredefinedRange())

	// Get reserve history (cached by mint + range)
	reserveHistory, err := s.getCachedReserveHistory(
		ctx,
		mintAccount.PublicKey().ToBase58(),
		req.GetPredefinedRange(),
		startTime,
		endTime,
		interval,
	)
	if err != nil {
		log.With(zap.Error(err)).Warn("failed to load currency reserve history")
		return nil, status.Error(codes.Internal, "")
	}

	if len(reserveHistory) == 0 {
		return &currencypb.GetHistoricalMintDataResponse{
			Result: currencypb.GetHistoricalMintDataResponse_MISSING_DATA,
		}, nil
	}

	// Get exchange rate history (cached by currency code + range)
	exchangeRateHistory, err := s.getCachedExchangeRateHistory(
		ctx,
		currencyCode,
		req.GetPredefinedRange(),
		startTime,
		endTime,
		interval,
	)
	if err != nil {
		log.With(zap.Error(err)).Warn("failed to load exchange rate history")
		return nil, status.Error(codes.Internal, "")
	}

	if len(exchangeRateHistory) == 0 {
		return &currencypb.GetHistoricalMintDataResponse{
			Result: currencypb.GetHistoricalMintDataResponse_MISSING_DATA,
		}, nil
	}

	// Build historical data points with market cap
	data := make([]*currencypb.HistoricalMintData, 0, len(reserveHistory))
	for _, reserve := range reserveHistory {
		// Find the closest exchange rate for this time point
		exchangeRate, ok := findClosestExchangeRate(reserve.Time, exchangeRateHistory)
		if !ok {
			continue
		}

		data = append(data, &currencypb.HistoricalMintData{
			Timestamp: timestamppb.New(reserve.Time),
			MarketCap: calculateMarketCap(reserve.SupplyFromBonding, exchangeRate),
		})
	}

	if len(data) == 0 {
		return &currencypb.GetHistoricalMintDataResponse{
			Result: currencypb.GetHistoricalMintDataResponse_MISSING_DATA,
		}, nil
	}

	return &currencypb.GetHistoricalMintDataResponse{
		Result: currencypb.GetHistoricalMintDataResponse_OK,
		Data:   data,
	}, nil
}

func (s *currencyServer) loadExchangeRatesForTime(ctx context.Context, t time.Time) (*currency.MultiRateRecord, error) {
	record, err := s.data.GetAllExchangeRates(ctx, t)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get price record by date")
	}
	return record, nil
}

func (s *currencyServer) loadExchangeRatesLatest(ctx context.Context) (*currency.MultiRateRecord, error) {
	latest, err := s.data.GetAllExchangeRates(ctx, currency_util.GetLatestExchangeRateTime())
	if err != nil {
		return nil, errors.Wrap(err, "failed to get latest price record")
	}
	return latest, nil
}

func (s *currencyServer) getCachedReserveHistory(
	ctx context.Context,
	mint string,
	predefinedRange currencypb.GetHistoricalMintDataRequest_PredefinedRange,
	startTime, endTime time.Time,
	interval query.Interval,
) ([]*currency.ReserveRecord, error) {
	cacheKey := fmt.Sprintf("%s:%s:%d", mint, predefinedRange.String(), endTime.Unix())

	if cached, ok := s.reserveHistoryCache.Retrieve(cacheKey); ok {
		return cached.([]*currency.ReserveRecord), nil
	}

	reserveHistory, err := s.data.GetCurrencyReserveHistory(
		ctx,
		mint,
		query.WithStartTime(startTime),
		query.WithEndTime(endTime),
		query.WithInterval(interval),
		query.WithDirection(query.Ascending),
	)
	if err != nil {
		return nil, err
	}

	s.reserveHistoryCache.Insert(cacheKey, reserveHistory, 1)
	return reserveHistory, nil
}

func (s *currencyServer) getCachedExchangeRateHistory(
	ctx context.Context,
	currencyCode currency_lib.Code,
	predefinedRange currencypb.GetHistoricalMintDataRequest_PredefinedRange,
	startTime, endTime time.Time,
	interval query.Interval,
) ([]*currency.ExchangeRateRecord, error) {
	cacheKey := fmt.Sprintf("%s:%s:%d", currencyCode, predefinedRange.String(), endTime.Unix())

	if cached, ok := s.exchangeRateHistoryCache.Retrieve(cacheKey); ok {
		return cached.([]*currency.ExchangeRateRecord), nil
	}

	exchangeRateHistory, err := s.data.GetExchangeRateHistory(
		ctx,
		currencyCode,
		query.WithStartTime(startTime),
		query.WithEndTime(endTime),
		query.WithInterval(interval),
		query.WithDirection(query.Ascending),
	)
	if err != nil {
		return nil, err
	}

	s.exchangeRateHistoryCache.Insert(cacheKey, exchangeRateHistory, 1)
	return exchangeRateHistory, nil
}

// findClosestExchangeRate finds the exchange rate closest to the given time.
func findClosestExchangeRate(t time.Time, history []*currency.ExchangeRateRecord) (float64, bool) {
	// Find the closest preceding exchange rate
	var closestRate float64
	var found bool
	for _, record := range history {
		if record.Time.After(t) {
			break
		}
		closestRate = record.Rate
		found = true
	}

	return closestRate, found
}

// calculateMarketCap calculates the market cap for a currency creator mint.
// Market cap = price per token × circulating supply.
func calculateMarketCap(supplyFromBonding uint64, exchangeRate float64) float64 {
	// Calculate the spot price per token using the bonding curve
	// This returns the price in core mint tokens per currency creator token
	spotPrice := currencycreator.EstimateCurrentPrice(supplyFromBonding)
	spotPriceFloat, _ := spotPrice.Float64()

	// Convert spot price to the requested currency
	// spotPriceFloat is in core mint tokens, exchangeRate is currency per core mint token
	pricePerToken := spotPriceFloat * exchangeRate

	// Calculate circulating supply in token units (not quarks)
	circulatingSupply := float64(supplyFromBonding) / float64(currencycreator.DefaultMintQuarksPerUnit)

	// Market cap = price per token × circulating supply
	return pricePerToken * circulatingSupply
}

// getTimeRangeForPredefinedRange returns the start time and appropriate interval
// for the given predefined range.
func getTimeRangeForPredefinedRange(predefinedRange currencypb.GetHistoricalMintDataRequest_PredefinedRange) (time.Time, time.Time, query.Interval) {
	now := currency_util.GetLatestExchangeRateTime()

	switch predefinedRange {
	case currencypb.GetHistoricalMintDataRequest_LAST_DAY:
		return now.Add(-24 * time.Hour), now, query.IntervalMinute
	case currencypb.GetHistoricalMintDataRequest_LAST_WEEK:
		return now.Add(-7 * 24 * time.Hour), now, query.IntervalHour
	case currencypb.GetHistoricalMintDataRequest_LAST_MONTH:
		return now.Add(-30 * 24 * time.Hour), now, query.IntervalHour
	case currencypb.GetHistoricalMintDataRequest_LAST_YEAR:
		return now.Add(-365 * 24 * time.Hour), now, query.IntervalDay
	case currencypb.GetHistoricalMintDataRequest_ALL_TIME:
		fallthrough
	default:
		// For all time, go back 100 years with daily intervals
		return now.Add(-100 * 365 * 24 * time.Hour), now, query.IntervalDay
	}
}
