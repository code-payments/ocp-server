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
	currency_lib "github.com/code-payments/ocp-server/currency"

	"github.com/code-payments/ocp-server/database/query"
	"github.com/code-payments/ocp-server/grpc/client"
	"github.com/code-payments/ocp-server/ocp/common"
	currency_util "github.com/code-payments/ocp-server/ocp/currency"
	"github.com/code-payments/ocp-server/ocp/data/currency"
	"github.com/code-payments/ocp-server/ocp/rpc"
)

const (
	timePerHistoricalUpdate = 5 * time.Minute
)

func (s *currencyServer) GetHistoricalMintData(ctx context.Context, req *currencypb.GetHistoricalMintDataRequest) (*currencypb.GetHistoricalMintDataResponse, error) {
	log := s.log.With(zap.String("method", "GetHistoricalMintData"))
	log = client.InjectLoggingMetadata(ctx, log, rpc.UserAgentName)

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
	metadataRecord, err := s.data.GetCurrencyMetadata(ctx, mintAccount.PublicKey().ToBase58())
	if err == currency.ErrNotFound {
		return &currencypb.GetHistoricalMintDataResponse{
			Result: currencypb.GetHistoricalMintDataResponse_NOT_FOUND,
		}, nil
	} else if err != nil {
		log.With(zap.Error(err)).Warn("failed to load currency metadata")
		return nil, status.Error(codes.Internal, "")
	}
	if metadataRecord.State != currency.MetadataStateAvailable {
		return &currencypb.GetHistoricalMintDataResponse{
			Result: currencypb.GetHistoricalMintDataResponse_NOT_FOUND,
		}, nil
	}

	// Determine the time range and interval based on the predefined range.
	// The interval is adjusted based on the currency's age so younger
	// currencies get finer-grained data points.
	// endTime is getLatestHistoricalTime, which is used in cache keys to
	// invalidate entries when new market data is generated.
	startTime, endTime, interval := getTimeRangeForPredefinedRange(req.GetPredefinedRange(), metadataRecord.CreatedAt)

	// Get reserve history (cached by mint + range)
	reserveHistory, err := s.getCachedReserveHistory(
		ctx,
		mintAccount.PublicKey().ToBase58(),
		req.GetPredefinedRange(),
		startTime,
		endTime,
		interval,
	)
	if err == currency.ErrNotFound {
		return &currencypb.GetHistoricalMintDataResponse{
			Result: currencypb.GetHistoricalMintDataResponse_MISSING_DATA,
		}, nil
	} else if err != nil {
		log.With(zap.Error(err)).Warn("failed to load currency reserve history")
		return nil, status.Error(codes.Internal, "")
	}

	if len(reserveHistory) == 0 {
		return &currencypb.GetHistoricalMintDataResponse{
			Result: currencypb.GetHistoricalMintDataResponse_MISSING_DATA,
		}, nil
	}

	// Always use the latest FX rate for the entire data set
	liveExchangeRates, err := s.mintDataProvider.GetLiveExchangeRates(ctx)
	if err != nil {
		log.With(zap.Error(err)).Warn("failed to load live exchange rates")
		return nil, status.Error(codes.Internal, "")
	}
	if liveExchangeRates == nil {
		return &currencypb.GetHistoricalMintDataResponse{
			Result: currencypb.GetHistoricalMintDataResponse_MISSING_DATA,
		}, nil
	}
	latestExchangeRate, ok := liveExchangeRates.Rates[string(currencyCode)]
	if !ok {
		return &currencypb.GetHistoricalMintDataResponse{
			Result: currencypb.GetHistoricalMintDataResponse_MISSING_DATA,
		}, nil
	}

	// Build historical data points with market cap
	var data []*currencypb.HistoricalMintData

	if startTime.Before(metadataRecord.CreatedAt) {
		if req.GetPredefinedRange() != currencypb.PredefinedRange_ALL_TIME {
			data = append(
				data,
				// 0 market cap value at the range start time
				&currencypb.HistoricalMintData{
					Timestamp: timestamppb.New(startTime),
					MarketCap: 0,
				},
			)
		}
		data = append(
			data,
			// 0 market cap value at time of currency creation
			&currencypb.HistoricalMintData{
				Timestamp: timestamppb.New(metadataRecord.CreatedAt),
				MarketCap: 0,
			},
		)
	}

	for _, reserve := range reserveHistory {
		data = append(data, &currencypb.HistoricalMintData{
			Timestamp: timestamppb.New(reserve.Time),
			MarketCap: currency_util.CalculateMarketCap(reserve.SupplyFromBonding, latestExchangeRate),
		})
	}

	// Always include a latest data point
	latestReserve, err := s.mintDataProvider.GetLiveReserveState(ctx, mintAccount)
	if err == nil {
		data = append(data, &currencypb.HistoricalMintData{
			Timestamp: timestamppb.New(latestReserve.SignedState.ReserveState.Timestamp.AsTime()),
			MarketCap: currency_util.CalculateMarketCap(latestReserve.SupplyFromBonding, latestExchangeRate),
		})
	} else {
		log.With(zap.Error(err)).Warn("failed to load latest currency reserve")
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

func (s *currencyServer) getCachedReserveHistory(
	ctx context.Context,
	mint string,
	predefinedRange currencypb.PredefinedRange,
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
	predefinedRange currencypb.PredefinedRange,
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
	// Find the closest exchange rate
	var closestRate float64
	var found bool
	for _, record := range history {
		if record.Time.After(t) {
			if !found {
				closestRate = record.Rate
				found = true
			}
			break
		}
		closestRate = record.Rate
		found = true
	}

	return closestRate, found
}

// getTimeRangeForPredefinedRange returns the start time and appropriate interval
// for the given predefined range. The interval is adjusted based on the
// currency's age so that younger currencies get finer-grained data points.
func getTimeRangeForPredefinedRange(predefinedRange currencypb.PredefinedRange, createdAt time.Time) (time.Time, time.Time, query.Interval) {
	now := getLatestHistoricalTime()
	currencyAge := now.Sub(createdAt)

	switch predefinedRange {
	case currencypb.PredefinedRange_LAST_DAY:
		return now.Add(-24 * time.Hour), now, query.IntervalMinute
	case currencypb.PredefinedRange_LAST_WEEK:
		interval := query.IntervalHour
		if currencyAge < 2*24*time.Hour {
			interval = query.IntervalMinute
		}
		return now.Add(-7 * 24 * time.Hour), now, interval
	case currencypb.PredefinedRange_LAST_MONTH:
		interval := query.IntervalHour
		if currencyAge < 2*24*time.Hour {
			interval = query.IntervalMinute
		}
		return now.Add(-30 * 24 * time.Hour), now, interval
	case currencypb.PredefinedRange_LAST_YEAR:
		interval := query.IntervalDay
		if currencyAge < 2*24*time.Hour {
			interval = query.IntervalMinute
		} else if currencyAge < 2*7*24*time.Hour {
			interval = query.IntervalHour
		}
		return now.Add(-365 * 24 * time.Hour), now, interval
	case currencypb.PredefinedRange_ALL_TIME:
		fallthrough
	default:
		interval := query.IntervalDay
		if currencyAge < 2*24*time.Hour {
			interval = query.IntervalMinute
		} else if currencyAge < 2*7*24*time.Hour {
			interval = query.IntervalHour
		}
		// For all time, go back 100 years
		return now.Add(-100 * 365 * 24 * time.Hour), now, interval
	}
}

func getLatestHistoricalTime() time.Time {
	secondsInUpdateInterval := int64(timePerHistoricalUpdate / time.Second)
	queryTimeUnix := time.Now().Unix()
	queryTimeUnix = queryTimeUnix - (queryTimeUnix % secondsInUpdateInterval)
	return time.Unix(queryTimeUnix, 0)
}
