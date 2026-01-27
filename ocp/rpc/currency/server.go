package currency

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonpb "github.com/code-payments/ocp-protobuf-api/generated/go/common/v1"
	currencypb "github.com/code-payments/ocp-protobuf-api/generated/go/currency/v1"

	"github.com/code-payments/ocp-server/cache"
	currency_lib "github.com/code-payments/ocp-server/currency"
	"github.com/code-payments/ocp-server/database/query"
	"github.com/code-payments/ocp-server/grpc/client"
	"github.com/code-payments/ocp-server/ocp/common"
	"github.com/code-payments/ocp-server/ocp/config"
	currency_util "github.com/code-payments/ocp-server/ocp/currency"
	ocp_data "github.com/code-payments/ocp-server/ocp/data"
	"github.com/code-payments/ocp-server/ocp/data/currency"
	"github.com/code-payments/ocp-server/protoutil"
	"github.com/code-payments/ocp-server/solana/currencycreator"
	timelock_token "github.com/code-payments/ocp-server/solana/timelock/v1"
)

const (
	streamPingDelay          = 5 * time.Second
	streamInitialRecvTimeout = 250 * time.Millisecond

	timePerHistoricalUpdate = 5 * time.Minute
)

type currencyServer struct {
	log  *zap.Logger
	data ocp_data.Provider

	exchangeRateHistoryCache cache.Cache
	reserveHistoryCache      cache.Cache

	liveMintStateWorker *liveMintStateWorker

	currencypb.UnimplementedCurrencyServer
}

func NewCurrencyServer(
	log *zap.Logger,
	data ocp_data.Provider,
	configProvider ConfigProvider,
) currencypb.CurrencyServer {
	liveMintStateWorker := newLiveMintStateWorker(log, data, configProvider())
	liveMintStateWorker.start(context.Background())

	return &currencyServer{
		log:  log,
		data: data,

		exchangeRateHistoryCache: cache.NewCache(1_000),
		reserveHistoryCache:      cache.NewCache(1_000),

		liveMintStateWorker: liveMintStateWorker,
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
	metadataRecord, err := s.data.GetCurrencyMetadata(ctx, mintAccount.PublicKey().ToBase58())
	if err == currency.ErrNotFound {
		return &currencypb.GetHistoricalMintDataResponse{
			Result: currencypb.GetHistoricalMintDataResponse_NOT_FOUND,
		}, nil
	} else if err != nil {
		log.With(zap.Error(err)).Warn("failed to load currency metadata")
		return nil, status.Error(codes.Internal, "")
	}

	// Determine the time range and interval based on the predefined range.
	// endTime is getLatestHistoricalTime, which is used in cache keys to
	// invalidate entries when new market data is generated.
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

	// Get exchange rate history (cached by currency code + range)
	exchangeRateHistory, err := s.getCachedExchangeRateHistory(
		ctx,
		currencyCode,
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
		log.With(zap.Error(err)).Warn("failed to load exchange rate history")
		return nil, status.Error(codes.Internal, "")
	}

	if len(exchangeRateHistory) == 0 {
		return &currencypb.GetHistoricalMintDataResponse{
			Result: currencypb.GetHistoricalMintDataResponse_MISSING_DATA,
		}, nil
	}

	// Build historical data points with market cap
	var data []*currencypb.HistoricalMintData

	if startTime.Before(metadataRecord.CreatedAt) {
		if req.GetPredefinedRange() != currencypb.GetHistoricalMintDataRequest_ALL_TIME {
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

	// Always include a latest data point based on getLatestHistoricalTime
	// if it's newer than the last historical point
	latestTime := getLatestHistoricalTime()
	if len(data) == 0 || data[len(data)-1].Timestamp.AsTime().Before(latestTime) {
		func() {
			latestReserve, err := s.data.GetCurrencyReserveAtTime(ctx, mintAccount.PublicKey().ToBase58(), latestTime)
			if err != nil {
				log.With(zap.Error(err)).Warn("failed to load latest currency reserve")
				return
			}

			latestExchangeRate, err := s.data.GetExchangeRate(ctx, currencyCode, latestTime)
			if err != nil {
				log.With(zap.Error(err)).Warn("failed to load latest exchange rate")
				return
			}

			data = append(data, &currencypb.HistoricalMintData{
				Timestamp: timestamppb.New(latestTime),
				MarketCap: calculateMarketCap(latestReserve.SupplyFromBonding, latestExchangeRate.Rate),
			})
		}()
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
	now := getLatestHistoricalTime()

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

func getLatestHistoricalTime() time.Time {
	secondsInUpdateInterval := int64(timePerHistoricalUpdate / time.Second)
	queryTimeUnix := time.Now().Unix()
	queryTimeUnix = queryTimeUnix - (queryTimeUnix % secondsInUpdateInterval)
	return time.Unix(queryTimeUnix, 0)
}

func (s *currencyServer) StreamLiveMintData(
	streamer currencypb.Currency_StreamLiveMintDataServer,
) error {
	ctx := streamer.Context()

	log := s.log.With(zap.String("method", "StreamLiveMintData"))
	log = client.InjectLoggingMetadata(ctx, log)

	// Wait for the initial request to get the list of mints
	req, err := protoutil.BoundedReceive[currencypb.StreamLiveMintDataRequest](ctx, streamer, streamInitialRecvTimeout)
	if err != nil {
		log.With(zap.Error(err)).Debug("error receiving initial request")
		return err
	}

	// Must be a request type
	request := req.GetRequest()
	if request == nil {
		return status.Error(codes.InvalidArgument, "first message must be a request")
	}

	// Parse requested mints
	var requestedMints []*common.Account
	for _, protoMint := range request.GetMints() {
		mint, err := common.NewAccountFromProto(protoMint)
		if err != nil {
			log.With(zap.Error(err)).Warn("invalid mint")
			return status.Error(codes.Internal, "")
		}
		requestedMints = append(requestedMints, mint)
	}

	// Generate unique stream ID
	streamID := uuid.New().String()
	log = log.With(zap.String("stream_id", streamID))

	// Register stream with state worker
	stream := s.liveMintStateWorker.registerStream(streamID, requestedMints)
	defer s.liveMintStateWorker.unregisterStream(streamID)

	log.Debug("stream registered")

	// Wait for initial data to be available
	if err := s.liveMintStateWorker.waitForData(ctx); err != nil {
		log.With(zap.Error(err)).Debug("context cancelled while waiting for data")
		return status.Error(codes.Canceled, "")
	}

	// Initial flush: send current exchange rates if the stream wants them
	if stream.wantsExchangeRates() {
		exchangeRates := s.liveMintStateWorker.getExchangeRates()
		if exchangeRates != nil && exchangeRates.SignedResponse != nil {
			if err := streamer.Send(exchangeRates.SignedResponse); err != nil {
				log.With(zap.Error(err)).Debug("failed to send initial exchange rates")
				return err
			}
		}
	}

	// Initial flush: send current reserve states
	reserveStates := s.liveMintStateWorker.getReserveStates()
	if len(reserveStates) > 0 {
		// Filter based on requested mints and build batch response
		var filtered []*currencypb.VerifiedLaunchpadCurrencyReserveState
		for _, state := range reserveStates {
			if stream.wantsMint(state.Mint.PublicKey().ToBase58()) && state.SignedState != nil {
				filtered = append(filtered, state.SignedState)
			}
		}
		if len(filtered) > 0 {
			resp := &currencypb.StreamLiveMintDataResponse{
				Type: &currencypb.StreamLiveMintDataResponse_Data{
					Data: &currencypb.StreamLiveMintDataResponse_LiveData{
						Type: &currencypb.StreamLiveMintDataResponse_LiveData_LaunchpadCurrencyReserveStates{
							LaunchpadCurrencyReserveStates: &currencypb.VerifiedLaunchapdCurrencyReserveStateBatch{
								ReserveStates: filtered,
							},
						},
					},
				},
			}
			if err := streamer.Send(resp); err != nil {
				log.With(zap.Error(err)).Debug("failed to send initial reserve states")
				return err
			}
		}
	}

	log.Debug("initial flush complete")

	// Set up ping/pong health monitoring
	sendPingCh := time.After(0) // Send first ping immediately
	streamHealthCh := protoutil.MonitorStreamHealth(ctx, log, streamer, func(req *currencypb.StreamLiveMintDataRequest) bool {
		return req.GetPong() != nil
	})

	// Main loop: listen on stream channel and send updates
	for {
		select {
		case update, ok := <-stream.streamCh:
			if !ok {
				log.Debug("stream channel closed")
				return status.Error(codes.Aborted, "stream closed")
			}

			if update.response == nil {
				continue
			}

			if err := streamer.Send(update.response); err != nil {
				log.With(zap.Error(err)).Debug("failed to send update")
				return err
			}

		case <-sendPingCh:
			log.Debug("sending ping to client")
			sendPingCh = time.After(streamPingDelay)

			err := streamer.Send(&currencypb.StreamLiveMintDataResponse{
				Type: &currencypb.StreamLiveMintDataResponse_Ping{
					Ping: &commonpb.ServerPing{
						Timestamp: timestamppb.Now(),
						PingDelay: durationpb.New(streamPingDelay),
					},
				},
			})
			if err != nil {
				log.Debug("failed to send ping, stream is unhealthy")
				return status.Error(codes.Aborted, "terminating unhealthy stream")
			}

		case <-streamHealthCh:
			log.Debug("stream is unhealthy, terminating")
			return status.Error(codes.Aborted, "terminating unhealthy stream")

		case <-ctx.Done():
			log.Debug("context cancelled")
			return status.Error(codes.Canceled, "")
		}
	}
}
