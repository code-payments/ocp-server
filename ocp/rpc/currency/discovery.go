package currency

import (
	"sort"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	currencypb "github.com/code-payments/ocp-protobuf-api/generated/go/currency/v1"

	ocp_currency "github.com/code-payments/ocp-server/ocp/currency"
	"github.com/code-payments/ocp-server/ocp/data/currency"
	"github.com/code-payments/ocp-server/solana/currencycreator"
)

const (
	maxDiscoveredCurrencies         = 1000
	newDiscoveredCurrenciesAgeLimit = 7 * 24 * time.Hour // 1 week
)

var (
	minDiscoverySupply = currencycreator.ToQuarks(100)
)

func (s *currencyServer) Discover(req *currencypb.DiscoverRequest, stream currencypb.Currency_DiscoverServer) error {
	log := s.log.With(
		zap.String("method", "Discover"),
		zap.String("category", req.Category.String()),
	)
	ctx := stream.Context()

	var categoryFilterFunc func(record *currency.MetadataRecord) bool
	switch req.Category {
	case currencypb.DiscoverRequest_POPULAR:
		categoryFilterFunc = func(record *currency.MetadataRecord) bool {
			return true
		}
	case currencypb.DiscoverRequest_NEW:
		categoryFilterFunc = func(record *currency.MetadataRecord) bool {
			return time.Since(record.CreatedAt) < newDiscoveredCurrenciesAgeLimit
		}
	default:
		return stream.Send(&currencypb.DiscoverResponse{
			Result: currencypb.DiscoverResponse_NOT_FOUND,
		})
	}

	metadataRecords, err := s.mintDataProvider.GetAllCachedCurrencyMetadata(ctx)
	if err == currency.ErrNotFound {
		return stream.Send(&currencypb.DiscoverResponse{
			Result: currencypb.DiscoverResponse_OK,
		})
	} else if err != nil {
		log.With(zap.Error(err)).Warn("failure getting currency metadata records")
		return status.Error(codes.Internal, "")
	}
	if len(metadataRecords) == 0 {
		return stream.Send(&currencypb.DiscoverResponse{
			Result: currencypb.DiscoverResponse_OK,
		})
	}

	cachedReserves, err := s.mintDataProvider.GetAllCachedReserveStates(ctx)
	if err != nil {
		log.With(zap.Error(err)).Warn("failure getting cached reserve states")
		return status.Error(codes.Internal, "")
	}
	cachedHolders, err := s.mintDataProvider.GetAllCachedHolderCounts(ctx)
	if err != nil {
		log.With(zap.Error(err)).Warn("failure getting cached holder counts")
		return status.Error(codes.Internal, "")
	}

	type discoveredMint struct {
		record       *currency.MetadataRecord
		reserveState *ocp_currency.LiveReserveStateData
		holderData   *ocp_currency.LiveHolderCountData
	}

	var candidates []*discoveredMint
	for _, record := range metadataRecords {
		if record.State != currency.MetadataStateAvailable {
			continue
		}

		if !record.IsDiscoverable {
			continue
		}

		reserveState, ok := cachedReserves[record.Mint]
		if !ok || reserveState.SupplyFromBonding < minDiscoverySupply {
			continue
		}

		holderData, ok := cachedHolders[record.Mint]
		if !ok || holderData.CurrentHolders == 0 {
			continue
		}

		if !categoryFilterFunc(record) {
			continue
		}

		candidates = append(candidates, &discoveredMint{
			record:       record,
			reserveState: reserveState,
			holderData:   holderData,
		})
	}

	if len(candidates) == 0 {
		return stream.Send(&currencypb.DiscoverResponse{
			Result: currencypb.DiscoverResponse_OK,
		})
	}

	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].reserveState.SupplyFromBonding > candidates[j].reserveState.SupplyFromBonding
	})

	if len(candidates) > maxDiscoveredCurrencies {
		candidates = candidates[:maxDiscoveredCurrencies]
	}

	var protoMints []*currencypb.Mint
	for _, candidate := range candidates {
		log := log.With(zap.String("mint", candidate.record.Mint))

		protoMint, err := s.mintDataProvider.ToProtoMint(ctx, candidate.record, false, false)
		if err != nil {
			log.With(zap.Error(err)).Warn("failure converting metadata to proto mint")
			continue
		}

		ocp_currency.SetLaunchpadReserveData(protoMint, candidate.reserveState)
		ocp_currency.SetHolderMetrics(protoMint, candidate.holderData)

		protoMints = append(protoMints, protoMint)
	}

	return stream.Send(&currencypb.DiscoverResponse{
		Result: currencypb.DiscoverResponse_OK,
		Mints:  protoMints,
	})
}
