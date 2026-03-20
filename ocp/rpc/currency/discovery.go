package currency

import (
	"sort"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	currencypb "github.com/code-payments/ocp-protobuf-api/generated/go/currency/v1"

	"github.com/code-payments/ocp-server/ocp/data/currency"
	"github.com/code-payments/ocp-server/solana/currencycreator"
)

const (
	maxDiscoveredCurrencies         = 100
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

	var categoryFilterFunc func(mints []*currencypb.Mint) []*currencypb.Mint
	switch req.Category {
	case currencypb.DiscoverRequest_POPULAR:
		categoryFilterFunc = func(mints []*currencypb.Mint) []*currencypb.Mint {
			return mints
		}
	case currencypb.DiscoverRequest_NEW:
		categoryFilterFunc = func(mints []*currencypb.Mint) []*currencypb.Mint {
			var res []*currencypb.Mint
			for _, mint := range mints {
				if time.Since(mint.CreatedAt.AsTime()) < newDiscoveredCurrenciesAgeLimit {
					res = append(res, mint)
				}
			}
			return res
		}
	default:
		return status.Error(codes.InvalidArgument, "invalid category")
	}

	metadataRecords, err := s.data.GetAllCurrencyMetadataByState(ctx, currency.MetadataStateAvailable)
	if err == currency.ErrNotFound {
		return stream.Send(&currencypb.DiscoverResponse{
			Result: currencypb.DiscoverResponse_NOT_FOUND,
		})
	} else if err != nil {
		log.With(zap.Error(err)).Warn("failure getting currency metadata records")
		return status.Error(codes.Internal, "")
	}

	var protoMints []*currencypb.Mint
	for _, record := range metadataRecords {
		log := log.With(zap.String("mint", record.Mint))

		protoMint, err := s.mintDataProvider.ToProtoMint(ctx, record)
		if err != nil {
			log.With(zap.Error(err)).Warn("failure converting metadata to proto mint")
			continue
		}

		if protoMint.LaunchpadMetadata.SupplyFromBonding < minDiscoverySupply {
			continue
		}
		if protoMint.HolderMetrics.CurrentHolders == 0 {
			continue
		}

		protoMints = append(protoMints, protoMint)
	}

	protoMints = categoryFilterFunc(protoMints)

	if len(protoMints) == 0 {
		return stream.Send(&currencypb.DiscoverResponse{
			Result: currencypb.DiscoverResponse_NOT_FOUND,
		})
	}

	sort.Slice(protoMints, func(i, j int) bool {
		if protoMints[i].HolderMetrics.CurrentHolders == protoMints[j].HolderMetrics.CurrentHolders {
			return protoMints[i].LaunchpadMetadata.SupplyFromBonding > protoMints[j].LaunchpadMetadata.SupplyFromBonding
		}
		return protoMints[i].HolderMetrics.CurrentHolders > protoMints[j].HolderMetrics.CurrentHolders
	})

	if len(protoMints) > maxDiscoveredCurrencies {
		protoMints = protoMints[:maxDiscoveredCurrencies]
	}

	return stream.Send(&currencypb.DiscoverResponse{
		Result: currencypb.DiscoverResponse_OK,
		Mints:  protoMints,
	})
}
