package currency

import (
	"sort"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	currencypb "github.com/code-payments/ocp-protobuf-api/generated/go/currency/v1"

	"github.com/code-payments/ocp-server/ocp/data/currency"
	"github.com/code-payments/ocp-server/solana/currencycreator"
)

const (
	maxDiscoveredCurrencies = 1024
)

var (
	minDiscoverySupply = currencycreator.ToQuarks(100)
)

func (s *currencyServer) Discover(req *currencypb.DiscoverRequest, stream currencypb.Currency_DiscoverServer) error {
	log := s.log.With(zap.String("method", "Discover"))
	ctx := stream.Context()

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

		protoMints = append(protoMints, protoMint)
	}

	if len(protoMints) == 0 {
		return stream.Send(&currencypb.DiscoverResponse{
			Result: currencypb.DiscoverResponse_NOT_FOUND,
		})
	}

	sort.Slice(protoMints, func(i, j int) bool {
		return protoMints[i].LaunchpadMetadata.SupplyFromBonding > protoMints[j].LaunchpadMetadata.SupplyFromBonding
	})

	if len(protoMints) > maxDiscoveredCurrencies {
		protoMints = protoMints[:maxDiscoveredCurrencies]
	}

	return stream.Send(&currencypb.DiscoverResponse{
		Result: currencypb.DiscoverResponse_OK,
		Mint:   protoMints,
	})
}
