package currency

import (
	"context"
	"strings"
	"time"

	"github.com/gogo/protobuf/proto"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	currencypb "github.com/code-payments/ocp-protobuf-api/generated/go/currency/v1"

	"github.com/code-payments/ocp-server/grpc/client"
	"github.com/code-payments/ocp-server/ocp/common"
	"github.com/code-payments/ocp-server/ocp/config"
	"github.com/code-payments/ocp-server/ocp/data/currency"
	"github.com/code-payments/ocp-server/solana/currencycreator"
	timelock_token "github.com/code-payments/ocp-server/solana/timelock/v1"
)

const (
	getMintsCacheTTL = 5 * time.Minute
)

func (s *currencyServer) GetMints(ctx context.Context, req *currencypb.GetMintsRequest) (*currencypb.GetMintsResponse, error) {
	log := s.log.With(zap.String("method", "GetMints"))
	log = client.InjectLoggingMetadata(ctx, log)

	// Track all requested mints so the worker polls their reserve state
	var requestedMints []*common.Account
	for _, protoMintAddress := range req.Addresses {
		mintAccount, err := common.NewAccountFromProto(protoMintAddress)
		if err != nil {
			continue
		}
		requestedMints = append(requestedMints, mintAccount)
	}
	if err := s.liveMintStateWorker.trackMints(ctx, requestedMints); err != nil {
		if err == errMintNotSupported {
			return &currencypb.GetMintsResponse{Result: currencypb.GetMintsResponse_NOT_FOUND}, nil
		}
		log.With(zap.Error(err)).Warn("failed to track requested mints")
		return nil, status.Error(codes.Internal, "")
	}

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

		// Check cache first
		if cached, ok := s.getCachedProtoMint(mintAccount); ok {
			// Always overlay fresh circulating supply for launchpad currencies
			if cached.LaunchpadMetadata != nil {
				liveReserveState, err := s.liveMintStateWorker.getReserveState(mintAccount)
				if err != nil {
					log.With(zap.Error(err)).Warn("failed to get live mint reserve state")
					return nil, status.Error(codes.Internal, "")
				}

				spotPrice, _ := currencycreator.EstimateCurrentPrice(liveReserveState.SupplyFromBonding).Float64()
				marketCap := calculateMarketCap(liveReserveState.SupplyFromBonding, 1.0)
				cached.LaunchpadMetadata.SupplyFromBonding = liveReserveState.SupplyFromBonding
				cached.LaunchpadMetadata.Price = spotPrice
				cached.LaunchpadMetadata.MarketCap = marketCap
			}

			resp.MetadataByAddress[mintAccount.PublicKey().ToBase58()] = cached
			continue
		}

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
			if metadataRecord.State != currency.MetadataStateAvailable {
				return &currencypb.GetMintsResponse{Result: currencypb.GetMintsResponse_NOT_FOUND}, nil
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

			err = s.liveMintStateWorker.waitForReserveState(ctx, mintAccount)
			if err != nil {
				log.With(zap.Error(err)).Warn("failed to wait for live mint reserve state")
				return nil, status.Error(codes.Internal, "")
			}

			liveReserveState, err := s.liveMintStateWorker.getReserveState(mintAccount)
			if err != nil {
				log.With(zap.Error(err)).Warn("failed to get live mint reserve state")
				return nil, status.Error(codes.Internal, "")
			}

			spotPrice, _ := currencycreator.EstimateCurrentPrice(liveReserveState.SupplyFromBonding).Float64()
			marketCap := calculateMarketCap(liveReserveState.SupplyFromBonding, 1.0)

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
					SupplyFromBonding: liveReserveState.SupplyFromBonding,
					SellFeeBps:        uint32(metadataRecord.SellFeeBps),
					Price:             spotPrice,
					MarketCap:         marketCap,
				},
				CreatedAt: timestamppb.New(metadataRecord.CreatedAt),
			}

			billColors := metadataRecord.BillColors
			if len(billColors) == 0 {
				billColors = config.DefaultBillColors
			}
			var protoColors []*currencypb.Color
			for _, hex := range billColors {
				protoColors = append(protoColors, &currencypb.Color{Hex: hex})
			}
			protoMetadata.BillCustomization = &currencypb.BillCustomization{
				Colors: protoColors,
			}

			for _, link := range metadataRecord.SocialLinks {
				switch link.Type {
				case currency.SocialLinkTypeWebsite:
					protoMetadata.SocialLinks = append(protoMetadata.SocialLinks, &currencypb.SocialLink{
						Type: &currencypb.SocialLink_Website_{
							Website: &currencypb.SocialLink_Website{Url: link.Value},
						},
					})
				case currency.SocialLinkTypeX:
					protoMetadata.SocialLinks = append(protoMetadata.SocialLinks, &currencypb.SocialLink{
						Type: &currencypb.SocialLink_X_{
							X: &currencypb.SocialLink_X{Username: link.Value},
						},
					})
				}
			}
		}

		s.setCachedProtoMint(mintAccount, protoMetadata)
		resp.MetadataByAddress[mintAccount.PublicKey().ToBase58()] = protoMetadata
	}
	return resp, nil
}

func (s *currencyServer) getCachedProtoMint(mintAccount *common.Account) (*currencypb.Mint, bool) {
	s.getMintsCacheMu.RLock()
	defer s.getMintsCacheMu.RUnlock()

	entry, ok := s.getMintsCache[mintAccount.PublicKey().ToBase58()]
	if !ok || time.Since(entry.lastUpdatedAt) >= getMintsCacheTTL {
		return nil, false
	}
	return proto.Clone(entry.mint).(*currencypb.Mint), true
}

func (s *currencyServer) setCachedProtoMint(mintAccount *common.Account, protoMint *currencypb.Mint) {
	s.getMintsCacheMu.Lock()
	defer s.getMintsCacheMu.Unlock()

	s.getMintsCache[mintAccount.PublicKey().ToBase58()] = &cachedProtoMint{
		mint:          proto.Clone(protoMint).(*currencypb.Mint),
		lastUpdatedAt: time.Now(),
	}
}
