package currency

import (
	"context"
	"strings"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	currencypb "github.com/code-payments/ocp-protobuf-api/generated/go/currency/v1"

	"github.com/code-payments/ocp-server/pkg/code/common"
	"github.com/code-payments/ocp-server/pkg/code/config"
	currency_util "github.com/code-payments/ocp-server/pkg/code/currency"
	code_data "github.com/code-payments/ocp-server/pkg/code/data"
	"github.com/code-payments/ocp-server/pkg/code/data/currency"
	"github.com/code-payments/ocp-server/pkg/grpc/client"
	timelock_token "github.com/code-payments/ocp-server/pkg/solana/timelock/v1"
)

type currencyServer struct {
	log  *zap.Logger
	data code_data.Provider

	currencypb.UnimplementedCurrencyServer
}

func NewCurrencyServer(
	log *zap.Logger,
	data code_data.Provider,
) currencypb.CurrencyServer {
	return &currencyServer{
		log:  log,
		data: data,
	}
}

func (s *currencyServer) GetAllRates(ctx context.Context, req *currencypb.GetAllRatesRequest) (resp *currencypb.GetAllRatesResponse, err error) {
	log := s.log.With(zap.String("method", "GetAllRates"))
	log = client.InjectLoggingMetadata(ctx, log)

	var record *currency.MultiRateRecord
	if req.Timestamp != nil && req.Timestamp.AsTime().Before(time.Now().Add(-15*time.Minute)) {
		record, err = s.LoadExchangeRatesForTime(ctx, req.Timestamp.AsTime())
	} else if req.Timestamp == nil || req.Timestamp.AsTime().Sub(time.Now()) < time.Hour {
		record, err = s.LoadExchangeRatesLatest(ctx)
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
			protoMetadata = &currencypb.Mint{
				Address:     protoMintAddress,
				Decimals:    uint32(common.CoreMintDecimals),
				Name:        common.CoreMintName,
				Symbol:      strings.ToUpper(string(common.CoreMintSymbol)),
				Description: config.CoreMintDescription,
				ImageUrl:    config.CoreMintImageUrl,
				VmMetadata: &currencypb.VmMetadata{
					Vm:                 common.CodeVmAccount.ToProto(),
					Authority:          common.GetSubsidizer().ToProto(),
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
					CoreMintLocked:    reserveRecord.CoreMintLocked,
					SellFeeBps:        uint32(metadataRecord.SellFeeBps),
				},
				CreatedAt: timestamppb.New(metadataRecord.CreatedAt),
			}
		}

		resp.MetadataByAddress[mintAccount.PublicKey().ToBase58()] = protoMetadata
	}
	return resp, nil
}

func (s *currencyServer) LoadExchangeRatesForTime(ctx context.Context, t time.Time) (*currency.MultiRateRecord, error) {
	record, err := s.data.GetAllExchangeRates(ctx, t)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get price record by date")
	}
	return record, nil
}

func (s *currencyServer) LoadExchangeRatesLatest(ctx context.Context) (*currency.MultiRateRecord, error) {
	latest, err := s.data.GetAllExchangeRates(ctx, currency_util.GetLatestExchangeRateTime())
	if err != nil {
		return nil, errors.Wrap(err, "failed to get latest price record")
	}
	return latest, nil
}
