package balance

import (
	"context"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	balancepb "github.com/code-payments/ocp-protobuf-api/generated/go/balance/v1"

	"github.com/code-payments/ocp-server/grpc/client"
	"github.com/code-payments/ocp-server/ocp/balance"
	"github.com/code-payments/ocp-server/ocp/common"
	currency_util "github.com/code-payments/ocp-server/ocp/currency"
	ocp_data "github.com/code-payments/ocp-server/ocp/data"
	"github.com/code-payments/ocp-server/ocp/rpc"
	"github.com/code-payments/ocp-server/solana/currencycreator"
)

type server struct {
	log              *zap.Logger
	data             ocp_data.Provider
	mintDataProvider *currency_util.MintDataProvider

	balancepb.UnimplementedBalanceServer
}

func NewBalanceServer(log *zap.Logger, data ocp_data.Provider, mintDataProvider *currency_util.MintDataProvider) balancepb.BalanceServer {
	return &server{
		log:              log,
		data:             data,
		mintDataProvider: mintDataProvider,
	}
}

func (s *server) GetBalance(ctx context.Context, req *balancepb.GetBalanceRequest) (*balancepb.GetBalanceResponse, error) {
	log := s.log.With(zap.String("method", "GetBalance"))
	log = client.InjectLoggingMetadata(ctx, log, rpc.UserAgentName)

	owner, err := common.NewAccountFromProto(req.Owner)
	if err != nil {
		log.With(zap.Error(err)).Warn("invalid owner account")
		return nil, status.Error(codes.Internal, "")
	}
	log = log.With(zap.String("owner_account", owner.PublicKey().ToBase58()))

	ownerMetadata, err := common.GetOwnerMetadata(ctx, s.data, owner)
	if err == common.ErrOwnerNotFound {
		return &balancepb.GetBalanceResponse{
			Result: balancepb.GetBalanceResponse_NOT_FOUND,
		}, nil
	} else if err != nil {
		log.With(zap.Error(err)).Warn("failure getting owner metadata")
		return nil, status.Error(codes.Internal, "")
	}

	if ownerMetadata.Type != common.OwnerTypeUser12Words {
		return &balancepb.GetBalanceResponse{
			Result: balancepb.GetBalanceResponse_NOT_FOUND,
		}, nil
	}

	coreMintValue, err := s.calculateCoreMintValue(ctx, owner)
	if err != nil {
		log.With(zap.Error(err)).Warn("failure calculating core mint value")
		return nil, status.Error(codes.Internal, "")
	}

	return &balancepb.GetBalanceResponse{
		Result:        balancepb.GetBalanceResponse_OK,
		CoreMintValue: coreMintValue,
	}, nil
}

func (s *server) calculateCoreMintValue(ctx context.Context, owner *common.Account) (uint64, error) {
	recordsByMintAndType, err := common.GetLatestCodeTimelockAccountRecordsForOwner(ctx, s.data, owner)
	if err != nil {
		return 0, err
	}

	// Accounts that have left the L2 system don't have a cached balance that can
	// be trusted, so they're excluded from the calculation.
	mintByTokenAccount := make(map[string]string)
	var managedByCodeRecords []*common.AccountRecords
	for mint, recordsByType := range recordsByMintAndType {
		for _, recordsList := range recordsByType {
			for _, records := range recordsList {
				if !records.IsManagedByCode(ctx) {
					continue
				}

				mintByTokenAccount[records.General.TokenAccount] = mint
				managedByCodeRecords = append(managedByCodeRecords, records)
			}
		}
	}

	if len(managedByCodeRecords) == 0 {
		return 0, nil
	}

	balanceByTokenAccount, err := balance.BatchCalculateFromCacheWithAccountRecords(ctx, s.data, managedByCodeRecords...)
	if err != nil {
		return 0, err
	}

	quarksByMint := make(map[string]uint64)
	for tokenAccount, quarks := range balanceByTokenAccount {
		quarksByMint[mintByTokenAccount[tokenAccount]] += quarks
	}

	var coreMintValue uint64
	for mint, quarks := range quarksByMint {
		if quarks == 0 {
			continue
		}

		if mint == common.CoreMintAccount.PublicKey().ToBase58() {
			coreMintValue += quarks
			continue
		}

		mintAccount, err := common.NewAccountFromPublicKeyString(mint)
		if err != nil {
			return 0, err
		}

		reserveState, err := s.mintDataProvider.GetLiveReserveState(ctx, mintAccount)
		if err != nil {
			return 0, err
		}

		sellValue, _ := currencycreator.EstimateSell(&currencycreator.EstimateSellArgs{
			CurrentSupplyInQuarks: reserveState.SupplyFromBonding,
			SellAmountInQuarks:    quarks,
			ValueMintDecimals:     uint8(common.CoreMintDecimals),
			SellFeeBps:            0,
		})
		coreMintValue += sellValue
	}

	return coreMintValue, nil
}
