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
	dustValue        uint64

	balancepb.UnimplementedBalanceServer
}

func NewBalanceServer(log *zap.Logger, data ocp_data.Provider, mintDataProvider *currency_util.MintDataProvider, dustValue uint64) balancepb.BalanceServer {
	return &server{
		log:              log,
		data:             data,
		mintDataProvider: mintDataProvider,
		dustValue:        dustValue,
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

	var mintFilter map[string]struct{}
	if len(req.Mints) > 0 {
		mintFilter = make(map[string]struct{}, len(req.Mints))
		for i, protoMint := range req.Mints {
			mint, err := common.NewAccountFromProto(protoMint)
			if err != nil {
				log.With(zap.Error(err), zap.Int("index", i)).Warn("invalid mint account")
				return nil, status.Error(codes.Internal, "")
			}
			mintFilter[mint.PublicKey().ToBase58()] = struct{}{}
		}
	}

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

	balancesByMint, err := s.calculateCoreMintValueByMint(ctx, owner, mintFilter)
	if err != nil {
		log.With(zap.Error(err)).Warn("failure calculating core mint value")
		return nil, status.Error(codes.Internal, "")
	}

	var totalCoreMintValue uint64
	for _, mintBalance := range balancesByMint {
		totalCoreMintValue += mintBalance.CoreMintValue
	}

	return &balancepb.GetBalanceResponse{
		Result:         balancepb.GetBalanceResponse_OK,
		CoreMintValue:  totalCoreMintValue,
		BalancesByMint: balancesByMint,
	}, nil
}

// calculateCoreMintValueByMint values the owner's holdings in each mint they
// hold a non-zero balance of. A nil mintFilter includes every mint, otherwise
// only mints in the filter are included.
func (s *server) calculateCoreMintValueByMint(ctx context.Context, owner *common.Account, mintFilter map[string]struct{}) (map[string]*balancepb.MintBalance, error) {
	// The ledger holds a record for every account Code manages for the owner,
	// and each carries the mint it holds. Accounts that have left the L2 system
	// don't have a cached balance that can be trusted, so it omits them.
	balanceByTokenAccount, err := balance.BatchCalculateFromCacheByOwner(ctx, s.data, owner)
	if err != nil {
		return nil, err
	}

	quarksByMint := make(map[string]uint64)
	for _, cached := range balanceByTokenAccount {
		if mintFilter != nil {
			if _, ok := mintFilter[cached.MintAccount]; !ok {
				continue
			}
		}
		quarksByMint[cached.MintAccount] += cached.Quarks
	}

	balancesByMint := make(map[string]*balancepb.MintBalance)
	for mint, quarks := range quarksByMint {
		if quarks == 0 {
			continue
		}

		mintAccount, err := common.NewAccountFromPublicKeyString(mint)
		if err != nil {
			return nil, err
		}

		var coreMintValue uint64
		if mint == common.CoreMintAccount.PublicKey().ToBase58() {
			coreMintValue = quarks
		} else {
			reserveState, err := s.mintDataProvider.GetLiveReserveState(ctx, mintAccount)
			if err != nil {
				return nil, err
			}

			coreMintValue, _ = currencycreator.EstimateSell(&currencycreator.EstimateSellArgs{
				CurrentSupplyInQuarks: reserveState.SupplyFromBonding,
				SellAmountInQuarks:    quarks,
				ValueMintDecimals:     uint8(common.CoreMintDecimals),
				SellFeeBps:            0,
			})
		}

		if coreMintValue < s.dustValue {
			continue
		}

		balancesByMint[mint] = &balancepb.MintBalance{
			Mint:          mintAccount.ToProto(),
			CoreMintValue: coreMintValue,
		}
	}

	return balancesByMint, nil
}
