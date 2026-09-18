package balance

import (
	"context"
	"strings"

	"github.com/pkg/errors"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	balancepb "github.com/code-payments/ocp-protobuf-api/generated/go/balance/v1"
	commonpb "github.com/code-payments/ocp-protobuf-api/generated/go/common/v1"

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

func (s *server) GetBalances(ctx context.Context, req *balancepb.GetBalancesRequest) (*balancepb.GetBalancesResponse, error) {
	log := s.log.With(zap.String("method", "GetBalances"))
	log = client.InjectLoggingMetadata(ctx, log, rpc.UserAgentName)

	mints, err := newMintFilter(req.Mints)
	if err != nil {
		log.With(zap.Error(err)).Warn("invalid mint account")
		return nil, status.Error(codes.Internal, "")
	}

	// Duplicate owners collapse to a single entry
	seenOwners := make(map[string]struct{}, len(req.Owners))
	owners := make([]*common.Account, 0, len(req.Owners))
	for i, protoOwner := range req.Owners {
		owner, err := common.NewAccountFromProto(protoOwner)
		if err != nil {
			log.With(zap.Error(err), zap.Int("index", i)).Warn("invalid owner account")
			return nil, status.Error(codes.Internal, "")
		}

		if _, ok := seenOwners[owner.PublicKey().ToBase58()]; ok {
			continue
		}
		seenOwners[owner.PublicKey().ToBase58()] = struct{}{}
		owners = append(owners, owner)
	}

	// All owners' ledger records are read in a single batch, with the mint
	// filter applied at the ledger read. Owners the ledger holds nothing for
	// are absent from the result, and are reported with an empty balance.
	balanceByOwnerAndTokenAccount, err := balance.BatchCalculateFromCacheByOwners(ctx, s.data, owners, mints)
	if err != nil {
		log.With(zap.Error(err)).Warn("failure getting cached balances")
		return nil, status.Error(codes.Internal, "")
	}

	// A single reserve state cache values every owner's holdings in a mint
	// against the same supply, even if the mint data provider refreshes
	// mid-request.
	reserveStateCache := newReserveStateCache()

	// Exchange rates are pinned once per request so every balance denominated
	// in a currency uses the same rate. Currencies without a rate are dropped.
	exchangeRates, err := s.getExchangeRates(ctx, req.CurrencyCodes)
	if err != nil {
		log.With(zap.Error(err)).Warn("failure getting live exchange rates")
		return nil, status.Error(codes.Internal, "")
	}

	balancesByOwner := make(map[string]*balancepb.OwnerBalance, len(owners))
	for _, owner := range owners {
		ownerBalance, err := s.valueOwnerBalance(ctx, owner, balanceByOwnerAndTokenAccount[owner.PublicKey().ToBase58()], reserveStateCache, exchangeRates)
		if err != nil {
			log.With(zap.Error(err), zap.String("owner_account", owner.PublicKey().ToBase58())).Warn("failure valuing owner balance")
			return nil, status.Error(codes.Internal, "")
		}

		balancesByOwner[owner.PublicKey().ToBase58()] = ownerBalance
	}

	return &balancepb.GetBalancesResponse{
		Result:          balancepb.GetBalancesResponse_OK,
		BalancesByOwner: balancesByOwner,
	}, nil
}

// valueOwnerBalance values an owner's cached holdings. Owner metadata checks
// are intentionally skipped as an optimization, so an owner with no ledger
// records has an empty balance.
func (s *server) valueOwnerBalance(ctx context.Context, owner *common.Account, balanceByTokenAccount map[string]*balance.Balance, reserveStateCache reserveStateCache, exchangeRates exchangeRates) (*balancepb.OwnerBalance, error) {
	balancesByMint, err := s.calculateCoreMintValueByMint(ctx, balanceByTokenAccount, reserveStateCache)
	if err != nil {
		return nil, err
	}

	var totalCoreMintValue uint64
	for _, mintBalance := range balancesByMint {
		totalCoreMintValue += mintBalance.CoreMintValue
		mintBalance.FiatValuesByCurrency = exchangeRates.toFiatValues(mintBalance.CoreMintValue)
	}

	return &balancepb.OwnerBalance{
		Owner:                owner.ToProto(),
		CoreMintValue:        totalCoreMintValue,
		BalancesByMint:       balancesByMint,
		FiatValuesByCurrency: exchangeRates.toFiatValues(totalCoreMintValue),
	}, nil
}

// newMintFilter converts the request's mints into a deduplicated list for
// filtering the ledger read. A nil filter is returned when no mints are
// provided, which includes every mint.
func newMintFilter(protoMints []*commonpb.SolanaAccountId) ([]*common.Account, error) {
	if len(protoMints) == 0 {
		return nil, nil
	}

	seen := make(map[string]struct{}, len(protoMints))
	mints := make([]*common.Account, 0, len(protoMints))
	for i, protoMint := range protoMints {
		mint, err := common.NewAccountFromProto(protoMint)
		if err != nil {
			return nil, errors.Wrapf(err, "invalid mint account at index %d", i)
		}

		if _, ok := seen[mint.PublicKey().ToBase58()]; ok {
			continue
		}
		seen[mint.PublicKey().ToBase58()] = struct{}{}
		mints = append(mints, mint)
	}
	return mints, nil
}

// exchangeRates pins the live exchange rate observed for each requested
// currency code so every valuation within a single RPC uses the same rate.
type exchangeRates map[string]float64

// getExchangeRates resolves the requested currency codes against the live
// exchange rates. Codes without a live rate are omitted, so the result only
// holds currencies that can be valued. Nothing is fetched when no codes are
// requested.
func (s *server) getExchangeRates(ctx context.Context, currencyCodes []string) (exchangeRates, error) {
	if len(currencyCodes) == 0 {
		return nil, nil
	}

	liveExchangeRates, err := s.mintDataProvider.GetLiveExchangeRates(ctx)
	if err != nil {
		return nil, err
	}

	rates := make(exchangeRates, len(currencyCodes))
	for _, currencyCode := range currencyCodes {
		currencyCode = strings.ToLower(currencyCode)

		rate, ok := liveExchangeRates.Rates[currencyCode]
		if !ok {
			continue
		}
		rates[currencyCode] = rate
	}
	return rates, nil
}

// toFiatValues denominates a core mint value in every pinned currency. A nil
// map is returned when there are no pinned currencies, which leaves the proto
// field unset.
func (r exchangeRates) toFiatValues(coreMintValue uint64) map[string]float64 {
	if len(r) == 0 {
		return nil
	}

	fiatValues := make(map[string]float64, len(r))
	for currencyCode, rate := range r {
		fiatValues[currencyCode] = currency_util.CalculateFiatValueFromCoreMintQuarks(coreMintValue, rate)
	}
	return fiatValues
}

// reserveStateCache pins the live reserve state observed for each launchpad
// mint so every valuation within a single RPC uses the same supply.
type reserveStateCache map[string]*currency_util.LiveReserveStateData

func newReserveStateCache() reserveStateCache {
	return make(reserveStateCache)
}

func (s *server) getLiveReserveState(ctx context.Context, mint *common.Account, cache reserveStateCache) (*currency_util.LiveReserveStateData, error) {
	if reserveState, ok := cache[mint.PublicKey().ToBase58()]; ok {
		return reserveState, nil
	}

	reserveState, err := s.mintDataProvider.GetLiveReserveState(ctx, mint)
	if err != nil {
		return nil, err
	}
	cache[mint.PublicKey().ToBase58()] = reserveState
	return reserveState, nil
}

// calculateCoreMintValueByMint values cached holdings in each mint with a
// non-zero balance. Each balance carries the mint it holds.
func (s *server) calculateCoreMintValueByMint(ctx context.Context, balanceByTokenAccount map[string]*balance.Balance, reserveStateCache reserveStateCache) (map[string]*balancepb.MintBalance, error) {
	quarksByMint := make(map[string]uint64)
	for _, cached := range balanceByTokenAccount {
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
			reserveState, err := s.getLiveReserveState(ctx, mintAccount, reserveStateCache)
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
