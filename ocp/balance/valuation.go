package balance

import (
	"context"

	"github.com/code-payments/ocp-server/ocp/common"
	currency_util "github.com/code-payments/ocp-server/ocp/currency"
	ocp_data "github.com/code-payments/ocp-server/ocp/data"
	"github.com/code-payments/ocp-server/solana/currencycreator"
)

// Valuer values cached holdings in the core mint. It is the one definition
// of what an owner's balance is worth: the Balance service's GetBalances and
// its watches both value through it, so a watch can never disagree with the
// balance a client reads at the same moment.
//
// A core mint holding is worth its quarks. A launchpad currency holding is
// worth what the bonding curve would pay to sell all of it at the mint's live
// reserve state, with no sell fee taken off. Nothing else is valued: the
// ledger only holds those two kinds of mint.
type Valuer struct {
	data             ocp_data.Provider
	mintDataProvider *currency_util.MintDataProvider
}

func NewValuer(data ocp_data.Provider, mintDataProvider *currency_util.MintDataProvider) *Valuer {
	return &Valuer{
		data:             data,
		mintDataProvider: mintDataProvider,
	}
}

// ReserveStateCache pins the live reserve state observed for each launchpad
// mint, so every valuation within one operation — one RPC, one evaluation of
// an owner — uses the same supply for a mint even if the mint data provider
// refreshes midway. Make one per operation with NewReserveStateCache.
type ReserveStateCache map[string]*currency_util.LiveReserveStateData

func NewReserveStateCache() ReserveStateCache {
	return make(ReserveStateCache)
}

// ValueOwner reads an owner's cached holdings, limited to mints when any are
// given, and values them: one ledger read, then ValueHoldings.
func (v *Valuer) ValueOwner(ctx context.Context, owner *common.Account, mints []*common.Account, cache ReserveStateCache) (map[string]uint64, error) {
	balanceByTokenAccount, err := BatchCalculateFromCacheByOwner(ctx, v.data, owner, mints...)
	if err != nil {
		return nil, err
	}
	return v.ValueHoldings(ctx, balanceByTokenAccount, cache)
}

// ValueHoldings values cached holdings and returns the core mint value of the
// holding in each mint, keyed by mint. Every account in a mint is summed
// before the mint is valued, since the curve pays less per token the more is
// sold. A mint holding nothing is left out.
func (v *Valuer) ValueHoldings(ctx context.Context, balanceByTokenAccount map[string]*Balance, cache ReserveStateCache) (map[string]uint64, error) {
	quarksByMint := make(map[string]uint64)
	for _, cached := range balanceByTokenAccount {
		quarksByMint[cached.MintAccount] += cached.Quarks
	}

	coreMintValueByMint := make(map[string]uint64, len(quarksByMint))
	for mint, quarks := range quarksByMint {
		if quarks == 0 {
			continue
		}

		if mint == common.CoreMintAccount.PublicKey().ToBase58() {
			coreMintValueByMint[mint] = quarks
			continue
		}

		mintAccount, err := common.NewAccountFromPublicKeyString(mint)
		if err != nil {
			return nil, err
		}
		reserveState, err := v.getLiveReserveState(ctx, mintAccount, cache)
		if err != nil {
			return nil, err
		}

		coreMintValue, _ := currencycreator.EstimateSell(&currencycreator.EstimateSellArgs{
			CurrentSupplyInQuarks: reserveState.SupplyFromBonding,
			SellAmountInQuarks:    quarks,
			ValueMintDecimals:     uint8(common.CoreMintDecimals),
			SellFeeBps:            0,
		})
		coreMintValueByMint[mint] = coreMintValue
	}
	return coreMintValueByMint, nil
}

// GetLiveExchangeRates returns the live fiat rates per core mint unit, keyed
// by lower-case currency code: the rates a fiat valuation uses. A caller
// values everything in one operation against one snapshot.
func (v *Valuer) GetLiveExchangeRates(ctx context.Context) (map[string]float64, error) {
	rates, err := v.mintDataProvider.GetLiveExchangeRates(ctx)
	if err != nil {
		return nil, err
	}
	return rates.Rates, nil
}

func (v *Valuer) getLiveReserveState(ctx context.Context, mint *common.Account, cache ReserveStateCache) (*currency_util.LiveReserveStateData, error) {
	if reserveState, ok := cache[mint.PublicKey().ToBase58()]; ok {
		return reserveState, nil
	}

	reserveState, err := v.mintDataProvider.GetLiveReserveState(ctx, mint)
	if err != nil {
		return nil, err
	}
	cache[mint.PublicKey().ToBase58()] = reserveState
	return reserveState, nil
}

// SumCoreMintValue totals per-mint core mint values, limited to mints when
// any are given. It is how a watch with a mint filter reads a valuation made
// for the whole owner: the values are per mint, so restricting the sum is
// the same as restricting the ledger read would have been.
func SumCoreMintValue(coreMintValueByMint map[string]uint64, mints []string) uint64 {
	var total uint64
	if len(mints) == 0 {
		for _, value := range coreMintValueByMint {
			total += value
		}
		return total
	}
	seen := make(map[string]struct{}, len(mints))
	for _, mint := range mints {
		if _, ok := seen[mint]; ok {
			continue
		}
		seen[mint] = struct{}{}
		total += coreMintValueByMint[mint]
	}
	return total
}
