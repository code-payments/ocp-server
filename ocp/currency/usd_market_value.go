package currency

import (
	"context"
	"time"

	currency_lib "github.com/code-payments/ocp-server/currency"
	"github.com/code-payments/ocp-server/ocp/common"
	ocp_data "github.com/code-payments/ocp-server/ocp/data"
	"github.com/code-payments/ocp-server/solana/currencycreator"
)

// CalculateUsdMarketValue calculates the current USD market value of a crypto
// amount in quarks.
func CalculateUsdMarketValue(ctx context.Context, data ocp_data.Provider, mint *common.Account, quarks uint64, at time.Time) (float64, float64, error) {
	_, err := common.GetVmConfigForMint(ctx, data, mint)
	if err != nil {
		return 0, 0, err
	}

	usdExchangeRecord, err := data.GetExchangeRate(ctx, currency_lib.USD, at)
	if err != nil {
		return 0, 0, err
	}

	coreMintQuarksPerUnit := common.GetMintQuarksPerUnit(common.CoreMintAccount)

	if common.IsCoreMint(mint) {
		units := float64(quarks) / float64(coreMintQuarksPerUnit)
		marketValue := usdExchangeRecord.Rate * units
		return marketValue, usdExchangeRecord.Rate, nil
	}

	reserveRecord, err := data.GetCurrencyReserveAtTime(ctx, mint.PublicKey().ToBase58(), at)
	if err != nil {
		return 0, 0, err
	}

	coreMintSellValueInQuarks, _ := currencycreator.EstimateSell(&currencycreator.EstimateSellArgs{
		CurrentSupplyInQuarks: reserveRecord.SupplyFromBonding,
		SellAmountInQuarks:    quarks,
		ValueMintDecimals:     uint8(common.CoreMintDecimals),
		SellFeeBps:            0,
	})

	coreMintSellValueInUnits := float64(coreMintSellValueInQuarks) / float64(coreMintQuarksPerUnit)
	otherMintUnits := float64(quarks) / float64(common.GetMintQuarksPerUnit(mint))
	marketValue := usdExchangeRecord.Rate * coreMintSellValueInUnits
	rate := marketValue / otherMintUnits
	return marketValue, rate, nil
}
