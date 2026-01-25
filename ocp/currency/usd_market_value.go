package currency

import (
	"context"
	"errors"
	"math/big"
	"time"

	currency_lib "github.com/code-payments/ocp-server/currency"
	"github.com/code-payments/ocp-server/ocp/common"
	ocp_data "github.com/code-payments/ocp-server/ocp/data"
	"github.com/code-payments/ocp-server/ocp/data/currency"
	"github.com/code-payments/ocp-server/solana/currencycreator"
)

// CalculateUsdMarketValueFromFiatAmount calculates the USD market value for a given
// fiat value and exchange rate.
func CalculateUsdMarketValueFromFiatAmount(fiatAmount, fiatToUsdRate float64) (float64, error) {
	if !common.IsCoreMintUsdStableCoin() {
		return 0, errors.New("non-usd stable coin not implemented")
	}

	fiatAmountBig := big.NewFloat(fiatAmount).SetPrec(defaultPrecision)
	fiatToUsdRateBig := big.NewFloat(fiatToUsdRate).SetPrec(defaultPrecision)
	usdMarketValue, _ := new(big.Float).Quo(fiatAmountBig, fiatToUsdRateBig).Float64()
	return usdMarketValue, nil
}

// CalculateUsdMarketValueFromTokenAmount calculates the current USD market value
// of a crypto amount in quarks.
func CalculateUsdMarketValueFromTokenAmount(ctx context.Context, data ocp_data.Provider, mint *common.Account, quarks uint64, at time.Time) (float64, error) {
	isSupportedMint, err := common.IsSupportedMint(ctx, data, mint)
	if err != nil {
		return 0, err
	} else if !isSupportedMint {
		return 0, common.ErrUnsupportedMint
	}

	coreMintQuarksPerUnitBig := big.NewFloat(float64(common.GetMintQuarksPerUnit(common.CoreMintAccount))).SetPrec(defaultPrecision)

	var exchangeRateRecord *currency.ExchangeRateRecord
	if common.IsCoreMintUsdStableCoin() {
		exchangeRateRecord = &currency.ExchangeRateRecord{
			Symbol: string(currency_lib.USD),
			Rate:   1.0,
			Time:   at,
		}
	} else {
		exchangeRateRecord, err = data.GetExchangeRate(ctx, currency_lib.USD, at)
		if err != nil {
			return 0, err
		}
	}
	exchangeRateBig := big.NewFloat(exchangeRateRecord.Rate).SetPrec(defaultPrecision)

	if common.IsCoreMint(mint) {
		quarksBig := big.NewFloat(float64(quarks)).SetPrec(defaultPrecision)
		unitsBig := new(big.Float).Quo(quarksBig, coreMintQuarksPerUnitBig)
		marketValue, _ := new(big.Float).Mul(exchangeRateBig, unitsBig).Float64()
		return marketValue, nil
	}

	reserveRecord, err := data.GetCurrencyReserveAtTime(ctx, mint.PublicKey().ToBase58(), at)
	if err != nil {
		return 0, err
	}

	coreMintSellValueInQuarks, _ := currencycreator.EstimateSell(&currencycreator.EstimateSellArgs{
		CurrentSupplyInQuarks: reserveRecord.SupplyFromBonding,
		SellAmountInQuarks:    quarks,
		ValueMintDecimals:     uint8(common.CoreMintDecimals),
		SellFeeBps:            0,
	})
	coreMintSellValueInQuarksBig := big.NewFloat(float64(coreMintSellValueInQuarks)).SetPrec(defaultPrecision)

	coreMintSellValueInUnitsBig := new(big.Float).Quo(coreMintSellValueInQuarksBig, coreMintQuarksPerUnitBig)
	marketValue, _ := new(big.Float).Mul(exchangeRateBig, coreMintSellValueInUnitsBig).Float64()
	return marketValue, nil
}
