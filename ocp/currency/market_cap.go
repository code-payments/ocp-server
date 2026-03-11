package currency

import "github.com/code-payments/ocp-server/solana/currencycreator"

// CalculateMarketCap calculates the market cap for a currency creator mint.
// Market cap = price per token × circulating supply.
func CalculateMarketCap(supplyFromBonding uint64, exchangeRate float64) float64 {
	// Calculate the spot price per token using the bonding curve
	// This returns the price in core mint tokens per currency creator token
	spotPrice := currencycreator.EstimateCurrentPrice(supplyFromBonding)
	spotPriceFloat, _ := spotPrice.Float64()

	// Convert spot price to the requested currency
	// spotPriceFloat is in core mint tokens, exchangeRate is currency per core mint token
	pricePerToken := spotPriceFloat * exchangeRate

	// Calculate circulating supply in token units (not quarks)
	circulatingSupply := float64(supplyFromBonding) / float64(currencycreator.DefaultMintQuarksPerUnit)

	// Market cap = price per token × circulating supply
	return pricePerToken * circulatingSupply
}
