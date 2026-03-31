package currencycreator

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEstimateCurrentPrice_CrossPlatform(t *testing.T) {
	fmt.Println(EstimateCurrentPrice(0).Text('f', DefaultCurveDecimals))
	fmt.Println(EstimateCurrentPrice(12345678_1234567890).Text('f', DefaultCurveDecimals))
	fmt.Println(EstimateCurrentPrice(DefaultMintMaxQuarkSupply).Text('f', DefaultCurveDecimals))
}

func TestEstimateBuy_CrossPlatform(t *testing.T) {
	received := EstimateBuy(&EstimateBuyArgs{
		BuyAmountInQuarks:     123_123456,       // $123.123456
		CurrentSupplyInQuarks: 98765_9876543210, // 9,876.9876543210 tokens
		ValueMintDecimals:     6,
	})
	fmt.Printf("%d total\n", received)
}

func TestEstimateSell_CrossPlatform(t *testing.T) {
	received, fees := EstimateSell(&EstimateSellArgs{
		SellAmountInQuarks:    987_9876543210,   // 987.9876543210 tokens
		CurrentSupplyInQuarks: 12345_1234567890, // 12,345.1234567890 tokens
		ValueMintDecimals:     6,
		SellFeeBps:            0, // 0%
	})
	fmt.Printf("%d total, %d received, %d fees\n", received+fees, received, fees)

	received, fees = EstimateSell(&EstimateSellArgs{
		SellAmountInQuarks:    987_9876543210,   // 987.9876543210 tokens
		CurrentSupplyInQuarks: 12345_1234567890, // 12,345.1234567890 tokens
		ValueMintDecimals:     6,
		SellFeeBps:            100, // 1%
	})
	fmt.Printf("%d total, %d received, %d fees\n", received+fees, received, fees)
}

func TestEstimateValueExchange_CrossPlatform(t *testing.T) {
	quarks := EstimateValueExchange(&EstimateValueExchangeArgs{
		CurrentSupplyInQuarks: 12345_1234567890, // 12,345.1234567890 tokens
		ValueInQuarks:         10_000000,        // $10
		ValueMintDecimals:     6,
	})
	fmt.Printf("%d quarks\n", quarks)
}

func TestEstimators_CrossCheck(t *testing.T) {
	const valueMintDecimals uint8 = 6

	type scenario struct {
		name             string
		supply           uint64 // current circulating supply in token quarks
		buyValue         uint64 // value to buy/exchange in USDC quarks
		sellTokens       uint64 // tokens to sell in token quarks
		exchangeValue    uint64 // value to exchange in USDC quarks
		sellFeeBps       uint16
		additionalValue1 uint64 // first value for additivity tests
		additionalValue2 uint64 // second value for additivity tests
	}

	scenarios := []scenario{
		{
			name:             "low_supply_mid_step",
			supply:           500_0000000000, // 500 tokens (mid step 5)
			buyValue:         1_000000,       // $1
			sellTokens:       100_0000000000, // 100 tokens
			exchangeValue:    500000,         // $0.50
			sellFeeBps:       100,            // 1%
			additionalValue1: 500000,         // $0.50
			additionalValue2: 2_000000,       // $2
		},
		{
			name:             "step_boundary_exact",
			supply:           1000_0000000000, // 1,000 tokens (exact step 10 boundary)
			buyValue:         2_000000,        // $2
			sellTokens:       200_0000000000,  // 200 tokens
			exchangeValue:    2_000000,        // $2
			sellFeeBps:       500,             // 5%
			additionalValue1: 1_000000,        // $1
			additionalValue2: 5_000000,        // $5
		},
		{
			name:             "fractional_supply",
			supply:           12345_1234567890, // 12,345.123456789 tokens
			buyValue:         50_000000,        // $50
			sellTokens:       987_9876543210,   // 987.987654321 tokens
			exchangeValue:    25_000000,        // $25
			sellFeeBps:       1000,             // 10%
			additionalValue1: 25_000000,        // $25
			additionalValue2: 50_000000,        // $50
		},
		{
			name:             "large_supply",
			supply:           1_000_000_0000000000, // 1,000,000 tokens (step 10,000)
			buyValue:         1_000_000000,         // $1,000
			sellTokens:       10_000_0000000000,    // 10,000 tokens
			exchangeValue:    1_000_000000,         // $1,000
			sellFeeBps:       2500,                 // 25%
			additionalValue1: 1_000_000000,         // $1,000
			additionalValue2: 10_000_000000,        // $10,000
		},
		{
			name:             "high_supply",
			supply:           10_000_000_0000000000, // 10,000,000 tokens (step 100,000)
			buyValue:         10_000_000000,         // $10,000
			sellTokens:       100_000_0000000000,    // 100,000 tokens
			exchangeValue:    10_000_000000,         // $10,000
			sellFeeBps:       100,                   // 1%
			additionalValue1: 10_000_000000,         // $10,000
			additionalValue2: 100_000_000000,        // $100,000
		},
		{
			name:             "near_step_boundary",
			supply:           100_0000000000 - 1, // just before step 1 boundary
			buyValue:         250000,             // $0.25
			sellTokens:       50_0000000000,      // 50 tokens
			exchangeValue:    250000,             // $0.25
			sellFeeBps:       500,                // 5%
			additionalValue1: 100000,             // $0.10
			additionalValue2: 250000,             // $0.25
		},
		{
			name:             "just_past_step_boundary",
			supply:           100_0000000001, // just past step 1 boundary
			buyValue:         250000,         // $0.25
			sellTokens:       50_0000000000,  // 50 tokens
			exchangeValue:    250000,         // $0.25
			sellFeeBps:       1000,           // 10%
			additionalValue1: 100000,         // $0.10
			additionalValue2: 250000,         // $0.25
		},
	}

	for _, sc := range scenarios {
		sc := sc
		t.Run(sc.name, func(t *testing.T) {
			// Cross-check 1: Buy then sell should recover the original value
			tokensBought := EstimateBuy(&EstimateBuyArgs{
				CurrentSupplyInQuarks: sc.supply,
				BuyAmountInQuarks:     sc.buyValue,
				ValueMintDecimals:     valueMintDecimals,
			})
			require.True(t, tokensBought > 0, "should buy tokens")

			newSupply := sc.supply + tokensBought
			sellValue, _ := EstimateSell(&EstimateSellArgs{
				CurrentSupplyInQuarks: newSupply,
				SellAmountInQuarks:    tokensBought,
				ValueMintDecimals:     valueMintDecimals,
				SellFeeBps:            0,
			})

			diff := int64(sc.buyValue) - int64(sellValue)
			require.True(t, diff >= -1 && diff <= 1,
				"buy-sell roundtrip: buyValue=%d sellValue=%d diff=%d",
				sc.buyValue, sellValue, diff)

			// Cross-check 2: Exchange tokens, then sell them to recover the same value
			tokensExchanged := EstimateValueExchange(&EstimateValueExchangeArgs{
				CurrentSupplyInQuarks: sc.supply,
				ValueInQuarks:         sc.exchangeValue,
				ValueMintDecimals:     valueMintDecimals,
			})
			require.True(t, tokensExchanged > 0, "should exchange tokens")
			require.True(t, tokensExchanged <= sc.supply, "should not exceed supply")

			exchangeSellValue, _ := EstimateSell(&EstimateSellArgs{
				CurrentSupplyInQuarks: sc.supply,
				SellAmountInQuarks:    tokensExchanged,
				ValueMintDecimals:     valueMintDecimals,
				SellFeeBps:            0,
			})

			diff = int64(sc.exchangeValue) - int64(exchangeSellValue)
			require.True(t, diff >= -1 && diff <= 1,
				"exchange-sell consistency: exchangeValue=%d sellValue=%d diff=%d",
				sc.exchangeValue, exchangeSellValue, diff)

			// Cross-check 3: Sell → exchange → sell closed loop in the value domain
			// Sell T tokens → V. Exchange V → T'. Sell T' → V'. V should equal V'.
			sellValueForTokens, _ := EstimateSell(&EstimateSellArgs{
				CurrentSupplyInQuarks: sc.supply,
				SellAmountInQuarks:    sc.sellTokens,
				ValueMintDecimals:     valueMintDecimals,
				SellFeeBps:            0,
			})
			require.True(t, sellValueForTokens > 0, "should have sell value")

			exchangeBackTokens := EstimateValueExchange(&EstimateValueExchangeArgs{
				CurrentSupplyInQuarks: sc.supply,
				ValueInQuarks:         sellValueForTokens,
				ValueMintDecimals:     valueMintDecimals,
			})
			require.True(t, exchangeBackTokens > 0, "should exchange tokens back")

			sellValueRoundtrip, _ := EstimateSell(&EstimateSellArgs{
				CurrentSupplyInQuarks: sc.supply,
				SellAmountInQuarks:    exchangeBackTokens,
				ValueMintDecimals:     valueMintDecimals,
				SellFeeBps:            0,
			})

			diff = int64(sellValueForTokens) - int64(sellValueRoundtrip)
			require.True(t, diff >= -1 && diff <= 1,
				"sell-exchange-sell roundtrip: original=%d roundtrip=%d diff=%d",
				sellValueForTokens, sellValueRoundtrip, diff)

			// Cross-check 4: After exchanging, remaining supply's sell value should equal totalValue - exchangeValue
			totalValue, _ := EstimateSell(&EstimateSellArgs{
				CurrentSupplyInQuarks: sc.supply,
				SellAmountInQuarks:    sc.supply,
				ValueMintDecimals:     valueMintDecimals,
				SellFeeBps:            0,
			})

			remainingSupply := sc.supply - tokensExchanged
			remainingValue, _ := EstimateSell(&EstimateSellArgs{
				CurrentSupplyInQuarks: remainingSupply,
				SellAmountInQuarks:    remainingSupply,
				ValueMintDecimals:     valueMintDecimals,
				SellFeeBps:            0,
			})

			expectedRemainingValue := totalValue - exchangeSellValue
			diff = int64(remainingValue) - int64(expectedRemainingValue)
			require.True(t, diff >= -1 && diff <= 1,
				"remaining value after exchange: remaining=%d expected=%d diff=%d",
				remainingValue, expectedRemainingValue, diff)

			// Cross-check 5: Sell fee decomposition (received + fees = no-fee value)
			baseValue, baseFees := EstimateSell(&EstimateSellArgs{
				CurrentSupplyInQuarks: sc.supply,
				SellAmountInQuarks:    sc.sellTokens,
				ValueMintDecimals:     valueMintDecimals,
				SellFeeBps:            0,
			})
			require.EqualValues(t, 0, baseFees)
			require.True(t, baseValue > 0, "should have sell value")

			received, fees := EstimateSell(&EstimateSellArgs{
				CurrentSupplyInQuarks: sc.supply,
				SellAmountInQuarks:    sc.sellTokens,
				ValueMintDecimals:     valueMintDecimals,
				SellFeeBps:            sc.sellFeeBps,
			})
			require.EqualValues(t, baseValue, received+fees,
				"fee decomposition: base=%d received+fees=%d", baseValue, received+fees)
			require.True(t, fees > 0, "should have fees")
			require.True(t, received < baseValue, "received should be less than base")

			// Cross-check 6: Buy additivity (buy v1 then v2 = buy v1+v2)
			tokens1 := EstimateBuy(&EstimateBuyArgs{
				CurrentSupplyInQuarks: sc.supply,
				BuyAmountInQuarks:     sc.additionalValue1,
				ValueMintDecimals:     valueMintDecimals,
			})
			tokens2 := EstimateBuy(&EstimateBuyArgs{
				CurrentSupplyInQuarks: sc.supply + tokens1,
				BuyAmountInQuarks:     sc.additionalValue2,
				ValueMintDecimals:     valueMintDecimals,
			})
			tokensTotal := EstimateBuy(&EstimateBuyArgs{
				CurrentSupplyInQuarks: sc.supply,
				BuyAmountInQuarks:     sc.additionalValue1 + sc.additionalValue2,
				ValueMintDecimals:     valueMintDecimals,
			})

			diff = int64(tokens1+tokens2) - int64(tokensTotal)
			require.True(t, diff >= -2 && diff <= 2,
				"buy additivity: parts=%d whole=%d diff=%d",
				tokens1+tokens2, tokensTotal, diff)

			// Cross-check 7: Exchange additivity (exchange v1 then v2 = exchange v1+v2)
			exchangeTokens1 := EstimateValueExchange(&EstimateValueExchangeArgs{
				CurrentSupplyInQuarks: sc.supply,
				ValueInQuarks:         sc.additionalValue1,
				ValueMintDecimals:     valueMintDecimals,
			})
			require.True(t, exchangeTokens1 > 0 && exchangeTokens1 <= sc.supply,
				"first exchange should produce valid tokens")

			exchangeTokens2 := EstimateValueExchange(&EstimateValueExchangeArgs{
				CurrentSupplyInQuarks: sc.supply - exchangeTokens1,
				ValueInQuarks:         sc.additionalValue2,
				ValueMintDecimals:     valueMintDecimals,
			})
			require.True(t, exchangeTokens2 > 0,
				"second exchange should produce tokens")

			exchangeTokensTotal := EstimateValueExchange(&EstimateValueExchangeArgs{
				CurrentSupplyInQuarks: sc.supply,
				ValueInQuarks:         sc.additionalValue1 + sc.additionalValue2,
				ValueMintDecimals:     valueMintDecimals,
			})

			diff = int64(exchangeTokens1+exchangeTokens2) - int64(exchangeTokensTotal)
			require.True(t, diff >= -2 && diff <= 2,
				"exchange additivity: parts=%d whole=%d diff=%d",
				exchangeTokens1+exchangeTokens2, exchangeTokensTotal, diff)

			// Cross-check 8: Buy and exchange are consistent
			// Buying V gives T tokens at supply S. Exchanging V at supply S+T should return T tokens.
			exchangeFromBuy := EstimateValueExchange(&EstimateValueExchangeArgs{
				CurrentSupplyInQuarks: newSupply,
				ValueInQuarks:         sc.buyValue,
				ValueMintDecimals:     valueMintDecimals,
			})

			diff = int64(tokensBought) - int64(exchangeFromBuy)
			require.True(t, diff >= -1 && diff <= 1,
				"buy-exchange consistency: bought=%d exchangeFromBuy=%d diff=%d",
				tokensBought, exchangeFromBuy, diff)
		})
	}
}

func TestEstimates_ExtremeValues(t *testing.T) {
	price := EstimateCurrentPrice(22_000_000_0000000000) // 22mm tokens
	require.Equal(t, "999999.999988634528021217", price.Text('f', DefaultCurveDecimals))

	received := EstimateBuy(&EstimateBuyArgs{
		BuyAmountInQuarks:     2_000_000_000_000_000_000, // $2T
		CurrentSupplyInQuarks: 0,
		ValueMintDecimals:     6,
	})
	require.EqualValues(t, DefaultMintMaxQuarkSupply, received)

	received = EstimateBuy(&EstimateBuyArgs{
		BuyAmountInQuarks:     2_000_000_000_000_000000, // $2T
		CurrentSupplyInQuarks: 10_000_000_0000000000,    // 10mm tokens
		ValueMintDecimals:     6,
	})
	require.EqualValues(t, 11_000_000_0000000000, received)

	received, _ = EstimateSell(&EstimateSellArgs{
		SellAmountInQuarks:    2000_0000000000, // 2000 tokens
		CurrentSupplyInQuarks: 1000_0000000000, // 1000 tokens
		ValueMintDecimals:     6,
		SellFeeBps:            0, // 0%
	})
	require.EqualValues(t, 20016675, received)
}
