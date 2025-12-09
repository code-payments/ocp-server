package currencycreator

import (
	"fmt"
	"math/big"
	"testing"
)

func assertApproxEq(t *testing.T, actual, expected *big.Float, tolerance float64, msg string) {
	t.Helper()
	diff := new(big.Float).Sub(actual, expected)
	diff.Abs(diff)
	tolFloat := big.NewFloat(tolerance)
	if diff.Cmp(tolFloat) > 0 {
		t.Errorf("%s: got %s, expected %s, diff %s exceeds tolerance %f",
			msg, actual.Text('f', 18), expected.Text('f', 18), diff.Text('f', 18), tolerance)
	}
}

func TestDiscreteSpotPrice(t *testing.T) {
	curve := DefaultDiscreteExponentialCurve()

	// Test at supply 0
	supply0 := big.NewFloat(0)
	price0 := curve.SpotPriceAtSupply(supply0)

	// Should be the first entry in the table: 10000000000000000 / 10^18 = 0.01
	expected0 := big.NewFloat(0.01)
	assertApproxEq(t, price0, expected0, 0.0000000001, "Price at supply 0")

	// Test at supply 50 (should use same price as supply 0, since both are in step 0)
	supply50 := big.NewFloat(50)
	price50 := curve.SpotPriceAtSupply(supply50)
	assertApproxEq(t, price50, expected0, 0.0000000001, "Price at supply 50")

	// Test at supply 100 (should use price from step 1)
	supply100 := big.NewFloat(100)
	price100 := curve.SpotPriceAtSupply(supply100)
	// Price at step 1 should be slightly higher than at step 0
	if price100.Cmp(price0) <= 0 {
		t.Errorf("Price at supply 100 should be > price at supply 0")
	}
}

func TestDiscreteSpotPriceAtStepBoundaries(t *testing.T) {
	curve := DefaultDiscreteExponentialCurve()

	// Test that prices change exactly at step boundaries (multiples of 100)
	for step := 0; step < 10; step++ {
		boundary := float64(step * DiscretePricingStepSize)
		supplyAtBoundary := big.NewFloat(boundary)
		priceAtBoundary := curve.SpotPriceAtSupply(supplyAtBoundary)

		// Just before next boundary should still use current step's price
		if boundary+99 < float64(len(DiscretePricingTable)*DiscretePricingStepSize) {
			supplyBeforeNext := big.NewFloat(boundary + 99)
			priceBeforeNext := curve.SpotPriceAtSupply(supplyBeforeNext)
			assertApproxEq(t, priceBeforeNext, priceAtBoundary, 0.0000000001,
				"Price at supply before next boundary should match boundary")
		}
	}
}

func TestDiscreteSpotPriceBeyondTableReturnsNil(t *testing.T) {
	curve := DefaultDiscreteExponentialCurve()

	// Supply beyond the table should return nil
	maxValidSupply := float64((len(DiscretePricingTable) - 1) * DiscretePricingStepSize)
	supplyBeyond := big.NewFloat(maxValidSupply + float64(DiscretePricingStepSize))
	price := curve.SpotPriceAtSupply(supplyBeyond)
	if price != nil {
		t.Errorf("Supply beyond table should return nil, got %s", price.Text('f', 18))
	}
}

func TestDiscreteTokensToValueZeroTokens(t *testing.T) {
	curve := DefaultDiscreteExponentialCurve()

	// Test buying 0 tokens from various supplies
	for _, supplyVal := range []float64{0, 50, 100, 1000, 10000} {
		supply := big.NewFloat(supplyVal)
		tokens := big.NewFloat(0)
		cost := curve.TokensToValue(supply, tokens)
		if cost.Cmp(big.NewFloat(0)) != 0 {
			t.Errorf("Buying 0 tokens from supply %f should cost 0, got %s",
				supplyVal, cost.Text('f', 18))
		}
	}
}

func TestDiscreteTokensToValueWithinSingleStep(t *testing.T) {
	curve := DefaultDiscreteExponentialCurve()

	// Test buying tokens entirely within a single step
	supply := big.NewFloat(0)
	tokens50 := big.NewFloat(50)
	cost50 := curve.TokensToValue(supply, tokens50)

	// Expected cost: 50 tokens * 0.01 = 0.5
	price0 := big.NewFloat(0.01)
	expected50 := new(big.Float).Mul(price0, tokens50)
	assertApproxEq(t, cost50, expected50, 0.0000001, "Cost of 50 tokens at supply 0")
}

func TestDiscreteTokensToValueExactStep(t *testing.T) {
	curve := DefaultDiscreteExponentialCurve()
	tokens100 := big.NewFloat(100)

	// Test buying exactly 100 tokens from supply 0
	supply := big.NewFloat(0)
	cost100 := curve.TokensToValue(supply, tokens100)

	// Expected cost: 100 tokens * 0.01 = 1.0
	price0 := big.NewFloat(0.01)
	expected100 := new(big.Float).Mul(price0, tokens100)
	assertApproxEq(t, cost100, expected100, 0.0000001, "Cost of 100 tokens at supply 0")
}

func TestDiscreteTokensToValueCrossingOneBoundary(t *testing.T) {
	curve := DefaultDiscreteExponentialCurve()

	// Test buying 200 tokens from supply 0 (crosses one boundary)
	supply := big.NewFloat(0)
	tokens200 := big.NewFloat(200)
	cost200 := curve.TokensToValue(supply, tokens200)

	// First 100 at price0, next 100 at price1
	// Price at step 0 and step 1 should be very close (slightly increasing)
	// Total should be slightly more than 2.0
	if cost200.Cmp(big.NewFloat(2.0)) <= 0 {
		t.Errorf("Cost of 200 tokens should be > 2.0, got %s", cost200.Text('f', 18))
	}
	if cost200.Cmp(big.NewFloat(2.01)) >= 0 {
		t.Errorf("Cost of 200 tokens should be < 2.01, got %s", cost200.Text('f', 18))
	}
}

func TestDiscreteTokensToValueIsAdditive(t *testing.T) {
	curve := DefaultDiscreteExponentialCurve()

	// Cost of buying A + B tokens should equal cost of A then cost of B from new supply
	supply := big.NewFloat(50)
	tokensA := big.NewFloat(200)
	tokensB := big.NewFloat(150)
	tokensTotal := new(big.Float).Add(tokensA, tokensB)

	costTotal := curve.TokensToValue(supply, tokensTotal)

	costA := curve.TokensToValue(supply, tokensA)
	newSupply := new(big.Float).Add(supply, tokensA)
	costB := curve.TokensToValue(newSupply, tokensB)
	costSum := new(big.Float).Add(costA, costB)

	assertApproxEq(t, costTotal, costSum, 0.0000001, "TokensToValue should be additive")
}

func TestDiscreteValueToTokensZeroValue(t *testing.T) {
	curve := DefaultDiscreteExponentialCurve()

	// Test with 0 value from various supplies
	for _, supplyVal := range []float64{0, 50, 100, 1000, 10000} {
		supply := big.NewFloat(supplyVal)
		value := big.NewFloat(0)
		tokens := curve.ValueToTokens(supply, value)
		if tokens.Cmp(big.NewFloat(0)) != 0 {
			t.Errorf("0 value from supply %f should yield 0 tokens, got %s",
				supplyVal, tokens.Text('f', 18))
		}
	}
}

func TestDiscreteValueToTokensWithinSingleStep(t *testing.T) {
	curve := DefaultDiscreteExponentialCurve()
	supply := big.NewFloat(0)
	price0 := big.NewFloat(0.01)

	// Test buying approximately 50 tokens at price0
	tokens50 := big.NewFloat(50)
	valueFor50 := new(big.Float).Mul(price0, tokens50)
	tokensResult50 := curve.ValueToTokens(supply, valueFor50)
	assertApproxEq(t, tokensResult50, tokens50, 1, "Tokens for value of 50 tokens")
}

func TestDiscreteValueToTokensCrossingBoundary(t *testing.T) {
	curve := DefaultDiscreteExponentialCurve()
	supply := big.NewFloat(0)

	// Calculate cost for 150 tokens using TokensToValue
	tokens150 := big.NewFloat(150)
	valueFor150 := curve.TokensToValue(supply, tokens150)

	// Now convert back
	tokensResult := curve.ValueToTokens(supply, valueFor150)
	assertApproxEq(t, tokensResult, tokens150, 1, "Tokens for value crossing boundary")
}

func TestDiscreteRoundtripTokensToValueToTokens(t *testing.T) {
	curve := DefaultDiscreteExponentialCurve()

	// Test roundtrip: tokens -> value -> tokens should be approximately equal
	testCases := []struct {
		supply float64
		tokens float64
	}{
		{0, 100},
		{0, 250},
		{0, 500},
		{50, 150},
		{100, 200},
		{1000, 1000},
		{10000, 5000},
	}

	for _, tc := range testCases {
		supply := big.NewFloat(tc.supply)
		tokens := big.NewFloat(tc.tokens)

		// Convert tokens to value
		value := curve.TokensToValue(supply, tokens)
		if value == nil {
			t.Errorf("TokensToValue returned nil for supply=%f, tokens=%f", tc.supply, tc.tokens)
			continue
		}

		// Convert value back to tokens
		tokensBack := curve.ValueToTokens(supply, value)
		if tokensBack == nil {
			t.Errorf("ValueToTokens returned nil for supply=%f, value=%s", tc.supply, value.Text('f', 18))
			continue
		}

		// Should get approximately the same number of tokens back
		assertApproxEq(t, tokensBack, tokens, 1,
			"Roundtrip tokens->value->tokens")
	}
}

func TestDiscreteSpotPriceMatchesTokensToValueForSmallAmounts(t *testing.T) {
	curve := DefaultDiscreteExponentialCurve()

	// For very small purchases within a single step, the cost should be
	// exactly tokens * spot_price
	for step := 0; step < 10; step++ {
		supply := big.NewFloat(float64(step * DiscretePricingStepSize))
		tokens1 := big.NewFloat(1)

		spotPrice := curve.SpotPriceAtSupply(supply)
		cost := curve.TokensToValue(supply, tokens1)

		assertApproxEq(t, spotPrice, cost, 0.0000001,
			"Cost of 1 token should equal spot price")
	}
}

func TestDiscreteBuyingInPartsEqualsBuyingAllAtOnce(t *testing.T) {
	curve := DefaultDiscreteExponentialCurve()
	supply := big.NewFloat(0)

	// Buying 100 + 200 + 150 should equal buying 450
	tokens100 := big.NewFloat(100)
	tokens200 := big.NewFloat(200)
	tokens150 := big.NewFloat(150)
	tokens450 := big.NewFloat(450)

	cost100 := curve.TokensToValue(supply, tokens100)
	supplyAfter100 := new(big.Float).Add(supply, tokens100)

	cost200 := curve.TokensToValue(supplyAfter100, tokens200)
	supplyAfter300 := new(big.Float).Add(supplyAfter100, tokens200)

	cost150 := curve.TokensToValue(supplyAfter300, tokens150)

	totalCostParts := new(big.Float).Add(cost100, cost200)
	totalCostParts.Add(totalCostParts, cost150)

	totalCostOnce := curve.TokensToValue(supply, tokens450)

	assertApproxEq(t, totalCostParts, totalCostOnce, 0.0000001,
		"Buying in parts should equal buying all at once")
}

func TestDiscreteLargePurchaseAcrossManySteps(t *testing.T) {
	curve := DefaultDiscreteExponentialCurve()
	supply := big.NewFloat(1234567)

	// Buy a large amount that spans many steps
	largeTokens := big.NewFloat(10000) // 100 steps
	value := curve.TokensToValue(supply, largeTokens)

	// Verify it's positive
	if value == nil || value.Cmp(big.NewFloat(0)) <= 0 {
		t.Errorf("Large purchase should have positive value")
		return
	}

	// Verify roundtrip is close
	tokensBack := curve.ValueToTokens(supply, value)
	assertApproxEq(t, tokensBack, largeTokens, 1, "Large purchase roundtrip")
}

func TestDiscreteTokensToValueConsistencyWithCumulativeTable(t *testing.T) {
	curve := DefaultDiscreteExponentialCurve()

	// Verify that buying from 0 to step boundary equals cumulative table
	for _, step := range []int{0, 10, 100, 1000, 10000} {
		if step >= len(DiscreteCumulativeValueTable) {
			continue
		}

		supply := big.NewFloat(0)
		tokens := big.NewFloat(float64(step * DiscretePricingStepSize))

		value := curve.TokensToValue(supply, tokens)

		// Get expected from cumulative table
		cumulativeScaled := DiscreteCumulativeValueTable[step]
		scale := new(big.Int).Exp(big.NewInt(10), big.NewInt(DiscretePriceDecimals), nil)
		expected := new(big.Float).SetPrec(defaultCurvePrec).SetInt(cumulativeScaled)
		expected.Quo(expected, new(big.Float).SetInt(scale))

		assertApproxEq(t, value, expected, 0.0000001,
			"TokensToValue should match cumulative table")
	}
}

func TestDiscreteFractionalTokensToValue(t *testing.T) {
	curve := DefaultDiscreteExponentialCurve()

	// Test that fractional tokens are handled correctly without rounding
	supply := big.NewFloat(0)
	price0 := big.NewFloat(0.01)

	// Test buying 50.5 tokens at price0
	tokens := big.NewFloat(50.5)
	cost := curve.TokensToValue(supply, tokens)

	expected := new(big.Float).Mul(price0, tokens) // 0.505
	assertApproxEq(t, cost, expected, 0.0000000001, "Cost of 50.5 fractional tokens")

	// Test buying 0.1 tokens
	tokensTiny := big.NewFloat(0.1)
	costTiny := curve.TokensToValue(supply, tokensTiny)
	expectedTiny := new(big.Float).Mul(price0, tokensTiny) // 0.001
	assertApproxEq(t, costTiny, expectedTiny, 0.0000000001, "Cost of 0.1 fractional tokens")

	// Test fractional tokens across a step boundary
	// From supply 99.5, buy 1.0 tokens (ends at 100.5)
	supplyNearBoundary := big.NewFloat(99.5)
	tokensCrossBoundary := big.NewFloat(1.0)
	costCross := curve.TokensToValue(supplyNearBoundary, tokensCrossBoundary)

	// 0.5 tokens at step 0 price + 0.5 tokens at step 1 price
	step0Price := big.NewFloat(0.01)
	step1Price := curve.SpotPriceAtSupply(big.NewFloat(100))
	expectedCross := new(big.Float).Add(
		new(big.Float).Mul(big.NewFloat(0.5), step0Price),
		new(big.Float).Mul(big.NewFloat(0.5), step1Price),
	)
	assertApproxEq(t, costCross, expectedCross, 0.0000000001, "Cost crossing step boundary with fractional tokens")
}

func TestDiscreteFractionalValueToTokens(t *testing.T) {
	curve := DefaultDiscreteExponentialCurve()

	// Test that fractional tokens are returned without rounding
	supply := big.NewFloat(0)
	price0 := big.NewFloat(0.01)

	// Value for 50.5 tokens should return 50.5 tokens
	valueFor50_5 := new(big.Float).Mul(price0, big.NewFloat(50.5))
	tokens := curve.ValueToTokens(supply, valueFor50_5)
	assertApproxEq(t, tokens, big.NewFloat(50.5), 0.0000000001, "Tokens for value of 50.5 tokens")

	// Value for 0.1 tokens should return 0.1 tokens
	valueFor0_1 := new(big.Float).Mul(price0, big.NewFloat(0.1))
	tokensTiny := curve.ValueToTokens(supply, valueFor0_1)
	assertApproxEq(t, tokensTiny, big.NewFloat(0.1), 0.0000000001, "Tokens for value of 0.1 tokens")

	// Value for 0.001 tokens should return 0.001 tokens
	valueFor0_001 := new(big.Float).Mul(price0, big.NewFloat(0.001))
	tokensVeryTiny := curve.ValueToTokens(supply, valueFor0_001)
	assertApproxEq(t, tokensVeryTiny, big.NewFloat(0.001), 0.0000000001, "Tokens for value of 0.001 tokens")
}

func TestDiscreteFractionalRoundtrip(t *testing.T) {
	curve := DefaultDiscreteExponentialCurve()

	// Test roundtrip with fractional tokens: tokens -> value -> tokens should be equal
	testCases := []struct {
		supply float64
		tokens float64
	}{
		{0, 50.5},
		{0, 0.1},
		{0, 0.001},
		{50.25, 100.75},
		{99.5, 150.3},
		{1000.123, 500.456},
		{10000.5, 1234.567},
	}

	for _, tc := range testCases {
		supply := big.NewFloat(tc.supply)
		tokens := big.NewFloat(tc.tokens)

		// Convert tokens to value
		value := curve.TokensToValue(supply, tokens)
		if value == nil {
			t.Errorf("TokensToValue returned nil for supply=%f, tokens=%f", tc.supply, tc.tokens)
			continue
		}

		// Convert value back to tokens
		tokensBack := curve.ValueToTokens(supply, value)
		if tokensBack == nil {
			t.Errorf("ValueToTokens returned nil for supply=%f, value=%s", tc.supply, value.Text('f', 18))
			continue
		}

		// Should get exactly the same number of tokens back (within floating point precision)
		assertApproxEq(t, tokensBack, tokens, 0.0000000001,
			fmt.Sprintf("Roundtrip fractional tokens->value->tokens for supply=%f, tokens=%f", tc.supply, tc.tokens))
	}
}

func TestDiscreteFractionalSupply(t *testing.T) {
	curve := DefaultDiscreteExponentialCurve()

	// Test that fractional supply values are handled correctly
	// Buy tokens starting from a fractional supply
	supplyFractional := big.NewFloat(50.25)
	tokens := big.NewFloat(10)
	cost := curve.TokensToValue(supplyFractional, tokens)

	// All tokens are within step 0, so cost should be tokens * price0
	price0 := big.NewFloat(0.01)
	expected := new(big.Float).Mul(tokens, price0)
	assertApproxEq(t, cost, expected, 0.0000000001, "Cost from fractional supply")

	// Test ValueToTokens with fractional supply
	value := big.NewFloat(0.1) // should buy 10 tokens at price0
	tokensBack := curve.ValueToTokens(supplyFractional, value)
	assertApproxEq(t, tokensBack, big.NewFloat(10), 0.0000000001, "Tokens from fractional supply")
}

func TestGenerateDiscreteCurveTable(t *testing.T) {
	t.Skip()

	curve := DefaultDiscreteExponentialCurve()

	fmt.Println("|------|----------------|----------------------------------|----------------------------|")
	fmt.Println("| %    | S              | R(S)                             | R'(S)                      |")
	fmt.Println("|------|----------------|----------------------------------|----------------------------|")

	zero := big.NewFloat(0)
	buyAmount := big.NewFloat(210000)
	supply := new(big.Float).Copy(zero)

	for i := 0; i <= 100; i++ {
		cost := curve.TokensToValue(zero, supply)
		spotPrice := curve.SpotPriceAtSupply(supply)

		fmt.Printf("| %3d%% | %14s | %32s | %26s |\n",
			i,
			supply.Text('f', 0),
			cost.Text('f', DefaultCurveDecimals),
			spotPrice.Text('f', DefaultCurveDecimals))

		supply = supply.Add(supply, buyAmount)
	}

	fmt.Println("|------|----------------|----------------------------------|----------------------------|")
}
