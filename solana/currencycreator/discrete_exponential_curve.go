package currencycreator

import (
	"math/big"
)

const (
	// DefaultCurveDecimals is the number of decimal places supported by the curve
	DefaultCurveDecimals = 18

	// DiscretePricingStepSize is the token supply interval between price steps
	DiscretePricingStepSize = 100

	// DiscretePriceDecimals is the number of decimal places for scaled prices (18 decimals)
	DiscretePriceDecimals = DefaultCurveDecimals

	defaultCurvePrec = 128
)

// DiscreteExponentialCurve implements a discrete pricing curve using pre-computed tables.
// Prices are stored as scaled u128 values with 18 decimal precision.
type DiscreteExponentialCurve struct {
	scale *big.Int // 10^18 for converting between scaled and unscaled values
}

// DefaultDiscreteExponentialCurve creates a new default discrete exponential curve instance.
func DefaultDiscreteExponentialCurve() *DiscreteExponentialCurve {
	scale := new(big.Int).Exp(big.NewInt(10), big.NewInt(DiscretePriceDecimals), nil)
	return &DiscreteExponentialCurve{scale: scale}
}

// SpotPriceAtSupply returns the current price per token at the given supply level.
// Returns nil if the supply is beyond the table bounds.
func (c *DiscreteExponentialCurve) SpotPriceAtSupply(currentSupply *big.Float) *big.Float {
	supplyInt, _ := currentSupply.Int(nil)
	stepIndex := new(big.Int).Div(supplyInt, big.NewInt(DiscretePricingStepSize)).Int64()

	if stepIndex < 0 || int(stepIndex) >= len(DiscretePricingTable) {
		return nil
	}

	priceScaled := DiscretePricingTable[stepIndex]
	price := new(big.Float).SetPrec(defaultCurvePrec).SetInt(priceScaled)
	return price.Quo(price, new(big.Float).SetInt(c.scale))
}

// TokensToValue calculates the total cost to buy `tokens` starting at `currentSupply`.
// This is equivalent to "How much does it cost to get X tokens?"
// Returns nil if the purchase would exceed table bounds.
// Supports fractional tokens - does not round up or down.
func (c *DiscreteExponentialCurve) TokensToValue(currentSupply, tokens *big.Float) *big.Float {
	zero := big.NewFloat(0)
	if tokens.Cmp(zero) == 0 {
		return big.NewFloat(0)
	}

	endSupply := new(big.Float).SetPrec(defaultCurvePrec).Add(currentSupply, tokens)

	stepSizeFloat := big.NewFloat(DiscretePricingStepSize)
	scaleFloat := new(big.Float).SetPrec(defaultCurvePrec).SetInt(c.scale)

	// Calculate start and end steps (integer division, truncating toward zero)
	startStepFloat := new(big.Float).SetPrec(defaultCurvePrec).Quo(currentSupply, stepSizeFloat)
	startStepInt, _ := startStepFloat.Int(nil)
	startStep := startStepInt.Int64()

	endStepFloat := new(big.Float).SetPrec(defaultCurvePrec).Quo(endSupply, stepSizeFloat)
	endStepInt, _ := endStepFloat.Int(nil)
	endStep := endStepInt.Int64()

	if endStep < 0 || int(endStep) >= len(DiscretePricingTable) {
		return nil
	}

	// Calculate partial tokens in start step (from current_supply to next step boundary)
	startStepBoundary := new(big.Float).SetPrec(defaultCurvePrec).Mul(big.NewFloat(float64(startStep+1)), stepSizeFloat)
	var tokensInStartStep *big.Float
	if startStepBoundary.Cmp(endSupply) > 0 {
		// All tokens are within the same step
		tokensInStartStep = new(big.Float).SetPrec(defaultCurvePrec).Copy(tokens)
	} else {
		tokensInStartStep = new(big.Float).SetPrec(defaultCurvePrec).Sub(startStepBoundary, currentSupply)
	}

	// Calculate partial tokens in end step (from end step boundary to end_supply)
	endStepBoundary := new(big.Float).SetPrec(defaultCurvePrec).Mul(big.NewFloat(float64(endStep)), stepSizeFloat)
	tokensInEndStep := new(big.Float).SetPrec(defaultCurvePrec).Sub(endSupply, endStepBoundary)

	// Cost for partial start step (tokens * price / scale)
	startPrice := new(big.Float).SetPrec(defaultCurvePrec).SetInt(DiscretePricingTable[startStep])
	startCost := new(big.Float).SetPrec(defaultCurvePrec).Mul(tokensInStartStep, startPrice)
	startCost.Quo(startCost, scaleFloat)

	// If start and end are in the same step, we're done
	if startStep == endStep {
		return startCost
	}

	// Cost for complete steps between start_step+1 and end_step-1 (inclusive)
	// Use cumulative table: cumulative[end_step] - cumulative[start_step + 1]
	cumulativeStart := new(big.Float).SetPrec(defaultCurvePrec).SetInt(DiscreteCumulativeValueTable[startStep+1])
	cumulativeEnd := new(big.Float).SetPrec(defaultCurvePrec).SetInt(DiscreteCumulativeValueTable[endStep])
	middleCost := new(big.Float).SetPrec(defaultCurvePrec).Sub(cumulativeEnd, cumulativeStart)
	middleCost.Quo(middleCost, scaleFloat)

	// Cost for partial end step
	endPrice := new(big.Float).SetPrec(defaultCurvePrec).SetInt(DiscretePricingTable[endStep])
	endCost := new(big.Float).SetPrec(defaultCurvePrec).Mul(tokensInEndStep, endPrice)
	endCost.Quo(endCost, scaleFloat)

	// Total cost
	totalCost := new(big.Float).SetPrec(defaultCurvePrec).Add(startCost, middleCost)
	totalCost.Add(totalCost, endCost)

	return totalCost
}

// ValueToTokens calculates the number of tokens received for a given value starting at currentSupply.
// This is equivalent to "How many tokens can I get for Y value?"
// Returns nil if the supply is beyond table bounds.
// Supports fractional tokens - does not round up or down.
func (c *DiscreteExponentialCurve) ValueToTokens(currentSupply, value *big.Float) *big.Float {
	zero := big.NewFloat(0)
	if value.Cmp(zero) == 0 {
		return big.NewFloat(0)
	}

	stepSizeFloat := big.NewFloat(DiscretePricingStepSize)
	scaleFloat := new(big.Float).SetPrec(defaultCurvePrec).SetInt(c.scale)

	// Calculate start step
	startStepFloat := new(big.Float).SetPrec(defaultCurvePrec).Quo(currentSupply, stepSizeFloat)
	startStepInt, _ := startStepFloat.Int(nil)
	startStep := startStepInt.Int64()

	if startStep < 0 || int(startStep) >= len(DiscretePricingTable)-1 {
		return nil
	}

	// Convert value to scaled float for comparison with scaled prices
	valueScaled := new(big.Float).SetPrec(defaultCurvePrec).Mul(value, scaleFloat)

	// Calculate tokens available in current partial step and their cost
	startStepBoundary := new(big.Float).SetPrec(defaultCurvePrec).Mul(big.NewFloat(float64(startStep+1)), stepSizeFloat)
	tokensToCompleteStartStep := new(big.Float).SetPrec(defaultCurvePrec).Sub(startStepBoundary, currentSupply)
	startPrice := new(big.Float).SetPrec(defaultCurvePrec).SetInt(DiscretePricingTable[startStep])
	costToCompleteStartStep := new(big.Float).SetPrec(defaultCurvePrec).Mul(tokensToCompleteStartStep, startPrice)

	// If we can't even complete the start step, divide value by price for fractional tokens
	if valueScaled.Cmp(costToCompleteStartStep) < 0 {
		tokens := new(big.Float).SetPrec(defaultCurvePrec).Quo(valueScaled, startPrice)
		return tokens
	}

	// We can at least complete the start step
	remainingValueScaled := new(big.Float).SetPrec(defaultCurvePrec).Sub(valueScaled, costToCompleteStartStep)

	// Calculate the cumulative value at start_step + 1 (where we'll be after completing start step)
	baseCumulative := new(big.Float).SetPrec(defaultCurvePrec).SetInt(DiscreteCumulativeValueTable[startStep+1])

	// Target cumulative = base_cumulative + remaining_value
	targetCumulative := new(big.Float).SetPrec(defaultCurvePrec).Add(baseCumulative, remainingValueScaled)

	// Convert to big.Int for binary search comparison
	targetCumulativeInt, _ := targetCumulative.Int(nil)

	// Binary search for the step where cumulative value exceeds or equals target
	low := int(startStep + 1)
	high := len(DiscreteCumulativeValueTable) - 1

	for low < high {
		mid := (low + high + 1) / 2
		midCumulative := DiscreteCumulativeValueTable[mid]

		if midCumulative.Cmp(targetCumulativeInt) <= 0 {
			low = mid
		} else {
			high = mid - 1
		}
	}

	// low is now the last step where cumulative <= target
	endStep := low

	if endStep >= len(DiscretePricingTable) {
		return nil
	}

	// Calculate tokens from complete steps
	endStepSupply := new(big.Float).SetPrec(defaultCurvePrec).Mul(big.NewFloat(float64(endStep)), stepSizeFloat)
	tokensFromCompleteSteps := new(big.Float).SetPrec(defaultCurvePrec).Sub(endStepSupply, startStepBoundary)

	// Calculate remaining value after complete steps
	cumulativeAtEndStep := new(big.Float).SetPrec(defaultCurvePrec).SetInt(DiscreteCumulativeValueTable[endStep])
	valueUsedForCompleteSteps := new(big.Float).SetPrec(defaultCurvePrec).Sub(cumulativeAtEndStep, baseCumulative)
	remainingValue := new(big.Float).SetPrec(defaultCurvePrec).Sub(remainingValueScaled, valueUsedForCompleteSteps)

	// Buy fractional tokens in end step with remaining value
	endPrice := new(big.Float).SetPrec(defaultCurvePrec).SetInt(DiscretePricingTable[endStep])
	tokensInEndStep := new(big.Float).SetPrec(defaultCurvePrec).Quo(remainingValue, endPrice)

	// Total tokens = tokens to complete start step + tokens from complete middle steps + fractional tokens in end step
	totalTokens := new(big.Float).SetPrec(defaultCurvePrec).Add(tokensToCompleteStartStep, tokensFromCompleteSteps)
	totalTokens.Add(totalTokens, tokensInEndStep)

	return totalTokens
}
