package currency

import (
	"crypto/ed25519"
	"math"
	"math/big"
	"time"

	currencypb "github.com/code-payments/ocp-protobuf-api/generated/go/currency/v1"
	transactionpb "github.com/code-payments/ocp-protobuf-api/generated/go/transaction/v1"

	currency_lib "github.com/code-payments/ocp-server/currency"
	"github.com/code-payments/ocp-server/ocp/auth"
	"github.com/code-payments/ocp-server/ocp/common"
	"github.com/code-payments/ocp-server/solana/currencycreator"
)

const (
	defaultPrecision = 128

	// maxVerifiedExchangeRateAge is the maximum age of a verified exchange rate
	maxVerifiedExchangeRateAge = 15 * time.Minute

	// maxVerifiedReserveStateAge is the maximum age of a verified reserve state
	maxVerifiedReserveStateAge = 2 * time.Minute
)

// ValidateVerifiedExchangeRate validates a server-signed exchange rate provided by a client.
// It verifies that the signature is valid and that the timestamp is not too far in the past.
func ValidateVerifiedExchangeRate(proto *currencypb.VerifiedCoreMintFiatExchangeRate) (bool, string) {
	if proto == nil || proto.ExchangeRate == nil || proto.Signature == nil {
		return false, "missing required fields"
	}

	// Verify the timestamp is not too old
	if proto.ExchangeRate.Timestamp == nil {
		return false, "missing timestamp"
	}

	age := time.Since(proto.ExchangeRate.Timestamp.AsTime())
	if age > maxVerifiedExchangeRateAge {
		return false, "exchange rate is stale"
	}

	// Verify the signature
	messageBytes, err := auth.ForceConsistentMarshal(proto.ExchangeRate)
	if err != nil {
		return false, "failed to marshal exchange rate"
	}

	if !ed25519.Verify(common.GetSubsidizer().PublicKey().ToBytes(), messageBytes, proto.Signature.Value) {
		return false, "invalid signature"
	}

	return true, ""
}

// ValidateVerifiedReserveState validates a server-signed reserve state provided by a client.
// It verifies that the signature is valid and that the timestamp is not too far in the past.
func ValidateVerifiedReserveState(proto *currencypb.VerifiedLaunchpadCurrencyReserveState) (bool, string) {
	if proto == nil || proto.ReserveState == nil || proto.Signature == nil {
		return false, "missing required fields"
	}

	// Verify the timestamp is not too old
	if proto.ReserveState.Timestamp == nil {
		return false, "missing timestamp"
	}

	age := time.Since(proto.ReserveState.Timestamp.AsTime())
	if age > maxVerifiedReserveStateAge {
		return false, "reserve state is stale"
	}

	// Verify the signature
	messageBytes, err := auth.ForceConsistentMarshal(proto.ReserveState)
	if err != nil {
		return false, "failed to marshal reserve state"
	}

	if !ed25519.Verify(common.GetSubsidizer().PublicKey().ToBytes(), messageBytes, proto.Signature.Value) {
		return false, "invalid signature"
	}

	return true, ""
}

// ValidateClientExchangeData validates client-provided exchange data with
// provable exchange rates and reserve states that were provided by server.
func ValidateClientExchangeData(proto *transactionpb.VerifiedExchangeData) (bool, string) {
	if proto == nil {
		return false, "verified exchange data is required"
	}

	// Validate mint is present
	if proto.Mint == nil {
		return false, "mint is required"
	}

	mintAccount, err := common.NewAccountFromProto(proto.Mint)
	if err != nil {
		return false, "invalid mint"
	}

	// Validate quarks
	if proto.Quarks == 0 {
		return false, "quarks must be greater than zero"
	}

	// Validate native amount
	if proto.NativeAmount <= 0 {
		return false, "native amount must be greater than zero"
	}

	// Exchange rate is always required
	if proto.CoreMintFiatExchangeRate == nil {
		return false, "core mint fiat exchange rate is required"
	}

	// Validate the exchange rate
	if valid, msg := ValidateVerifiedExchangeRate(proto.CoreMintFiatExchangeRate); !valid {
		return false, "exchange rate validation failed: " + msg
	}

	// For core mint, only exchange rate validation is needed along with value checks
	if common.IsCoreMint(mintAccount) {
		if proto.LaunchpadCurrencyReserveState != nil {
			return false, "launchpad currency reserve state is not required for core mint"
		}

		return validateCoreMintVerifiedExchangeData(proto)
	}

	// For launchpad currencies, reserve state is required
	if proto.LaunchpadCurrencyReserveState == nil {
		return false, "reserve state is required for launchpad currencies"
	}

	// Validate the reserve state
	if valid, msg := ValidateVerifiedReserveState(proto.LaunchpadCurrencyReserveState); !valid {
		return false, "reserve state validation failed: " + msg
	}

	// Verify the reserve state mint matches the expected mint
	reserveMint, err := common.NewAccountFromProto(proto.LaunchpadCurrencyReserveState.ReserveState.Mint)
	if err != nil {
		return false, "invalid mint in reserve state"
	}

	if reserveMint.PublicKey().ToBase58() != mintAccount.PublicKey().ToBase58() {
		return false, "reserve state mint does not match expected mint"
	}

	return validateLaunchpadCurrencyVerifiedExchangeData(proto)
}

func validateCoreMintVerifiedExchangeData(proto *transactionpb.VerifiedExchangeData) (bool, string) {
	coreMintQuarksPerUnit := common.GetMintQuarksPerUnit(common.CoreMintAccount)

	clientRate := big.NewFloat(proto.CoreMintFiatExchangeRate.ExchangeRate.ExchangeRate).SetPrec(defaultPrecision)
	clientNativeAmount := big.NewFloat(proto.NativeAmount).SetPrec(defaultPrecision)
	clientQuarks := big.NewFloat(float64(proto.Quarks)).SetPrec(defaultPrecision)

	currencyDecimals := currency_lib.GetDecimals(currency_lib.Code(proto.CoreMintFiatExchangeRate.ExchangeRate.CurrencyCode))
	one := big.NewFloat(1.0).SetPrec(defaultPrecision)
	minTransferValue := new(big.Float).Quo(one, big.NewFloat(math.Pow10(currencyDecimals)))

	nativeAmountErrorThreshold := new(big.Float).Quo(minTransferValue, big.NewFloat(2.0))
	nativeAmountLowerBound := new(big.Float).Sub(clientNativeAmount, nativeAmountErrorThreshold)
	nativeAmountUpperBound := new(big.Float).Add(clientNativeAmount, nativeAmountErrorThreshold)
	if nativeAmountLowerBound.Cmp(nativeAmountErrorThreshold) < 0 {
		nativeAmountLowerBound = nativeAmountErrorThreshold
	}
	quarksLowerBound := new(big.Float).Mul(new(big.Float).Quo(nativeAmountLowerBound, clientRate), big.NewFloat(float64(coreMintQuarksPerUnit)))
	quarksUpperBound := new(big.Float).Mul(new(big.Float).Quo(nativeAmountUpperBound, clientRate), big.NewFloat(float64(coreMintQuarksPerUnit)))

	if clientNativeAmount.Cmp(nativeAmountErrorThreshold) < 0 {
		return false, "native amount is less than minimum transfer value error threshold"
	}

	// Validate that the native amount within half of the minimum transfer value
	if clientQuarks.Cmp(quarksLowerBound) < 0 || clientQuarks.Cmp(quarksUpperBound) > 0 {
		return false, "native amount and quark value mismatch"
	}

	return true, ""
}

func validateLaunchpadCurrencyVerifiedExchangeData(proto *transactionpb.VerifiedExchangeData) (bool, string) {
	if !common.IsCoreMintUsdStableCoin() {
		return false, "launchpad currencies only support a stable coin core mint"
	}

	coreMintQuarksPerUnit := common.GetMintQuarksPerUnit(common.CoreMintAccount)

	clientFiatExchangeRate := big.NewFloat(proto.CoreMintFiatExchangeRate.ExchangeRate.ExchangeRate).SetPrec(defaultPrecision)
	clientNativeAmount := big.NewFloat(proto.NativeAmount).SetPrec(defaultPrecision)

	currencyDecimals := currency_lib.GetDecimals(currency_lib.Code(proto.CoreMintFiatExchangeRate.ExchangeRate.CurrencyCode))
	one := big.NewFloat(1.0).SetPrec(defaultPrecision)
	minTransferValue := new(big.Float).Quo(one, big.NewFloat(math.Pow10(currencyDecimals)))

	nativeAmountErrorThreshold := new(big.Float).Quo(minTransferValue, big.NewFloat(2.0))
	nativeAmountLowerBound := new(big.Float).Sub(clientNativeAmount, nativeAmountErrorThreshold)
	nativeAmountUpperBound := new(big.Float).Add(clientNativeAmount, nativeAmountErrorThreshold)
	if nativeAmountLowerBound.Cmp(nativeAmountErrorThreshold) < 0 {
		nativeAmountLowerBound = nativeAmountErrorThreshold
	}

	if clientNativeAmount.Cmp(nativeAmountErrorThreshold) < 0 {
		return false, "native amount is less than minimum transfer value error threshold"
	}

	// Calculate how much core mint would be received for a sell against the currency creator program
	coreMintSellValueInQuarks, _ := currencycreator.EstimateSell(&currencycreator.EstimateSellArgs{
		CurrentSupplyInQuarks: proto.LaunchpadCurrencyReserveState.ReserveState.SupplyFromBonding,
		SellAmountInQuarks:    proto.Quarks,
		ValueMintDecimals:     uint8(common.CoreMintDecimals),
		SellFeeBps:            0,
	})

	// Validate that the native amount within half of the minimum transfer value
	coreMintSellValueInUnits := new(big.Float).Quo(
		big.NewFloat(float64(coreMintSellValueInQuarks)).SetPrec(defaultPrecision),
		big.NewFloat(float64(coreMintQuarksPerUnit)).SetPrec(defaultPrecision),
	)
	potentialNativeAmount := new(big.Float).Mul(clientFiatExchangeRate, coreMintSellValueInUnits)

	if potentialNativeAmount.Cmp(nativeAmountLowerBound) < 0 || potentialNativeAmount.Cmp(nativeAmountUpperBound) > 0 {
		return false, "native amount does not match expected sell value"
	}

	return true, ""
}
