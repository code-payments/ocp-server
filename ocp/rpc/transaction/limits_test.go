package transaction

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	transactionpb "github.com/code-payments/ocp-protobuf-api/generated/go/transaction/v1"

	"github.com/code-payments/ocp-server/ocp/common"
	currency_util "github.com/code-payments/ocp-server/ocp/currency"
	"github.com/code-payments/ocp-server/ocp/data/intent"
)

func TestValidateIntentQuarks(t *testing.T) {
	launchpadMint, err := common.NewRandomAccount()
	require.NoError(t, err)

	for _, tc := range []struct {
		mint      *common.Account
		maxQuarks uint64
	}{
		{common.CoreMintAccount, currency_util.MaxCoreMintQuarks},
		{launchpadMint, currency_util.MaxLaunchpadCurrencyQuarks},
	} {
		singleQuantity := func(intentType intent.Type, quarks uint64) *intent.Record {
			record := &intent.Record{IntentType: intentType}
			switch intentType {
			case intent.SendPublicPayment:
				record.SendPublicPaymentMetadata = &intent.SendPublicPaymentMetadata{Quantity: quarks}
			case intent.ReceivePaymentsPublicly:
				record.ReceivePaymentsPubliclyMetadata = &intent.ReceivePaymentsPubliclyMetadata{Quantity: quarks}
			}
			return record
		}
		multiPayment := func(quarks ...uint64) *intent.Record {
			metadata := &intent.SendPublicMultiPaymentMetadata{}
			for _, q := range quarks {
				metadata.Payments = append(metadata.Payments, &intent.Payment{Quantity: q})
				metadata.Quantity += q
			}
			return &intent.Record{IntentType: intent.SendPublicMultiPayment, SendPublicMultiPaymentMetadata: metadata}
		}
		distribution := func(quarks ...uint64) *intent.Record {
			metadata := &intent.PublicDistributionMetadata{}
			for _, q := range quarks {
				metadata.Distributions = append(metadata.Distributions, &intent.Distribution{Quantity: q})
				metadata.Quantity += q
			}
			return &intent.Record{IntentType: intent.PublicDistribution, PublicDistributionMetadata: metadata}
		}

		require.NoError(t, validateIntentQuarks(tc.mint, &intent.Record{IntentType: intent.OpenAccounts}))

		for _, intentType := range []intent.Type{intent.SendPublicPayment, intent.ReceivePaymentsPublicly} {
			assert.NoError(t, validateIntentQuarks(tc.mint, singleQuantity(intentType, tc.maxQuarks)))
			assertQuarksValidationError(t, validateIntentQuarks(tc.mint, singleQuantity(intentType, tc.maxQuarks+1)))
			assertQuarksValidationError(t, validateIntentQuarks(tc.mint, singleQuantity(intentType, math.MaxUint64)))
		}

		for _, newRecord := range []func(...uint64) *intent.Record{multiPayment, distribution} {
			assert.NoError(t, validateIntentQuarks(tc.mint, newRecord(tc.maxQuarks/2, tc.maxQuarks/2)))
			assertQuarksValidationError(t, validateIntentQuarks(tc.mint, newRecord(1, tc.maxQuarks)))
			assertQuarksValidationError(t, validateIntentQuarks(tc.mint, newRecord(tc.maxQuarks+1)))

			// The summed quantity wraps to 0, but each part is still rejected
			assertQuarksValidationError(t, validateIntentQuarks(tc.mint, newRecord(1<<63, 1<<63)))
		}
	}

	assert.Error(t, validateIntentQuarks(common.CoreMintAccount, &intent.Record{IntentType: intent.ExternalDeposit}))
}

func TestValidateActionQuarks(t *testing.T) {
	launchpadMint, err := common.NewRandomAccount()
	require.NoError(t, err)

	action := &transactionpb.Action{}

	assert.NoError(t, validateActionQuarks(action, common.CoreMintAccount, currency_util.MaxCoreMintQuarks))
	assertQuarksValidationError(t, validateActionQuarks(action, common.CoreMintAccount, currency_util.MaxCoreMintQuarks+1))
	assertQuarksValidationError(t, validateActionQuarks(action, common.CoreMintAccount, math.MaxUint64))

	assert.NoError(t, validateActionQuarks(action, launchpadMint, currency_util.MaxLaunchpadCurrencyQuarks))
	assertQuarksValidationError(t, validateActionQuarks(action, launchpadMint, currency_util.MaxLaunchpadCurrencyQuarks+1))
	assertQuarksValidationError(t, validateActionQuarks(action, launchpadMint, math.MaxUint64))
}

func assertQuarksValidationError(t *testing.T, err error) {
	require.Error(t, err)
	assert.IsType(t, IntentValidationError{}, err)
	assert.Contains(t, err.Error(), "exceeds max")
}
