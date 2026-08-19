package swap

import (
	"time"

	"github.com/code-payments/ocp-server/config"
	"github.com/code-payments/ocp-server/config/env"
	"github.com/code-payments/ocp-server/ocp/common"
)

const (
	envConfigPrefix = "SWAP_RUNTIME_"

	BatchSizeConfigEnvName = envConfigPrefix + "WORKER_BATCH_SIZE"
	defaultBatchSize       = 100

	ClientTimeoutToFundConfigEnvName = envConfigPrefix + "CLIENT_TIMEOUT_TO_FUND"
	defaultClientTimeoutToFund       = 30 * time.Second

	ExternalWalletFinalizationTimeoutConfigEnvName = envConfigPrefix + "EXTERNAL_WALLET_FINALIZATION_TIMEOUT"
	defaultExternalWalletFinalizationTimeout       = 30 * time.Second

	CoinbaseOnrampOrderTimeoutConfigEnvName = envConfigPrefix + "COINBASE_ONRAMP_ORDER_TIMEOUT"
	defaultCoinbaseOnrampOrderTimeout       = 5 * time.Minute

	// The amounts a currency launch is charged. These MUST be kept equal to the
	// transaction service's TRANSACTION_SERVICE_NEW_CURRENCY_PURCHASE_QUARKS and
	// TRANSACTION_SERVICE_NEW_CURRENCY_FEE_QUARKS, which are what a launch is
	// actually validated and charged against. Nothing checks the two agree, and
	// a divergence silently misstates the launch fee in transaction history.
	NewCurrencyPurchaseQuarksConfigEnvName = envConfigPrefix + "NEW_CURRENCY_PURCHASE_QUARKS"

	NewCurrencyFeeQuarksConfigEnvName = envConfigPrefix + "NEW_CURRENCY_FEE_QUARKS"
)

// Assumes a USD stable coin core mint
var (
	defaultNewCurrencyPurchaseQuarks = 10 * common.CoreMintQuarksPerUnit // $10
	defaultNewCurrencyFeeQuarks      = 10 * common.CoreMintQuarksPerUnit // $10
)

type conf struct {
	batchSize                         config.Uint64
	clientTimeoutToFund               config.Duration
	externalWalletFinalizationTimeout config.Duration
	coinbaseOnrampOrderTimeout        config.Duration
	newCurrencyPurchaseQuarks         config.Uint64
	newCurrencyFeeQuarks              config.Uint64
}

// ConfigProvider defines how config values are pulled
type ConfigProvider func() *conf

// WithEnvConfigs returns configuration pulled from environment variables
func WithEnvConfigs() ConfigProvider {
	return func() *conf {
		return &conf{
			batchSize:                         env.NewUint64Config(BatchSizeConfigEnvName, defaultBatchSize),
			clientTimeoutToFund:               env.NewDurationConfig(ClientTimeoutToFundConfigEnvName, defaultClientTimeoutToFund),
			externalWalletFinalizationTimeout: env.NewDurationConfig(ExternalWalletFinalizationTimeoutConfigEnvName, defaultExternalWalletFinalizationTimeout),
			coinbaseOnrampOrderTimeout:        env.NewDurationConfig(CoinbaseOnrampOrderTimeoutConfigEnvName, defaultCoinbaseOnrampOrderTimeout),
			newCurrencyPurchaseQuarks:         env.NewUint64Config(NewCurrencyPurchaseQuarksConfigEnvName, defaultNewCurrencyPurchaseQuarks),
			newCurrencyFeeQuarks:              env.NewUint64Config(NewCurrencyFeeQuarksConfigEnvName, defaultNewCurrencyFeeQuarks),
		}
	}
}
