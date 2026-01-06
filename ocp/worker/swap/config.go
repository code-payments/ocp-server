package swap

import (
	"time"

	"github.com/code-payments/ocp-server/config"
	"github.com/code-payments/ocp-server/config/env"
)

const (
	envConfigPrefix = "SWAP_RUNTIME_"

	BatchSizeConfigEnvName      = envConfigPrefix + "WORKER_BATCH_SIZE"
	defaultFulfillmentBatchSize = 100

	ClientTimeoutToFundConfigEnvName = envConfigPrefix + "CLIENT_TIMEOUT_TO_FUND"
	defaultClientTimeoutToFund       = 30 * time.Second

	ExternalWalletFinalizationTimeoutConfigEnvName = envConfigPrefix + "EXTERNAL_WALLET_FINALIZATION_TIMEOUT"
	defaultExternalWalletFinalizationTimeout       = 30 * time.Second
)

type conf struct {
	batchSize                         config.Uint64
	clientTimeoutToFund               config.Duration
	externalWalletFinalizationTimeout config.Duration
}

// ConfigProvider defines how config values are pulled
type ConfigProvider func() *conf

// WithEnvConfigs returns configuration pulled from environment variables
func WithEnvConfigs() ConfigProvider {
	return func() *conf {
		return &conf{
			batchSize:                         env.NewUint64Config(BatchSizeConfigEnvName, defaultFulfillmentBatchSize),
			clientTimeoutToFund:               env.NewDurationConfig(ClientTimeoutToFundConfigEnvName, defaultClientTimeoutToFund),
			externalWalletFinalizationTimeout: env.NewDurationConfig(ExternalWalletFinalizationTimeoutConfigEnvName, defaultExternalWalletFinalizationTimeout),
		}
	}
}
