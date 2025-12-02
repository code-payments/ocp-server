package nonce

import (
	"github.com/code-payments/ocp-server/config"
	"github.com/code-payments/ocp-server/config/env"
)

const (
	envConfigPrefix = "NONCE_RUNTIME_"

	solanaMainnetNoncePubkeyPrefixConfigEnvName = envConfigPrefix + "SOLANA_MAINNET_NONCE_PUBKEY_PREFIX"
	defaultSolanaMainnetNoncePubkeyPrefix       = "non"

	onDemandTransactiontNoncePoolSizeConfigEnvName = envConfigPrefix + "ON_DEMAND_TRANSACTION_NONCE_POOL_SIZE"
	defaultOnDemandTransactionNoncePoolSize        = 1000

	clientSwapNoncePoolSizeConfigEnvName = envConfigPrefix + "CLIENT_SWAP_NONCE_POOL_SIZE"
	defaultClientSwapNoncePoolSize       = 1000
)

type conf struct {
	solanaMainnetNoncePubkeyPrefix   config.String
	onDemandTransactionNoncePoolSize config.Uint64
	clientSwapNoncePoolSize          config.Uint64
}

// ConfigProvider defines how config values are pulled
type ConfigProvider func() *conf

// WithEnvConfigs returns configuration pulled from environment variables
func WithEnvConfigs() ConfigProvider {
	return func() *conf {
		return &conf{
			solanaMainnetNoncePubkeyPrefix:   env.NewStringConfig(solanaMainnetNoncePubkeyPrefixConfigEnvName, defaultSolanaMainnetNoncePubkeyPrefix),
			onDemandTransactionNoncePoolSize: env.NewUint64Config(onDemandTransactiontNoncePoolSizeConfigEnvName, defaultOnDemandTransactionNoncePoolSize),
			clientSwapNoncePoolSize:          env.NewUint64Config(clientSwapNoncePoolSizeConfigEnvName, defaultClientSwapNoncePoolSize),
		}
	}
}
