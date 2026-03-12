package currency

import (
	"github.com/code-payments/ocp-server/config"
	"github.com/code-payments/ocp-server/config/env"
)

const (
	envConfigPrefix = "CURRENCY_SERVICE_"

	AdminPublicKeyConfigEnvName = envConfigPrefix + "ADMIN_PUBLIC_KEY"
	defaultAdminPublicKey       = "admJSWL9vzQfoFm9HoLsgTHCK5G1SKzdsMJCdAtKXnN"

	MaxCurrencyCountConfigEnvName = envConfigPrefix + "MAX_CURRENCY_COUNT"
	defaultMaxCurrencyCount       = uint64(32)
)

type conf struct {
	adminPublicKey   config.String
	maxCurrencyCount config.Uint64
}

// ConfigProvider defines how config values are pulled
type ConfigProvider func() *conf

// WithEnvConfigs returns configuration pulled from environment variables
func WithEnvConfigs() ConfigProvider {
	return func() *conf {
		return &conf{
			adminPublicKey:   env.NewStringConfig(AdminPublicKeyConfigEnvName, defaultAdminPublicKey),
			maxCurrencyCount: env.NewUint64Config(MaxCurrencyCountConfigEnvName, defaultMaxCurrencyCount),
		}
	}
}
