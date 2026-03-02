package currency

import (
	"time"

	"github.com/code-payments/ocp-server/config"
	"github.com/code-payments/ocp-server/config/env"
)

const (
	envConfigPrefix = "CURRENCY_SERVICE_"

	ExchangeRatePollIntervalConfigEnvName = envConfigPrefix + "EXCHANGE_RATE_POLL_INTERVAL"
	defaultExchangeRatePollInterval       = 5 * time.Minute

	ReserveStatePollIntervalConfigEnvName = envConfigPrefix + "RESERVE_STATE_POLL_INTERVAL"
	defaultReserveStatePollInterval       = 15 * time.Second

	AdminPublicKeyConfigEnvName = envConfigPrefix + "ADMIN_PUBLIC_KEY"
	defaultAdminPublicKey       = "admJSWL9vzQfoFm9HoLsgTHCK5G1SKzdsMJCdAtKXnN"

	MaxCurrencyCountConfigEnvName = envConfigPrefix + "MAX_CURRENCY_COUNT"
	defaultMaxCurrencyCount       = uint64(32)
)

type conf struct {
	exchangeRatePollInterval config.Duration
	reserveStatePollInterval config.Duration
	adminPublicKey           config.String
	maxCurrencyCount         config.Uint64
}

// ConfigProvider defines how config values are pulled
type ConfigProvider func() *conf

// WithEnvConfigs returns configuration pulled from environment variables
func WithEnvConfigs() ConfigProvider {
	return func() *conf {
		return &conf{
			exchangeRatePollInterval: env.NewDurationConfig(ExchangeRatePollIntervalConfigEnvName, defaultExchangeRatePollInterval),
			reserveStatePollInterval: env.NewDurationConfig(ReserveStatePollIntervalConfigEnvName, defaultReserveStatePollInterval),
			adminPublicKey:           env.NewStringConfig(AdminPublicKeyConfigEnvName, defaultAdminPublicKey),
			maxCurrencyCount:         env.NewUint64Config(MaxCurrencyCountConfigEnvName, defaultMaxCurrencyCount),
		}
	}
}
