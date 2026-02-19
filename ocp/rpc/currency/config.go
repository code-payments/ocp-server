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
)

type conf struct {
	exchangeRatePollInterval config.Duration
	reserveStatePollInterval config.Duration
}

// ConfigProvider defines how config values are pulled
type ConfigProvider func() *conf

// WithEnvConfigs returns configuration pulled from environment variables
func WithEnvConfigs() ConfigProvider {
	return func() *conf {
		return &conf{
			exchangeRatePollInterval: env.NewDurationConfig(ExchangeRatePollIntervalConfigEnvName, defaultExchangeRatePollInterval),
			reserveStatePollInterval: env.NewDurationConfig(ReserveStatePollIntervalConfigEnvName, defaultReserveStatePollInterval),
		}
	}
}
