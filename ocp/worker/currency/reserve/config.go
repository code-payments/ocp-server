package reserve

import (
	"time"

	"github.com/code-payments/ocp-server/config"
	"github.com/code-payments/ocp-server/config/env"
)

const (
	envConfigPrefix = "CURRENCY_RESERVE_RUNTIME_"

	StaleThresholdConfigEnvName = envConfigPrefix + "STALE_THRESHOLD"
	defaultStaleThreshold       = 24 * time.Hour
)

type conf struct {
	staleThreshold config.Duration
}

// ConfigProvider defines how config values are pulled
type ConfigProvider func() *conf

// WithEnvConfigs returns configuration pulled from environment variables
func WithEnvConfigs() ConfigProvider {
	return func() *conf {
		return &conf{
			staleThreshold: env.NewDurationConfig(StaleThresholdConfigEnvName, defaultStaleThreshold),
		}
	}
}
