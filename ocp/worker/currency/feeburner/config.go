package feeburner

import (
	"github.com/code-payments/ocp-server/config"
	"github.com/code-payments/ocp-server/config/env"
)

const (
	envConfigPrefix = "CURRENCY_FEE_BURNER_RUNTIME_"

	SubsidizerConfigEnvName = envConfigPrefix + "SUBSIDIZER"
	defaultSubsidizer       = "invalid"

	BatchSizeConfigEnvName = envConfigPrefix + "WORKER_BATCH_SIZE"
	defaultBatchSize       = 100

	MaxBurnsPerBatchConfigEnvName = envConfigPrefix + "MAX_BURNS_PER_BATCH"
	defaultMaxBurnsPerBatch       = 10
)

type conf struct {
	subsidizer       config.String
	batchSize        config.Uint64
	maxBurnsPerBatch config.Uint64
}

// ConfigProvider defines how config values are pulled
type ConfigProvider func() *conf

// WithEnvConfigs returns configuration pulled from environment variables
func WithEnvConfigs() ConfigProvider {
	return func() *conf {
		return &conf{
			subsidizer:       env.NewStringConfig(SubsidizerConfigEnvName, defaultSubsidizer),
			batchSize:        env.NewUint64Config(BatchSizeConfigEnvName, defaultBatchSize),
			maxBurnsPerBatch: env.NewUint64Config(MaxBurnsPerBatchConfigEnvName, defaultMaxBurnsPerBatch),
		}
	}
}
