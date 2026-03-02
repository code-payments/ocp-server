package account

const (
	envConfigPrefix = "ACCOUNT_RUNTIME_"
)

type conf struct {
}

// ConfigProvider defines how config values are pulled
type ConfigProvider func() *conf

// WithEnvConfigs returns configuration pulled from environment variables
func WithEnvConfigs() ConfigProvider {
	return func() *conf {
		return &conf{}
	}
}
