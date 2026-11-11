package app

import (
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"

	"github.com/code-payments/ocp-server/grpc/client"
)

func TestBaseConfig_TrustedProxies(t *testing.T) {
	t.Run("one by default", func(t *testing.T) {
		config := defaultConfig
		require.NoError(t, viper.Unmarshal(&config))
		require.Equal(t, client.DefaultTrustedProxies, config.TrustedProxies)
		require.Equal(t, 1, config.TrustedProxies)
	})

	t.Run("read from the environment", func(t *testing.T) {
		t.Setenv("TRUSTED_PROXIES", "2")

		config := defaultConfig
		require.NoError(t, viper.Unmarshal(&config))
		require.Equal(t, 2, config.TrustedProxies)
	})
}
