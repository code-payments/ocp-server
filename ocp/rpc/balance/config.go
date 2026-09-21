package balance

import (
	"time"

	"github.com/code-payments/ocp-server/config"
	"github.com/code-payments/ocp-server/config/env"
	"github.com/code-payments/ocp-server/config/memory"
	"github.com/code-payments/ocp-server/config/wrapper"
)

const (
	envConfigPrefix = "BALANCE_SERVICE_"

	// WatchSubscribersConfigEnvName is the comma-separated base58 public keys
	// of the subscribers allowed to use the watch RPCs. A subscriber not
	// listed is DENIED whatever it signs. Empty means no subscribers.
	WatchSubscribersConfigEnvName = envConfigPrefix + "WATCH_SUBSCRIBERS"
	defaultWatchSubscribers       = ""

	// WatchMaxWatchesPerOwnerConfigEnvName caps how many watches one
	// subscriber may hold on one owner. It is a quota, not a storage limit:
	// the store carries any number, and one evaluation of the owner costs
	// the same read however many there are.
	WatchMaxWatchesPerOwnerConfigEnvName = envConfigPrefix + "WATCH_MAX_WATCHES_PER_OWNER"
	defaultWatchMaxWatchesPerOwner       = 1024

	// WatchMaxGraceConfigEnvName caps a watch's grace period.
	WatchMaxGraceConfigEnvName = envConfigPrefix + "WATCH_MAX_GRACE"
	defaultWatchMaxGrace       = 30 * 24 * time.Hour

	// WatchDefaultEventLeaseConfigEnvName is the lease a poll gets when it
	// asks for none; WatchMaxEventLeaseConfigEnvName is the most it may ask
	// for.
	WatchDefaultEventLeaseConfigEnvName = envConfigPrefix + "WATCH_DEFAULT_EVENT_LEASE"
	defaultWatchDefaultEventLease       = 30 * time.Second

	WatchMaxEventLeaseConfigEnvName = envConfigPrefix + "WATCH_MAX_EVENT_LEASE"
	defaultWatchMaxEventLease       = 5 * time.Minute
)

type conf struct {
	watchSubscribers        config.String
	watchMaxWatchesPerOwner config.Uint64
	watchMaxGrace           config.Duration
	watchDefaultEventLease  config.Duration
	watchMaxEventLease      config.Duration
}

// ConfigProvider defines how config values are pulled
type ConfigProvider func() *conf

// WithEnvConfigs returns configuration pulled from environment variables
func WithEnvConfigs() ConfigProvider {
	return func() *conf {
		return &conf{
			watchSubscribers:        env.NewStringConfig(WatchSubscribersConfigEnvName, defaultWatchSubscribers),
			watchMaxWatchesPerOwner: env.NewUint64Config(WatchMaxWatchesPerOwnerConfigEnvName, defaultWatchMaxWatchesPerOwner),
			watchMaxGrace:           env.NewDurationConfig(WatchMaxGraceConfigEnvName, defaultWatchMaxGrace),
			watchDefaultEventLease:  env.NewDurationConfig(WatchDefaultEventLeaseConfigEnvName, defaultWatchDefaultEventLease),
			watchMaxEventLease:      env.NewDurationConfig(WatchMaxEventLeaseConfigEnvName, defaultWatchMaxEventLease),
		}
	}
}

type testOverrides struct {
	watchSubscribers        string
	watchMaxWatchesPerOwner uint64
}

func withManualTestOverrides(overrides *testOverrides) ConfigProvider {
	maxWatchesPerOwner := overrides.watchMaxWatchesPerOwner
	if maxWatchesPerOwner == 0 {
		maxWatchesPerOwner = defaultWatchMaxWatchesPerOwner
	}
	return func() *conf {
		return &conf{
			watchSubscribers:        wrapper.NewStringConfig(memory.NewConfig(overrides.watchSubscribers), defaultWatchSubscribers),
			watchMaxWatchesPerOwner: wrapper.NewUint64Config(memory.NewConfig(maxWatchesPerOwner), defaultWatchMaxWatchesPerOwner),
			watchMaxGrace:           wrapper.NewDurationConfig(memory.NewConfig(defaultWatchMaxGrace), defaultWatchMaxGrace),
			watchDefaultEventLease:  wrapper.NewDurationConfig(memory.NewConfig(defaultWatchDefaultEventLease), defaultWatchDefaultEventLease),
			watchMaxEventLease:      wrapper.NewDurationConfig(memory.NewConfig(defaultWatchMaxEventLease), defaultWatchMaxEventLease),
		}
	}
}
