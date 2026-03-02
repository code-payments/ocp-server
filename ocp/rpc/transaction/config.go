package transaction

import (
	"time"

	"github.com/code-payments/ocp-server/config"
	"github.com/code-payments/ocp-server/config/env"
	"github.com/code-payments/ocp-server/config/memory"
	"github.com/code-payments/ocp-server/config/wrapper"
)

const (
	envConfigPrefix = "TRANSACTION_SERVICE_"

	DisableSubmitIntentConfigEnvName = envConfigPrefix + "DISABLE_SUBMIT_INTENT"
	defaultDisableSubmitIntent       = false

	DisableSwapsConfigEnvName = envConfigPrefix + "DISABLE_SWAPS"
	defaultDisableSwaps       = false

	SubmitIntentTimeoutConfigEnvName = envConfigPrefix + "SUBMIT_INTENT_TIMEOUT"
	defaultSubmitIntentTimeout       = 15 * time.Second

	SwapTimeoutConfigEnvName = envConfigPrefix + "SWAP_TIMEOUT"
	defaultSwapTimeout       = 15 * time.Second

	ClientReceiveTimeoutConfigEnvName = envConfigPrefix + "CLIENT_RECEIVE_TIMEOUT"
	defaultClientReceiveTimeout       = 2 * time.Second

	FeeCollectorOwnerPublicKeyConfigEnvName = envConfigPrefix + "FEE_COLLECTOR_OWNER_PUBLIC_KEY"
	defaultFeeCollectorPublicKey            = "invalid" // Ensure something valid is set

	CreateOnSendWithdrawalUsdFeeConfigEnvName = envConfigPrefix + "CREATE_ON_SEND_WITHDRAWAL_USD_FEE"
	defaultCreateOnSendWithdrawalUsdFee       = 0.50
)

type conf struct {
	disableSubmitIntent          config.Bool
	disableSwaps                 config.Bool
	disableAntispamChecks        config.Bool // To avoid limits during testing
	disableAmlChecks             config.Bool // To avoid limits during testing
	disableBlockchainChecks      config.Bool // To avoid blockchain checks during testing
	submitIntentTimeout          config.Duration
	swapTimeout                  config.Duration
	clientReceiveTimeout         config.Duration
	feeCollectorOwnerPublicKey   config.String
	createOnSendWithdrawalUsdFee config.Float64
}

// ConfigProvider defines how config values are pulled
type ConfigProvider func() *conf

// WithEnvConfigs returns configuration pulled from environment variables
func WithEnvConfigs() ConfigProvider {
	return func() *conf {
		return &conf{
			disableSubmitIntent:          env.NewBoolConfig(DisableSubmitIntentConfigEnvName, defaultDisableSubmitIntent),
			disableSwaps:                 env.NewBoolConfig(DisableSwapsConfigEnvName, defaultDisableSwaps),
			disableAntispamChecks:        wrapper.NewBoolConfig(memory.NewConfig(false), false),
			disableAmlChecks:             wrapper.NewBoolConfig(memory.NewConfig(false), false),
			disableBlockchainChecks:      wrapper.NewBoolConfig(memory.NewConfig(false), false),
			submitIntentTimeout:          env.NewDurationConfig(SubmitIntentTimeoutConfigEnvName, defaultSubmitIntentTimeout),
			swapTimeout:                  env.NewDurationConfig(SwapTimeoutConfigEnvName, defaultSwapTimeout),
			clientReceiveTimeout:         env.NewDurationConfig(ClientReceiveTimeoutConfigEnvName, defaultClientReceiveTimeout),
			feeCollectorOwnerPublicKey:   env.NewStringConfig(FeeCollectorOwnerPublicKeyConfigEnvName, defaultFeeCollectorPublicKey),
			createOnSendWithdrawalUsdFee: env.NewFloat64Config(CreateOnSendWithdrawalUsdFeeConfigEnvName, defaultCreateOnSendWithdrawalUsdFee),
		}
	}
}

type testOverrides struct {
	disableSubmitIntent        bool
	enableAntispamChecks       bool
	enableAmlChecks            bool
	clientReceiveTimeout       time.Duration
	feeCollectorOwnerPublicKey string
}

func withManualTestOverrides(overrides *testOverrides) ConfigProvider {
	return func() *conf {
		return &conf{
			disableSubmitIntent:          wrapper.NewBoolConfig(memory.NewConfig(overrides.disableSubmitIntent), defaultDisableSubmitIntent),
			disableAntispamChecks:        wrapper.NewBoolConfig(memory.NewConfig(!overrides.enableAntispamChecks), false),
			disableAmlChecks:             wrapper.NewBoolConfig(memory.NewConfig(!overrides.enableAmlChecks), false),
			disableBlockchainChecks:      wrapper.NewBoolConfig(memory.NewConfig(true), true),
			submitIntentTimeout:          wrapper.NewDurationConfig(memory.NewConfig(defaultSubmitIntentTimeout), defaultSubmitIntentTimeout),
			swapTimeout:                  wrapper.NewDurationConfig(memory.NewConfig(defaultSwapTimeout), defaultSwapTimeout),
			clientReceiveTimeout:         wrapper.NewDurationConfig(memory.NewConfig(overrides.clientReceiveTimeout), defaultClientReceiveTimeout),
			feeCollectorOwnerPublicKey:   wrapper.NewStringConfig(memory.NewConfig(overrides.feeCollectorOwnerPublicKey), defaultFeeCollectorPublicKey),
			createOnSendWithdrawalUsdFee: wrapper.NewFloat64Config(memory.NewConfig(defaultCreateOnSendWithdrawalUsdFee), defaultCreateOnSendWithdrawalUsdFee),
		}
	}
}
