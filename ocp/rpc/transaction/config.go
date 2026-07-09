package transaction

import (
	"time"

	"github.com/code-payments/ocp-server/config"
	"github.com/code-payments/ocp-server/config/env"
	"github.com/code-payments/ocp-server/config/memory"
	"github.com/code-payments/ocp-server/config/wrapper"
	"github.com/code-payments/ocp-server/ocp/common"
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

	CreateOnSendWithdrawalFeeQuarksConfigEnvName = envConfigPrefix + "CREATE_ON_SEND_WITHDRAWAL_FEE_QUARKS"

	SwapTreasuryOwnerPublicKeyConfigEnvName = envConfigPrefix + "SWAP_TREASURY_OWNER_PUBLIC_KEY"
	defaultSwapTreasuryOwnerPublicKey       = "invalid" // Ensure something valid is set

	NewCurrencyPurchaseQuarksConfigEnvName = envConfigPrefix + "NEW_CURRENCY_PURCHASE_QUARKS"

	NewCurrencyFeeQuarksConfigEnvName = envConfigPrefix + "NEW_CURRENCY_FEE_QUARKS"
)

// Assumes a USD stable coin core mint
var (
	defaultCreateOnSendWithdrawalFeeQuarks = common.CoreMintQuarksPerUnit / 2 // $0.50
	defaultNewCurrencyPurchaseQuarks       = 10 * common.CoreMintQuarksPerUnit
	defaultNewCurrencyFeeQuarks            = 10 * common.CoreMintQuarksPerUnit
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
	feeCollectorOwnerPublicKey      config.String
	createOnSendWithdrawalFeeQuarks config.Uint64
	swapTreasuryOwnerPublicKey      config.String
	newCurrencyPurchaseQuarks       config.Uint64
	newCurrencyFeeQuarks            config.Uint64
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
			feeCollectorOwnerPublicKey:      env.NewStringConfig(FeeCollectorOwnerPublicKeyConfigEnvName, defaultFeeCollectorPublicKey),
			createOnSendWithdrawalFeeQuarks: env.NewUint64Config(CreateOnSendWithdrawalFeeQuarksConfigEnvName, defaultCreateOnSendWithdrawalFeeQuarks),
			swapTreasuryOwnerPublicKey:      env.NewStringConfig(SwapTreasuryOwnerPublicKeyConfigEnvName, defaultSwapTreasuryOwnerPublicKey),
			newCurrencyPurchaseQuarks:       env.NewUint64Config(NewCurrencyPurchaseQuarksConfigEnvName, defaultNewCurrencyPurchaseQuarks),
			newCurrencyFeeQuarks:            env.NewUint64Config(NewCurrencyFeeQuarksConfigEnvName, defaultNewCurrencyFeeQuarks),
		}
	}
}

type testOverrides struct {
	disableSubmitIntent        bool
	enableAntispamChecks       bool
	enableAmlChecks            bool
	clientReceiveTimeout       time.Duration
	feeCollectorOwnerPublicKey string
	swapTreasuryOwnerPublicKey string
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
			feeCollectorOwnerPublicKey:      wrapper.NewStringConfig(memory.NewConfig(overrides.feeCollectorOwnerPublicKey), defaultFeeCollectorPublicKey),
			createOnSendWithdrawalFeeQuarks: wrapper.NewUint64Config(memory.NewConfig(defaultCreateOnSendWithdrawalFeeQuarks), defaultCreateOnSendWithdrawalFeeQuarks),
			swapTreasuryOwnerPublicKey:      wrapper.NewStringConfig(memory.NewConfig(overrides.swapTreasuryOwnerPublicKey), defaultSwapTreasuryOwnerPublicKey),
			newCurrencyPurchaseQuarks:       wrapper.NewUint64Config(memory.NewConfig(defaultNewCurrencyPurchaseQuarks), defaultNewCurrencyPurchaseQuarks),
			newCurrencyFeeQuarks:            wrapper.NewUint64Config(memory.NewConfig(defaultNewCurrencyFeeQuarks), defaultNewCurrencyFeeQuarks),
		}
	}
}
