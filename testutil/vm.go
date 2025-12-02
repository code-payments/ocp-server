package testutil

import (
	"testing"

	"github.com/code-payments/ocp-server/ocp/common"
)

func NewRandomVmConfig(t *testing.T, isCore bool) *common.VmConfig {
	if isCore {
		return &common.VmConfig{
			Authority: common.GetSubsidizer(),
			Vm:        common.CoreMintVmAccount,
			Omnibus:   common.CoreMintVmOmnibusAccount,
			Mint:      common.CoreMintAccount,
		}
	}
	return &common.VmConfig{
		Authority: NewRandomAccount(t),
		Vm:        NewRandomAccount(t),
		Omnibus:   NewRandomAccount(t),
		Mint:      NewRandomAccount(t),
	}
}
