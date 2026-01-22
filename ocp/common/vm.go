package common

import (
	"context"

	"github.com/code-payments/ocp-server/ocp/config"
	ocp_data "github.com/code-payments/ocp-server/ocp/data"
)

var (
	CoreMintVmAccount, _        = NewAccountFromPublicKeyString(config.CoreMintVmAccountPublicKey)
	CoreMintVmOmnibusAccount, _ = NewAccountFromPublicKeyString(config.CoreMintVmOmnibusPublicKey)

	// todo: DB store to track VM per mint
	jeffyAuthority, _        = NewAccountFromPublicKeyString(config.JeffyAuthorityPublicKey)
	jeffyVmAccount, _        = NewAccountFromPublicKeyString(config.JeffyVmAccountPublicKey)
	jeffyVmOmnibusAccount, _ = NewAccountFromPublicKeyString(config.JeffyVmOmnibusPublicKey)
)

type VmConfig struct {
	Authority *Account
	Vm        *Account
	Omnibus   *Account
	Mint      *Account
}

func GetVmConfigForMint(ctx context.Context, data ocp_data.Provider, mintAccount *Account) (*VmConfig, error) {
	if !IsCoreMint(mintAccount) && !IsCoreMintUsdStableCoin() {
		return nil, ErrUnsupportedMint
	}

	switch mintAccount.PublicKey().ToBase58() {
	case CoreMintAccount.PublicKey().ToBase58():
		return &VmConfig{
			Authority: GetSubsidizer(),
			Vm:        CoreMintVmAccount,
			Omnibus:   CoreMintVmOmnibusAccount,
			Mint:      CoreMintAccount,
		}, nil
	case jeffyMintAccount.PublicKey().ToBase58():
		if jeffyAuthority.PrivateKey() == nil {
			vaultRecord, err := data.GetKey(ctx, jeffyAuthority.PublicKey().ToBase58())
			if err != nil {
				return nil, err
			}

			jeffyAuthority, err = NewAccountFromPrivateKeyString(vaultRecord.PrivateKey)
			if err != nil {
				return nil, err
			}
		}

		return &VmConfig{
			Authority: jeffyAuthority,
			Vm:        jeffyVmAccount,
			Omnibus:   jeffyVmOmnibusAccount,
			Mint:      mintAccount,
		}, nil
	default:
		return nil, ErrUnsupportedMint
	}
}
