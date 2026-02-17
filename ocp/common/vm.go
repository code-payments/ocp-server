package common

import (
	"context"
	"sync"

	"github.com/code-payments/ocp-server/ocp/config"
	ocp_data "github.com/code-payments/ocp-server/ocp/data"
	vm_metadata "github.com/code-payments/ocp-server/ocp/data/vm/metadata"
)

var (
	CoreMintVmAccount, _        = NewAccountFromPublicKeyString(config.CoreMintVmAccountPublicKey)
	CoreMintVmOmnibusAccount, _ = NewAccountFromPublicKeyString(config.CoreMintVmOmnibusPublicKey)

	vmConfigCacheMu sync.RWMutex
	vmConfigCache   = make(map[string]*VmConfig)
)

type VmConfig struct {
	Authority *Account
	Vm        *Account
	Omnibus   *Account
	Mint      *Account
}

func GetVmConfigForMint(ctx context.Context, data ocp_data.Provider, mintAccount *Account) (*VmConfig, error) {
	if IsCoreMint(mintAccount) {
		return &VmConfig{
			Authority: GetSubsidizer(),
			Vm:        CoreMintVmAccount,
			Omnibus:   CoreMintVmOmnibusAccount,
			Mint:      CoreMintAccount,
		}, nil
	}

	mintAddress := mintAccount.PublicKey().ToBase58()

	vmConfigCacheMu.RLock()
	cached, ok := vmConfigCache[mintAddress]
	vmConfigCacheMu.RUnlock()
	if ok {
		return cached, nil
	}

	record, err := data.GetVmMetadataByMint(ctx, mintAddress)
	if err == vm_metadata.ErrNotFound {
		return nil, ErrUnsupportedMint
	} else if err != nil {
		return nil, err
	}

	vaultRecord, err := data.GetKey(ctx, record.Authority)
	if err != nil {
		return nil, err
	}

	authority, err := NewAccountFromPrivateKeyString(vaultRecord.PrivateKey)
	if err != nil {
		return nil, err
	}

	vmAccount, err := NewAccountFromPublicKeyString(record.Vm)
	if err != nil {
		return nil, err
	}

	omnibusAccount, err := NewAccountFromPublicKeyString(record.Omnibus)
	if err != nil {
		return nil, err
	}

	vmConfig := &VmConfig{
		Authority: authority,
		Vm:        vmAccount,
		Omnibus:   omnibusAccount,
		Mint:      mintAccount,
	}

	vmConfigCacheMu.Lock()
	vmConfigCache[mintAddress] = vmConfig
	vmConfigCacheMu.Unlock()

	return vmConfig, nil
}
