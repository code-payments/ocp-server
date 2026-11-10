package testutil

import (
	"fmt"
	"testing"
	"time"

	"github.com/mr-tron/base58/base58"
	"github.com/stretchr/testify/require"

	"github.com/code-payments/ocp-server/ocp/common"
	ocp_data "github.com/code-payments/ocp-server/ocp/data"
	"github.com/code-payments/ocp-server/ocp/data/currency"
	"github.com/code-payments/ocp-server/ocp/data/currency/holder"
	"github.com/code-payments/ocp-server/ocp/data/currency/reserve"
	"github.com/code-payments/ocp-server/ocp/data/vault"
	vm_metadata "github.com/code-payments/ocp-server/ocp/data/vm/metadata"
	"github.com/code-payments/ocp-server/solana/currencycreator"
	timelock_token "github.com/code-payments/ocp-server/solana/timelock/v1"
	"github.com/code-payments/ocp-server/solana/vm"
)

// SetupLaunchpadCurrency creates a random non-core VM config and all data
// records needed for a launchpad currency to be fully functional: currency
// metadata (Available), VM metadata (Available), authority private key in
// vault, and a live reserve record.
func SetupLaunchpadCurrency(t *testing.T, data ocp_data.Provider, reserveStore reserve.Store, holderStore holder.Store) *common.Account {
	return SetupLaunchpadCurrencyInState(t, data, reserveStore, holderStore, currency.MetadataStateAvailable, NewRandomAccount(t))
}

// SetupLaunchpadCurrencyInState is SetupLaunchpadCurrency for a currency in
// the provided state and created by the provided owner, which are fixed once
// the metadata is saved. Program addresses are derived as the Launch RPC
// derives them, so clients can rebuild currency creation transactions.
func SetupLaunchpadCurrencyInState(t *testing.T, data ocp_data.Provider, reserveStore reserve.Store, holderStore holder.Store, state currency.MetadataState, createdBy *common.Account) *common.Account {
	authority := NewRandomAccount(t)
	seed := NewRandomAccount(t)
	name := fmt.Sprintf("Test%s", seed.PublicKey().ToBase58()[:8])

	mintAddress, mintBump, err := currencycreator.GetMintAddress(&currencycreator.GetMintAddressArgs{
		Authority: authority.PublicKey().ToBytes(),
		Name:      name,
		Seed:      seed.PublicKey().ToBytes(),
	})
	require.NoError(t, err)
	mint, err := common.NewAccountFromPublicKeyBytes(mintAddress)
	require.NoError(t, err)

	currencyConfigAddress, currencyConfigBump, err := currencycreator.GetCurrencyAddress(&currencycreator.GetCurrencyAddressArgs{Mint: mintAddress})
	require.NoError(t, err)
	poolAddress, poolBump, err := currencycreator.GetPoolAddress(&currencycreator.GetPoolAddressArgs{Currency: currencyConfigAddress})
	require.NoError(t, err)
	vaultCoreAddress, vaultCoreBump, err := currencycreator.GetVaultAddress(&currencycreator.GetVaultAddressArgs{Pool: poolAddress, Mint: common.CoreMintAccount.PublicKey().ToBytes()})
	require.NoError(t, err)
	vaultMintAddress, vaultMintBump, err := currencycreator.GetVaultAddress(&currencycreator.GetVaultAddressArgs{Pool: poolAddress, Mint: mintAddress})
	require.NoError(t, err)
	vmAddress, vmBump, err := vm.GetVmAddress(&vm.GetVmAddressArgs{
		Mint:         mintAddress,
		VmAuthority:  authority.PublicKey().ToBytes(),
		LockDuration: timelock_token.DefaultNumDaysLocked,
	})
	require.NoError(t, err)
	omnibusAddress, omnibusBump, err := vm.GetVmObnibusAddress(&vm.GetVmObnibusAddressArgs{Vm: vmAddress})
	require.NoError(t, err)

	metadataRecord := &currency.MetadataRecord{
		Name:        name,
		Symbol:      fmt.Sprintf("T%s", seed.PublicKey().ToBase58()[:4]),
		Description: "Test currency",
		ImageUrl:    "https://example.com/icon.png",

		Seed:      seed.PublicKey().ToBase58(),
		Authority: authority.PublicKey().ToBase58(),

		Mint:     mint.PublicKey().ToBase58(),
		MintBump: mintBump,
		Decimals: currencycreator.DefaultMintDecimals,

		CurrencyConfig:     base58.Encode(currencyConfigAddress),
		CurrencyConfigBump: currencyConfigBump,

		LiquidityPool:     base58.Encode(poolAddress),
		LiquidityPoolBump: poolBump,

		VaultMint:     base58.Encode(vaultMintAddress),
		VaultMintBump: vaultMintBump,

		VaultCore:     base58.Encode(vaultCoreAddress),
		VaultCoreBump: vaultCoreBump,

		SellFeeBps: currencycreator.DefaultSellFeeBps,

		Alt: NewRandomAccount(t).PublicKey().ToBase58(),

		State: state,

		CreatedBy: createdBy.PublicKey().ToBase58(),
		CreatedAt: time.Now(),
	}
	require.NoError(t, data.SaveCurrencyMetadata(t.Context(), metadataRecord))

	vmMetadataRecord := &vm_metadata.Record{
		Mint:        mint.PublicKey().ToBase58(),
		Authority:   authority.PublicKey().ToBase58(),
		Vm:          base58.Encode(vmAddress),
		VmBump:      vmBump,
		Omnibus:     base58.Encode(omnibusAddress),
		OmnibusBump: omnibusBump,
		DaysLocked:  timelock_token.DefaultNumDaysLocked,
		State:       vm_metadata.StateAvailable,
	}
	require.NoError(t, data.SaveVmMetadata(t.Context(), vmMetadataRecord))

	vaultRecord := &vault.Record{
		PublicKey:  authority.PublicKey().ToBase58(),
		PrivateKey: authority.PrivateKey().ToBase58(),
		State:      vault.StateAvailable,
	}
	require.NoError(t, data.SaveKey(t.Context(), vaultRecord))

	reserveRecord := &currency.ReserveRecord{
		Mint:              mint.PublicKey().ToBase58(),
		SupplyFromBonding: currencycreator.ToQuarks(1000),
		Slot:              1,
		Time:              time.Now(),
	}
	require.NoError(t, reserveStore.PutLiveReserve(t.Context(), reserveRecord))

	holderCountRecord := &currency.HolderCountRecord{
		Mint:        mint.PublicKey().ToBase58(),
		HolderCount: 32,
		Time:        time.Now(),
	}
	require.NoError(t, holderStore.PutLiveHolderCount(t.Context(), holderCountRecord))

	return mint
}
