package testutil

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/code-payments/ocp-server/ocp/common"
	ocp_data "github.com/code-payments/ocp-server/ocp/data"
	"github.com/code-payments/ocp-server/ocp/data/currency"
	"github.com/code-payments/ocp-server/ocp/data/vault"
	vm_metadata "github.com/code-payments/ocp-server/ocp/data/vm/metadata"
	"github.com/code-payments/ocp-server/solana/currencycreator"
)

// SetupLaunchpadCurrency creates a random non-core VM config and all data
// records needed for a launchpad currency to be fully functional: currency
// metadata (Available), VM metadata (Available), authority private key in
// vault, and a live reserve record.
func SetupLaunchpadCurrency(t *testing.T, data ocp_data.Provider) *common.Account {
	vmConfig := NewRandomVmConfig(t, false)

	metadataRecord := &currency.MetadataRecord{
		Name:        fmt.Sprintf("Test%s", vmConfig.Mint.PublicKey().ToBase58()[:8]),
		Symbol:      fmt.Sprintf("T%s", vmConfig.Mint.PublicKey().ToBase58()[:4]),
		Description: "Test currency",
		ImageUrl:    "https://example.com/icon.png",

		Seed:      NewRandomAccount(t).PublicKey().ToBase58(),
		Authority: vmConfig.Authority.PublicKey().ToBase58(),

		Mint:     vmConfig.Mint.PublicKey().ToBase58(),
		MintBump: 255,
		Decimals: currencycreator.DefaultMintDecimals,

		CurrencyConfig:     NewRandomAccount(t).PublicKey().ToBase58(),
		CurrencyConfigBump: 255,

		LiquidityPool:     NewRandomAccount(t).PublicKey().ToBase58(),
		LiquidityPoolBump: 255,

		VaultMint:     NewRandomAccount(t).PublicKey().ToBase58(),
		VaultMintBump: 255,

		VaultCore:     NewRandomAccount(t).PublicKey().ToBase58(),
		VaultCoreBump: 255,

		SellFeeBps: currencycreator.DefaultSellFeeBps,

		Alt: NewRandomAccount(t).PublicKey().ToBase58(),

		State: currency.MetadataStateAvailable,

		CreatedBy: NewRandomAccount(t).PublicKey().ToBase58(),
		CreatedAt: time.Now(),
	}
	require.NoError(t, data.SaveCurrencyMetadata(t.Context(), metadataRecord))

	vmMetadataRecord := &vm_metadata.Record{
		Mint:        vmConfig.Mint.PublicKey().ToBase58(),
		Authority:   vmConfig.Authority.PublicKey().ToBase58(),
		Vm:          vmConfig.Vm.PublicKey().ToBase58(),
		VmBump:      255,
		Omnibus:     vmConfig.Omnibus.PublicKey().ToBase58(),
		OmnibusBump: 255,
		DaysLocked:  21,
		State:       vm_metadata.StateAvailable,
	}
	require.NoError(t, data.SaveVmMetadata(t.Context(), vmMetadataRecord))

	vaultRecord := &vault.Record{
		PublicKey:  vmConfig.Authority.PublicKey().ToBase58(),
		PrivateKey: vmConfig.Authority.PrivateKey().ToBase58(),
		State:      vault.StateAvailable,
	}
	require.NoError(t, data.SaveKey(t.Context(), vaultRecord))

	reserveRecord := &currency.ReserveRecord{
		Mint:              vmConfig.Mint.PublicKey().ToBase58(),
		SupplyFromBonding: currencycreator.ToQuarks(1000),
		Slot:              1,
		Time:              time.Now(),
	}
	require.NoError(t, data.PutLiveCurrencyReserve(t.Context(), reserveRecord))

	return vmConfig.Mint
}
