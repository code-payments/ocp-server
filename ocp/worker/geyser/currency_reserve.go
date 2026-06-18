package geyser

import (
	"context"
	"time"

	"github.com/code-payments/ocp-server/cache"
	"github.com/code-payments/ocp-server/ocp/common"
	ocp_data "github.com/code-payments/ocp-server/ocp/data"
	"github.com/code-payments/ocp-server/ocp/data/currency"
	"github.com/code-payments/ocp-server/ocp/data/currency/reserve"
	"github.com/code-payments/ocp-server/solana/currencycreator"
)

var (
	liquidityPoolVaultCache = cache.NewCache(10_000)
)

func processPotentialCirculatingSupplyUpdate(ctx context.Context, data ocp_data.Provider, reserveStore reserve.Store, tokenAccount, mintAccount *common.Account, amount uint64, slot uint64) error {
	cachedVault, ok := liquidityPoolVaultCache.Retrieve(mintAccount.PublicKey().ToBase58())
	if !ok {
		metadataRecord, err := data.GetCurrencyMetadata(ctx, mintAccount.PublicKey().ToBase58())
		if err == currency.ErrNotFound {
			return nil
		} else if err != nil {
			return err
		}

		cachedVault = metadataRecord.VaultMint
		liquidityPoolVaultCache.Insert(mintAccount.PublicKey().ToBase58(), cachedVault, 1)
	}

	if cachedVault != tokenAccount.PublicKey().ToBase58() {
		return nil
	}

	return updateLiveReserveState(ctx, reserveStore, mintAccount.PublicKey().ToBase58(), amount, slot)
}

func updateLiveReserveState(ctx context.Context, reserveStore reserve.Store, mint string, vaultAmount uint64, slot uint64) error {
	err := reserveStore.PutLiveReserve(ctx, &currency.ReserveRecord{
		Mint:              mint,
		SupplyFromBonding: currencycreator.DefaultMintMaxQuarkSupply - vaultAmount,
		Slot:              slot,
		Time:              time.Now(),
	})
	if err == currency.ErrStaleReserveState {
		return nil
	}
	return err
}
