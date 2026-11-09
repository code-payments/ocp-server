package launcher

// ALT support for legacy clients, which build v0 swaps with each currency's
// ALT. Delete this file, and its call sites, once ALTs are deprecated.

import (
	"context"

	"github.com/mr-tron/base58"
	"github.com/pkg/errors"

	"github.com/code-payments/ocp-server/ocp/common"
	ocp_data "github.com/code-payments/ocp-server/ocp/data"
	"github.com/code-payments/ocp-server/ocp/data/currency"
	"github.com/code-payments/ocp-server/solana"
	address_lookup_table "github.com/code-payments/ocp-server/solana/addresslookuptable"
	compute_budget "github.com/code-payments/ocp-server/solana/computebudget"
	"github.com/code-payments/ocp-server/solana/system"
)

// initializeAlt creates the currency's ALT and extends it with the currency's
// accounts. Every flow does this after the initial purchase, because the
// recent slot an ALT is derived from expires before a durable nonce purchase
// transaction might be submitted.
func (p *runtime) initializeAlt(ctx context.Context, currencyMetadataRecord *currency.MetadataRecord, accounts *newCurrencyAccounts) error {
	ok := isAltDerived(currencyMetadataRecord)
	if ok {
		var err error
		ok, err = validateAltExists(ctx, p.data, accounts.Alt)
		if err != nil {
			return errors.Wrap(err, "error checking if alt exists")
		}
	}
	if !ok {
		err := p.createAlt(ctx, currencyMetadataRecord, accounts)
		if err != nil {
			return errors.Wrap(err, "error creating alt")
		}
	}

	ok, err := validateAltIsExtended(ctx, p.data, accounts.Alt)
	if err != nil {
		return errors.Wrap(err, "error checking if alt is extended")
	} else if !ok {
		err = p.extendAlt(ctx, accounts)
		if err != nil {
			return errors.Wrap(err, "error extending alt")
		}
	}

	return nil
}

func validateAltExists(ctx context.Context, data ocp_data.Provider, account *common.Account) (bool, error) {
	ai, _, err := data.GetBlockchainAccountInfo(ctx, account.PublicKey().ToBase58())
	if err == solana.ErrNoAccountInfo {
		return false, nil
	} else if err != nil {
		return false, err
	}

	var state address_lookup_table.AddressLookupTableAccount
	err = state.Unmarshal(ai.Data)
	if err != nil {
		return false, err
	}

	return true, nil
}

func validateAltIsExtended(ctx context.Context, data ocp_data.Provider, account *common.Account) (bool, error) {
	ai, _, err := data.GetBlockchainAccountInfo(ctx, account.PublicKey().ToBase58())
	if err == solana.ErrNoAccountInfo {
		return false, nil
	} else if err != nil {
		return false, err
	}

	var state address_lookup_table.AddressLookupTableAccount
	err = state.Unmarshal(ai.Data)
	if err != nil {
		return false, err
	}

	// todo: validate addresses
	return len(state.Addresses) > 0, nil
}

func (p *runtime) deriveNewAlt(ctx context.Context, accounts *newCurrencyAccounts) error {
	recentSlot, err := p.data.GetBlockchainSlot(ctx)
	if err != nil {
		return err
	}

	address, bump, err := address_lookup_table.GetAddress(accounts.Authority.PublicKey().ToBytes(), recentSlot)
	if err != nil {
		return err
	}

	account, err := common.NewAccountFromPublicKeyBytes(address)
	if err != nil {
		return err
	}

	accounts.Alt = account
	accounts.AltBump = bump
	accounts.AltRecentSlot = recentSlot

	return nil
}

// createAlt derives a new ALT from a recent slot, saves it to the currency's
// metadata, and creates it on the blockchain. Every flow creates the ALT after
// the initial purchase, because the recent slot it's derived from expires
// before a durable nonce purchase transaction might be submitted. Legacy
// clients require the ALT to build v0 swaps.
//
// A previously saved ALT that was never created is replaced, since its recent
// slot may have expired.
func (p *runtime) createAlt(ctx context.Context, currencyMetadataRecord *currency.MetadataRecord, accounts *newCurrencyAccounts) error {
	err := p.deriveNewAlt(ctx, accounts)
	if err != nil {
		return errors.Wrap(err, "error deriving new alt")
	}

	currencyMetadataRecord.Alt = accounts.Alt.PublicKey().ToBase58()
	err = p.data.SaveCurrencyMetadata(ctx, currencyMetadataRecord)
	if err != nil {
		return errors.Wrap(err, "error saving new alt")
	}

	return p.submitAuthorityTransaction(
		ctx,
		accounts,
		compute_budget.SetComputeUnitLimit(50_000), // 10,481 CUs measured on mainnet
		compute_budget.SetComputeUnitPrice(10_000),
		address_lookup_table.Create(
			accounts.Alt.PublicKey().ToBytes(),
			accounts.Authority.PublicKey().ToBytes(),
			accounts.Authority.PublicKey().ToBytes(),
			accounts.AltRecentSlot,
			accounts.AltBump,
		),
	)
}

// extendAlt adds the currency's accounts to its ALT
func (p *runtime) extendAlt(ctx context.Context, accounts *newCurrencyAccounts) error {
	return p.submitAuthorityTransaction(
		ctx,
		accounts,
		compute_budget.SetComputeUnitLimit(50_000), // 8,422 CUs measured on mainnet
		compute_budget.SetComputeUnitPrice(10_000),
		address_lookup_table.Extend(
			accounts.Alt.PublicKey().ToBytes(),
			accounts.Authority.PublicKey().ToBytes(),
			accounts.Authority.PublicKey().ToBytes(),

			// Address ordering matters
			accounts.Vm.PublicKey().ToBytes(),
			accounts.Omnibus.PublicKey().ToBytes(),
			accounts.Mint.PublicKey().ToBytes(),
			accounts.LiquidityPool.PublicKey().ToBytes(),
			accounts.VaultBase.PublicKey().ToBytes(),
			accounts.VaultMint.PublicKey().ToBytes(),
			common.CoreMintAccount.PublicKey().ToBytes(),
			system.RentSysVar,
			system.RecentBlockhashesSysVar,
		),
	)
}

// isAltDerived reports whether the currency's ALT has been derived. Currencies
// are created with a placeholder until the launcher derives it.
func isAltDerived(currencyMetadataRecord *currency.MetadataRecord) bool {
	return currencyMetadataRecord.Alt != base58.Encode(system.ProgramKey[:])
}
