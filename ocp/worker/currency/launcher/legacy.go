package launcher

// Legacy v0 currency launch support. v0 initial purchases only initialize the
// currency, pool and VM, so the launcher initializes the rest around the
// purchase. Delete this file, and its call sites, once v0 currency launches
// are deprecated and none are in progress.

import (
	"context"

	"github.com/pkg/errors"

	"github.com/code-payments/ocp-server/ocp/common"
	ocp_data "github.com/code-payments/ocp-server/ocp/data"
	"github.com/code-payments/ocp-server/ocp/data/currency"
	"github.com/code-payments/ocp-server/ocp/data/swap"
	vm_metadata "github.com/code-payments/ocp-server/ocp/data/vm/metadata"
	transaction_util "github.com/code-payments/ocp-server/ocp/transaction"
	"github.com/code-payments/ocp-server/solana"
	compute_budget "github.com/code-payments/ocp-server/solana/computebudget"
	"github.com/code-payments/ocp-server/solana/currencycreator"
	"github.com/code-payments/ocp-server/solana/token"
	"github.com/code-payments/ocp-server/solana/vm"
)

// initCurrencyAheadOfLegacyPurchase initializes the currency on the blockchain
// before a v0 initial purchase funded outside the core mint, which requires it
// to exist. v1 initial purchases initialize it within the purchase, so
// initializing it here would make the purchase fail.
func (p *runtime) initCurrencyAheadOfLegacyPurchase(ctx context.Context, currencyMetadataRecord *currency.MetadataRecord, vmMetadataRecord *vm_metadata.Record, swapRecord *swap.Record) error {
	fromMint, err := common.NewAccountFromPublicKeyString(swapRecord.FromMint)
	if err != nil {
		return errors.Wrap(err, "invalid swap source mint")
	}
	if common.IsCoreMint(fromMint) {
		return nil
	}

	isV1, err := isV1InitialPurchase(swapRecord)
	if err != nil {
		return err
	}
	if isV1 {
		return nil
	}

	accounts, err := p.getNewCurrencyAccounts(ctx, currencyMetadataRecord, vmMetadataRecord)
	if err != nil {
		return errors.Wrap(err, "error getting currency accounts")
	}

	ok, err := validateMintExists(ctx, p.data, accounts.Mint)
	if err != nil {
		return errors.Wrap(err, "error checking if currency is initialized")
	} else if ok {
		return nil
	}

	err = p.initCurrencyOnBlockchain(ctx, currencyMetadataRecord, vmMetadataRecord, accounts)
	if err != nil {
		return errors.Wrap(err, "error initializing currency on blockchain")
	}
	return nil
}

// completeLegacyInitialization initializes the metadata, memory accounts and
// fee account after a v0 initial purchase. It's gated on blockchain state, so
// it does nothing after a v1 initial purchase.
func (p *runtime) completeLegacyInitialization(ctx context.Context, accounts *newCurrencyAccounts) error {
	ok, err := validateMemoryAccountExists(ctx, p.data, accounts.TimelockMemoryAccount)
	if err != nil {
		return errors.Wrap(err, "error checking if memory accounts are initialized")
	} else if !ok {
		err = p.initRemainingBlockchainAccounts(ctx, accounts)
		if err != nil {
			return errors.Wrap(err, "error initializing remaining blockchain accounts")
		}
	}

	return nil
}

func validateMemoryAccountExists(ctx context.Context, data ocp_data.Provider, account *common.Account) (bool, error) {
	ai, _, err := data.GetBlockchainAccountInfo(ctx, account.PublicKey().ToBase58())
	if err == solana.ErrNoAccountInfo {
		return false, nil
	} else if err != nil {
		return false, err
	}

	var state vm.MemoryAccount
	err = state.Unmarshal(ai.Data)
	if err != nil {
		return false, err
	}

	return true, nil
}

// Initializes the currency, liquidity pool, VM and creator's VM deposit ATA on
// the blockchain ahead of the initial purchase. This is only required for flows
// where the initial purchase transaction doesn't atomically perform initialization
// (ie. purchases funded outside the core mint).
func (p *runtime) initCurrencyOnBlockchain(ctx context.Context, currencyMetadataRecord *currency.MetadataRecord, vmMetadataRecord *vm_metadata.Record, accounts *newCurrencyAccounts) error {
	seedAccount, err := common.NewAccountFromPublicKeyString(currencyMetadataRecord.Seed)
	if err != nil {
		return errors.Wrap(err, "invalid seed")
	}

	creatorAccount, err := common.NewAccountFromPublicKeyString(currencyMetadataRecord.CreatedBy)
	if err != nil {
		return errors.Wrap(err, "invalid creator")
	}

	vmConfig := &common.VmConfig{
		Authority: accounts.Authority,
		Vm:        accounts.Vm,
		Omnibus:   accounts.Omnibus,
		Mint:      accounts.Mint,
	}
	creatorVmDepositAccounts, err := creatorAccount.GetVmDepositAccounts(vmConfig)
	if err != nil {
		return errors.Wrap(err, "error getting creator vm deposit accounts")
	}

	initCurrencyIxn := currencycreator.NewInitializeCurrencyInstruction(
		&currencycreator.InitializeCurrencyInstructionAccounts{
			Authority: accounts.Authority.PublicKey().ToBytes(),
			Mint:      accounts.Mint.PublicKey().ToBytes(),
			Currency:  accounts.CurrencyConfig.PublicKey().ToBytes(),
		},
		&currencycreator.InitializeCurrencyInstructionArgs{
			Name:     currencyMetadataRecord.Name,
			Symbol:   currencyMetadataRecord.Symbol,
			Seed:     seedAccount.PublicKey().ToBytes(),
			Bump:     accounts.CurrencyConfigBump,
			MintBump: accounts.MintBump,
		},
	)

	initPoolIxn := currencycreator.NewInitializePoolInstruction(
		&currencycreator.InitializePoolInstructionAccounts{
			Authority:   accounts.Authority.PublicKey().ToBytes(),
			Currency:    accounts.CurrencyConfig.PublicKey().ToBytes(),
			TargetMint:  accounts.Mint.PublicKey().ToBytes(),
			BaseMint:    common.CoreMintAccount.PublicKey().ToBytes(),
			Pool:        accounts.LiquidityPool.PublicKey().ToBytes(),
			VaultTarget: accounts.VaultMint.PublicKey().ToBytes(),
			VaultBase:   accounts.VaultBase.PublicKey().ToBytes(),
		},
		&currencycreator.InitializePoolInstructionArgs{
			SellFee:         currencyMetadataRecord.SellFeeBps,
			Bump:            accounts.LiquidityPoolBump,
			VaultTargetBump: accounts.VaultMintBump,
			VaultBaseBump:   accounts.VaultBaseBump,
		},
	)

	initVmIxn := vm.NewInitVmInstruction(
		&vm.InitVmInstructionAccounts{
			VmAuthority: accounts.Authority.PublicKey().ToBytes(),
			Vm:          accounts.Vm.PublicKey().ToBytes(),
			VmOmnibus:   accounts.Omnibus.PublicKey().ToBytes(),
			Mint:        accounts.Mint.PublicKey().ToBytes(),
		},
		&vm.InitVmInstructionArgs{
			LockDuration:  vmMetadataRecord.DaysLocked,
			VmBump:        accounts.VmBump,
			VmOmnibusBump: accounts.OmnibusBump,
		},
	)

	createCreatorVmDepositAtaIxn, _, err := token.CreateAssociatedTokenAccountIdempotent(
		accounts.Authority.PublicKey().ToBytes(),
		creatorVmDepositAccounts.Pda.PublicKey().ToBytes(),
		accounts.Mint.PublicKey().ToBytes(),
	)
	if err != nil {
		return errors.Wrap(err, "error creating creator vm deposit ata ixn")
	}

	txn := solana.NewLegacyTransaction(
		accounts.Authority.PublicKey().ToBytes(),
		compute_budget.SetComputeUnitLimit(200_000),
		compute_budget.SetComputeUnitPrice(10_000),
		initCurrencyIxn,
		initPoolIxn,
		initVmIxn,
		createCreatorVmDepositAtaIxn,
	)

	bh, err := p.data.GetBlockchainLatestBlockhash(ctx)
	if err != nil {
		return errors.Wrap(err, "error getting latest blockhash")
	}
	txn.SetBlockhash(bh)

	err = txn.Sign(accounts.Authority.PrivateKey().ToBytes())
	if err != nil {
		return errors.Wrap(err, "error signing transaction")
	}

	return transaction_util.SubmitAndWaitForFinalization(ctx, p.data, &txn)
}

// initRemainingBlockchainAccounts initializes the metadata, memory accounts
// and fee account after a v0 initial purchase. v1 initial purchases initialize
// them within the purchase transaction.
func (p *runtime) initRemainingBlockchainAccounts(ctx context.Context, accounts *newCurrencyAccounts) error {
	initMetadataIxn := transaction_util.NewInitializeMetadataInstruction(
		accounts.Authority,
		accounts.Mint,
		accounts.CurrencyConfig,
		accounts.MetaplexMetadata,
	)

	err := transaction_util.ValidateInitNonceMemoryAccounts(accounts.NonceMemory.NumAccounts)
	if err != nil {
		return err
	}

	initNonceMemoryIxn := accounts.NonceMemory.NewInitMemoryInstruction(accounts.Authority, accounts.Vm)
	initTimelockMemoryIxn := accounts.TimelockMemory.NewInitMemoryInstruction(accounts.Authority, accounts.Vm)

	initFeeAtaIxn, _, err := token.CreateAssociatedTokenAccountIdempotent(
		accounts.Authority.PublicKey().ToBytes(),
		common.GetSubsidizer().PublicKey().ToBytes(),
		accounts.Mint.PublicKey().ToBytes(),
	)
	if err != nil {
		return errors.Wrap(err, "error creating init fee ata ixn")
	}

	return p.submitAuthorityTransaction(
		ctx,
		accounts,
		compute_budget.SetComputeUnitLimit(300_000), // todo: optimize
		compute_budget.SetComputeUnitPrice(10_000),
		initMetadataIxn,
		initNonceMemoryIxn,
		initTimelockMemoryIxn,
		initFeeAtaIxn,
	)
}

// isV1InitialPurchase reports whether the initial purchase swap's transaction
// is v1, which initializes every account other than the ALT within the
// purchase
func isV1InitialPurchase(swapRecord *swap.Record) (bool, error) {
	version, err := transaction_util.GetInitialPurchaseTransactionVersion(swapRecord)
	if err != nil {
		return false, err
	}
	return version == solana.MessageVersion1, nil
}
