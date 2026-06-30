package launcher

import (
	"context"
	"database/sql"
	"slices"
	"sync"
	"time"

	"github.com/mr-tron/base58"
	"github.com/pkg/errors"

	commonpb "github.com/code-payments/ocp-protobuf-api/generated/go/common/v1"

	"github.com/code-payments/ocp-server/ocp/common"
	ocp_data "github.com/code-payments/ocp-server/ocp/data"
	"github.com/code-payments/ocp-server/ocp/data/account"
	"github.com/code-payments/ocp-server/ocp/data/action"
	"github.com/code-payments/ocp-server/ocp/data/currency"
	"github.com/code-payments/ocp-server/ocp/data/fulfillment"
	"github.com/code-payments/ocp-server/ocp/data/intent"
	"github.com/code-payments/ocp-server/ocp/data/nonce"
	vm_metadata "github.com/code-payments/ocp-server/ocp/data/vm/metadata"
	"github.com/code-payments/ocp-server/ocp/data/vm/ram"
	transaction_util "github.com/code-payments/ocp-server/ocp/transaction"
	"github.com/code-payments/ocp-server/solana"
	address_lookup_table "github.com/code-payments/ocp-server/solana/addresslookuptable"
	compute_budget "github.com/code-payments/ocp-server/solana/computebudget"
	"github.com/code-payments/ocp-server/solana/currencycreator"
	"github.com/code-payments/ocp-server/solana/system"
	"github.com/code-payments/ocp-server/solana/token"
	"github.com/code-payments/ocp-server/solana/vm"
)

// todo: some of these utilities can be promoted into a common package

type newCurrencyAccounts struct {
	Authority *common.Account

	Mint                 *common.Account
	MintBump             uint8
	CurrencyConfig       *common.Account
	CurrencyConfigBump   uint8
	LiquidityPool        *common.Account
	LiquidityPoolBump    uint8
	MetaplexMetadata     *common.Account
	MetaplexMetadataBump uint8
	VaultBase            *common.Account
	VaultBaseBump        uint8
	VaultMint            *common.Account
	VaultMintBump        uint8

	Vm                    *common.Account
	VmBump                uint8
	Omnibus               *common.Account
	OmnibusBump           uint8
	NonceMemoryAccount    *common.Account
	NonceMemoryBump       uint8
	TimelockMemoryAccount *common.Account
	TimelockMemoryBump    uint8

	Alt           *common.Account
	AltBump       uint8
	AltRecentSlot uint64

	Fees *common.Account
}

func (p *runtime) validateCurrencyMetadataState(record *currency.MetadataRecord, states ...currency.MetadataState) error {
	if slices.Contains(states, record.State) {
		return nil
	}
	return errors.New("invalid currency metadata state")
}

func (p *runtime) markCurrencyMetadataExecutingInitialPurchase(ctx context.Context, record *currency.MetadataRecord) error {
	err := p.validateCurrencyMetadataState(record, currency.MetadataStateFundingAuthority)
	if err != nil {
		return err
	}

	record.State = currency.MetadataStateExecutingInitialPurchase
	return p.data.SaveCurrencyMetadata(ctx, record)
}

func (p *runtime) markCurrencyMetadataFinalValidation(ctx context.Context, record *currency.MetadataRecord) error {
	err := p.validateCurrencyMetadataState(record, currency.MetadataStateCompletingInitialization)
	if err != nil {
		return err
	}

	record.State = currency.MetadataStateFinalValidation
	return p.data.SaveCurrencyMetadata(ctx, record)
}

func (p *runtime) markCurrencyMetadataAvailable(ctx context.Context, record *currency.MetadataRecord) error {
	err := p.validateCurrencyMetadataState(record, currency.MetadataStateFinalValidation)
	if err != nil {
		return err
	}

	record.State = currency.MetadataStateAvailable
	return p.data.SaveCurrencyMetadata(ctx, record)
}

func (p *runtime) markCurrencyMetadataAbandoning(ctx context.Context, record *currency.MetadataRecord) error {
	err := p.validateCurrencyMetadataState(record, currency.MetadataStateWaitingForInitialPurchase)
	if err != nil {
		return err
	}

	record.State = currency.MetadataStateAbandoning
	return p.data.SaveCurrencyMetadata(ctx, record)
}

func (p *runtime) markCurrencyMetadataAbandoned(ctx context.Context, record *currency.MetadataRecord) error {
	err := p.validateCurrencyMetadataState(record, currency.MetadataStateAbandoning)
	if err != nil {
		return err
	}

	record.State = currency.MetadataStateAbandoned
	return p.data.SaveCurrencyMetadata(ctx, record)
}

func (p *runtime) putInitialReserveState(_ context.Context, _ *currency.MetadataRecord) error {
	// Note: The live reserve state is initialized by the swap worker on initial purchase

	return nil
}

func (p *runtime) putInitialHolderCount(ctx context.Context, record *currency.MetadataRecord) error {
	err := p.holderStore.PutLiveHolderCount(ctx, &currency.HolderCountRecord{
		Mint:        record.Mint,
		HolderCount: 1,
		Time:        time.Now(),
	})
	if err != nil && err != currency.ErrStaleHolderState {
		return errors.Wrap(err, "error putting initial live holder count")
	}

	return nil
}

func (p *runtime) validateVmMetadataState(record *vm_metadata.Record, states ...vm_metadata.State) error {
	if slices.Contains(states, record.State) {
		return nil
	}
	return errors.New("invalid vm metadata state")
}

func (p *runtime) markVmMetadataInitializing(ctx context.Context, record *vm_metadata.Record) error {
	err := p.validateVmMetadataState(record, vm_metadata.StateUnknown)
	if err != nil {
		return err
	}

	record.State = vm_metadata.StateInitializing
	return p.data.SaveVmMetadata(ctx, record)
}

func (p *runtime) markVmMetadataAvailable(ctx context.Context, record *vm_metadata.Record) error {
	err := p.validateVmMetadataState(record, vm_metadata.StateInitializing)
	if err != nil {
		return err
	}

	record.State = vm_metadata.StateAvailable
	return p.data.SaveVmMetadata(ctx, record)
}

func validateAuthorityPrivateKeyExists(ctx context.Context, data ocp_data.Provider, account *common.Account) (bool, error) {
	vaultRecord, err := data.GetKey(ctx, account.PublicKey().ToBase58())
	if err != nil {
		return false, errors.Wrap(err, "error getting authority vault record")
	}

	fromPrivateKey, err := common.NewAccountFromPrivateKeyString(vaultRecord.PrivateKey)
	if err != nil {
		return false, errors.Wrap(err, "invalid authority private key")
	}

	if fromPrivateKey.PublicKey().ToBase58() != account.PublicKey().ToBase58() {
		return false, nil
	}
	return true, nil
}

func validateMinimumAuthorityFunding(ctx context.Context, data ocp_data.Provider, account *common.Account, amount uint64) (bool, uint64, error) {
	ai, _, err := data.GetBlockchainAccountInfo(ctx, account.PublicKey().ToBase58(), solana.CommitmentFinalized)
	switch err {
	case nil:
		if ai.Lamports >= amount {
			return true, 0, nil
		}
		return false, amount - ai.Lamports, nil
	case solana.ErrNoAccountInfo:
		return false, amount, nil
	default:
		return false, 0, errors.Wrap(err, "error getting authority account info")
	}
}

func returnAuthorityFundsToSubsidizer(ctx context.Context, data ocp_data.Provider, subsidizer, authority *common.Account) error {
	ai, _, err := data.GetBlockchainAccountInfo(ctx, authority.PublicKey().ToBase58(), solana.CommitmentFinalized)
	if err == solana.ErrNoAccountInfo {
		return nil
	} else if err != nil {
		return errors.Wrap(err, "error getting authority account info")
	}

	if ai.Lamports == 0 {
		return nil
	}

	bh, err := data.GetBlockchainLatestBlockhash(ctx)
	if err != nil {
		return errors.Wrap(err, "error getting latest blockhash")
	}

	txn, err := transaction_util.MakeSolanaTransferTransaction(subsidizer, authority, subsidizer, ai.Lamports, bh)
	if err != nil {
		return errors.Wrap(err, "error making solana transfer transaction")
	}

	err = txn.Sign(subsidizer.PrivateKey().ToBytes(), authority.PrivateKey().ToBytes())
	if err != nil {
		return errors.Wrap(err, "error signing transaction")
	}

	return transaction_util.SubmitAndWaitForFinalization(ctx, data, &txn)
}

func fundAuthority(ctx context.Context, data ocp_data.Provider, subsidizer, account *common.Account, amount uint64) error {
	bh, err := data.GetBlockchainLatestBlockhash(ctx)
	if err != nil {
		return errors.Wrap(err, "error getting latest blockhash")
	}
	txn, err := transaction_util.MakeSolanaTransferTransaction(subsidizer, subsidizer, account, amount, bh)
	if err != nil {
		return errors.Wrap(err, "error making solana transfer transaction")
	}

	err = txn.Sign(subsidizer.PrivateKey().ToBytes())
	if err != nil {
		return errors.Wrap(err, "error signing transaction")
	}

	return transaction_util.SubmitAndWaitForFinalization(ctx, data, &txn)
}

func validateMintExists(ctx context.Context, data ocp_data.Provider, account *common.Account) (bool, error) {
	ai, _, err := data.GetBlockchainAccountInfo(ctx, account.PublicKey().ToBase58(), solana.CommitmentFinalized)
	if err == solana.ErrNoAccountInfo {
		return false, nil
	} else if err != nil {
		return false, err
	}

	var state token.Mint
	ok := state.Unmarshal(ai.Data)
	if !ok {
		return false, errors.New("invalid mint account data")
	}

	return true, nil
}

func validateCurrencyConfigExists(ctx context.Context, data ocp_data.Provider, account *common.Account) (bool, error) {
	ai, _, err := data.GetBlockchainAccountInfo(ctx, account.PublicKey().ToBase58(), solana.CommitmentFinalized)
	if err == solana.ErrNoAccountInfo {
		return false, nil
	} else if err != nil {
		return false, err
	}

	var state currencycreator.CurrencyConfigAccount
	err = state.Unmarshal(ai.Data)
	if err != nil {
		return false, err
	}

	return true, nil
}

func validateLiquidityPoolExists(ctx context.Context, data ocp_data.Provider, account *common.Account) (bool, error) {
	ai, _, err := data.GetBlockchainAccountInfo(ctx, account.PublicKey().ToBase58(), solana.CommitmentFinalized)
	if err == solana.ErrNoAccountInfo {
		return false, nil
	} else if err != nil {
		return false, err
	}

	var state currencycreator.LiquidityPoolAccount
	err = state.Unmarshal(ai.Data)
	if err != nil {
		return false, err
	}

	return true, nil
}

func validateVmExists(ctx context.Context, data ocp_data.Provider, account *common.Account) (bool, error) {
	ai, _, err := data.GetBlockchainAccountInfo(ctx, account.PublicKey().ToBase58(), solana.CommitmentFinalized)
	if err == solana.ErrNoAccountInfo {
		return false, nil
	} else if err != nil {
		return false, err
	}

	var state vm.CodeVmAccount
	err = state.Unmarshal(ai.Data)
	if err != nil {
		return false, err
	}

	return true, nil
}

func validateAltExists(ctx context.Context, data ocp_data.Provider, account *common.Account) (bool, error) {
	ai, _, err := data.GetBlockchainAccountInfo(ctx, account.PublicKey().ToBase58(), solana.CommitmentFinalized)
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
	ai, _, err := data.GetBlockchainAccountInfo(ctx, account.PublicKey().ToBase58(), solana.CommitmentFinalized)
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

func validateFeeAccountExists(ctx context.Context, data ocp_data.Provider, account *common.Account) (bool, error) {
	ai, _, err := data.GetBlockchainAccountInfo(ctx, account.PublicKey().ToBase58(), solana.CommitmentFinalized)
	if err == solana.ErrNoAccountInfo {
		return false, nil
	} else if err != nil {
		return false, err
	}

	var state token.Account
	ok := state.Unmarshal(ai.Data)
	if !ok {
		return false, errors.New("invalid token account state")
	}

	return true, nil
}

func validateMemoryAccountExists(ctx context.Context, data ocp_data.Provider, account *common.Account) (bool, error) {
	ai, _, err := data.GetBlockchainAccountInfo(ctx, account.PublicKey().ToBase58(), solana.CommitmentFinalized)
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

func validateMemoryAccountIsResized(ctx context.Context, data ocp_data.Provider, account *common.Account) (bool, error) {
	ai, _, err := data.GetBlockchainAccountInfo(ctx, account.PublicKey().ToBase58(), solana.CommitmentFinalized)
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

	actualSize := len(ai.Data)
	desiredCapacity := vm.MemoryAccountSize + vm.GetSliceAllocatorSize(int(state.NumAccounts), int(state.AccountSize))
	return actualSize == desiredCapacity, nil
}

func validateNonceMemoryAccountPopulated(ctx context.Context, data ocp_data.Provider, account *common.Account) (bool, error) {
	ai, _, err := data.GetBlockchainAccountInfo(ctx, account.PublicKey().ToBase58(), solana.CommitmentFinalized)
	if err == solana.ErrNoAccountInfo {
		return false, nil
	} else if err != nil {
		return false, err
	}

	var state vm.MemoryAccountWithData
	err = state.Unmarshal(ai.Data)
	if err != nil {
		return false, err
	}

	for i := range int(state.NumAccounts) {
		if !state.Data.IsAllocated(i) {
			return false, nil
		}

		data, ok := state.Data.Read(i)
		if !ok {
			return false, nil
		}

		var vdn vm.VirtualDurableNonce
		err = vdn.UnmarshalFromMemory(data)
		if err != nil {
			return false, err
		}
	}

	return true, nil
}

func validateNoncePoolInitialized(ctx context.Context, data ocp_data.Provider, account *common.Account) (bool, error) {
	ai, _, err := data.GetBlockchainAccountInfo(ctx, account.PublicKey().ToBase58(), solana.CommitmentFinalized)
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

	count, err := data.GetNonceCount(ctx, nonce.EnvironmentVm, base58.Encode(state.Vm))
	if err != nil {
		return false, err
	}
	return count == uint64(state.NumAccounts), nil
}

func (p *runtime) deriveNewAlt(ctx context.Context, accounts *newCurrencyAccounts) error {
	recentSlot, err := p.data.GetBlockchainSlot(ctx, solana.CommitmentFinalized)
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

func (p *runtime) initRemainingBlockchainAccounts(ctx context.Context, currencyMetadataRecord *currency.MetadataRecord, accounts *newCurrencyAccounts) error {
	initMetadataIxn := currencycreator.NewInitializeMetadataInstruction(
		&currencycreator.InitializeMetadataInstructionAccounts{
			Authority: accounts.Authority.PublicKey().ToBytes(),
			Mint:      accounts.Mint.PublicKey().ToBytes(),
			Currency:  accounts.CurrencyConfig.PublicKey().ToBytes(),
			Metadata:  accounts.MetaplexMetadata.PublicKey().ToBytes(),
		},
		&currencycreator.InitializeMetadataInstructionArgs{},
	)

	initNonceMemoryIxn := vm.NewInitMemoryInstruction(
		&vm.InitMemoryInstructionAccounts{
			VmAuthority: accounts.Authority.PublicKey().ToBytes(),
			Vm:          accounts.Vm.PublicKey().ToBytes(),
			VmMemory:    accounts.NonceMemoryAccount.PublicKey().ToBytes(),
		},
		&vm.InitMemoryInstructionArgs{
			Name:         initialNonceMemoryAccountName,
			NumAccounts:  uint32(initialNoncePoolSize),
			AccountSize:  uint16(vm.GetVirtualAccountSizeInMemory(vm.VirtualAccountTypeDurableNonce)),
			VmMemoryBump: accounts.NonceMemoryBump,
		},
	)

	initTimelockMemoryIxn := vm.NewInitMemoryInstruction(
		&vm.InitMemoryInstructionAccounts{
			VmAuthority: accounts.Authority.PublicKey().ToBytes(),
			Vm:          accounts.Vm.PublicKey().ToBytes(),
			VmMemory:    accounts.TimelockMemoryAccount.PublicKey().ToBytes(),
		},
		&vm.InitMemoryInstructionArgs{
			Name:         initialTimelockMemoryAccountName,
			NumAccounts:  uint32(initialTimelockAccounts),
			AccountSize:  uint16(vm.GetVirtualAccountSizeInMemory(vm.VirtualAccountTypeTimelock)),
			VmMemoryBump: accounts.TimelockMemoryBump,
		},
	)

	initFeeAtaIxn, _, err := token.CreateAssociatedTokenAccountIdempotent(
		accounts.Authority.PublicKey().ToBytes(),
		common.GetSubsidizer().PublicKey().ToBytes(),
		accounts.Mint.PublicKey().ToBytes(),
	)
	if err != nil {
		return errors.Wrap(err, "error creating init fee ata ixn")
	}

	initAltIxn := address_lookup_table.Create(
		accounts.Alt.PublicKey().ToBytes(),
		accounts.Authority.PublicKey().ToBytes(),
		accounts.Authority.PublicKey().ToBytes(),
		accounts.AltRecentSlot,
		accounts.AltBump,
	)

	txn := solana.NewLegacyTransaction(
		accounts.Authority.PublicKey().ToBytes(),
		compute_budget.SetComputeUnitLimit(300_000), // todo: optimize
		compute_budget.SetComputeUnitPrice(10_000),
		initMetadataIxn,
		initNonceMemoryIxn,
		initTimelockMemoryIxn,
		initFeeAtaIxn,
		initAltIxn,
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

func (p *runtime) resizeAndExtendBlockchainAccounts(ctx context.Context, accounts *newCurrencyAccounts) error {
	ixns := []solana.Instruction{
		compute_budget.SetComputeUnitLimit(350_000),
		compute_budget.SetComputeUnitPrice(10_000),
	}

	ai, _, err := p.data.GetBlockchainAccountInfo(ctx, accounts.NonceMemoryAccount.PublicKey().ToBase58(), solana.CommitmentFinalized)
	if err != nil {
		return errors.Wrap(err, "error getting nonce memory account info")
	}
	var nonceMemoryAccountState vm.MemoryAccount
	err = nonceMemoryAccountState.Unmarshal(ai.Data)
	if err != nil {
		return errors.Wrap(err, "error unmarshalling nonce memory account")
	}

	ai, _, err = p.data.GetBlockchainAccountInfo(ctx, accounts.TimelockMemoryAccount.PublicKey().ToBase58(), solana.CommitmentFinalized)
	if err != nil {
		return errors.Wrap(err, "error getting timelock memory account info")
	}
	var timelockMemoryAccountState vm.MemoryAccount
	err = timelockMemoryAccountState.Unmarshal(ai.Data)
	if err != nil {
		return errors.Wrap(err, "error unmarshalling timelock memory account")
	}

	memoryResizeChunkSize := 10 * 1024

	desiredNonceMemoryCapacity := vm.MemoryAccountSize + vm.GetSliceAllocatorSize(
		int(nonceMemoryAccountState.NumAccounts),
		int(vm.GetVirtualAccountSizeInMemory(vm.VirtualAccountTypeDurableNonce)),
	)
	for i := 1; i <= desiredNonceMemoryCapacity/memoryResizeChunkSize+1; i++ {
		nextSize := min(i*memoryResizeChunkSize, desiredNonceMemoryCapacity)
		ixns = append(ixns, vm.NewResizeMemoryInstruction(
			&vm.ResizeMemoryInstructionAccounts{
				VmAuthority: accounts.Authority.PublicKey().ToBytes(),
				Vm:          accounts.Vm.PublicKey().ToBytes(),
				VmMemory:    accounts.NonceMemoryAccount.PublicKey().ToBytes(),
			},
			&vm.ResizeMemoryInstructionArgs{
				AccountSize: uint32(nextSize),
			},
		))
	}

	desiredTimelockMemoryCapacity := vm.MemoryAccountSize + vm.GetSliceAllocatorSize(
		int(timelockMemoryAccountState.NumAccounts),
		int(vm.GetVirtualAccountSizeInMemory(vm.VirtualAccountTypeTimelock)),
	)
	for i := 1; i <= desiredTimelockMemoryCapacity/memoryResizeChunkSize+1; i++ {
		nextSize := min(i*memoryResizeChunkSize, desiredTimelockMemoryCapacity)
		ixns = append(ixns, vm.NewResizeMemoryInstruction(
			&vm.ResizeMemoryInstructionAccounts{
				VmAuthority: accounts.Authority.PublicKey().ToBytes(),
				Vm:          accounts.Vm.PublicKey().ToBytes(),
				VmMemory:    accounts.TimelockMemoryAccount.PublicKey().ToBytes(),
			},
			&vm.ResizeMemoryInstructionArgs{
				AccountSize: uint32(nextSize),
			},
		))
	}

	ixns = append(ixns, address_lookup_table.Extend(
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
	))

	txn := solana.NewLegacyTransaction(
		accounts.Authority.PublicKey().ToBytes(),
		ixns...,
	)

	if len(txn.Marshal()) > solana.MaxTransactionSize {
		return errors.New("transaction exceeds maximum size")
	}

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

func (p *runtime) populateNonceMemory(ctx context.Context, accounts *newCurrencyAccounts) error {
	ai, _, err := p.data.GetBlockchainAccountInfo(ctx, accounts.NonceMemoryAccount.PublicKey().ToBase58(), solana.CommitmentFinalized)
	if err != nil {
		return err
	}

	var state vm.MemoryAccountWithData
	err = state.Unmarshal(ai.Data)
	if err != nil {
		return err
	}

	initVdnIxnsPerTxn := 22
	initVdnBatchTxns := int(state.NumAccounts) / initVdnIxnsPerTxn
	if int(state.NumAccounts)%initVdnIxnsPerTxn != 0 {
		initVdnBatchTxns += 1
	}

	var mu sync.Mutex
	var anyError error
	var wg sync.WaitGroup
	wg.Add(initVdnBatchTxns)
	for batch := range initVdnBatchTxns {
		go func(batch int) {
			defer wg.Done()

			err := func() error {
				ixns := []solana.Instruction{
					compute_budget.SetComputeUnitLimit(550_000),
					compute_budget.SetComputeUnitPrice(10_000),
				}
				for i := range initVdnIxnsPerTxn {
					randomOwner, err := common.NewRandomAccount()
					if err != nil {
						return errors.Wrap(err, "error generating random nonce owner")
					}

					memoryIndex := batch*initVdnIxnsPerTxn + i

					if state.Data.IsAllocated(memoryIndex) {
						continue
					}
					if memoryIndex >= int(state.NumAccounts) {
						break
					}

					ixns = append(ixns, vm.NewInitNonceInstruction(
						&vm.InitNonceInstructionAccounts{
							VmAuthority:         accounts.Authority.PublicKey().ToBytes(),
							Vm:                  accounts.Vm.PublicKey().ToBytes(),
							VmMemory:            accounts.NonceMemoryAccount.PublicKey().ToBytes(),
							VirtualAccountOwner: randomOwner.PublicKey().ToBytes(),
						},
						&vm.InitNonceInstructionArgs{
							AccountIndex: uint16(memoryIndex),
						},
					))
				}

				txn := solana.NewLegacyTransaction(
					accounts.Authority.PublicKey().ToBytes(),
					ixns...,
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
			}()

			if err != nil {
				mu.Lock()
				anyError = err
				mu.Unlock()
			}
		}(batch)
	}
	wg.Wait()

	return anyError
}

func (p *runtime) initializeNoncePool(ctx context.Context, accounts *newCurrencyAccounts) error {
	ai, _, err := p.data.GetBlockchainAccountInfo(ctx, accounts.NonceMemoryAccount.PublicKey().ToBase58(), solana.CommitmentFinalized)
	if err != nil {
		return errors.Wrap(err, "error getting nonce memory account info")
	}

	var state vm.MemoryAccountWithData
	err = state.Unmarshal(ai.Data)
	if err != nil {
		return errors.Wrap(err, "error unmarshalling nonce memory account")
	}

	vdns := make([]vm.VirtualDurableNonce, state.NumAccounts)
	for i := range int(state.NumAccounts) {
		if !state.Data.IsAllocated(i) {
			return errors.Errorf("memory account state has uninitialized data at index %d", i)
		}

		rawVdn, ok := state.Data.Read(i)
		if !ok {
			return errors.Errorf("unable to read from allocated memory at index %d", i)
		}

		var vdn vm.VirtualDurableNonce
		err = vdn.UnmarshalFromMemory(rawVdn)
		if err != nil {
			return err
		}

		vdns[i] = vdn
	}

	return p.data.ExecuteInTx(ctx, sql.LevelDefault, func(ctx context.Context) error {
		for _, vdn := range vdns {
			record := nonce.Record{
				Address:   base58.Encode(vdn.Address),
				Authority: accounts.Authority.PublicKey().ToBase58(),
				Blockhash: base58.Encode(vdn.Value[:]),

				Environment:         nonce.EnvironmentVm,
				EnvironmentInstance: accounts.Vm.PublicKey().ToBase58(),

				Purpose: nonce.PurposeClientIntent,
				State:   nonce.StateAvailable,

				Signature: "",
			}
			err = p.data.SaveNonce(ctx, &record)
			if err != nil {
				return err
			}
		}

		return nil
	})
}

func (p *runtime) addTimelockMemoryAccountToDb(ctx context.Context, accounts *newCurrencyAccounts) error {
	ai, _, err := p.data.GetBlockchainAccountInfo(ctx, accounts.TimelockMemoryAccount.PublicKey().ToBase58(), solana.CommitmentFinalized)
	if err != nil {
		return errors.Wrap(err, "error getting timelock memory account info")
	}

	var state vm.MemoryAccountWithData
	err = state.Unmarshal(ai.Data)
	if err != nil {
		return errors.Wrap(err, "error unmarshalling timelock memory account")
	}

	record := &ram.Record{
		Vm: accounts.Vm.PublicKey().ToBase58(),

		Address: accounts.TimelockMemoryAccount.PublicKey().ToBase58(),

		Capacity:   uint16(state.NumAccounts),
		NumSectors: 1,
		NumPages:   uint16(state.NumAccounts),
		PageSize:   uint8(state.AccountSize),

		StoredAccountType: vm.VirtualAccountTypeTimelock,

		CreatedAt: time.Now(),
	}
	err = p.data.InitializeVmMemory(ctx, record)
	if err == ram.ErrAlreadyInitialized {
		return nil
	}
	return err
}

func (p *runtime) getNewCurrencyAccounts(ctx context.Context, currencyMetadataRecord *currency.MetadataRecord, vmMetadataRecord *vm_metadata.Record) (*newCurrencyAccounts, error) {
	authorityVaultRecord, err := p.data.GetKey(ctx, currencyMetadataRecord.Authority)
	if err != nil {
		return nil, errors.Wrap(err, "error getting authority vault record")
	}

	authority, err := common.NewAccountFromPrivateKeyString(authorityVaultRecord.PrivateKey)
	if err != nil {
		return nil, errors.Wrap(err, "invalid authority")
	}

	// Accounts directly from the currency metadata record
	mint, err := common.NewAccountFromPublicKeyString(currencyMetadataRecord.Mint)
	if err != nil {
		return nil, errors.Wrap(err, "invalid mint")
	}
	currencyConfig, err := common.NewAccountFromPublicKeyString(currencyMetadataRecord.CurrencyConfig)
	if err != nil {
		return nil, errors.Wrap(err, "invalid currency config")
	}
	liquidityPool, err := common.NewAccountFromPublicKeyString(currencyMetadataRecord.LiquidityPool)
	if err != nil {
		return nil, errors.Wrap(err, "invalid liquidity pool")
	}
	vaultBase, err := common.NewAccountFromPublicKeyString(currencyMetadataRecord.VaultCore)
	if err != nil {
		return nil, errors.Wrap(err, "invalid vault base")
	}
	vaultMint, err := common.NewAccountFromPublicKeyString(currencyMetadataRecord.VaultMint)
	if err != nil {
		return nil, errors.Wrap(err, "invalid vault mint")
	}
	alt, err := common.NewAccountFromPublicKeyString(currencyMetadataRecord.Alt)
	if err != nil {
		return nil, errors.Wrap(err, "invalid alt")
	}

	// Accounts directly from the VM metadata record
	vmAccount, err := common.NewAccountFromPublicKeyString(vmMetadataRecord.Vm)
	if err != nil {
		return nil, errors.Wrap(err, "invalid vm")
	}
	omnibus, err := common.NewAccountFromPublicKeyString(vmMetadataRecord.Omnibus)
	if err != nil {
		return nil, errors.Wrap(err, "invalid omnibus")
	}

	// Derived metaplex metadata address
	metaplexMetadataAddress, metaplexMetadataBump, err := currencycreator.GetMetadataAddress(&currencycreator.GetMetadataAddressArgs{
		Mint: mint.PublicKey().ToBytes(),
	})
	if err != nil {
		return nil, errors.Wrap(err, "error deriving metaplex metadata address")
	}
	metaplexMetadata, err := common.NewAccountFromPublicKeyBytes(metaplexMetadataAddress)
	if err != nil {
		return nil, errors.Wrap(err, "invalid metaplex metadata account")
	}

	// Derived memory account addresses
	nonceMemoryAddress, nonceMemoryBump, err := vm.GetMemoryAccountAddress(&vm.GetMemoryAccountAddressArgs{
		Name: initialNonceMemoryAccountName,
		Vm:   vmAccount.PublicKey().ToBytes(),
	})
	if err != nil {
		return nil, errors.Wrap(err, "error deriving nonce memory account address")
	}
	nonceMemoryAccount, err := common.NewAccountFromPublicKeyBytes(nonceMemoryAddress)
	if err != nil {
		return nil, errors.Wrap(err, "invalid nonce memory account")
	}

	timelockMemoryAddress, timelockMemoryBump, err := vm.GetMemoryAccountAddress(&vm.GetMemoryAccountAddressArgs{
		Name: initialTimelockMemoryAccountName,
		Vm:   vmAccount.PublicKey().ToBytes(),
	})
	if err != nil {
		return nil, errors.Wrap(err, "error deriving timelock memory account address")
	}
	timelockMemoryAccount, err := common.NewAccountFromPublicKeyBytes(timelockMemoryAddress)
	if err != nil {
		return nil, errors.Wrap(err, "invalid timelock memory account")
	}

	// Derived fee account address
	feeAtaAddress, err := token.GetAssociatedAccount(common.GetSubsidizer().PublicKey().ToBytes(), mint.PublicKey().ToBytes())
	if err != nil {
		return nil, errors.Wrap(err, "error deriving fee ata address")
	}
	fees, err := common.NewAccountFromPublicKeyBytes(feeAtaAddress)
	if err != nil {
		return nil, errors.Wrap(err, "invalid fee ata address")
	}

	return &newCurrencyAccounts{
		Authority: authority,

		Mint:     mint,
		MintBump: currencyMetadataRecord.MintBump,

		CurrencyConfig:     currencyConfig,
		CurrencyConfigBump: currencyMetadataRecord.CurrencyConfigBump,

		LiquidityPool:     liquidityPool,
		LiquidityPoolBump: currencyMetadataRecord.LiquidityPoolBump,

		MetaplexMetadata:     metaplexMetadata,
		MetaplexMetadataBump: metaplexMetadataBump,

		VaultBase:     vaultBase,
		VaultBaseBump: currencyMetadataRecord.VaultCoreBump,

		VaultMint:     vaultMint,
		VaultMintBump: currencyMetadataRecord.VaultMintBump,

		Vm:     vmAccount,
		VmBump: vmMetadataRecord.VmBump,

		Omnibus:     omnibus,
		OmnibusBump: vmMetadataRecord.OmnibusBump,

		NonceMemoryAccount: nonceMemoryAccount,
		NonceMemoryBump:    nonceMemoryBump,

		TimelockMemoryAccount: timelockMemoryAccount,
		TimelockMemoryBump:    timelockMemoryBump,

		Alt:           alt,
		AltBump:       0, // Not tracked
		AltRecentSlot: 0, // Not tracked

		Fees: fees,
	}, nil
}

func (p *runtime) initializeCreatorAcccount(ctx context.Context, currencyMetadataRecord *currency.MetadataRecord, accounts *newCurrencyAccounts) error {
	creatorOwner, err := common.NewAccountFromPublicKeyString(currencyMetadataRecord.CreatedBy)
	if err != nil {
		return errors.Wrap(err, "invalid creator account")
	}

	vmConfig := &common.VmConfig{
		Authority: accounts.Authority,
		Vm:        accounts.Vm,
		Omnibus:   accounts.Omnibus,
		Mint:      accounts.Mint,
	}

	timelockAccounts, err := creatorOwner.GetTimelockAccounts(vmConfig)
	if err != nil {
		return errors.Wrap(err, "error getting creator timelock accounts")
	}

	now := time.Now()
	vaultAddress := timelockAccounts.Vault.PublicKey().ToBase58()

	// Create the open accounts intent record
	intentId, err := common.NewRandomAccount()
	if err != nil {
		return errors.Wrap(err, "error generating intent id")
	}

	intentRecord := &intent.Record{
		IntentId:   intentId.PublicKey().ToBase58(),
		IntentType: intent.OpenAccounts,

		MintAccount: accounts.Mint.PublicKey().ToBase58(),

		InitiatorOwnerAccount: creatorOwner.PublicKey().ToBase58(),

		OpenAccountsMetadata: &intent.OpenAccountsMetadata{},

		State: intent.StatePending,

		CreatedAt: now,
	}
	err = p.data.SaveIntent(ctx, intentRecord)
	if err != nil {
		return errors.Wrap(err, "error saving creator open accounts intent")
	}

	// Create the open account action record
	actionRecord := &action.Record{
		Intent:     intentId.PublicKey().ToBase58(),
		IntentType: intent.OpenAccounts,

		ActionId:   0,
		ActionType: action.OpenAccount,

		Source: vaultAddress,

		State: action.StatePending,

		CreatedAt: now,
	}
	err = p.data.PutAllActions(ctx, actionRecord)
	if err != nil {
		return errors.Wrap(err, "error saving creator open account action")
	}

	// Create the timelock record
	timelockRecord := timelockAccounts.ToDBRecord()
	err = p.data.SaveTimelock(ctx, timelockRecord)
	if err != nil {
		return errors.Wrap(err, "error saving creator timelock record")
	}

	// Create the account info record with deposit sync enabled so
	// Geyser can process the initial purchase
	accountInfoRecord := &account.Record{
		OwnerAccount:        creatorOwner.PublicKey().ToBase58(),
		AuthorityAccount:    creatorOwner.PublicKey().ToBase58(),
		TokenAccount:        vaultAddress,
		MintAccount:         timelockAccounts.Mint.PublicKey().ToBase58(),
		AccountType:         commonpb.AccountType_PRIMARY,
		Index:               0,
		RequiresDepositSync: true,
	}
	err = p.data.CreateAccountInfo(ctx, accountInfoRecord)
	if err != nil {
		return errors.Wrap(err, "error saving creator account info record")
	}

	// Create the fulfillment record for initializing the timelock account
	fulfillmentRecord := &fulfillment.Record{
		Intent:     intentId.PublicKey().ToBase58(),
		IntentType: intent.OpenAccounts,

		ActionId:   0,
		ActionType: action.OpenAccount,

		FulfillmentType: fulfillment.InitializeLockedTimelockAccount,

		Source: vaultAddress,

		IntentOrderingIndex:      intentRecord.Id,
		ActionOrderingIndex:      0,
		FulfillmentOrderingIndex: 0,

		DisableActiveScheduling: false,

		State: fulfillment.StateUnknown,

		CreatedAt: now,
	}
	return p.data.PutAllFulfillments(ctx, fulfillmentRecord)
}
