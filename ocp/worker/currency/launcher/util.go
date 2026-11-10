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

	"github.com/code-payments/ocp-server/ocp/balance"
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
	compute_budget "github.com/code-payments/ocp-server/solana/computebudget"
	"github.com/code-payments/ocp-server/solana/currencycreator"
	"github.com/code-payments/ocp-server/solana/token"
	"github.com/code-payments/ocp-server/solana/vm"
)

// todo: some of these utilities can be promoted into a common package

type newCurrencyAccounts struct {
	Authority *common.Account

	Mint               *common.Account
	MintBump           uint8
	CurrencyConfig     *common.Account
	CurrencyConfigBump uint8
	LiquidityPool      *common.Account
	LiquidityPoolBump  uint8
	MetaplexMetadata   *common.Account
	VaultBase          *common.Account
	VaultBaseBump      uint8
	VaultMint          *common.Account
	VaultMintBump      uint8

	Vm                    *common.Account
	VmBump                uint8
	Omnibus               *common.Account
	OmnibusBump           uint8
	NonceMemory           *transaction_util.CurrencyLaunchMemoryAccount
	NonceMemoryAccount    *common.Account
	TimelockMemory        *transaction_util.CurrencyLaunchMemoryAccount
	TimelockMemoryAccount *common.Account

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
	err := p.validateCurrencyMetadataState(record, currency.MetadataStatePrePurchaseSetup)
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
	ai, _, err := data.GetBlockchainAccountInfo(ctx, account.PublicKey().ToBase58())
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
	ai, _, err := data.GetBlockchainAccountInfo(ctx, authority.PublicKey().ToBase58())
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
	ai, _, err := data.GetBlockchainAccountInfo(ctx, account.PublicKey().ToBase58())
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
	ai, _, err := data.GetBlockchainAccountInfo(ctx, account.PublicKey().ToBase58())
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
	ai, _, err := data.GetBlockchainAccountInfo(ctx, account.PublicKey().ToBase58())
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
	ai, _, err := data.GetBlockchainAccountInfo(ctx, account.PublicKey().ToBase58())
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

func validateFeeAccountExists(ctx context.Context, data ocp_data.Provider, account *common.Account) (bool, error) {
	ai, _, err := data.GetBlockchainAccountInfo(ctx, account.PublicKey().ToBase58())
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

func validateMemoryAccountIsResized(ctx context.Context, data ocp_data.Provider, account *common.Account) (bool, error) {
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

	actualSize := len(ai.Data)
	desiredCapacity := vm.MemoryAccountSize + vm.GetSliceAllocatorSize(int(state.NumAccounts), int(state.AccountSize))
	return actualSize == desiredCapacity, nil
}

func validateNonceMemoryAccountPopulated(ctx context.Context, data ocp_data.Provider, account *common.Account) (bool, error) {
	ai, _, err := data.GetBlockchainAccountInfo(ctx, account.PublicKey().ToBase58())
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

	count, err := data.GetNonceCount(ctx, nonce.EnvironmentVm, base58.Encode(state.Vm))
	if err != nil {
		return false, err
	}
	return count == uint64(state.NumAccounts), nil
}

// resizeMemoryAccounts grows the nonce and timelock memory accounts to their
// full capacity. Sizes come from the on-chain account counts, which can differ
// from the current initial sizes for currencies launched earlier, and only
// sizes beyond each account's current size are included.
func (p *runtime) resizeMemoryAccounts(ctx context.Context, accounts *newCurrencyAccounts) error {
	ixns := []solana.Instruction{
		compute_budget.SetComputeUnitLimit(350_000),
		compute_budget.SetComputeUnitPrice(10_000),
	}

	var numResizes int
	for _, memoryAccount := range []*common.Account{accounts.NonceMemoryAccount, accounts.TimelockMemoryAccount} {
		ai, _, err := p.data.GetBlockchainAccountInfo(ctx, memoryAccount.PublicKey().ToBase58())
		if err != nil {
			return errors.Wrap(err, "error getting memory account info")
		}
		var state vm.MemoryAccount
		err = state.Unmarshal(ai.Data)
		if err != nil {
			return errors.Wrap(err, "error unmarshalling memory account")
		}

		var sizes []uint32
		for _, size := range transaction_util.MemoryResizeSizes(state.NumAccounts, state.AccountSize) {
			if int(size) > len(ai.Data) {
				sizes = append(sizes, size)
			}
		}

		ixns = append(ixns, transaction_util.NewResizeMemoryInstructions(accounts.Authority, accounts.Vm, memoryAccount, sizes)...)
		numResizes += len(sizes)
	}

	if numResizes == 0 {
		return nil
	}
	return p.submitAuthorityTransaction(ctx, accounts, ixns...)
}

// submitAuthorityTransaction submits a legacy transaction paid for and signed
// by the currency authority, and waits for it to finalize
func (p *runtime) submitAuthorityTransaction(ctx context.Context, accounts *newCurrencyAccounts, ixns ...solana.Instruction) error {
	txn := solana.NewLegacyTransaction(accounts.Authority.PublicKey().ToBytes(), ixns...)

	marshalledTxn, err := txn.Marshal()
	if err != nil {
		return errors.Wrap(err, "error marshalling transaction")
	}
	if len(marshalledTxn) > solana.MaxLegacyTransactionSize {
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
	ai, _, err := p.data.GetBlockchainAccountInfo(ctx, accounts.NonceMemoryAccount.PublicKey().ToBase58())
	if err != nil {
		return err
	}

	var state vm.MemoryAccountWithData
	err = state.Unmarshal(ai.Data)
	if err != nil {
		return err
	}

	err = transaction_util.ValidateInitNonceMemoryAccounts(state.NumAccounts)
	if err != nil {
		return err
	}

	batches := initNonceBatches(int(state.NumAccounts), state.Data.IsAllocated, initNoncesPerTxn)

	var mu sync.Mutex
	var anyError error
	var wg sync.WaitGroup
	wg.Add(len(batches))
	for _, memoryIndexes := range batches {
		go func(memoryIndexes []int) {
			defer wg.Done()

			err := func() error {
				bh, err := p.data.GetBlockchainLatestBlockhash(ctx)
				if err != nil {
					return errors.Wrap(err, "error getting latest blockhash")
				}

				txn, err := makeInitNonceTransaction(accounts, memoryIndexes, bh)
				if err != nil {
					return err
				}

				return transaction_util.SubmitAndWaitForFinalization(ctx, p.data, &txn)
			}()

			if err != nil {
				mu.Lock()
				anyError = err
				mu.Unlock()
			}
		}(memoryIndexes)
	}
	wg.Wait()

	return anyError
}

// initNonceBatches groups the unallocated indexes of a nonce memory account
// into batches of at most perTxn indexes, one batch per transaction.
func initNonceBatches(numAccounts int, isAllocated func(index int) bool, perTxn int) [][]int {
	var batches [][]int
	for start := 0; start < numAccounts; start += perTxn {
		var batch []int
		for memoryIndex := start; memoryIndex < min(start+perTxn, numAccounts); memoryIndex++ {
			if !isAllocated(memoryIndex) {
				batch = append(batch, memoryIndex)
			}
		}
		if len(batch) > 0 {
			batches = append(batches, batch)
		}
	}
	return batches
}

// makeInitNonceTransaction builds a signed v1 transaction initializing a
// virtual durable nonce with a random owner at each memory index.
func makeInitNonceTransaction(accounts *newCurrencyAccounts, memoryIndexes []int, blockhash solana.Blockhash) (solana.Transaction, error) {
	ixns := make([]solana.Instruction, 0, len(memoryIndexes))
	for _, memoryIndex := range memoryIndexes {
		randomOwner, err := common.NewRandomAccount()
		if err != nil {
			return solana.Transaction{}, errors.Wrap(err, "error generating random nonce owner")
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

	config := transaction_util.NewV1TransactionConfig(
		transaction_util.InitNonceBatchComputeUnitLimit(len(memoryIndexes)),
		10_000,
		transaction_util.InitNonceLoadedAccountsDataSizeLimit,
	)
	txn, err := solana.NewV1Transaction(accounts.Authority.PublicKey().ToBytes(), config, ixns...)
	if err != nil {
		return solana.Transaction{}, errors.Wrap(err, "error creating transaction")
	}
	txn.SetBlockhash(blockhash)

	err = txn.Sign(accounts.Authority.PrivateKey().ToBytes())
	if err != nil {
		return solana.Transaction{}, errors.Wrap(err, "error signing transaction")
	}
	return txn, nil
}

func (p *runtime) initializeNoncePool(ctx context.Context, accounts *newCurrencyAccounts) error {
	ai, _, err := p.data.GetBlockchainAccountInfo(ctx, accounts.NonceMemoryAccount.PublicKey().ToBase58())
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
	ai, _, err := p.data.GetBlockchainAccountInfo(ctx, accounts.TimelockMemoryAccount.PublicKey().ToBase58())
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
	metaplexMetadata, _, err := transaction_util.GetMetaplexMetadataAccount(mint)
	if err != nil {
		return nil, errors.Wrap(err, "error deriving metaplex metadata address")
	}

	// Derived memory account addresses
	memoryAccounts, err := transaction_util.GetCurrencyLaunchMemoryAccounts(vmAccount)
	if err != nil {
		return nil, err
	}
	nonceMemory, timelockMemory := memoryAccounts[0], memoryAccounts[1]

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

		MetaplexMetadata: metaplexMetadata,

		VaultBase:     vaultBase,
		VaultBaseBump: currencyMetadataRecord.VaultCoreBump,

		VaultMint:     vaultMint,
		VaultMintBump: currencyMetadataRecord.VaultMintBump,

		Vm:     vmAccount,
		VmBump: vmMetadataRecord.VmBump,

		Omnibus:     omnibus,
		OmnibusBump: vmMetadataRecord.OmnibusBump,

		NonceMemory:        nonceMemory,
		NonceMemoryAccount: nonceMemory.Address,

		TimelockMemory:        timelockMemory,
		TimelockMemoryAccount: timelockMemory.Address,

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
	err = balance.CreateRecordInTx(ctx, p.data, accountInfoRecord)
	if err != nil {
		return errors.Wrap(err, "error creating creator balance record")
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
