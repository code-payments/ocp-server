package launcher

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/binary"
	"sync"
	"testing"
	"time"

	"github.com/mr-tron/base58"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/code-payments/ocp-server/ocp/common"
	ocp_data "github.com/code-payments/ocp-server/ocp/data"
	"github.com/code-payments/ocp-server/ocp/data/currency"
	holder_memory "github.com/code-payments/ocp-server/ocp/data/currency/holder/memory"
	reserve_memory "github.com/code-payments/ocp-server/ocp/data/currency/reserve/memory"
	"github.com/code-payments/ocp-server/ocp/data/swap"
	vm_metadata "github.com/code-payments/ocp-server/ocp/data/vm/metadata"
	transaction_util "github.com/code-payments/ocp-server/ocp/transaction"
	"github.com/code-payments/ocp-server/pointer"
	"github.com/code-payments/ocp-server/solana"
	address_lookup_table "github.com/code-payments/ocp-server/solana/addresslookuptable"
	"github.com/code-payments/ocp-server/solana/currencycreator"
	"github.com/code-payments/ocp-server/solana/system"
	"github.com/code-payments/ocp-server/solana/token"
	"github.com/code-payments/ocp-server/solana/vm"
	"github.com/code-payments/ocp-server/testutil"
)

// fakeChain simulates the blockchain state the launcher's post-purchase
// phases read and write. It applies the VM memory and ALT instructions that
// change that state, and fails a transaction atomically when an instruction
// would fail on chain, such as initializing an existing account or resizing
// a memory account to a size it already has.
type fakeChain struct {
	ocp_data.Provider

	mu        sync.Mutex
	accounts  map[string][]byte
	slot      uint64
	submitted []solana.Transaction
}

func newFakeChain(data ocp_data.Provider) *fakeChain {
	return &fakeChain{
		Provider: data,
		accounts: map[string][]byte{
			// The system program, which the placeholder ALT points to, exists
			// on chain and isn't an ALT
			base58.Encode(system.ProgramKey[:]): []byte("system_program"),
		},
		slot: 1_000,
	}
}

func (c *fakeChain) GetBlockchainAccountInfo(_ context.Context, account string) (*solana.AccountInfo, uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	data, ok := c.accounts[account]
	if !ok {
		return nil, 0, solana.ErrNoAccountInfo
	}
	return &solana.AccountInfo{Data: bytes.Clone(data)}, c.slot, nil
}

func (c *fakeChain) GetBlockchainBalance(_ context.Context, _ string) (uint64, uint64, error) {
	return 2 * initialAuthorityFundingLamports, c.slot, nil
}

func (c *fakeChain) GetBlockchainSlot(_ context.Context) (uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.slot++
	return c.slot, nil
}

func (c *fakeChain) GetBlockchainLatestBlockhash(_ context.Context) (solana.Blockhash, error) {
	return solana.Blockhash{1}, nil
}

func (c *fakeChain) SubmitBlockchainTransaction(_ context.Context, txn *solana.Transaction) (solana.Signature, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	message, err := txn.Message.Marshal()
	if err != nil {
		return solana.Signature{}, err
	}
	for i, signature := range txn.Signatures {
		if !ed25519.Verify(txn.Message.Accounts[i], message, signature[:]) {
			return solana.Signature{}, errors.Errorf("signature %d is invalid", i)
		}
	}

	var ixns []solana.Instruction
	for _, compiled := range txn.Message.Instructions {
		ixn := solana.Instruction{
			Program: txn.Message.Accounts[compiled.ProgramIndex],
			Data:    compiled.Data,
		}
		for _, index := range compiled.Accounts {
			ixn.Accounts = append(ixn.Accounts, solana.AccountMeta{PublicKey: txn.Message.Accounts[index]})
		}
		ixns = append(ixns, ixn)
	}

	if err := c.applyLocked(ixns...); err != nil {
		return solana.Signature{}, err
	}

	c.submitted = append(c.submitted, *txn)
	var sig solana.Signature
	copy(sig[:], txn.Signature())
	return sig, nil
}

func (c *fakeChain) GetBlockchainTransaction(_ context.Context, sig string) (*solana.TransactionWithMeta, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, txn := range c.submitted {
		if base58.Encode(txn.Signature()) == sig {
			return &solana.TransactionWithMeta{Slot: c.slot, Transaction: txn}, nil
		}
	}
	return nil, solana.ErrSignatureNotFound
}

func (c *fakeChain) apply(t *testing.T, ixns ...solana.Instruction) {
	c.mu.Lock()
	defer c.mu.Unlock()
	require.NoError(t, c.applyLocked(ixns...))
}

func (c *fakeChain) setAccount(account *common.Account, data []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.accounts[account.PublicKey().ToBase58()] = data
}

func (c *fakeChain) getSubmitted() []solana.Transaction {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]solana.Transaction(nil), c.submitted...)
}

// applyLocked applies the instructions to a copy of the state, and only
// commits it when every instruction succeeds
func (c *fakeChain) applyLocked(ixns ...solana.Instruction) error {
	state := make(map[string][]byte, len(c.accounts))
	for account, data := range c.accounts {
		state[account] = bytes.Clone(data)
	}

	for _, ixn := range ixns {
		switch {
		case bytes.Equal(ixn.Program, vm.PROGRAM_ID) && vm.CodeInstruction(ixn.Data[0]) == vm.CodeInstructionInitMemory:
			memory := base58.Encode(ixn.Accounts[2].PublicKey)
			if _, ok := state[memory]; ok {
				return errors.New("memory account already exists")
			}

			data := make([]byte, vm.MemoryAccountSize)
			copy(data, vm.MemoryAccountDiscriminator)
			copy(data[8:], ixn.Accounts[1].PublicKey)
			copy(data[40:], ixn.Data[1:33])     // name
			data[72] = ixn.Data[39]             // bump
			data[73] = byte(vm.MemoryVersionV1) // version
			copy(data[74:76], ixn.Data[37:39])  // account size
			copy(data[76:80], ixn.Data[33:37])  // num accounts
			state[memory] = data

		case bytes.Equal(ixn.Program, vm.PROGRAM_ID) && vm.CodeInstruction(ixn.Data[0]) == vm.CodeInstructionResizeMemory:
			memory := base58.Encode(ixn.Accounts[2].PublicKey)
			data, ok := state[memory]
			if !ok {
				return errors.New("memory account doesn't exist")
			}
			size := int(binary.LittleEndian.Uint32(ixn.Data[1:5]))
			if size <= len(data) {
				return errors.Errorf("memory account is already %d bytes", len(data))
			}
			if size-len(data) > transaction_util.MemoryResizeStepSize {
				return errors.New("resize exceeds the realloc limit")
			}
			state[memory] = append(data, make([]byte, size-len(data))...)

		case bytes.Equal(ixn.Program, address_lookup_table.ProgramKey[:]) && binary.LittleEndian.Uint32(ixn.Data) == 0:
			alt := base58.Encode(ixn.Accounts[0].PublicKey)
			if _, ok := state[alt]; ok {
				return errors.New("alt already exists")
			}

			data := make([]byte, 56)
			binary.LittleEndian.PutUint32(data, 1)
			data[21] = 1
			copy(data[22:], ixn.Accounts[1].PublicKey)
			state[alt] = data

		case bytes.Equal(ixn.Program, address_lookup_table.ProgramKey[:]) && binary.LittleEndian.Uint32(ixn.Data) == 2:
			alt := base58.Encode(ixn.Accounts[0].PublicKey)
			data, ok := state[alt]
			if !ok {
				return errors.New("alt doesn't exist")
			}
			state[alt] = append(data, ixn.Data[12:]...)
		}
	}

	c.accounts = state
	return nil
}

type launcherTestEnv struct {
	ctx      context.Context
	chain    *fakeChain
	runtime  *runtime
	mint     *common.Account
	creator  *common.Account
	accounts *newCurrencyAccounts
}

func setupLauncherTest(t *testing.T, state currency.MetadataState) *launcherTestEnv {
	ctx := t.Context()
	chain := newFakeChain(ocp_data.NewTestDataProvider())
	creator := testutil.NewRandomAccount(t)

	mint := testutil.SetupLaunchpadCurrencyInState(t, chain, reserve_memory.New(), holder_memory.New(), state, creator)

	// Launched currencies start with a placeholder ALT, like the Launch RPC
	// creates them
	metadataRecord, err := chain.GetCurrencyMetadata(ctx, mint.PublicKey().ToBase58())
	require.NoError(t, err)
	metadataRecord.Alt = base58.Encode(system.ProgramKey[:])
	require.NoError(t, chain.SaveCurrencyMetadata(ctx, metadataRecord))

	env := &launcherTestEnv{
		ctx:   ctx,
		chain: chain,
		runtime: &runtime{
			log:        zap.NewNop(),
			data:       chain,
			subsidizer: testutil.SetupRandomSubsidizer(t, chain),
		},
		mint:    mint,
		creator: creator,
	}
	env.accounts = env.getAccounts(t)
	return env
}

func (e *launcherTestEnv) getMetadata(t *testing.T) *currency.MetadataRecord {
	record, err := e.chain.GetCurrencyMetadata(e.ctx, e.mint.PublicKey().ToBase58())
	require.NoError(t, err)
	return record
}

func (e *launcherTestEnv) getAccounts(t *testing.T) *newCurrencyAccounts {
	vmMetadataRecord, err := e.chain.GetVmMetadataByMint(e.ctx, e.mint.PublicKey().ToBase58())
	require.NoError(t, err)
	accounts, err := e.runtime.getNewCurrencyAccounts(e.ctx, e.getMetadata(t), vmMetadataRecord)
	require.NoError(t, err)
	return accounts
}

func (e *launcherTestEnv) setMintExists() {
	data := make([]byte, token.MintSize)
	data[45] = 1 // initialized
	e.chain.setAccount(e.mint, data)
}

// setV1PurchaseState applies what a v1 initial purchase leaves on chain
// besides the currency itself: initialized memory accounts, which the launcher
// still has to resize
func (e *launcherTestEnv) setV1PurchaseState(t *testing.T) {
	e.setMintExists()
	for _, memoryAccount := range []*transaction_util.CurrencyLaunchMemoryAccount{e.accounts.NonceMemory, e.accounts.TimelockMemory} {
		e.chain.apply(t, memoryAccount.NewInitMemoryInstruction(e.accounts.Authority, e.accounts.Vm))
	}
}

func (e *launcherTestEnv) completeBlockchainInitialization(t *testing.T) error {
	return e.runtime.completeBlockchainInitialization(e.ctx, e.getMetadata(t), e.getAccounts(t))
}

// requireFullyInitialized requires every account the post-purchase phases are
// responsible for to be in its final state
func (e *launcherTestEnv) requireFullyInitialized(t *testing.T) {
	accounts := e.getAccounts(t)

	require.True(t, isAltDerived(e.getMetadata(t)))

	for _, memoryAccount := range []*common.Account{accounts.NonceMemoryAccount, accounts.TimelockMemoryAccount} {
		ok, err := validateMemoryAccountIsResized(e.ctx, e.chain, memoryAccount)
		require.NoError(t, err)
		assert.True(t, ok, "memory account isn't resized")
	}

	ai, _, err := e.chain.GetBlockchainAccountInfo(e.ctx, accounts.Alt.PublicKey().ToBase58())
	require.NoError(t, err)
	var alt address_lookup_table.AddressLookupTableAccount
	require.NoError(t, alt.Unmarshal(ai.Data))
	assert.Equal(t, accounts.Authority.PublicKey().ToBytes(), []byte(alt.Authority))
	assert.Equal(t, []ed25519.PublicKey{
		accounts.Vm.PublicKey().ToBytes(),
		accounts.Omnibus.PublicKey().ToBytes(),
		accounts.Mint.PublicKey().ToBytes(),
		accounts.LiquidityPool.PublicKey().ToBytes(),
		accounts.VaultBase.PublicKey().ToBytes(),
		accounts.VaultMint.PublicKey().ToBytes(),
		common.CoreMintAccount.PublicKey().ToBytes(),
		system.RentSysVar,
		system.RecentBlockhashesSysVar,
	}, alt.Addresses)

	// Every phase is gated on chain state, so another attempt does nothing
	before := len(e.chain.getSubmitted())
	require.NoError(t, e.completeBlockchainInitialization(t))
	assert.Len(t, e.chain.getSubmitted(), before, "a completed initialization submitted more transactions")
}

// submittedPrograms summarizes each submitted transaction by the programs it
// invokes, excluding ComputeBudget
func submittedPrograms(txns []solana.Transaction) [][]string {
	names := map[string]string{
		base58.Encode(vm.PROGRAM_ID):                          "vm",
		base58.Encode(currencycreator.PROGRAM_ID):             "currency_creator",
		base58.Encode(address_lookup_table.ProgramKey[:]):     "alt",
		base58.Encode(token.AssociatedTokenAccountProgramKey): "ata",
		base58.Encode(system.ProgramKey[:]):                   "system",
	}

	var res [][]string
	for _, txn := range txns {
		var programs []string
		for _, ixn := range txn.Message.Instructions {
			if name, ok := names[base58.Encode(txn.Message.Accounts[ixn.ProgramIndex])]; ok {
				programs = append(programs, name)
			}
		}
		res = append(res, programs)
	}
	return res
}

func TestCompleteBlockchainInitialization(t *testing.T) {
	t.Run("v1 initial purchase needs the resizes and alt", func(t *testing.T) {
		env := setupLauncherTest(t, currency.MetadataStateCompletingInitialization)
		env.setV1PurchaseState(t)

		require.NoError(t, env.completeBlockchainInitialization(t))

		assert.Equal(t, [][]string{
			{"vm", "vm", "vm", "vm", "vm", "vm", "vm", "vm", "vm", "vm"}, // 2 nonce + 8 timelock resizes
			{"alt"}, // create
			{"alt"}, // extend
		}, submittedPrograms(env.chain.getSubmitted()))
		env.requireFullyInitialized(t)
	})

	t.Run("v0 initial purchase initializes everything after the purchase", func(t *testing.T) {
		env := setupLauncherTest(t, currency.MetadataStateCompletingInitialization)
		env.setMintExists()

		require.NoError(t, env.completeBlockchainInitialization(t))

		assert.Equal(t, [][]string{
			{"currency_creator", "vm", "vm", "ata"},                      // metadata, memory accounts, fee account
			{"vm", "vm", "vm", "vm", "vm", "vm", "vm", "vm", "vm", "vm"}, // 2 nonce + 8 timelock resizes
			{"alt"}, // create
			{"alt"}, // extend
		}, submittedPrograms(env.chain.getSubmitted()))
		env.requireFullyInitialized(t)
	})

	t.Run("launch in progress before the deploy keeps its alt", func(t *testing.T) {
		env := setupLauncherTest(t, currency.MetadataStateCompletingInitialization)
		env.setMintExists()

		// The previous phase 2 transaction initialized the memory accounts and
		// created the ALT together, but nothing was resized or extended
		altAccount := testutil.NewRandomAccount(t)
		metadataRecord := env.getMetadata(t)
		metadataRecord.Alt = altAccount.PublicKey().ToBase58()
		require.NoError(t, env.chain.SaveCurrencyMetadata(env.ctx, metadataRecord))
		env.chain.apply(t,
			env.accounts.NonceMemory.NewInitMemoryInstruction(env.accounts.Authority, env.accounts.Vm),
			env.accounts.TimelockMemory.NewInitMemoryInstruction(env.accounts.Authority, env.accounts.Vm),
			address_lookup_table.Create(altAccount.PublicKey().ToBytes(), env.accounts.Authority.PublicKey().ToBytes(), env.accounts.Authority.PublicKey().ToBytes(), 1, 255),
		)

		require.NoError(t, env.completeBlockchainInitialization(t))

		assert.Equal(t, [][]string{
			{"vm", "vm", "vm", "vm", "vm", "vm", "vm", "vm", "vm", "vm"},
			{"alt"}, // extend
		}, submittedPrograms(env.chain.getSubmitted()))
		assert.Equal(t, altAccount.PublicKey().ToBase58(), env.getMetadata(t).Alt)
		env.requireFullyInitialized(t)
	})

	t.Run("saved alt that was never created is replaced", func(t *testing.T) {
		env := setupLauncherTest(t, currency.MetadataStateCompletingInitialization)
		env.setV1PurchaseState(t)

		uncreatedAlt := testutil.NewRandomAccount(t).PublicKey().ToBase58()
		metadataRecord := env.getMetadata(t)
		metadataRecord.Alt = uncreatedAlt
		require.NoError(t, env.chain.SaveCurrencyMetadata(env.ctx, metadataRecord))

		require.NoError(t, env.completeBlockchainInitialization(t))

		assert.Equal(t, [][]string{
			{"vm", "vm", "vm", "vm", "vm", "vm", "vm", "vm", "vm", "vm"}, // 2 nonce + 8 timelock resizes
			{"alt"}, // create
			{"alt"}, // extend
		}, submittedPrograms(env.chain.getSubmitted()))
		assert.NotEqual(t, uncreatedAlt, env.getMetadata(t).Alt)
		env.requireFullyInitialized(t)
	})

	t.Run("memory accounts launched with the earlier 1,000 nonce pool", func(t *testing.T) {
		env := setupLauncherTest(t, currency.MetadataStateCompletingInitialization)
		env.setMintExists()

		nonceMemory := *env.accounts.NonceMemory
		nonceMemory.NumAccounts = 1_000
		env.chain.apply(t,
			nonceMemory.NewInitMemoryInstruction(env.accounts.Authority, env.accounts.Vm),
			env.accounts.TimelockMemory.NewInitMemoryInstruction(env.accounts.Authority, env.accounts.Vm),
		)

		require.NoError(t, env.completeBlockchainInitialization(t))

		assert.Equal(t, [][]string{
			{"vm", "vm", "vm", "vm", "vm", "vm", "vm", "vm", "vm", "vm", "vm", "vm", "vm", "vm", "vm"}, // 7 nonce + 8 timelock resizes
			{"alt"},
			{"alt"},
		}, submittedPrograms(env.chain.getSubmitted()))
		env.requireFullyInitialized(t)
	})

	t.Run("partially resized memory accounts only grow", func(t *testing.T) {
		env := setupLauncherTest(t, currency.MetadataStateCompletingInitialization)
		env.setMintExists()

		env.chain.apply(t,
			env.accounts.NonceMemory.NewInitMemoryInstruction(env.accounts.Authority, env.accounts.Vm),
			env.accounts.TimelockMemory.NewInitMemoryInstruction(env.accounts.Authority, env.accounts.Vm),
		)
		env.chain.apply(t, env.accounts.NonceMemory.NewResizeMemoryInstructions(env.accounts.Authority, env.accounts.Vm)...)

		require.NoError(t, env.completeBlockchainInitialization(t))

		assert.Equal(t, [][]string{
			{"vm", "vm", "vm", "vm", "vm", "vm", "vm", "vm"}, // timelock resizes only
			{"alt"},
			{"alt"},
		}, submittedPrograms(env.chain.getSubmitted()))
		env.requireFullyInitialized(t)
	})

	t.Run("missing mint", func(t *testing.T) {
		env := setupLauncherTest(t, currency.MetadataStateCompletingInitialization)

		require.Error(t, env.completeBlockchainInitialization(t))
		assert.Empty(t, env.chain.getSubmitted())
	})
}

func TestHandleStatePrePurchaseSetup(t *testing.T) {
	for _, tc := range []struct {
		name                      string
		treasuryFunded            bool
		transactionVersion        solana.MessageVersion
		expectCurrencyInitialized bool
	}{
		{"core mint funded v0", false, solana.MessageVersion0, false},
		{"core mint funded v1", false, solana.MessageVersion1, false},
		{"treasury funded v0", true, solana.MessageVersion0, true},
		{"treasury funded v1", true, solana.MessageVersion1, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env := setupLauncherTest(t, currency.MetadataStatePrePurchaseSetup)

			vmMetadataRecord, err := env.chain.GetVmMetadataByMint(env.ctx, env.mint.PublicKey().ToBase58())
			require.NoError(t, err)
			vmMetadataRecord.State = vm_metadata.StateUnknown
			require.NoError(t, env.chain.SaveVmMetadata(env.ctx, vmMetadataRecord))

			fromMint := common.CoreMintAccount
			if tc.treasuryFunded {
				fromMint = testutil.NewRandomAccount(t)
			}
			env.saveSubmittingSwap(t, fromMint, tc.transactionVersion)

			require.NoError(t, env.runtime.handleStatePrePurchaseSetup(env.ctx, env.getMetadata(t)))

			// The authority is funded in every flow. The currency is only
			// initialized ahead of v0 purchases funded outside the core mint,
			// because v1 purchases initialize it themselves.
			expected := [][]string{{"system"}}
			if tc.expectCurrencyInitialized {
				expected = append(expected, []string{"currency_creator", "currency_creator", "vm", "ata"})
			}
			assert.Equal(t, expected, submittedPrograms(env.chain.getSubmitted()))

			assert.Equal(t, currency.MetadataStateExecutingInitialPurchase, env.getMetadata(t).State)
			vmMetadataRecord, err = env.chain.GetVmMetadataByMint(env.ctx, env.mint.PublicKey().ToBase58())
			require.NoError(t, err)
			assert.Equal(t, vm_metadata.StateInitializing, vmMetadataRecord.State)
		})
	}
}

func (e *launcherTestEnv) saveSubmittingSwap(t *testing.T, fromMint *common.Account, transactionVersion solana.MessageVersion) {
	payer := testutil.NewRandomAccount(t)
	ixn := system.AdvanceNonce(testutil.NewRandomAccount(t).PublicKey().ToBytes(), payer.PublicKey().ToBytes())

	var txn solana.Transaction
	if transactionVersion == solana.MessageVersion1 {
		var err error
		txn, err = solana.NewV1Transaction(payer.PublicKey().ToBytes(), solana.TransactionConfig{
			ComputeUnitLimit:            pointer.Uint32(100_000),
			LoadedAccountsDataSizeLimit: pointer.Uint32(1024 * 1024),
		}, ixn)
		require.NoError(t, err)
	} else {
		txn = solana.NewV0Transaction(payer.PublicKey().ToBytes(), nil, []solana.Instruction{ixn})
	}
	require.NoError(t, txn.Sign(payer.PrivateKey().ToBytes()))
	blob, err := txn.Marshal()
	require.NoError(t, err)

	record := &swap.Record{
		SwapId:               testutil.NewRandomAccount(t).PublicKey().ToBase58(),
		Kind:                 swap.KindReserve,
		Owner:                e.creator.PublicKey().ToBase58(),
		FromMint:             fromMint.PublicKey().ToBase58(),
		ToMint:               e.mint.PublicKey().ToBase58(),
		SwapAmount:           1,
		FundingSource:        swap.FundingSourceSubmitIntent,
		FundingId:            testutil.NewRandomAccount(t).PublicKey().ToBase58(),
		Nonce:                testutil.NewRandomAccount(t).PublicKey().ToBase58(),
		Blockhash:            base58.Encode(make([]byte, 32)),
		ProofSignature:       base58.Encode(make([]byte, 64)),
		TransactionSignature: base58.Encode(txn.Signature()),
		TransactionBlob:      blob,
		State:                swap.StateSubmitting,
		CreatedAt:            time.Now(),
	}
	require.NoError(t, e.chain.SaveSwap(e.ctx, record))
}

func TestIsV1InitialPurchase(t *testing.T) {
	_, err := isV1InitialPurchase(&swap.Record{TransactionBlob: []byte{1, 2, 3}})
	assert.Error(t, err)
}
