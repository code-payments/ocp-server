package balance

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	commonpb "github.com/code-payments/ocp-protobuf-api/generated/go/common/v1"

	currency_lib "github.com/code-payments/ocp-server/currency"
	"github.com/code-payments/ocp-server/ocp/common"
	ocp_data "github.com/code-payments/ocp-server/ocp/data"
	"github.com/code-payments/ocp-server/ocp/data/account"
	"github.com/code-payments/ocp-server/ocp/data/action"
	"github.com/code-payments/ocp-server/ocp/data/currency"
	"github.com/code-payments/ocp-server/ocp/data/deposit"
	"github.com/code-payments/ocp-server/ocp/data/intent"
	"github.com/code-payments/ocp-server/ocp/data/swap"
	"github.com/code-payments/ocp-server/ocp/data/transaction"
	"github.com/code-payments/ocp-server/solana/currencycreator"
	timelock_token_v1 "github.com/code-payments/ocp-server/solana/timelock/v1"
	"github.com/code-payments/ocp-server/testutil"
)

func TestDefaultCalculationMethods_NewCodeAccount(t *testing.T) {
	env := setupBalanceTestEnv(t)

	vmConfig := testutil.NewRandomVmConfig(t, true)
	newOwnerAccount := testutil.NewRandomAccount(t)
	newTokenAccount, err := newOwnerAccount.ToTimelockVault(vmConfig)
	require.NoError(t, err)

	data := &balanceTestData{
		vmConfig:  vmConfig,
		codeUsers: []*common.Account{newOwnerAccount},
	}

	setupBalanceTestData(t, env, data)

	accountRecords, err := common.GetLatestTokenAccountRecordsForOwner(env.ctx, env.data, newOwnerAccount)
	require.NoError(t, err)

	balance, err := CalculateFromCache(env.ctx, env.data, newTokenAccount)
	require.NoError(t, err)
	assert.EqualValues(t, 0, balance)

	balanceByAccount, err := BatchCalculateFromCacheWithAccountRecords(env.ctx, env.data, accountRecords[vmConfig.Mint.PublicKey().ToBase58()][commonpb.AccountType_PRIMARY][0])
	require.NoError(t, err)
	require.Len(t, balanceByAccount, 1)
	assert.EqualValues(t, 0, balanceByAccount[newTokenAccount.PublicKey().ToBase58()])

	balanceByAccount, err = BatchCalculateFromCacheWithTokenAccounts(env.ctx, env.data, newTokenAccount)
	require.NoError(t, err)
	require.Len(t, balanceByAccount, 1)
	assert.EqualValues(t, 0, balanceByAccount[newTokenAccount.PublicKey().ToBase58()])
}

func TestDefaultCalculationMethods_DepositFromExternalWallet(t *testing.T) {
	env := setupBalanceTestEnv(t)

	vmConfig := testutil.NewRandomVmConfig(t, true)
	owner := testutil.NewRandomAccount(t)
	depositAccount, err := owner.ToTimelockVault(vmConfig)
	require.NoError(t, err)

	externalAccount := testutil.NewRandomAccount(t)

	data := &balanceTestData{
		vmConfig:  vmConfig,
		codeUsers: []*common.Account{owner},
		transactions: []balanceTestTransaction{
			// The following entries are added to the balance
			{source: externalAccount, destination: depositAccount, quantity: 1, transactionState: transaction.ConfirmationFinalized},
			{source: externalAccount, destination: depositAccount, quantity: 10, transactionState: transaction.ConfirmationFinalized},
			// The following entries aren't added to the balance because they aren't finalized
			{source: externalAccount, destination: depositAccount, quantity: 100, transactionState: transaction.ConfirmationFailed},
			{source: externalAccount, destination: depositAccount, quantity: 1000, transactionState: transaction.ConfirmationPending},
			{source: externalAccount, destination: depositAccount, quantity: 10000, transactionState: transaction.ConfirmationUnknown},
		},
	}
	setupBalanceTestData(t, env, data)

	balance, err := CalculateFromCache(env.ctx, env.data, depositAccount)
	require.NoError(t, err)
	assert.EqualValues(t, 11, balance)

	accountRecords, err := common.GetLatestTokenAccountRecordsForOwner(env.ctx, env.data, owner)
	require.NoError(t, err)

	balanceByAccount, err := BatchCalculateFromCacheWithAccountRecords(env.ctx, env.data, accountRecords[vmConfig.Mint.PublicKey().ToBase58()][commonpb.AccountType_PRIMARY][0])
	require.NoError(t, err)
	require.Len(t, balanceByAccount, 1)
	assert.EqualValues(t, 11, balanceByAccount[depositAccount.PublicKey().ToBase58()])

	balanceByAccount, err = BatchCalculateFromCacheWithTokenAccounts(env.ctx, env.data, depositAccount)
	require.NoError(t, err)
	require.Len(t, balanceByAccount, 1)
	assert.EqualValues(t, 11, balanceByAccount[depositAccount.PublicKey().ToBase58()])
}

func TestDefaultCalculationMethods_MultipleIntents(t *testing.T) {
	env := setupBalanceTestEnv(t)

	vmConfig := testutil.NewRandomVmConfig(t, true)

	owner1 := testutil.NewRandomAccount(t)
	a1, err := owner1.ToTimelockVault(vmConfig)
	require.NoError(t, err)

	owner2 := testutil.NewRandomAccount(t)
	a2, err := owner2.ToTimelockVault(vmConfig)
	require.NoError(t, err)

	owner3 := testutil.NewRandomAccount(t)
	a3, err := owner3.ToTimelockVault(vmConfig)
	require.NoError(t, err)

	owner4 := testutil.NewRandomAccount(t)
	a4, err := owner4.ToTimelockVault(vmConfig)
	require.NoError(t, err)

	externalAccount := testutil.NewRandomAccount(t)

	data := &balanceTestData{
		vmConfig:  vmConfig,
		codeUsers: []*common.Account{owner1, owner2, owner3, owner4},
		transactions: []balanceTestTransaction{
			// Fund account a1 through a4 with an external deposit
			{source: externalAccount, destination: a1, quantity: 1, transactionState: transaction.ConfirmationFinalized},
			{source: externalAccount, destination: a2, quantity: 10, transactionState: transaction.ConfirmationFinalized},
			{source: externalAccount, destination: a3, quantity: 100, transactionState: transaction.ConfirmationFinalized},
			{source: externalAccount, destination: a4, quantity: 1000, transactionState: transaction.ConfirmationFinalized},
			// Confirmed intents are incorporated into balance calculations
			{source: a4, destination: a1, quantity: 1, intentID: "i1", intentState: intent.StateConfirmed, actionState: action.StateConfirmed, transactionState: transaction.ConfirmationFinalized},
			{source: a4, destination: a1, quantity: 2, intentID: "i2", intentState: intent.StateConfirmed, actionState: action.StateConfirmed, transactionState: transaction.ConfirmationFinalized},
			// Pending intents are incorporated into balance calculations
			{source: a4, destination: a2, quantity: 3, intentID: "i3", intentState: intent.StatePending, actionState: action.StatePending},
			{source: a4, destination: a2, quantity: 4, intentID: "i4", intentState: intent.StatePending, actionState: action.StatePending},
			// Failed intents are incorporated into balance calculations. We'll
			// always make the user whole.
			{source: a4, destination: a3, quantity: 5, intentID: "i5", intentState: intent.StateFailed, actionState: action.StateFailed},
			{source: a4, destination: a3, quantity: 6, intentID: "i6", intentState: intent.StateFailed, actionState: action.StateFailed},
			// Intents in the unknown state are incorporated differently depending
			// on the intent type, since it infers which intent system it came from.
			// Legacy intents are not incorporated, as the intent is not committed by
			// the client. Intents could theoretically by in the unknown state under
			// the new system, but we should limit this as much as possible.
			{source: a4, destination: a1, quantity: 7, intentID: "i7", intentState: intent.StateUnknown, actionState: action.StateUnknown},
			// Revoked intents are not incorporated into balance calculations.
			{source: a4, destination: a2, quantity: 8, intentID: "i8", intentState: intent.StateRevoked, actionState: action.StateRevoked},
		},
	}

	setupBalanceTestData(t, env, data)

	balance, err := CalculateFromCache(env.ctx, env.data, a1)
	require.NoError(t, err)
	assert.EqualValues(t, 11, balance)

	balance, err = CalculateFromCache(env.ctx, env.data, a2)
	require.NoError(t, err)
	assert.EqualValues(t, 17, balance)

	balance, err = CalculateFromCache(env.ctx, env.data, a3)
	require.NoError(t, err)
	assert.EqualValues(t, 111, balance)

	balance, err = CalculateFromCache(env.ctx, env.data, a4)
	require.NoError(t, err)
	assert.EqualValues(t, 972, balance)

	accountRecords1, err := common.GetLatestTokenAccountRecordsForOwner(env.ctx, env.data, owner1)
	require.NoError(t, err)

	accountRecords2, err := common.GetLatestTokenAccountRecordsForOwner(env.ctx, env.data, owner2)
	require.NoError(t, err)

	accountRecords3, err := common.GetLatestTokenAccountRecordsForOwner(env.ctx, env.data, owner3)
	require.NoError(t, err)

	accountRecords4, err := common.GetLatestTokenAccountRecordsForOwner(env.ctx, env.data, owner4)
	require.NoError(t, err)

	balanceByAccount, err := BatchCalculateFromCacheWithAccountRecords(env.ctx, env.data, accountRecords1[vmConfig.Mint.PublicKey().ToBase58()][commonpb.AccountType_PRIMARY][0], accountRecords2[vmConfig.Mint.PublicKey().ToBase58()][commonpb.AccountType_PRIMARY][0], accountRecords3[vmConfig.Mint.PublicKey().ToBase58()][commonpb.AccountType_PRIMARY][0], accountRecords4[vmConfig.Mint.PublicKey().ToBase58()][commonpb.AccountType_PRIMARY][0])
	require.NoError(t, err)
	require.Len(t, balanceByAccount, 4)
	assert.EqualValues(t, 11, balanceByAccount[a1.PublicKey().ToBase58()])
	assert.EqualValues(t, 17, balanceByAccount[a2.PublicKey().ToBase58()])
	assert.EqualValues(t, 111, balanceByAccount[a3.PublicKey().ToBase58()])
	assert.EqualValues(t, 972, balanceByAccount[a4.PublicKey().ToBase58()])

	balanceByAccount, err = BatchCalculateFromCacheWithTokenAccounts(env.ctx, env.data, a1, a2, a3, a4)
	require.NoError(t, err)
	require.Len(t, balanceByAccount, 4)
	assert.EqualValues(t, 11, balanceByAccount[a1.PublicKey().ToBase58()])
	assert.EqualValues(t, 17, balanceByAccount[a2.PublicKey().ToBase58()])
	assert.EqualValues(t, 111, balanceByAccount[a3.PublicKey().ToBase58()])
	assert.EqualValues(t, 972, balanceByAccount[a4.PublicKey().ToBase58()])
}

func TestDefaultCalculationMethods_BackAndForth(t *testing.T) {
	env := setupBalanceTestEnv(t)

	vmConfig := testutil.NewRandomVmConfig(t, true)

	owner1 := testutil.NewRandomAccount(t)
	a1, err := owner1.ToTimelockVault(vmConfig)
	require.NoError(t, err)

	owner2 := testutil.NewRandomAccount(t)
	a2, err := owner2.ToTimelockVault(vmConfig)
	require.NoError(t, err)

	externalAccount := testutil.NewRandomAccount(t)

	data := &balanceTestData{
		vmConfig:  vmConfig,
		codeUsers: []*common.Account{owner1, owner2},
		transactions: []balanceTestTransaction{
			// Fund account a1 through an external deposit
			{source: externalAccount, destination: a1, quantity: 1, transactionState: transaction.ConfirmationFinalized},
			// Setup a set of intents that result in back and forth movement of the Kin
			{source: a1, destination: a2, quantity: 1, intentID: "i1", intentState: intent.StateConfirmed, actionState: action.StateConfirmed, transactionState: transaction.ConfirmationFinalized},
			{source: a2, destination: a1, quantity: 1, intentID: "i2", intentState: intent.StateConfirmed, actionState: action.StateConfirmed, transactionState: transaction.ConfirmationFinalized},
			{source: a1, destination: a2, quantity: 1, intentID: "i3", intentState: intent.StatePending, actionState: action.StatePending},
			{source: a2, destination: a1, quantity: 1, intentID: "i4", intentState: intent.StatePending, actionState: action.StatePending},
			{source: a1, destination: a2, quantity: 1, intentID: "i5", intentState: intent.StatePending, actionState: action.StatePending},
		},
	}

	setupBalanceTestData(t, env, data)

	balance, err := CalculateFromCache(env.ctx, env.data, a1)
	require.NoError(t, err)
	assert.EqualValues(t, 0, balance)

	balance, err = CalculateFromCache(env.ctx, env.data, a2)
	require.NoError(t, err)
	assert.EqualValues(t, 1, balance)

	accountRecords1, err := common.GetLatestTokenAccountRecordsForOwner(env.ctx, env.data, owner1)
	require.NoError(t, err)

	accountRecords2, err := common.GetLatestTokenAccountRecordsForOwner(env.ctx, env.data, owner2)
	require.NoError(t, err)

	balanceByAccount, err := BatchCalculateFromCacheWithAccountRecords(env.ctx, env.data, accountRecords1[vmConfig.Mint.PublicKey().ToBase58()][commonpb.AccountType_PRIMARY][0], accountRecords2[vmConfig.Mint.PublicKey().ToBase58()][commonpb.AccountType_PRIMARY][0])
	require.NoError(t, err)
	require.Len(t, balanceByAccount, 2)
	assert.EqualValues(t, 0, balanceByAccount[a1.PublicKey().ToBase58()])
	assert.EqualValues(t, 1, balanceByAccount[a2.PublicKey().ToBase58()])

	balanceByAccount, err = BatchCalculateFromCacheWithTokenAccounts(env.ctx, env.data, a1, a2)
	require.NoError(t, err)
	require.Len(t, balanceByAccount, 2)
	assert.EqualValues(t, 0, balanceByAccount[a1.PublicKey().ToBase58()])
	assert.EqualValues(t, 1, balanceByAccount[a2.PublicKey().ToBase58()])
}

func TestDefaultCalculationMethods_SelfPayments(t *testing.T) {
	env := setupBalanceTestEnv(t)

	vmConfig := testutil.NewRandomVmConfig(t, true)
	ownerAccount := testutil.NewRandomAccount(t)
	tokenAccount, err := ownerAccount.ToTimelockVault(vmConfig)
	require.NoError(t, err)

	externalAccount := testutil.NewRandomAccount(t)

	data := &balanceTestData{
		vmConfig:  vmConfig,
		codeUsers: []*common.Account{ownerAccount},
		transactions: []balanceTestTransaction{
			// Fund account the token account through an external deposit
			{source: externalAccount, destination: tokenAccount, quantity: 1, transactionState: transaction.ConfirmationFinalized},
			// Setup a set of intents that result in self-payments and no-ops to
			// the balance calculation
			{source: tokenAccount, destination: tokenAccount, quantity: 1, intentID: "i1", intentState: intent.StateConfirmed, actionState: action.StateConfirmed, transactionState: transaction.ConfirmationFinalized},
			{source: tokenAccount, destination: tokenAccount, quantity: 1, intentID: "i2", intentState: intent.StateConfirmed, actionState: action.StateConfirmed, transactionState: transaction.ConfirmationFinalized},
			{source: tokenAccount, destination: tokenAccount, quantity: 1, intentID: "i3", intentState: intent.StatePending, actionState: action.StatePending},
			{source: tokenAccount, destination: tokenAccount, quantity: 1, intentID: "i4", intentState: intent.StatePending, actionState: action.StatePending},
		},
	}

	setupBalanceTestData(t, env, data)

	balance, err := CalculateFromCache(env.ctx, env.data, tokenAccount)
	require.NoError(t, err)
	assert.EqualValues(t, 1, balance)

	accountRecords, err := common.GetLatestTokenAccountRecordsForOwner(env.ctx, env.data, ownerAccount)
	require.NoError(t, err)

	balanceByAccount, err := BatchCalculateFromCacheWithAccountRecords(env.ctx, env.data, accountRecords[vmConfig.Mint.PublicKey().ToBase58()][commonpb.AccountType_PRIMARY][0])
	require.NoError(t, err)
	require.Len(t, balanceByAccount, 1)
	assert.EqualValues(t, 1, balanceByAccount[tokenAccount.PublicKey().ToBase58()])

	balanceByAccount, err = BatchCalculateFromCacheWithTokenAccounts(env.ctx, env.data, tokenAccount)
	require.NoError(t, err)
	require.Len(t, balanceByAccount, 1)
	assert.EqualValues(t, 1, balanceByAccount[tokenAccount.PublicKey().ToBase58()])
}

func TestDefaultCalculationMethods_NotManagedByCode(t *testing.T) {
	env := setupBalanceTestEnv(t)

	vmConfig := testutil.NewRandomVmConfig(t, true)
	ownerAccount := testutil.NewRandomAccount(t)
	tokenAccount, err := ownerAccount.ToTimelockVault(vmConfig)
	require.NoError(t, err)

	data := &balanceTestData{
		vmConfig:  vmConfig,
		codeUsers: []*common.Account{ownerAccount},
	}

	setupBalanceTestData(t, env, data)

	timelockRecord, err := env.data.GetTimelockByVault(env.ctx, tokenAccount.PublicKey().ToBase58())
	require.NoError(t, err)
	timelockRecord.VaultState = timelock_token_v1.StateWaitingForTimeout
	timelockRecord.Block += 1
	require.NoError(t, env.data.SaveTimelock(env.ctx, timelockRecord))

	accountRecords, err := common.GetLatestTokenAccountRecordsForOwner(env.ctx, env.data, ownerAccount)
	require.NoError(t, err)

	_, err = CalculateFromCache(env.ctx, env.data, tokenAccount)
	assert.Equal(t, ErrNotManagedByCode, err)

	_, err = BatchCalculateFromCacheWithAccountRecords(env.ctx, env.data, accountRecords[vmConfig.Mint.PublicKey().ToBase58()][commonpb.AccountType_PRIMARY][0])
	assert.Equal(t, ErrNotManagedByCode, err)

	_, err = BatchCalculateFromCacheWithTokenAccounts(env.ctx, env.data, tokenAccount)
	assert.Equal(t, ErrNotManagedByCode, err)
}

func TestDefaultCalculation_ExternalAccount(t *testing.T) {
	env := setupBalanceTestEnv(t)
	externalAccount := testutil.NewRandomAccount(t)
	_, err := CalculateFromCache(env.ctx, env.data, externalAccount)
	assert.Equal(t, ErrNotManagedByCode, err)

	// Note: not possible with batch method, since we wouldn't have account records
}

type balanceTestEnv struct {
	ctx  context.Context
	data ocp_data.Provider
}

type balanceTestData struct {
	vmConfig     *common.VmConfig
	codeUsers    []*common.Account
	transactions []balanceTestTransaction
}

type balanceTestTransaction struct {
	source, destination *common.Account
	quantity            uint64

	intentID    string
	intentState intent.State
	actionState action.State

	transactionState transaction.Confirmation
}

func setupBalanceTestEnv(t *testing.T) (env balanceTestEnv) {
	env.ctx = context.Background()
	env.data = ocp_data.NewTestDataProvider()
	testutil.SetupRandomSubsidizer(t, env.data)
	return env
}

func setupBalanceTestData(t *testing.T, env balanceTestEnv, data *balanceTestData) {
	for _, owner := range data.codeUsers {
		timelockAccounts, err := owner.GetTimelockAccounts(data.vmConfig)
		require.NoError(t, err)
		timelockRecord := timelockAccounts.ToDBRecord()
		timelockRecord.VaultState = timelock_token_v1.StateLocked
		timelockRecord.Block += 1
		require.NoError(t, env.data.SaveTimelock(env.ctx, timelockRecord))

		accountInfoRecord := &account.Record{
			OwnerAccount:     owner.PublicKey().ToBase58(),
			AuthorityAccount: owner.PublicKey().ToBase58(),
			TokenAccount:     timelockRecord.VaultAddress,
			MintAccount:      data.vmConfig.Mint.PublicKey().ToBase58(),
			AccountType:      commonpb.AccountType_PRIMARY,
		}
		require.NoError(t, env.data.CreateAccountInfo(env.ctx, accountInfoRecord))
	}

	for i, txn := range data.transactions {
		// Setup the intent record with an equivalent action record
		if len(txn.intentID) > 0 {
			intentRecord := &intent.Record{
				IntentId:              txn.intentID,
				IntentType:            intent.SendPublicPayment,
				MintAccount:           data.vmConfig.Mint.PublicKey().ToBase58(),
				InitiatorOwnerAccount: "owner",
				SendPublicPaymentMetadata: &intent.SendPublicPaymentMetadata{
					DestinationOwnerAccount: testutil.NewRandomAccount(t).PublicKey().ToBase58(),
					DestinationTokenAccount: txn.destination.PublicKey().ToBase58(),
					Quantity:                txn.quantity,

					ExchangeCurrency: currency_lib.USD,
					ExchangeRate:     1.0,
					NativeAmount:     1.0,
					UsdMarketValue:   1.0,
				},
				State:     txn.intentState,
				CreatedAt: time.Now(),
			}
			require.NoError(t, env.data.SaveIntent(env.ctx, intentRecord))

			actionRecord := &action.Record{
				Intent:     txn.intentID,
				IntentType: intent.SendPublicPayment,

				ActionId:   0,
				ActionType: action.NoPrivacyTransfer,

				Source:      txn.source.PublicKey().ToBase58(),
				Destination: &intentRecord.SendPublicPaymentMetadata.DestinationTokenAccount,
				Quantity:    &intentRecord.SendPublicPaymentMetadata.Quantity,

				State: txn.actionState,
			}
			require.NoError(t, env.data.PutAllActions(env.ctx, actionRecord))
		}

		// There's no intent, so we have an external deposit
		if len(txn.intentID) == 0 && txn.transactionState != transaction.ConfirmationUnknown {
			depositRecord := &deposit.Record{
				Signature:   fmt.Sprintf("txn%d", i),
				Destination: txn.destination.PublicKey().ToBase58(),
				Amount:      txn.quantity,

				Slot:              12345,
				ConfirmationState: txn.transactionState,

				CreatedAt: time.Now(),
			}
			require.NoError(t, env.data.SaveExternalDeposit(env.ctx, depositRecord))
		}
	}
}

func TestGetDeltaQuarksFromPendingSwaps_NoPendingSwaps(t *testing.T) {
	env := setupBalanceTestEnv(t)
	owner := testutil.NewRandomAccount(t)

	deltaByMint, err := GetDeltaQuarksFromPendingSwaps(env.ctx, env.data, owner.PublicKey().ToBase58())
	require.NoError(t, err)
	assert.Empty(t, deltaByMint)
}

func TestGetDeltaQuarksFromPendingSwaps_FiltersByFundingSource(t *testing.T) {
	env := setupBalanceTestEnv(t)
	owner := testutil.NewRandomAccount(t)
	launchpadMint := testutil.NewRandomAccount(t)

	// Create reserve for the launchpad mint
	reserveRecord := &currency.ReserveRecord{
		Mint:              launchpadMint.PublicKey().ToBase58(),
		SupplyFromBonding: 1_000_000_000_000, // 100 tokens at 10 decimals
		Time:              time.Now(),
	}
	require.NoError(t, env.data.PutCurrencyReserve(env.ctx, reserveRecord))

	// Create swap with ExternalWallet funding source (should be filtered out)
	swapRecord := &swap.Record{
		SwapId:               "swap1",
		Owner:                owner.PublicKey().ToBase58(),
		FromMint:             common.CoreMintAccount.PublicKey().ToBase58(),
		ToMint:               launchpadMint.PublicKey().ToBase58(),
		Amount:               1_000_000, // 1 core mint unit
		FundingId:            "funding1",
		FundingSource:        swap.FundingSourceExternalWallet,
		Nonce:                testutil.NewRandomAccount(t).PublicKey().ToBase58(),
		Blockhash:            "blockhash1",
		ProofSignature:       "proofsig1",
		TransactionSignature: "txsig1",
		State:                swap.StateFunding,
	}
	require.NoError(t, env.data.SaveSwap(env.ctx, swapRecord))

	deltaByMint, err := GetDeltaQuarksFromPendingSwaps(env.ctx, env.data, owner.PublicKey().ToBase58())
	require.NoError(t, err)
	assert.Empty(t, deltaByMint)
}

func TestGetDeltaQuarksFromPendingSwaps_FiltersByState(t *testing.T) {
	env := setupBalanceTestEnv(t)
	owner := testutil.NewRandomAccount(t)
	launchpadMint := testutil.NewRandomAccount(t)

	// Create reserve for the launchpad mint
	reserveRecord := &currency.ReserveRecord{
		Mint:              launchpadMint.PublicKey().ToBase58(),
		SupplyFromBonding: 1_000_000_000_000,
		Time:              time.Now(),
	}
	require.NoError(t, env.data.PutCurrencyReserve(env.ctx, reserveRecord))

	// Create swaps in non-pending states (should not be included)
	nonPendingStates := []swap.State{
		swap.StateCreated,
		swap.StateFinalized,
		swap.StateFailed,
		swap.StateCancelling,
		swap.StateCancelled,
	}

	for i, state := range nonPendingStates {
		swapRecord := &swap.Record{
			SwapId:               fmt.Sprintf("swap%d", i),
			Owner:                owner.PublicKey().ToBase58(),
			FromMint:             common.CoreMintAccount.PublicKey().ToBase58(),
			ToMint:               launchpadMint.PublicKey().ToBase58(),
			Amount:               1_000_000,
			FundingId:            fmt.Sprintf("funding%d", i),
			FundingSource:        swap.FundingSourceSubmitIntent,
			Nonce:                testutil.NewRandomAccount(t).PublicKey().ToBase58(),
			Blockhash:            fmt.Sprintf("blockhash%d", i),
			ProofSignature:       fmt.Sprintf("proofsig%d", i),
			TransactionSignature: fmt.Sprintf("txsig%d", i),
			State:                state,
		}
		require.NoError(t, env.data.SaveSwap(env.ctx, swapRecord))
	}

	deltaByMint, err := GetDeltaQuarksFromPendingSwaps(env.ctx, env.data, owner.PublicKey().ToBase58())
	require.NoError(t, err)
	assert.Empty(t, deltaByMint)
}

func TestGetDeltaQuarksFromPendingSwaps_BuySwap(t *testing.T) {
	env := setupBalanceTestEnv(t)
	owner := testutil.NewRandomAccount(t)
	launchpadMint := testutil.NewRandomAccount(t)

	supplyFromBonding := uint64(1_000_000_000_000) // 100 tokens
	coreMintAmount := uint64(1_000_000)            // 1 core mint unit

	// Create reserve for the launchpad mint
	reserveRecord := &currency.ReserveRecord{
		Mint:              launchpadMint.PublicKey().ToBase58(),
		SupplyFromBonding: supplyFromBonding,
		Time:              time.Now(),
	}
	require.NoError(t, env.data.PutCurrencyReserve(env.ctx, reserveRecord))

	// Create buy swap: core mint -> launchpad currency
	swapRecord := &swap.Record{
		SwapId:               "swap1",
		Owner:                owner.PublicKey().ToBase58(),
		FromMint:             common.CoreMintAccount.PublicKey().ToBase58(),
		ToMint:               launchpadMint.PublicKey().ToBase58(),
		Amount:               coreMintAmount,
		FundingId:            "funding1",
		FundingSource:        swap.FundingSourceSubmitIntent,
		Nonce:                testutil.NewRandomAccount(t).PublicKey().ToBase58(),
		Blockhash:            "blockhash1",
		ProofSignature:       "proofsig1",
		TransactionSignature: "txsig1",
		State:                swap.StateFunding,
	}
	require.NoError(t, env.data.SaveSwap(env.ctx, swapRecord))

	deltaByMint, err := GetDeltaQuarksFromPendingSwaps(env.ctx, env.data, owner.PublicKey().ToBase58())
	require.NoError(t, err)
	require.Len(t, deltaByMint, 1)

	// Calculate expected output
	expectedOutput := currencycreator.EstimateBuy(&currencycreator.EstimateBuyArgs{
		CurrentSupplyInQuarks: supplyFromBonding,
		BuyAmountInQuarks:     coreMintAmount,
		ValueMintDecimals:     uint8(common.CoreMintDecimals),
	})

	pendingBalance := deltaByMint[launchpadMint.PublicKey().ToBase58()]
	assert.Equal(t, expectedOutput, pendingBalance.DeltaQuarks)
	assert.Equal(t, launchpadMint.PublicKey().ToBase58(), pendingBalance.TargetMint.PublicKey().ToBase58())
	require.Len(t, pendingBalance.Swaps, 1)
	assert.Equal(t, swapRecord.SwapId, pendingBalance.Swaps[0].SwapId)
}

func TestGetDeltaQuarksFromPendingSwaps_SellSwap(t *testing.T) {
	env := setupBalanceTestEnv(t)
	owner := testutil.NewRandomAccount(t)
	launchpadMint := testutil.NewRandomAccount(t)

	supplyFromBonding := uint64(1_000_000_000_000) // 100 tokens
	sellAmount := uint64(10_000_000_000)          // 1 token

	// Create reserve for the launchpad mint
	reserveRecord := &currency.ReserveRecord{
		Mint:              launchpadMint.PublicKey().ToBase58(),
		SupplyFromBonding: supplyFromBonding,
		Time:              time.Now(),
	}
	require.NoError(t, env.data.PutCurrencyReserve(env.ctx, reserveRecord))

	// Create sell swap: launchpad currency -> core mint
	swapRecord := &swap.Record{
		SwapId:               "swap1",
		Owner:                owner.PublicKey().ToBase58(),
		FromMint:             launchpadMint.PublicKey().ToBase58(),
		ToMint:               common.CoreMintAccount.PublicKey().ToBase58(),
		Amount:               sellAmount,
		FundingId:            "funding1",
		FundingSource:        swap.FundingSourceSubmitIntent,
		Nonce:                testutil.NewRandomAccount(t).PublicKey().ToBase58(),
		Blockhash:            "blockhash1",
		ProofSignature:       "proofsig1",
		TransactionSignature: "txsig1",
		State:                swap.StateFunded,
	}
	require.NoError(t, env.data.SaveSwap(env.ctx, swapRecord))

	deltaByMint, err := GetDeltaQuarksFromPendingSwaps(env.ctx, env.data, owner.PublicKey().ToBase58())
	require.NoError(t, err)
	require.Len(t, deltaByMint, 1)

	// Calculate expected output (with fees)
	expectedOutput, _ := currencycreator.EstimateSell(&currencycreator.EstimateSellArgs{
		CurrentSupplyInQuarks: supplyFromBonding,
		SellAmountInQuarks:    sellAmount,
		ValueMintDecimals:     uint8(common.CoreMintDecimals),
		SellFeeBps:            currencycreator.DefaultSellFeeBps,
	})

	pendingBalance := deltaByMint[common.CoreMintAccount.PublicKey().ToBase58()]
	assert.Equal(t, expectedOutput, pendingBalance.DeltaQuarks)
	assert.Equal(t, common.CoreMintAccount.PublicKey().ToBase58(), pendingBalance.TargetMint.PublicKey().ToBase58())
	require.Len(t, pendingBalance.Swaps, 1)
	assert.Equal(t, swapRecord.SwapId, pendingBalance.Swaps[0].SwapId)
}

func TestGetDeltaQuarksFromPendingSwaps_BuySellSwap(t *testing.T) {
	env := setupBalanceTestEnv(t)
	owner := testutil.NewRandomAccount(t)
	launchpadMintA := testutil.NewRandomAccount(t)
	launchpadMintB := testutil.NewRandomAccount(t)

	supplyA := uint64(1_000_000_000_000) // 100 tokens
	supplyB := uint64(2_000_000_000_000) // 200 tokens
	sellAmount := uint64(10_000_000_000) // 1 token

	// Create reserves for both launchpad mints
	require.NoError(t, env.data.PutCurrencyReserve(env.ctx, &currency.ReserveRecord{
		Mint:              launchpadMintA.PublicKey().ToBase58(),
		SupplyFromBonding: supplyA,
		Time:              time.Now(),
	}))
	require.NoError(t, env.data.PutCurrencyReserve(env.ctx, &currency.ReserveRecord{
		Mint:              launchpadMintB.PublicKey().ToBase58(),
		SupplyFromBonding: supplyB,
		Time:              time.Now(),
	}))

	// Create buy-sell swap: launchpad A -> launchpad B
	swapRecord := &swap.Record{
		SwapId:               "swap1",
		Owner:                owner.PublicKey().ToBase58(),
		FromMint:             launchpadMintA.PublicKey().ToBase58(),
		ToMint:               launchpadMintB.PublicKey().ToBase58(),
		Amount:               sellAmount,
		FundingId:            "funding1",
		FundingSource:        swap.FundingSourceSubmitIntent,
		Nonce:                testutil.NewRandomAccount(t).PublicKey().ToBase58(),
		Blockhash:            "blockhash1",
		ProofSignature:       "proofsig1",
		TransactionSignature: "txsig1",
		State:                swap.StateSubmitting,
	}
	require.NoError(t, env.data.SaveSwap(env.ctx, swapRecord))

	deltaByMint, err := GetDeltaQuarksFromPendingSwaps(env.ctx, env.data, owner.PublicKey().ToBase58())
	require.NoError(t, err)
	require.Len(t, deltaByMint, 1)

	// Calculate expected output: sell A -> core mint -> buy B
	coreMintQuarks, _ := currencycreator.EstimateSell(&currencycreator.EstimateSellArgs{
		CurrentSupplyInQuarks: supplyA,
		SellAmountInQuarks:    sellAmount,
		ValueMintDecimals:     uint8(common.CoreMintDecimals),
		SellFeeBps:            currencycreator.DefaultSellFeeBps,
	})
	expectedOutput := currencycreator.EstimateBuy(&currencycreator.EstimateBuyArgs{
		CurrentSupplyInQuarks: supplyB,
		BuyAmountInQuarks:     coreMintQuarks,
		ValueMintDecimals:     uint8(common.CoreMintDecimals),
	})

	pendingBalance := deltaByMint[launchpadMintB.PublicKey().ToBase58()]
	assert.Equal(t, expectedOutput, pendingBalance.DeltaQuarks)
	assert.Equal(t, launchpadMintB.PublicKey().ToBase58(), pendingBalance.TargetMint.PublicKey().ToBase58())
	require.Len(t, pendingBalance.Swaps, 1)
	assert.Equal(t, swapRecord.SwapId, pendingBalance.Swaps[0].SwapId)
}

func TestGetDeltaQuarksFromPendingSwaps_MultipleSwapsSameDestination(t *testing.T) {
	env := setupBalanceTestEnv(t)
	owner := testutil.NewRandomAccount(t)
	launchpadMint := testutil.NewRandomAccount(t)

	supplyFromBonding := uint64(1_000_000_000_000)
	coreMintAmount := uint64(1_000_000)

	// Create reserve for the launchpad mint
	reserveRecord := &currency.ReserveRecord{
		Mint:              launchpadMint.PublicKey().ToBase58(),
		SupplyFromBonding: supplyFromBonding,
		Time:              time.Now(),
	}
	require.NoError(t, env.data.PutCurrencyReserve(env.ctx, reserveRecord))

	// Create multiple buy swaps to the same destination
	pendingStates := []swap.State{swap.StateFunding, swap.StateFunded, swap.StateSubmitting}
	for i, state := range pendingStates {
		swapRecord := &swap.Record{
			SwapId:               fmt.Sprintf("swap%d", i),
			Owner:                owner.PublicKey().ToBase58(),
			FromMint:             common.CoreMintAccount.PublicKey().ToBase58(),
			ToMint:               launchpadMint.PublicKey().ToBase58(),
			Amount:               coreMintAmount,
			FundingId:            fmt.Sprintf("funding%d", i),
			FundingSource:        swap.FundingSourceSubmitIntent,
			Nonce:                testutil.NewRandomAccount(t).PublicKey().ToBase58(),
			Blockhash:            fmt.Sprintf("blockhash%d", i),
			ProofSignature:       fmt.Sprintf("proofsig%d", i),
			TransactionSignature: fmt.Sprintf("txsig%d", i),
			State:                state,
		}
		require.NoError(t, env.data.SaveSwap(env.ctx, swapRecord))
	}

	deltaByMint, err := GetDeltaQuarksFromPendingSwaps(env.ctx, env.data, owner.PublicKey().ToBase58())
	require.NoError(t, err)
	require.Len(t, deltaByMint, 1)

	// Calculate expected output for a single swap
	singleSwapOutput := currencycreator.EstimateBuy(&currencycreator.EstimateBuyArgs{
		CurrentSupplyInQuarks: supplyFromBonding,
		BuyAmountInQuarks:     coreMintAmount,
		ValueMintDecimals:     uint8(common.CoreMintDecimals),
	})

	// Total should be 3x the single swap output
	expectedTotal := singleSwapOutput * 3
	pendingBalance := deltaByMint[launchpadMint.PublicKey().ToBase58()]
	assert.Equal(t, expectedTotal, pendingBalance.DeltaQuarks)
	assert.Equal(t, launchpadMint.PublicKey().ToBase58(), pendingBalance.TargetMint.PublicKey().ToBase58())
	require.Len(t, pendingBalance.Swaps, 3)
}

func TestGetDeltaQuarksFromPendingSwaps_MultipleDestinations(t *testing.T) {
	env := setupBalanceTestEnv(t)
	owner := testutil.NewRandomAccount(t)
	launchpadMintA := testutil.NewRandomAccount(t)
	launchpadMintB := testutil.NewRandomAccount(t)

	supplyA := uint64(1_000_000_000_000)
	supplyB := uint64(2_000_000_000_000)
	coreMintAmount := uint64(1_000_000)

	// Create reserves for both launchpad mints
	require.NoError(t, env.data.PutCurrencyReserve(env.ctx, &currency.ReserveRecord{
		Mint:              launchpadMintA.PublicKey().ToBase58(),
		SupplyFromBonding: supplyA,
		Time:              time.Now(),
	}))
	require.NoError(t, env.data.PutCurrencyReserve(env.ctx, &currency.ReserveRecord{
		Mint:              launchpadMintB.PublicKey().ToBase58(),
		SupplyFromBonding: supplyB,
		Time:              time.Now(),
	}))

	// Create buy swap to mint A
	swapRecordA := &swap.Record{
		SwapId:               "swapA",
		Owner:                owner.PublicKey().ToBase58(),
		FromMint:             common.CoreMintAccount.PublicKey().ToBase58(),
		ToMint:               launchpadMintA.PublicKey().ToBase58(),
		Amount:               coreMintAmount,
		FundingId:            "fundingA",
		FundingSource:        swap.FundingSourceSubmitIntent,
		Nonce:                testutil.NewRandomAccount(t).PublicKey().ToBase58(),
		Blockhash:            "blockhashA",
		ProofSignature:       "proofsigA",
		TransactionSignature: "txsigA",
		State:                swap.StateFunding,
	}
	require.NoError(t, env.data.SaveSwap(env.ctx, swapRecordA))

	// Create buy swap to mint B
	swapRecordB := &swap.Record{
		SwapId:               "swapB",
		Owner:                owner.PublicKey().ToBase58(),
		FromMint:             common.CoreMintAccount.PublicKey().ToBase58(),
		ToMint:               launchpadMintB.PublicKey().ToBase58(),
		Amount:               coreMintAmount,
		FundingId:            "fundingB",
		FundingSource:        swap.FundingSourceSubmitIntent,
		Nonce:                testutil.NewRandomAccount(t).PublicKey().ToBase58(),
		Blockhash:            "blockhashB",
		ProofSignature:       "proofsigB",
		TransactionSignature: "txsigB",
		State:                swap.StateFunded,
	}
	require.NoError(t, env.data.SaveSwap(env.ctx, swapRecordB))

	deltaByMint, err := GetDeltaQuarksFromPendingSwaps(env.ctx, env.data, owner.PublicKey().ToBase58())
	require.NoError(t, err)
	require.Len(t, deltaByMint, 2)

	expectedOutputA := currencycreator.EstimateBuy(&currencycreator.EstimateBuyArgs{
		CurrentSupplyInQuarks: supplyA,
		BuyAmountInQuarks:     coreMintAmount,
		ValueMintDecimals:     uint8(common.CoreMintDecimals),
	})
	expectedOutputB := currencycreator.EstimateBuy(&currencycreator.EstimateBuyArgs{
		CurrentSupplyInQuarks: supplyB,
		BuyAmountInQuarks:     coreMintAmount,
		ValueMintDecimals:     uint8(common.CoreMintDecimals),
	})

	pendingBalanceA := deltaByMint[launchpadMintA.PublicKey().ToBase58()]
	assert.Equal(t, expectedOutputA, pendingBalanceA.DeltaQuarks)
	assert.Equal(t, launchpadMintA.PublicKey().ToBase58(), pendingBalanceA.TargetMint.PublicKey().ToBase58())
	require.Len(t, pendingBalanceA.Swaps, 1)
	assert.Equal(t, swapRecordA.SwapId, pendingBalanceA.Swaps[0].SwapId)

	pendingBalanceB := deltaByMint[launchpadMintB.PublicKey().ToBase58()]
	assert.Equal(t, expectedOutputB, pendingBalanceB.DeltaQuarks)
	assert.Equal(t, launchpadMintB.PublicKey().ToBase58(), pendingBalanceB.TargetMint.PublicKey().ToBase58())
	require.Len(t, pendingBalanceB.Swaps, 1)
	assert.Equal(t, swapRecordB.SwapId, pendingBalanceB.Swaps[0].SwapId)
}
