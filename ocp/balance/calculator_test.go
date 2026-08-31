package balance

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	commonpb "github.com/code-payments/ocp-protobuf-api/generated/go/common/v1"

	"github.com/code-payments/ocp-server/ocp/common"
	ocp_data "github.com/code-payments/ocp-server/ocp/data"
	"github.com/code-payments/ocp-server/ocp/data/account"
	"github.com/code-payments/ocp-server/ocp/data/balance"
	timelock_token_v1 "github.com/code-payments/ocp-server/solana/timelock/v1"
	"github.com/code-payments/ocp-server/testutil"
)

func TestDefaultCalculationMethods_BalanceRecord(t *testing.T) {
	env := setupBalanceTestEnv(t)

	first := newBalanceTestAccount(t, env)
	second := newBalanceTestAccount(t, env)

	// Quarks and cost basis come from the same record, so they can't disagree
	saveBalanceTestRecord(t, env, first, &balance.Record{Quarks: 42, UsdCostBasis: 4_200_000, IsOpen: true, IsLocked: true})
	saveBalanceTestRecord(t, env, second, &balance.Record{Quarks: 0, UsdCostBasis: -123456, IsOpen: true, IsLocked: true})

	for _, tc := range []struct {
		testAccount  *balanceTestAccount
		quarks       uint64
		usdCostBasis int64
	}{
		{first, 42, 4_200_000},
		{second, 0, -123456},
	} {
		actual, err := CalculateFromCache(env.ctx, env.data, tc.testAccount.tokenAccount)
		require.NoError(t, err)
		assert.EqualValues(t, tc.quarks, actual)
	}

	balanceByAccount, err := BatchCalculateFromCache(env.ctx, env.data, first.tokenAccount, second.tokenAccount)
	require.NoError(t, err)
	assert.Equal(t, map[string]*Balance{
		first.tokenAccount.PublicKey().ToBase58():  {Quarks: 42, UsdCostBasis: 4_200_000},
		second.tokenAccount.PublicKey().ToBase58(): {Quarks: 0, UsdCostBasis: -123456},
	}, balanceByAccount)
}

func TestDefaultCalculationMethods_MissingBalanceRecord(t *testing.T) {
	env := setupBalanceTestEnv(t)

	tracked := newBalanceTestAccount(t, env)
	saveBalanceTestRecord(t, env, tracked, &balance.Record{Quarks: 42, IsOpen: true, IsLocked: true})
	untracked := newBalanceTestAccount(t, env)

	// The ledger record is the whole answer, so an account without one simply
	// isn't managed by Code
	_, err := CalculateFromCache(env.ctx, env.data, untracked.tokenAccount)
	assert.Equal(t, ErrNotManagedByCode, err)

	// The batch variant says so by omission, and the rest of the batch still
	// resolves
	balanceByAccount, err := BatchCalculateFromCache(env.ctx, env.data, tracked.tokenAccount, untracked.tokenAccount)
	require.NoError(t, err)
	require.Len(t, balanceByAccount, 1)
	assert.EqualValues(t, 42, balanceByAccount[tracked.tokenAccount.PublicKey().ToBase58()].Quarks)
}

func TestDefaultCalculationMethods_NotManagedByCode(t *testing.T) {
	env := setupBalanceTestEnv(t)

	testAccount := newBalanceTestAccount(t, env)
	saveBalanceTestRecord(t, env, testAccount, &balance.Record{Quarks: 42, IsOpen: true, IsLocked: true})

	// The vault unlocks. In production the timelock and ledger records move in
	// the same transaction, so they can't disagree.
	timelockRecord, err := env.data.GetTimelockByVault(env.ctx, testAccount.tokenAccount.PublicKey().ToBase58())
	require.NoError(t, err)
	timelockRecord.VaultState = timelock_token_v1.StateWaitingForTimeout
	timelockRecord.Block += 1
	require.NoError(t, env.data.SaveTimelock(env.ctx, timelockRecord))
	require.NoError(t, env.data.MarkBalanceAsUnlocked(env.ctx, testAccount.tokenAccount.PublicKey().ToBase58()))

	// A record for an unlocked vault holds the last managed state rather than
	// a live balance, so it is refused outright and omitted from a batch
	_, err = CalculateFromCache(env.ctx, env.data, testAccount.tokenAccount)
	assert.Equal(t, ErrNotManagedByCode, err)

	balanceByAccount, err := BatchCalculateFromCache(env.ctx, env.data, testAccount.tokenAccount)
	require.NoError(t, err)
	assert.Empty(t, balanceByAccount)
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

type balanceTestAccount struct {
	vmConfig     *common.VmConfig
	owner        *common.Account
	tokenAccount *common.Account
}

func setupBalanceTestEnv(t *testing.T) (env balanceTestEnv) {
	env.ctx = context.Background()
	env.data = ocp_data.NewTestDataProvider()
	testutil.SetupRandomSubsidizer(t, env.data)
	return env
}

// newBalanceTestAccount creates a locked timelock account, with an account
// info record but no ledger record.
func newBalanceTestAccount(t *testing.T, env balanceTestEnv) *balanceTestAccount {
	vmConfig := testutil.NewRandomVmConfig(t, true)
	owner := testutil.NewRandomAccount(t)

	timelockAccounts, err := owner.GetTimelockAccounts(vmConfig)
	require.NoError(t, err)
	timelockRecord := timelockAccounts.ToDBRecord()
	timelockRecord.VaultState = timelock_token_v1.StateLocked
	timelockRecord.Block += 1
	require.NoError(t, env.data.SaveTimelock(env.ctx, timelockRecord))

	require.NoError(t, env.data.CreateAccountInfo(env.ctx, &account.Record{
		OwnerAccount:     owner.PublicKey().ToBase58(),
		AuthorityAccount: owner.PublicKey().ToBase58(),
		TokenAccount:     timelockRecord.VaultAddress,
		MintAccount:      vmConfig.Mint.PublicKey().ToBase58(),
		AccountType:      commonpb.AccountType_PRIMARY,
	}))

	tokenAccount, err := common.NewAccountFromPublicKeyString(timelockRecord.VaultAddress)
	require.NoError(t, err)

	return &balanceTestAccount{
		vmConfig:     vmConfig,
		owner:        owner,
		tokenAccount: tokenAccount,
	}
}

// saveBalanceTestRecord creates the account's ledger record, filling in the
// identifying fields from the account.
func saveBalanceTestRecord(t *testing.T, env balanceTestEnv, testAccount *balanceTestAccount, record *balance.Record) {
	record.TokenAccount = testAccount.tokenAccount.PublicKey().ToBase58()
	record.OwnerAccount = testAccount.owner.PublicKey().ToBase58()
	record.MintAccount = testAccount.vmConfig.Mint.PublicKey().ToBase58()
	require.NoError(t, env.data.CreateBalance(env.ctx, record))
}
