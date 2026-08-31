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

	saveBalanceTestRecord(t, env, first, &balance.Record{Quarks: 42, IsOpen: true, IsLocked: true})
	saveBalanceTestRecord(t, env, second, &balance.Record{Quarks: 0, IsOpen: true, IsLocked: true})

	expected := map[string]uint64{
		first.tokenAccount.PublicKey().ToBase58():  42,
		second.tokenAccount.PublicKey().ToBase58(): 0,
	}

	for _, testAccount := range []*balanceTestAccount{first, second} {
		actual, err := CalculateFromCache(env.ctx, env.data, testAccount.tokenAccount)
		require.NoError(t, err)
		assert.EqualValues(t, expected[testAccount.tokenAccount.PublicKey().ToBase58()], actual)
	}

	balanceByAccount, err := BatchCalculateFromCacheWithTokenAccounts(env.ctx, env.data, first.tokenAccount, second.tokenAccount)
	require.NoError(t, err)
	assert.Equal(t, expected, balanceByAccount)

	balanceByAccount, err = BatchCalculateFromCacheWithAccountRecords(env.ctx, env.data, first.accountRecords(t, env), second.accountRecords(t, env))
	require.NoError(t, err)
	assert.Equal(t, expected, balanceByAccount)
}

func TestDefaultCalculationMethods_MissingBalanceRecord(t *testing.T) {
	env := setupBalanceTestEnv(t)

	// The account is managed, but the ledger has no record for it, which is a
	// broken invariant rather than a balance of zero
	testAccount := newBalanceTestAccount(t, env)

	_, err := CalculateFromCache(env.ctx, env.data, testAccount.tokenAccount)
	assert.Equal(t, balance.ErrRecordNotFound, err)

	_, err = BatchCalculateFromCacheWithTokenAccounts(env.ctx, env.data, testAccount.tokenAccount)
	assert.Equal(t, balance.ErrRecordNotFound, err)

	_, err = BatchCalculateFromCacheWithAccountRecords(env.ctx, env.data, testAccount.accountRecords(t, env))
	assert.Equal(t, balance.ErrRecordNotFound, err)

	_, err = CalculateUsdCostBasisFromCache(env.ctx, env.data, testAccount.tokenAccount)
	assert.Equal(t, balance.ErrRecordNotFound, err)

	_, err = BatchCalculateUsdCostBasisFromCache(env.ctx, env.data, testAccount.tokenAccount)
	assert.Equal(t, balance.ErrRecordNotFound, err)
}

func TestDefaultCalculationMethods_NotManagedByCode(t *testing.T) {
	env := setupBalanceTestEnv(t)

	testAccount := newBalanceTestAccount(t, env)
	saveBalanceTestRecord(t, env, testAccount, &balance.Record{Quarks: 42, IsOpen: true, IsLocked: true})

	timelockRecord, err := env.data.GetTimelockByVault(env.ctx, testAccount.tokenAccount.PublicKey().ToBase58())
	require.NoError(t, err)
	timelockRecord.VaultState = timelock_token_v1.StateWaitingForTimeout
	timelockRecord.Block += 1
	require.NoError(t, env.data.SaveTimelock(env.ctx, timelockRecord))

	_, err = CalculateFromCache(env.ctx, env.data, testAccount.tokenAccount)
	assert.Equal(t, ErrNotManagedByCode, err)

	_, err = BatchCalculateFromCacheWithTokenAccounts(env.ctx, env.data, testAccount.tokenAccount)
	assert.Equal(t, ErrNotManagedByCode, err)

	_, err = BatchCalculateFromCacheWithAccountRecords(env.ctx, env.data, testAccount.accountRecords(t, env))
	assert.Equal(t, ErrNotManagedByCode, err)
}

func TestDefaultCalculationMethods_UnlockedBalanceRecord(t *testing.T) {
	env := setupBalanceTestEnv(t)

	// A record for an unlocked vault holds the last managed state, not a live
	// balance, so it is refused even though the timelock record
	// still passes the managed check. That pairing is inconsistent by
	// construction: the timelock check normally rejects first, so the fixture
	// exists to exercise the record's own guard.
	testAccount := newBalanceTestAccount(t, env)
	saveBalanceTestRecord(t, env, testAccount, &balance.Record{Quarks: 42, UsdCostBasis: 4_200_000, IsOpen: true})

	_, err := CalculateFromCache(env.ctx, env.data, testAccount.tokenAccount)
	assert.Equal(t, ErrNotManagedByCode, err)

	_, err = BatchCalculateFromCacheWithTokenAccounts(env.ctx, env.data, testAccount.tokenAccount)
	assert.Equal(t, ErrNotManagedByCode, err)

	_, err = CalculateUsdCostBasisFromCache(env.ctx, env.data, testAccount.tokenAccount)
	assert.Equal(t, ErrNotManagedByCode, err)

	_, err = BatchCalculateUsdCostBasisFromCache(env.ctx, env.data, testAccount.tokenAccount)
	assert.Equal(t, ErrNotManagedByCode, err)
}

func TestDefaultCalculationMethods_BalanceWithUsdCostBasis(t *testing.T) {
	env := setupBalanceTestEnv(t)

	first := newBalanceTestAccount(t, env)
	second := newBalanceTestAccount(t, env)

	// Both values come from the same record, so they can't disagree
	saveBalanceTestRecord(t, env, first, &balance.Record{Quarks: 42, UsdCostBasis: 4_200_000, IsOpen: true, IsLocked: true})
	saveBalanceTestRecord(t, env, second, &balance.Record{Quarks: 33, UsdCostBasis: -123456, IsOpen: true, IsLocked: true})

	res, err := BatchCalculateWithUsdCostBasisFromCache(env.ctx, env.data, first.accountRecords(t, env), second.accountRecords(t, env))
	require.NoError(t, err)
	require.Len(t, res, 2)

	cached := res[first.tokenAccount.PublicKey().ToBase58()]
	require.NotNil(t, cached)
	assert.EqualValues(t, 42, cached.Quarks)
	assert.EqualValues(t, 4_200_000, cached.UsdCostBasis)

	cached = res[second.tokenAccount.PublicKey().ToBase58()]
	require.NotNil(t, cached)
	assert.EqualValues(t, 33, cached.Quarks)
	assert.EqualValues(t, -123456, cached.UsdCostBasis)
}

func TestUsdCostBasisCalculationMethods(t *testing.T) {
	env := setupBalanceTestEnv(t)

	first := newBalanceTestAccount(t, env)
	second := newBalanceTestAccount(t, env)

	saveBalanceTestRecord(t, env, first, &balance.Record{UsdCostBasis: -123456, IsOpen: true, IsLocked: true})
	saveBalanceTestRecord(t, env, second, &balance.Record{Quarks: 1, UsdCostBasis: 1_500_000, IsOpen: true, IsLocked: true})

	expected := map[string]int64{
		first.tokenAccount.PublicKey().ToBase58():  -123456,
		second.tokenAccount.PublicKey().ToBase58(): 1_500_000,
	}

	for _, testAccount := range []*balanceTestAccount{first, second} {
		actual, err := CalculateUsdCostBasisFromCache(env.ctx, env.data, testAccount.tokenAccount)
		require.NoError(t, err)
		assert.EqualValues(t, expected[testAccount.tokenAccount.PublicKey().ToBase58()], actual)
	}

	usdCostBasisByAccount, err := BatchCalculateUsdCostBasisFromCache(env.ctx, env.data, first.tokenAccount, second.tokenAccount)
	require.NoError(t, err)
	assert.Equal(t, expected, usdCostBasisByAccount)
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

func (a *balanceTestAccount) accountRecords(t *testing.T, env balanceTestEnv) *common.AccountRecords {
	generalRecord, err := env.data.GetAccountInfoByTokenAddress(env.ctx, a.tokenAccount.PublicKey().ToBase58())
	require.NoError(t, err)
	timelockRecord, err := env.data.GetTimelockByVault(env.ctx, a.tokenAccount.PublicKey().ToBase58())
	require.NoError(t, err)
	return &common.AccountRecords{
		General:  generalRecord,
		Timelock: timelockRecord,
	}
}
