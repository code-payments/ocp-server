package balance

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	commonpb "github.com/code-payments/ocp-protobuf-api/generated/go/common/v1"

	"github.com/code-payments/ocp-server/config/memory"
	"github.com/code-payments/ocp-server/config/wrapper"
	ocp_data "github.com/code-payments/ocp-server/ocp/data"
	"github.com/code-payments/ocp-server/ocp/data/account"
	"github.com/code-payments/ocp-server/ocp/data/balance"
	"github.com/code-payments/ocp-server/testutil"
)

func TestApplyDeltasInTx_WritesDisabled(t *testing.T) {
	ctx := context.Background()
	data := ocp_data.NewTestDataProvider()

	source := newLedgerTestAccount(t, ctx, data, commonpb.AccountType_PRIMARY)

	require.NoError(t, ApplyDeltasInTx(ctx, data, &balance.Delta{
		TokenAccount: source,
		Kind:         balance.DeltaDebit,
		Quarks:       100,
	}))

	_, err := data.GetBalance(ctx, source)
	assert.Equal(t, balance.ErrRecordNotFound, err)
}

func TestApplyDeltasInTx_SeedsTimelockAccounts(t *testing.T) {
	ctx := context.Background()
	data := ocp_data.NewTestDataProvider()
	enableLedgerWritesForTest(t)

	source := newLedgerTestAccount(t, ctx, data, commonpb.AccountType_PRIMARY)
	destination := newLedgerTestAccount(t, ctx, data, commonpb.AccountType_REMOTE_SEND_GIFT_CARD)
	swap := newLedgerTestAccount(t, ctx, data, commonpb.AccountType_SWAP)
	external := testutil.NewRandomAccount(t).PublicKey().ToBase58()

	require.NoError(t, ApplyDeltasInTx(ctx, data,
		&balance.Delta{TokenAccount: source, Kind: balance.DeltaDebit, Quarks: 100, UsdCostBasis: 1_000_000},
		&balance.Delta{TokenAccount: destination, Kind: balance.DeltaCredit, Quarks: 60, UsdCostBasis: 600_000},
		&balance.Delta{TokenAccount: swap, Kind: balance.DeltaCredit, Quarks: 20, UsdCostBasis: 200_000},
		&balance.Delta{TokenAccount: external, Kind: balance.DeltaCredit, Quarks: 20, UsdCostBasis: 200_000},
	))

	// Existing accounts get a non-backfilled row that accumulates freely,
	// including a negative balance for a source that predates the ledger
	record, err := data.GetBalance(ctx, source)
	require.NoError(t, err)
	assert.EqualValues(t, -100, record.Quarks)
	assert.EqualValues(t, -1_000_000, record.UsdCostBasis)
	assert.False(t, record.IsBackfilled)
	assert.True(t, record.IsOpen)

	record, err = data.GetBalance(ctx, destination)
	require.NoError(t, err)
	assert.EqualValues(t, 60, record.Quarks)
	assert.EqualValues(t, 600_000, record.UsdCostBasis)
	assert.False(t, record.IsBackfilled)

	// Credits to accounts OCP doesn't hold a timelock for are dropped, and
	// those accounts never get a row
	_, err = data.GetBalance(ctx, swap)
	assert.Equal(t, balance.ErrRecordNotFound, err)
	_, err = data.GetBalance(ctx, external)
	assert.Equal(t, balance.ErrRecordNotFound, err)

	// Once backfilled, predicates are enforced
	require.NoError(t, data.BackfillBalance(ctx, source, func(context.Context) (*balance.BackfillResult, error) {
		return &balance.BackfillResult{Quarks: 400, UsdCostBasis: 4_000_000, IsOpen: true, IsLocked: true}, nil
	}))
	err = ApplyDeltasInTx(ctx, data, &balance.Delta{TokenAccount: source, Kind: balance.DeltaDebit, Quarks: 401})
	assert.Equal(t, balance.ErrInsufficientBalance, err)
	require.NoError(t, ApplyDeltasInTx(ctx, data, &balance.Delta{TokenAccount: source, Kind: balance.DeltaDebit, Quarks: 400, UsdCostBasis: 4_000_000}))

	record, err = data.GetBalance(ctx, source)
	require.NoError(t, err)
	assert.EqualValues(t, 0, record.Quarks)
	assert.True(t, record.IsBackfilled)
}

func TestApplyDeltasInTx_UnknownSource(t *testing.T) {
	ctx := context.Background()
	data := ocp_data.NewTestDataProvider()
	enableLedgerWritesForTest(t)

	external := testutil.NewRandomAccount(t).PublicKey().ToBase58()
	swap := newLedgerTestAccount(t, ctx, data, commonpb.AccountType_SWAP)
	for _, source := range []string{external, swap} {
		err := ApplyDeltasInTx(ctx, data, &balance.Delta{TokenAccount: source, Kind: balance.DeltaDebit, Quarks: 1})
		assert.ErrorIs(t, err, ErrUntrackedAccount)
	}
}

func TestApplyDeltasInTx_OnlyUntrackedCredits(t *testing.T) {
	ctx := context.Background()
	data := ocp_data.NewTestDataProvider()
	enableLedgerWritesForTest(t)

	external := testutil.NewRandomAccount(t).PublicKey().ToBase58()
	require.NoError(t, ApplyDeltasInTx(ctx, data, &balance.Delta{TokenAccount: external, Kind: balance.DeltaCredit, Quarks: 1}))
	_, err := data.GetBalance(ctx, external)
	assert.Equal(t, balance.ErrRecordNotFound, err)
}

func TestApplyDeltasInTx_UnlockedAccount(t *testing.T) {
	ctx := context.Background()
	data := ocp_data.NewTestDataProvider()
	enableLedgerWritesForTest(t)

	unlocked := newLedgerTestAccountInfo(t, ctx, data, commonpb.AccountType_PRIMARY)
	require.NoError(t, CreateRecordInTx(ctx, data, unlocked))
	require.NoError(t, ApplyDeltasInTx(ctx, data, &balance.Delta{TokenAccount: unlocked.TokenAccount, Kind: balance.DeltaCredit, Quarks: 100}))
	require.NoError(t, data.MarkBalanceAsUnlocked(ctx, unlocked.TokenAccount))

	// Any delta against an unlocked account fails loudly, so a flow still
	// moving funds through it surfaces as a DB error
	err := ApplyDeltasInTx(ctx, data, &balance.Delta{TokenAccount: unlocked.TokenAccount, Kind: balance.DeltaCredit, Quarks: 1})
	assert.Equal(t, balance.ErrAccountUnlocked, err)
	err = ApplyDeltasInTx(ctx, data, &balance.Delta{TokenAccount: unlocked.TokenAccount, Kind: balance.DeltaDebit, Quarks: 1})
	assert.Equal(t, balance.ErrAccountUnlocked, err)

	record, err := data.GetBalance(ctx, unlocked.TokenAccount)
	require.NoError(t, err)
	assert.EqualValues(t, 100, record.Quarks)
	assert.False(t, record.IsLocked)
}

func TestApplyDeltasInTx_InvalidDelta(t *testing.T) {
	ctx := context.Background()
	data := ocp_data.NewTestDataProvider()
	enableLedgerWritesForTest(t)

	source := newLedgerTestAccount(t, ctx, data, commonpb.AccountType_PRIMARY)
	assert.Error(t, ApplyDeltasInTx(ctx, data, &balance.Delta{TokenAccount: source, Kind: balance.DeltaDebit}))

	_, err := data.GetBalance(ctx, source)
	assert.Equal(t, balance.ErrRecordNotFound, err)
}

func TestCreateRecordInTx(t *testing.T) {
	ctx := context.Background()
	data := ocp_data.NewTestDataProvider()

	// Disabled writes are a no-op
	primary := newLedgerTestAccountInfo(t, ctx, data, commonpb.AccountType_PRIMARY)
	require.NoError(t, CreateRecordInTx(ctx, data, primary))
	_, err := data.GetBalance(ctx, primary.TokenAccount)
	assert.Equal(t, balance.ErrRecordNotFound, err)

	enableLedgerWritesForTest(t)

	// A new timelock account starts backfilled at zero, so predicates are
	// enforced immediately
	require.NoError(t, CreateRecordInTx(ctx, data, primary))
	record, err := data.GetBalance(ctx, primary.TokenAccount)
	require.NoError(t, err)
	assert.Equal(t, primary.TokenAccount, record.TokenAccount)
	assert.Equal(t, primary.OwnerAccount, record.OwnerAccount)
	assert.Equal(t, primary.MintAccount, record.MintAccount)
	assert.EqualValues(t, 0, record.Quarks)
	assert.EqualValues(t, 0, record.UsdCostBasis)
	assert.True(t, record.IsOpen)
	assert.True(t, record.IsBackfilled)

	err = ApplyDeltasInTx(ctx, data, &balance.Delta{TokenAccount: primary.TokenAccount, Kind: balance.DeltaDebit, Quarks: 1})
	assert.Equal(t, balance.ErrInsufficientBalance, err)

	// Re-creating is idempotent and doesn't reset the record
	require.NoError(t, ApplyDeltasInTx(ctx, data, &balance.Delta{TokenAccount: primary.TokenAccount, Kind: balance.DeltaCredit, Quarks: 10}))
	require.NoError(t, CreateRecordInTx(ctx, data, primary))
	record, err = data.GetBalance(ctx, primary.TokenAccount)
	require.NoError(t, err)
	assert.EqualValues(t, 10, record.Quarks)

	// Non-timelock accounts are never tracked
	swap := newLedgerTestAccountInfo(t, ctx, data, commonpb.AccountType_SWAP)
	require.NoError(t, CreateRecordInTx(ctx, data, swap))
	_, err = data.GetBalance(ctx, swap.TokenAccount)
	assert.Equal(t, balance.ErrRecordNotFound, err)
}

func newLedgerTestAccount(t *testing.T, ctx context.Context, data ocp_data.Provider, accountType commonpb.AccountType) string {
	return newLedgerTestAccountInfo(t, ctx, data, accountType).TokenAccount
}

func newLedgerTestAccountInfo(t *testing.T, ctx context.Context, data ocp_data.Provider, accountType commonpb.AccountType) *account.Record {
	owner := testutil.NewRandomAccount(t)
	authority := owner
	if accountType == commonpb.AccountType_SWAP {
		authority = testutil.NewRandomAccount(t)
	}
	record := &account.Record{
		OwnerAccount:     owner.PublicKey().ToBase58(),
		AuthorityAccount: authority.PublicKey().ToBase58(),
		TokenAccount:     testutil.NewRandomAccount(t).PublicKey().ToBase58(),
		MintAccount:      testutil.NewRandomAccount(t).PublicKey().ToBase58(),
		AccountType:      accountType,
	}
	require.NoError(t, data.CreateAccountInfo(ctx, record))
	return record
}

func enableLedgerWritesForTest(t *testing.T) {
	previous := enableLedgerWrites
	enableLedgerWrites = wrapper.NewBoolConfig(memory.NewConfig(true), defaultEnableLedgerWrites)
	t.Cleanup(func() {
		enableLedgerWrites = previous
	})
}
