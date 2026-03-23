package tests

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	commonpb "github.com/code-payments/ocp-protobuf-api/generated/go/common/v1"

	"github.com/code-payments/ocp-server/ocp/data/balance"
)

func RunTests(t *testing.T, s balance.Store, teardown func()) {
	for _, tf := range []func(t *testing.T, s balance.Store){
		testCachedBalanceVersionHappyPath,
		testClosedAccountHappyPath,
		testExternalCheckpointHappyPath,
		testGetBalanceHappyPath,
		testGetBalanceBatchHappyPath,
		testAdjustBalanceHappyPath,
		testAdjustBalanceNegative,
	} {
		tf(t, s)
		teardown()
	}
}

func testCachedBalanceVersionHappyPath(t *testing.T, s balance.Store) {
	t.Run("testCachedBalanceVersionHappyPath", func(t *testing.T) {
		ctx := context.Background()

		for i := range 100 {
			for j := 0; j < 10; j++ {
				currentVersion, err := s.GetCachedVersion(ctx, "token_account_1")
				require.NoError(t, err)
				assert.EqualValues(t, i, currentVersion)
			}

			if i > 0 {
				assert.Equal(t, balance.ErrStaleCachedBalanceVersion, s.AdvanceCachedVersion(ctx, "token_account_1", uint64(i-1)))
			}
			assert.Equal(t, balance.ErrStaleCachedBalanceVersion, s.AdvanceCachedVersion(ctx, "token_account_1", uint64(i+1)))

			require.NoError(t, s.AdvanceCachedVersion(ctx, "token_account_1", uint64(i)))
		}

		currentVersion, err := s.GetCachedVersion(ctx, "token_account_2")
		require.NoError(t, err)
		assert.EqualValues(t, 0, currentVersion)
	})
}

func testClosedAccountHappyPath(t *testing.T, s balance.Store) {
	t.Run("testClosedAccountHappyPath", func(t *testing.T) {
		ctx := context.Background()

		require.NoError(t, s.CheckNotClosed(ctx, "token_account_1"))

		require.NoError(t, s.MarkAsClosed(ctx, "token_account_1"))

		assert.Equal(t, balance.ErrAccountClosed, s.CheckNotClosed(ctx, "token_account_1"))
		require.NoError(t, s.CheckNotClosed(ctx, "token_account_2s"))
	})
}

func testExternalCheckpointHappyPath(t *testing.T, s balance.Store) {
	t.Run("testExternalCheckpointHappyPath", func(t *testing.T) {
		ctx := context.Background()

		_, err := s.GetExternalCheckpoint(ctx, "token_account")
		assert.Equal(t, balance.ErrCheckpointNotFound, err)

		start := time.Now()

		expected := &balance.ExternalCheckpointRecord{
			TokenAccount:   "token_account",
			Quarks:         0,
			SlotCheckpoint: 0,
		}
		cloned := expected.Clone()

		require.NoError(t, s.SaveExternalCheckpoint(ctx, expected))
		assert.EqualValues(t, 1, expected.Id)
		assert.True(t, expected.LastUpdatedAt.After(start))

		actual, err := s.GetExternalCheckpoint(ctx, "token_account")
		require.NoError(t, err)
		assertEquivalentExternalCheckpoingRecords(t, actual, &cloned)

		start = time.Now()

		expected.Quarks = 12345
		expected.SlotCheckpoint = 10
		cloned = expected.Clone()

		require.NoError(t, s.SaveExternalCheckpoint(ctx, expected))
		assert.EqualValues(t, 1, expected.Id)
		assert.True(t, expected.LastUpdatedAt.After(start))

		actual, err = s.GetExternalCheckpoint(ctx, "token_account")
		require.NoError(t, err)
		assertEquivalentExternalCheckpoingRecords(t, actual, &cloned)

		expected.Quarks = 67890
		assert.Equal(t, balance.ErrStaleCheckpoint, s.SaveExternalCheckpoint(ctx, expected))

		actual, err = s.GetExternalCheckpoint(ctx, "token_account")
		require.NoError(t, err)
		assertEquivalentExternalCheckpoingRecords(t, actual, &cloned)

		expected.SlotCheckpoint -= 1
		assert.Equal(t, balance.ErrStaleCheckpoint, s.SaveExternalCheckpoint(ctx, expected))

		actual, err = s.GetExternalCheckpoint(ctx, "token_account")
		require.NoError(t, err)
		assertEquivalentExternalCheckpoingRecords(t, actual, &cloned)
	})
}

func assertEquivalentExternalCheckpoingRecords(t *testing.T, obj1, obj2 *balance.ExternalCheckpointRecord) {
	assert.Equal(t, obj1.TokenAccount, obj2.TokenAccount)
	assert.Equal(t, obj1.Quarks, obj2.Quarks)
	assert.Equal(t, obj1.SlotCheckpoint, obj2.SlotCheckpoint)
}

func testGetBalanceHappyPath(t *testing.T, s balance.Store) {
	t.Run("testGetBalanceHappyPath", func(t *testing.T) {
		ctx := context.Background()

		_, err := s.GetBalance(ctx, "token_account")
		assert.Equal(t, balance.ErrBalanceNotFound, err)

		require.NoError(t, s.AdjustBalance(ctx, "token_account", 1000, 1.50, "test_owner", "test_mint", commonpb.AccountType_PRIMARY))

		actual, err := s.GetBalance(ctx, "token_account")
		require.NoError(t, err)
		assert.EqualValues(t, 1000, actual.Quarks)
		assert.EqualValues(t, 1.50, actual.UsdCostBasis)
		assert.EqualValues(t, 1, actual.Version)

		require.NoError(t, s.AdjustBalance(ctx, "token_account", 500, 0.75, "test_owner", "test_mint", commonpb.AccountType_PRIMARY))

		actual, err = s.GetBalance(ctx, "token_account")
		require.NoError(t, err)
		assert.EqualValues(t, 1500, actual.Quarks)
		assert.EqualValues(t, 2.25, actual.UsdCostBasis)
		assert.EqualValues(t, 2, actual.Version)

		require.NoError(t, s.AdjustBalance(ctx, "token_account", -300, -0.45, "test_owner", "test_mint", commonpb.AccountType_PRIMARY))

		actual, err = s.GetBalance(ctx, "token_account")
		require.NoError(t, err)
		assert.EqualValues(t, 1200, actual.Quarks)
		assert.EqualValues(t, 1.80, actual.UsdCostBasis)
		assert.EqualValues(t, 3, actual.Version)
	})
}

func testGetBalanceBatchHappyPath(t *testing.T, s balance.Store) {
	t.Run("testGetBalanceBatchHappyPath", func(t *testing.T) {
		ctx := context.Background()

		// All missing accounts return zero-valued records
		res, err := s.GetBalanceBatch(ctx, "account_a", "account_b", "account_c")
		require.NoError(t, err)
		assert.EqualValues(t, 0, res["account_a"].Quarks)
		assert.EqualValues(t, 0, res["account_b"].Quarks)
		assert.EqualValues(t, 0, res["account_c"].Quarks)

		require.NoError(t, s.AdjustBalance(ctx, "account_a", 100, 0.10, "test_owner", "test_mint", commonpb.AccountType_PRIMARY))
		require.NoError(t, s.AdjustBalance(ctx, "account_b", 200, 0.20, "test_owner", "test_mint", commonpb.AccountType_PRIMARY))

		res, err = s.GetBalanceBatch(ctx, "account_a", "account_b", "account_c")
		require.NoError(t, err)
		assert.EqualValues(t, 100, res["account_a"].Quarks)
		assert.EqualValues(t, 0.10, res["account_a"].UsdCostBasis)
		assert.EqualValues(t, 200, res["account_b"].Quarks)
		assert.EqualValues(t, 0.20, res["account_b"].UsdCostBasis)
		assert.EqualValues(t, 0, res["account_c"].Quarks)

		// Empty batch
		res, err = s.GetBalanceBatch(ctx)
		require.NoError(t, err)
		assert.Empty(t, res)
	})
}

func testAdjustBalanceHappyPath(t *testing.T, s balance.Store) {
	t.Run("testAdjustBalanceHappyPath", func(t *testing.T) {
		ctx := context.Background()

		// Create via first adjustment
		require.NoError(t, s.AdjustBalance(ctx, "token_account", 1000, 1.00, "test_owner", "test_mint", commonpb.AccountType_PRIMARY))

		actual, err := s.GetBalance(ctx, "token_account")
		require.NoError(t, err)
		assert.EqualValues(t, 1000, actual.Quarks)

		// Multiple adjustments accumulate
		require.NoError(t, s.AdjustBalance(ctx, "token_account", 500, 0.50, "test_owner", "test_mint", commonpb.AccountType_PRIMARY))
		require.NoError(t, s.AdjustBalance(ctx, "token_account", -200, -0.20, "test_owner", "test_mint", commonpb.AccountType_PRIMARY))
		require.NoError(t, s.AdjustBalance(ctx, "token_account", 100, 0.10, "test_owner", "test_mint", commonpb.AccountType_PRIMARY))

		actual, err = s.GetBalance(ctx, "token_account")
		require.NoError(t, err)
		assert.EqualValues(t, 1400, actual.Quarks)
		assert.InDelta(t, 1.40, actual.UsdCostBasis, 1e-9)

		// Drain to zero
		require.NoError(t, s.AdjustBalance(ctx, "token_account", -1400, -1.40, "test_owner", "test_mint", commonpb.AccountType_PRIMARY))

		actual, err = s.GetBalance(ctx, "token_account")
		require.NoError(t, err)
		assert.EqualValues(t, 0, actual.Quarks)
		assert.InDelta(t, 0, actual.UsdCostBasis, 1e-9)

		// Independent accounts
		require.NoError(t, s.AdjustBalance(ctx, "other_account", 777, 0.77, "test_owner", "test_mint", commonpb.AccountType_PRIMARY))

		actual, err = s.GetBalance(ctx, "other_account")
		require.NoError(t, err)
		assert.EqualValues(t, 777, actual.Quarks)

		actual, err = s.GetBalance(ctx, "token_account")
		require.NoError(t, err)
		assert.EqualValues(t, 0, actual.Quarks)
	})
}

func testAdjustBalanceNegative(t *testing.T, s balance.Store) {
	t.Run("testAdjustBalanceNegative", func(t *testing.T) {
		ctx := context.Background()

		// Cannot go negative from zero
		assert.Equal(t, balance.ErrNegativeBalance, s.AdjustBalance(ctx, "token_account", -1, 0, "test_owner", "test_mint", commonpb.AccountType_PRIMARY))

		// Cannot overdraw
		require.NoError(t, s.AdjustBalance(ctx, "token_account", 500, 0.50, "test_owner", "test_mint", commonpb.AccountType_PRIMARY))
		assert.Equal(t, balance.ErrNegativeBalance, s.AdjustBalance(ctx, "token_account", -501, 0, "test_owner", "test_mint", commonpb.AccountType_PRIMARY))

		// Balance unchanged after failed adjustment
		actual, err := s.GetBalance(ctx, "token_account")
		require.NoError(t, err)
		assert.EqualValues(t, 500, actual.Quarks)
		assert.EqualValues(t, 0.50, actual.UsdCostBasis)
	})
}
