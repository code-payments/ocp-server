package tests

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/code-payments/ocp-server/database/query"
	"github.com/code-payments/ocp-server/ocp/data/balance"
)

func RunTests(t *testing.T, s balance.Store, teardown func()) {
	for _, tf := range []func(t *testing.T, s balance.Store){
		testRecordHappyPath,
		testGetAllByMint,
		testApplyDeltasBackfilled,
		testApplyDeltasNotBackfilled,
		testApplyDeltasAtomicity,
		testApplyDeltasConcurrency,
		testBackfill,
		testExternalCheckpointHappyPath,
	} {
		tf(t, s)
		teardown()
	}
}

func testRecordHappyPath(t *testing.T, s balance.Store) {
	t.Run("testRecordHappyPath", func(t *testing.T) {
		ctx := context.Background()

		_, err := s.Get(ctx, "token_account_1")
		assert.Equal(t, balance.ErrRecordNotFound, err)

		_, err = s.GetAllByOwner(ctx, "owner_1")
		assert.Equal(t, balance.ErrRecordNotFound, err)

		_, err = s.GetAllByOwnerAndMint(ctx, "owner_1", "mint_1")
		assert.Equal(t, balance.ErrRecordNotFound, err)

		batch, err := s.GetBatch(ctx, "token_account_1", "token_account_2")
		require.NoError(t, err)
		assert.Empty(t, batch)

		start := time.Now()

		expected := &balance.Record{
			TokenAccount: "token_account_1",
			OwnerAccount: "owner_1",
			MintAccount:  "mint_1",
			Quarks:       100,
			UsdCostBasis: 200,
			IsOpen:       true,
			IsBackfilled: true,
		}
		cloned := expected.Clone()

		require.NoError(t, s.Create(ctx, expected))
		assert.EqualValues(t, 1, expected.Id)
		assert.True(t, expected.UpdatedAt.After(start))

		assert.Equal(t, balance.ErrRecordExists, s.Create(ctx, &cloned))

		actual, err := s.Get(ctx, "token_account_1")
		require.NoError(t, err)
		assertEquivalentRecords(t, &cloned, actual)

		require.NoError(t, s.Create(ctx, &balance.Record{
			TokenAccount: "token_account_2",
			OwnerAccount: "owner_1",
			MintAccount:  "mint_2",
			IsOpen:       true,
		}))
		require.NoError(t, s.Create(ctx, &balance.Record{
			TokenAccount: "token_account_3",
			OwnerAccount: "owner_2",
			MintAccount:  "mint_1",
			IsOpen:       true,
		}))

		batch, err = s.GetBatch(ctx, "token_account_1", "token_account_3", "token_account_4")
		require.NoError(t, err)
		require.Len(t, batch, 2)
		assertEquivalentRecords(t, &cloned, batch["token_account_1"])
		assert.Equal(t, "token_account_3", batch["token_account_3"].TokenAccount)

		byOwner, err := s.GetAllByOwner(ctx, "owner_1")
		require.NoError(t, err)
		require.Len(t, byOwner, 2)
		assert.Equal(t, "token_account_1", byOwner[0].TokenAccount)
		assert.Equal(t, "token_account_2", byOwner[1].TokenAccount)

		byOwnerAndMint, err := s.GetAllByOwnerAndMint(ctx, "owner_1", "mint_2")
		require.NoError(t, err)
		require.Len(t, byOwnerAndMint, 1)
		assert.Equal(t, "token_account_2", byOwnerAndMint[0].TokenAccount)

		_, err = s.GetAllByOwnerAndMint(ctx, "owner_2", "mint_2")
		assert.Equal(t, balance.ErrRecordNotFound, err)

		assert.Error(t, s.Create(ctx, &balance.Record{
			TokenAccount: "token_account_5",
			OwnerAccount: "owner_1",
			MintAccount:  "mint_1",
			Quarks:       -1,
			IsBackfilled: true,
		}))
	})
}

func testGetAllByMint(t *testing.T, s balance.Store) {
	t.Run("testGetAllByMint", func(t *testing.T) {
		ctx := context.Background()

		_, err := s.GetAllByMint(ctx, "mint_1", 0, query.EmptyCursor, 10, query.Ascending)
		assert.Equal(t, balance.ErrRecordNotFound, err)

		for i := range 5 {
			require.NoError(t, s.Create(ctx, &balance.Record{
				TokenAccount: "token_account_" + string(rune('a'+i)),
				OwnerAccount: "owner",
				MintAccount:  "mint_1",
				Quarks:       int64(i * 10),
				IsOpen:       true,
				IsBackfilled: true,
			}))
		}
		require.NoError(t, s.Create(ctx, &balance.Record{
			TokenAccount: "token_account_other",
			OwnerAccount: "owner",
			MintAccount:  "mint_2",
			Quarks:       1000,
			IsOpen:       true,
			IsBackfilled: true,
		}))

		records, err := s.GetAllByMint(ctx, "mint_1", 0, query.EmptyCursor, 10, query.Ascending)
		require.NoError(t, err)
		require.Len(t, records, 5)
		for i, record := range records {
			assert.EqualValues(t, i+1, record.Id)
		}

		records, err = s.GetAllByMint(ctx, "mint_1", 20, query.EmptyCursor, 10, query.Ascending)
		require.NoError(t, err)
		require.Len(t, records, 3)
		assert.EqualValues(t, 20, records[0].Quarks)

		records, err = s.GetAllByMint(ctx, "mint_1", 0, query.EmptyCursor, 2, query.Ascending)
		require.NoError(t, err)
		require.Len(t, records, 2)
		assert.EqualValues(t, 1, records[0].Id)
		assert.EqualValues(t, 2, records[1].Id)

		records, err = s.GetAllByMint(ctx, "mint_1", 0, query.ToCursor(2), 2, query.Ascending)
		require.NoError(t, err)
		require.Len(t, records, 2)
		assert.EqualValues(t, 3, records[0].Id)
		assert.EqualValues(t, 4, records[1].Id)

		records, err = s.GetAllByMint(ctx, "mint_1", 0, query.EmptyCursor, 2, query.Descending)
		require.NoError(t, err)
		require.Len(t, records, 2)
		assert.EqualValues(t, 5, records[0].Id)
		assert.EqualValues(t, 4, records[1].Id)

		records, err = s.GetAllByMint(ctx, "mint_1", 0, query.ToCursor(4), 10, query.Descending)
		require.NoError(t, err)
		require.Len(t, records, 3)
		assert.EqualValues(t, 3, records[0].Id)

		_, err = s.GetAllByMint(ctx, "mint_1", 0, query.ToCursor(5), 10, query.Ascending)
		assert.Equal(t, balance.ErrRecordNotFound, err)

		// Counting by mint uses the same threshold semantics, but only over
		// backfilled records
		require.NoError(t, s.Create(ctx, &balance.Record{
			TokenAccount: "token_account_not_backfilled",
			OwnerAccount: "owner",
			MintAccount:  "mint_1",
			Quarks:       1000,
			IsOpen:       true,
			IsBackfilled: false,
		}))

		count, err := s.CountByMint(ctx, "mint_1", 0)
		require.NoError(t, err)
		assert.EqualValues(t, 5, count)

		count, err = s.CountByMint(ctx, "mint_1", 20)
		require.NoError(t, err)
		assert.EqualValues(t, 3, count)

		count, err = s.CountByMint(ctx, "mint_2", 0)
		require.NoError(t, err)
		assert.EqualValues(t, 1, count)

		count, err = s.CountByMint(ctx, "mint_3", 0)
		require.NoError(t, err)
		assert.EqualValues(t, 0, count)
	})
}

func testApplyDeltasBackfilled(t *testing.T, s balance.Store) {
	t.Run("testApplyDeltasBackfilled", func(t *testing.T) {
		ctx := context.Background()

		require.NoError(t, s.Create(ctx, &balance.Record{
			TokenAccount: "token_account_1",
			OwnerAccount: "owner",
			MintAccount:  "mint",
			IsOpen:       true,
			IsBackfilled: true,
		}))

		// Every kind of delta requires a record
		assert.Equal(t, balance.ErrRecordNotFound, s.ApplyDeltas(ctx, &balance.Delta{TokenAccount: "untracked", Kind: balance.DeltaCredit, Quarks: 1}))
		assert.Equal(t, balance.ErrRecordNotFound, s.ApplyDeltas(ctx, &balance.Delta{TokenAccount: "untracked", Kind: balance.DeltaDebit, Quarks: 1}))
		assert.Equal(t, balance.ErrRecordNotFound, s.ApplyDeltas(ctx, &balance.Delta{TokenAccount: "untracked", Kind: balance.DeltaDrain, Quarks: 1}))
		assert.Equal(t, balance.ErrRecordNotFound, s.ApplyDeltas(ctx, &balance.Delta{TokenAccount: "untracked", Kind: balance.DeltaClose}))

		// A batch mixing a tracked account with an untracked credit is rejected
		// as a whole, so the tracked side is untouched
		assert.Equal(t, balance.ErrRecordNotFound, s.ApplyDeltas(
			ctx,
			&balance.Delta{TokenAccount: "token_account_1", Kind: balance.DeltaCredit, Quarks: 1},
			&balance.Delta{TokenAccount: "untracked", Kind: balance.DeltaCredit, Quarks: 1},
		))
		assertBalance(t, s, "token_account_1", 0, 0, true)

		// Invalid deltas are rejected
		assert.Error(t, s.ApplyDeltas(ctx, &balance.Delta{TokenAccount: "token_account_1", Kind: balance.DeltaCredit}))
		assert.Error(t, s.ApplyDeltas(ctx, &balance.Delta{Kind: balance.DeltaCredit, Quarks: 1}))

		require.NoError(t, s.ApplyDeltas(ctx, &balance.Delta{TokenAccount: "token_account_1", Kind: balance.DeltaCredit, Quarks: 100, UsdCostBasis: 50}))
		assertBalance(t, s, "token_account_1", 100, 50, true)

		assert.Equal(t, balance.ErrInsufficientBalance, s.ApplyDeltas(ctx, &balance.Delta{TokenAccount: "token_account_1", Kind: balance.DeltaDebit, Quarks: 101, UsdCostBasis: 1}))
		assertBalance(t, s, "token_account_1", 100, 50, true)

		require.NoError(t, s.ApplyDeltas(ctx, &balance.Delta{TokenAccount: "token_account_1", Kind: balance.DeltaDebit, Quarks: 30, UsdCostBasis: 60}))
		assertBalance(t, s, "token_account_1", 70, -10, true)

		// A credit can carry a signed USD-only reconciliation
		require.NoError(t, s.ApplyDeltas(ctx, &balance.Delta{TokenAccount: "token_account_1", Kind: balance.DeltaCredit, UsdCostBasis: -5}))
		assertBalance(t, s, "token_account_1", 70, -15, true)

		assert.Equal(t, balance.ErrBalanceChanged, s.ApplyDeltas(ctx, &balance.Delta{TokenAccount: "token_account_1", Kind: balance.DeltaClose}))
		assert.Equal(t, balance.ErrBalanceChanged, s.ApplyDeltas(ctx, &balance.Delta{TokenAccount: "token_account_1", Kind: balance.DeltaDrain, Quarks: 69}))
		assert.Equal(t, balance.ErrBalanceChanged, s.ApplyDeltas(ctx, &balance.Delta{TokenAccount: "token_account_1", Kind: balance.DeltaDrain, Quarks: 71}))
		assertBalance(t, s, "token_account_1", 70, -15, true)

		require.NoError(t, s.ApplyDeltas(ctx, &balance.Delta{TokenAccount: "token_account_1", Kind: balance.DeltaDrain, Quarks: 70, UsdCostBasis: 12345}))
		assertBalance(t, s, "token_account_1", 0, 0, false)

		assert.Equal(t, balance.ErrAccountClosed, s.ApplyDeltas(ctx, &balance.Delta{TokenAccount: "token_account_1", Kind: balance.DeltaCredit, Quarks: 1}))
		assert.Equal(t, balance.ErrAccountClosed, s.ApplyDeltas(ctx, &balance.Delta{TokenAccount: "token_account_1", Kind: balance.DeltaDrain, Quarks: 0, UsdCostBasis: 1}))
		assert.Equal(t, balance.ErrAccountClosed, s.ApplyDeltas(ctx, &balance.Delta{TokenAccount: "token_account_1", Kind: balance.DeltaClose}))
		assert.Equal(t, balance.ErrInsufficientBalance, s.ApplyDeltas(ctx, &balance.Delta{TokenAccount: "token_account_1", Kind: balance.DeltaDebit, Quarks: 1}))
		assertBalance(t, s, "token_account_1", 0, 0, false)

		require.NoError(t, s.Create(ctx, &balance.Record{
			TokenAccount: "token_account_2",
			OwnerAccount: "owner",
			MintAccount:  "mint",
			IsOpen:       true,
			IsBackfilled: true,
		}))
		require.NoError(t, s.ApplyDeltas(ctx, &balance.Delta{TokenAccount: "token_account_2", Kind: balance.DeltaClose}))
		assertBalance(t, s, "token_account_2", 0, 0, false)
	})
}

func testApplyDeltasNotBackfilled(t *testing.T, s balance.Store) {
	t.Run("testApplyDeltasNotBackfilled", func(t *testing.T) {
		ctx := context.Background()

		require.NoError(t, s.Create(ctx, &balance.Record{
			TokenAccount: "token_account_1",
			OwnerAccount: "owner",
			MintAccount:  "mint",
			IsOpen:       true,
		}))

		// No predicates are enforced, and the balance can go negative
		require.NoError(t, s.ApplyDeltas(ctx, &balance.Delta{TokenAccount: "token_account_1", Kind: balance.DeltaDebit, Quarks: 30, UsdCostBasis: 10}))
		assertBalance(t, s, "token_account_1", -30, -10, true)

		require.NoError(t, s.ApplyDeltas(ctx, &balance.Delta{TokenAccount: "token_account_1", Kind: balance.DeltaCredit, Quarks: 100, UsdCostBasis: 50}))
		assertBalance(t, s, "token_account_1", 70, 40, true)

		// A drain records the delta rather than zeroing, but still closes
		require.NoError(t, s.ApplyDeltas(ctx, &balance.Delta{TokenAccount: "token_account_1", Kind: balance.DeltaDrain, Quarks: 100, UsdCostBasis: 60}))
		assertBalance(t, s, "token_account_1", -30, -20, false)

		// Closed accounts still accumulate
		require.NoError(t, s.ApplyDeltas(ctx, &balance.Delta{TokenAccount: "token_account_1", Kind: balance.DeltaCredit, Quarks: 5, UsdCostBasis: 5}))
		assertBalance(t, s, "token_account_1", -25, -15, false)

		require.NoError(t, s.ApplyDeltas(ctx, &balance.Delta{TokenAccount: "token_account_1", Kind: balance.DeltaClose}))
		assertBalance(t, s, "token_account_1", -25, -15, false)
	})
}

func testApplyDeltasAtomicity(t *testing.T, s balance.Store) {
	t.Run("testApplyDeltasAtomicity", func(t *testing.T) {
		ctx := context.Background()

		for _, tokenAccount := range []string{"token_account_1", "token_account_2"} {
			require.NoError(t, s.Create(ctx, &balance.Record{
				TokenAccount: tokenAccount,
				OwnerAccount: "owner",
				MintAccount:  "mint",
				Quarks:       100,
				IsOpen:       true,
				IsBackfilled: true,
			}))
		}

		// A transfer applies both sides
		require.NoError(t, s.ApplyDeltas(
			ctx,
			&balance.Delta{TokenAccount: "token_account_2", Kind: balance.DeltaCredit, Quarks: 40, UsdCostBasis: 4},
			&balance.Delta{TokenAccount: "token_account_1", Kind: balance.DeltaDebit, Quarks: 40, UsdCostBasis: 4},
		))
		assertBalance(t, s, "token_account_1", 60, -4, true)
		assertBalance(t, s, "token_account_2", 140, 4, true)

		// A failure on either side rolls back the other, regardless of order
		assert.Equal(t, balance.ErrInsufficientBalance, s.ApplyDeltas(
			ctx,
			&balance.Delta{TokenAccount: "token_account_1", Kind: balance.DeltaCredit, Quarks: 500},
			&balance.Delta{TokenAccount: "token_account_2", Kind: balance.DeltaDebit, Quarks: 141},
		))
		assertBalance(t, s, "token_account_1", 60, -4, true)
		assertBalance(t, s, "token_account_2", 140, 4, true)

		assert.Equal(t, balance.ErrInsufficientBalance, s.ApplyDeltas(
			ctx,
			&balance.Delta{TokenAccount: "token_account_1", Kind: balance.DeltaDebit, Quarks: 61},
			&balance.Delta{TokenAccount: "token_account_2", Kind: balance.DeltaCredit, Quarks: 500},
		))
		assertBalance(t, s, "token_account_1", 60, -4, true)
		assertBalance(t, s, "token_account_2", 140, 4, true)

		// Multiple deltas to the same account apply in kind order: credit, then debit
		require.NoError(t, s.ApplyDeltas(
			ctx,
			&balance.Delta{TokenAccount: "token_account_1", Kind: balance.DeltaDebit, Quarks: 100},
			&balance.Delta{TokenAccount: "token_account_1", Kind: balance.DeltaCredit, Quarks: 50},
		))
		assertBalance(t, s, "token_account_1", 10, -4, true)

		// Same-kind deltas to the same account are checked as one, so a pair of
		// debits that together exceed the balance fails even though each fits
		assert.Equal(t, balance.ErrInsufficientBalance, s.ApplyDeltas(
			ctx,
			&balance.Delta{TokenAccount: "token_account_1", Kind: balance.DeltaDebit, Quarks: 6, UsdCostBasis: 1},
			&balance.Delta{TokenAccount: "token_account_1", Kind: balance.DeltaDebit, Quarks: 6, UsdCostBasis: 1},
		))
		assertBalance(t, s, "token_account_1", 10, -4, true)

		require.NoError(t, s.ApplyDeltas(
			ctx,
			&balance.Delta{TokenAccount: "token_account_1", Kind: balance.DeltaDebit, Quarks: 4, UsdCostBasis: 1},
			&balance.Delta{TokenAccount: "token_account_1", Kind: balance.DeltaDebit, Quarks: 6, UsdCostBasis: 2},
			&balance.Delta{TokenAccount: "token_account_2", Kind: balance.DeltaCredit, Quarks: 4, UsdCostBasis: 1},
			&balance.Delta{TokenAccount: "token_account_2", Kind: balance.DeltaCredit, Quarks: 6, UsdCostBasis: 2},
		))
		assertBalance(t, s, "token_account_1", 0, -7, true)
		assertBalance(t, s, "token_account_2", 150, 7, true)
	})
}

func testApplyDeltasConcurrency(t *testing.T, s balance.Store) {
	t.Run("testApplyDeltasConcurrency", func(t *testing.T) {
		ctx := context.Background()

		const initialBalance = 20
		const attempts = 50

		require.NoError(t, s.Create(ctx, &balance.Record{
			TokenAccount: "sender",
			OwnerAccount: "owner_1",
			MintAccount:  "mint",
			Quarks:       initialBalance,
			IsOpen:       true,
			IsBackfilled: true,
		}))
		require.NoError(t, s.Create(ctx, &balance.Record{
			TokenAccount: "receiver",
			OwnerAccount: "owner_2",
			MintAccount:  "mint",
			IsOpen:       true,
			IsBackfilled: true,
		}))

		// Concurrent sends of 1 quark each: exactly initialBalance succeed, and
		// the rest fail with an insufficient balance. Every send credits the
		// receiver in the same batch.
		var wg sync.WaitGroup
		results := make(chan error, attempts)
		for range attempts {
			wg.Go(func() {
				results <- s.ApplyDeltas(
					ctx,
					&balance.Delta{TokenAccount: "sender", Kind: balance.DeltaDebit, Quarks: 1, UsdCostBasis: 1},
					&balance.Delta{TokenAccount: "receiver", Kind: balance.DeltaCredit, Quarks: 1, UsdCostBasis: 1},
				)
			})
		}
		wg.Wait()
		close(results)

		var succeeded, insufficient int
		for err := range results {
			switch err {
			case nil:
				succeeded++
			case balance.ErrInsufficientBalance:
				insufficient++
			default:
				require.NoError(t, err)
			}
		}
		assert.Equal(t, initialBalance, succeeded)
		assert.Equal(t, attempts-initialBalance, insufficient)

		assertBalance(t, s, "sender", 0, -initialBalance, true)
		assertBalance(t, s, "receiver", initialBalance, initialBalance, true)

		// Concurrent credits never fail
		wg = sync.WaitGroup{}
		results = make(chan error, attempts)
		for range attempts {
			wg.Go(func() {
				results <- s.ApplyDeltas(ctx, &balance.Delta{TokenAccount: "receiver", Kind: balance.DeltaCredit, Quarks: 1})
			})
		}
		wg.Wait()
		close(results)
		for err := range results {
			require.NoError(t, err)
		}
		assertBalance(t, s, "receiver", initialBalance+attempts, initialBalance, true)

		// Concurrent drains: exactly one wins
		wg = sync.WaitGroup{}
		results = make(chan error, attempts)
		for range attempts {
			wg.Go(func() {
				results <- s.ApplyDeltas(ctx, &balance.Delta{TokenAccount: "receiver", Kind: balance.DeltaDrain, Quarks: initialBalance + attempts})
			})
		}
		wg.Wait()
		close(results)

		var drained, closed int
		for err := range results {
			switch err {
			case nil:
				drained++
			case balance.ErrAccountClosed:
				closed++
			default:
				require.NoError(t, err)
			}
		}
		assert.Equal(t, 1, drained)
		assert.Equal(t, attempts-1, closed)
		assertBalance(t, s, "receiver", 0, 0, false)
	})
}

func testBackfill(t *testing.T, s balance.Store) {
	t.Run("testBackfill", func(t *testing.T) {
		ctx := context.Background()

		fn := func(quarks, usdCostBasis int64, isOpen bool) balance.BackfillFunc {
			return func(ctx context.Context) (*balance.BackfillResult, error) {
				return &balance.BackfillResult{Quarks: quarks, UsdCostBasis: usdCostBasis, IsOpen: isOpen}, nil
			}
		}

		assert.Equal(t, balance.ErrRecordNotFound, s.Backfill(ctx, "token_account_1", fn(1, 1, true)))

		require.NoError(t, s.Create(ctx, &balance.Record{
			TokenAccount: "token_account_1",
			OwnerAccount: "owner",
			MintAccount:  "mint",
			IsOpen:       true,
		}))

		// Deltas recorded before the backfill are discarded by it
		require.NoError(t, s.ApplyDeltas(ctx, &balance.Delta{TokenAccount: "token_account_1", Kind: balance.DeltaDebit, Quarks: 30, UsdCostBasis: 10}))
		assertBalance(t, s, "token_account_1", -30, -10, true)

		// A failing computation leaves the record untouched
		assert.Error(t, s.Backfill(ctx, "token_account_1", func(ctx context.Context) (*balance.BackfillResult, error) {
			return nil, assert.AnError
		}))
		record, err := s.Get(ctx, "token_account_1")
		require.NoError(t, err)
		assert.False(t, record.IsBackfilled)
		assert.EqualValues(t, -30, record.Quarks)

		// So does a negative computed balance
		assert.Equal(t, balance.ErrNegativeBalance, s.Backfill(ctx, "token_account_1", fn(-1, 0, true)))
		record, err = s.Get(ctx, "token_account_1")
		require.NoError(t, err)
		assert.False(t, record.IsBackfilled)
		assert.EqualValues(t, -30, record.Quarks)

		require.NoError(t, s.Backfill(ctx, "token_account_1", fn(500, 250, true)))
		record, err = s.Get(ctx, "token_account_1")
		require.NoError(t, err)
		assert.True(t, record.IsBackfilled)
		assertBalance(t, s, "token_account_1", 500, 250, true)

		// Predicates are enforced from now on
		assert.Equal(t, balance.ErrInsufficientBalance, s.ApplyDeltas(ctx, &balance.Delta{TokenAccount: "token_account_1", Kind: balance.DeltaDebit, Quarks: 501}))
		require.NoError(t, s.ApplyDeltas(ctx, &balance.Delta{TokenAccount: "token_account_1", Kind: balance.DeltaDebit, Quarks: 500}))
		assertBalance(t, s, "token_account_1", 0, 250, true)

		called := false
		assert.Equal(t, balance.ErrAlreadyBackfilled, s.Backfill(ctx, "token_account_1", func(ctx context.Context) (*balance.BackfillResult, error) {
			called = true
			return &balance.BackfillResult{}, nil
		}))
		assert.False(t, called)
		assertBalance(t, s, "token_account_1", 0, 250, true)

		// A backfill can establish the account as closed, e.g. a claimed gift card
		require.NoError(t, s.Create(ctx, &balance.Record{
			TokenAccount: "token_account_2",
			OwnerAccount: "owner",
			MintAccount:  "mint",
			IsOpen:       true,
		}))
		require.NoError(t, s.Backfill(ctx, "token_account_2", fn(0, 0, false)))
		assertBalance(t, s, "token_account_2", 0, 0, false)
		assert.Equal(t, balance.ErrAccountClosed, s.ApplyDeltas(ctx, &balance.Delta{TokenAccount: "token_account_2", Kind: balance.DeltaCredit, Quarks: 1}))
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

func assertBalance(t *testing.T, s balance.Store, tokenAccount string, quarks, usdCostBasis int64, isOpen bool) {
	record, err := s.Get(context.Background(), tokenAccount)
	require.NoError(t, err)
	assert.EqualValues(t, quarks, record.Quarks, "quarks")
	assert.EqualValues(t, usdCostBasis, record.UsdCostBasis, "usd market value")
	assert.Equal(t, isOpen, record.IsOpen, "is open")
}

func assertEquivalentRecords(t *testing.T, obj1, obj2 *balance.Record) {
	assert.Equal(t, obj1.TokenAccount, obj2.TokenAccount)
	assert.Equal(t, obj1.OwnerAccount, obj2.OwnerAccount)
	assert.Equal(t, obj1.MintAccount, obj2.MintAccount)
	assert.Equal(t, obj1.Quarks, obj2.Quarks)
	assert.Equal(t, obj1.UsdCostBasis, obj2.UsdCostBasis)
	assert.Equal(t, obj1.IsOpen, obj2.IsOpen)
	assert.Equal(t, obj1.IsBackfilled, obj2.IsBackfilled)
}

func assertEquivalentExternalCheckpoingRecords(t *testing.T, obj1, obj2 *balance.ExternalCheckpointRecord) {
	assert.Equal(t, obj1.TokenAccount, obj2.TokenAccount)
	assert.Equal(t, obj1.Quarks, obj2.Quarks)
	assert.Equal(t, obj1.SlotCheckpoint, obj2.SlotCheckpoint)
}
