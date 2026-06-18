package cache

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/code-payments/ocp-server/ocp/data/currency"
	"github.com/code-payments/ocp-server/ocp/data/currency/reserve/memory"
	"github.com/code-payments/ocp-server/ocp/data/currency/reserve/tests"
)

func TestReserve_CachedStore(t *testing.T) {
	testStore := New(memory.New()).(*store)
	teardown := func() {
		testStore.backing = memory.New()
		testStore.cache.Clear()
		testStore.liveSlots = make(map[string]uint64)
	}
	tests.RunStoreTests(t, testStore, teardown)
}

// TestReserve_CachedReadsServedFromCache verifies that once a reserve is read at
// a point in time, a later read in the same time bucket is served from the cache
// even after the backing store no longer holds it, while a read in a different
// bucket falls through to the (now empty) backing store.
func TestReserve_CachedReadsServedFromCache(t *testing.T) {
	ctx := context.Background()
	mint := "test-mint"
	now := time.Date(2021, 01, 29, 13, 0, 5, 0, time.UTC)

	s := New(memory.New()).(*store)
	require.NoError(t, s.PutHistoricalReserve(ctx, &currency.ReserveRecord{
		Mint:              mint,
		SupplyFromBonding: 1000,
		Time:              now,
	}))

	// Prime the cache with a point-in-time read.
	record, err := s.GetReserveAtTime(ctx, mint, now)
	require.NoError(t, err)
	assert.EqualValues(t, 1000, record.SupplyFromBonding)

	// Drop the backing data. A read in the same bucket is still served.
	s.backing = memory.New()

	record, err = s.GetReserveAtTime(ctx, mint, now)
	require.NoError(t, err)
	assert.EqualValues(t, 1000, record.SupplyFromBonding)

	// A read truncating to a different bucket misses the cache and falls through
	// to the empty backing store.
	otherBucket := now.Add(cacheBucket)
	_, err = s.GetReserveAtTime(ctx, mint, otherBucket)
	assert.Equal(t, currency.ErrNotFound, err)
}

// TestReserve_StaleLiveSlotRejectedLocally verifies that once a live reserve is
// saved, a write with an older or equal slot is rejected with a stale error
// without reaching the backing store, while a newer slot passes through.
func TestReserve_StaleLiveSlotRejectedLocally(t *testing.T) {
	ctx := context.Background()
	mint := "test-mint"

	s := New(memory.New()).(*store)
	require.NoError(t, s.PutLiveReserve(ctx, &currency.ReserveRecord{
		Mint:              mint,
		SupplyFromBonding: 1000,
		Slot:              200,
		Time:              time.Now(),
	}))

	// Swap in a fresh, empty backing. If the stale check were delegated to the
	// backing it would accept the older slot; the cache must reject it itself.
	s.backing = memory.New()

	// An older slot is rejected locally and never reaches the backing.
	err := s.PutLiveReserve(ctx, &currency.ReserveRecord{
		Mint:              mint,
		SupplyFromBonding: 999,
		Slot:              100,
		Time:              time.Now(),
	})
	assert.Equal(t, currency.ErrStaleReserveState, err)

	_, err = s.backing.GetLiveReserve(ctx, mint)
	assert.Equal(t, currency.ErrNotFound, err)

	// An equal slot is also rejected.
	err = s.PutLiveReserve(ctx, &currency.ReserveRecord{
		Mint:              mint,
		SupplyFromBonding: 999,
		Slot:              200,
		Time:              time.Now(),
	})
	assert.Equal(t, currency.ErrStaleReserveState, err)

	// A newer slot passes through to the backing and advances the tracked slot.
	require.NoError(t, s.PutLiveReserve(ctx, &currency.ReserveRecord{
		Mint:              mint,
		SupplyFromBonding: 1500,
		Slot:              300,
		Time:              time.Now(),
	}))
	rec, err := s.backing.GetLiveReserve(ctx, mint)
	require.NoError(t, err)
	assert.EqualValues(t, 300, rec.Slot)
}
