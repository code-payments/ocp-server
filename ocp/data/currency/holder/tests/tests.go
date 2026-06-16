// Package tests holds the shared conformance suite run against every
// holder.Store implementation.
package tests

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/code-payments/ocp-server/database/query"
	"github.com/code-payments/ocp-server/ocp/data/currency"
	"github.com/code-payments/ocp-server/ocp/data/currency/holder"
)

func RunStoreTests(t *testing.T, s holder.Store, teardown func()) {
	for _, tf := range []func(t *testing.T, s holder.Store){
		testHolderCountRoundTrip,
		testGetHolderCountsForDay,
		testGetHolderCountsInRange,
		testLiveHolderCountRoundTrip,
		testGetAllLiveHolderCounts,
	} {
		tf(t, s)
		teardown()
	}
}

func testHolderCountRoundTrip(t *testing.T, s holder.Store) {
	now := time.Date(2021, 01, 29, 13, 0, 5, 0, time.UTC)
	mint := "mint"

	record, err := s.GetHolderCountAtTime(context.Background(), mint, now)
	assert.Nil(t, record)
	assert.Equal(t, currency.ErrNotFound, err)

	expected := &currency.HolderCountRecord{
		Mint:        mint,
		HolderCount: 42,
		Time:        now,
	}
	require.NoError(t, s.PutHistoricalHolderCount(context.Background(), expected))

	// Duplicate timestamp for the mint fails.
	assert.Equal(t, currency.ErrExists, s.PutHistoricalHolderCount(context.Background(), expected))

	actual, err := s.GetHolderCountAtTime(context.Background(), mint, now)
	require.NoError(t, err)
	assert.Equal(t, now.Unix(), actual.Time.Unix())
	assert.EqualValues(t, expected.HolderCount, actual.HolderCount)

	// A later time returns the most recent record at or before it.
	actual, err = s.GetHolderCountAtTime(context.Background(), mint, time.Date(2021, 01, 29, 14, 0, 5, 0, time.UTC))
	require.NoError(t, err)
	assert.Equal(t, now.Unix(), actual.Time.Unix())
	assert.EqualValues(t, expected.HolderCount, actual.HolderCount)

	// A later day still returns it (no same-day requirement).
	tomorrow := time.Date(2021, 01, 30, 0, 0, 0, 0, time.UTC)
	actual, err = s.GetHolderCountAtTime(context.Background(), mint, tomorrow)
	require.NoError(t, err)
	assert.Equal(t, now.Unix(), actual.Time.Unix())

	// A time before any record exists is not found.
	before := time.Date(2021, 01, 28, 0, 0, 0, 0, time.UTC)
	actual, err = s.GetHolderCountAtTime(context.Background(), mint, before)
	assert.Nil(t, actual)
	assert.Equal(t, currency.ErrNotFound, err)

	// A different mint is independent.
	_, err = s.GetHolderCountAtTime(context.Background(), "other-mint", now)
	assert.Equal(t, currency.ErrNotFound, err)
}

func testGetHolderCountsForDay(t *testing.T, s holder.Store) {
	ctx := context.Background()
	day := time.Date(2022, 05, 10, 0, 0, 0, 0, time.UTC)

	// mintA: a prior-day record plus two on `day` (close = 8 at 20:00).
	require.NoError(t, s.PutHistoricalHolderCount(ctx, &currency.HolderCountRecord{Mint: "mintA", HolderCount: 1, Time: day.Add(-15 * time.Hour)}))
	require.NoError(t, s.PutHistoricalHolderCount(ctx, &currency.HolderCountRecord{Mint: "mintA", HolderCount: 5, Time: day.Add(10 * time.Hour)}))
	require.NoError(t, s.PutHistoricalHolderCount(ctx, &currency.HolderCountRecord{Mint: "mintA", HolderCount: 8, Time: day.Add(20 * time.Hour)}))
	// mintB: a single record on `day`.
	require.NoError(t, s.PutHistoricalHolderCount(ctx, &currency.HolderCountRecord{Mint: "mintB", HolderCount: 100, Time: day.Add(12 * time.Hour)}))
	// mintC: a record only on the next day — should be omitted for a `day` query.
	require.NoError(t, s.PutHistoricalHolderCount(ctx, &currency.HolderCountRecord{Mint: "mintC", HolderCount: 50, Time: day.AddDate(0, 0, 1).Add(8 * time.Hour)}))

	queryT := time.Date(2022, 05, 10, 23, 59, 59, 0, time.UTC)
	res, err := s.GetHolderCountsForDay(ctx, []string{"mintA", "mintB", "mintC", "mintD"}, queryT)
	require.NoError(t, err)
	require.Len(t, res, 2)

	// mintA: close of the day = 8 at 20:00.
	require.Contains(t, res, "mintA")
	assert.EqualValues(t, 8, res["mintA"].HolderCount)
	assert.Equal(t, day.Add(20*time.Hour).Unix(), res["mintA"].Time.Unix())

	// mintB: its single same-day record.
	require.Contains(t, res, "mintB")
	assert.EqualValues(t, 100, res["mintB"].HolderCount)

	// mintC has no record on `day`; mintD has none at all — both omitted.
	assert.NotContains(t, res, "mintC")
	assert.NotContains(t, res, "mintD")

	// Empty input yields an empty map, not an error.
	empty, err := s.GetHolderCountsForDay(ctx, nil, queryT)
	require.NoError(t, err)
	assert.Empty(t, empty)
}

func testGetHolderCountsInRange(t *testing.T, s holder.Store) {
	var counts []currency.HolderCountRecord

	now := time.Now().UTC()
	mint := "test-mint"

	for i := 0; i < 100; i++ {
		counts = append(counts, currency.HolderCountRecord{
			Mint:        mint,
			HolderCount: uint64(1000 + i),
			Time:        now.Add(time.Duration(i) * time.Hour),
		})
	}

	for _, item := range counts {
		itemCopy := item
		require.NoError(t, s.PutHistoricalHolderCount(context.Background(), &itemCopy))
	}

	result, err := s.GetHolderCountsInRange(context.Background(), mint, query.IntervalRaw, counts[0].Time, counts[99].Time, query.Ascending)
	require.NoError(t, err)
	assert.Equal(t, len(result), 100)
	for i, item := range result {
		assert.Equal(t, counts[i].Time.Unix(), item.Time.Unix())
		assert.EqualValues(t, counts[i].HolderCount, item.HolderCount)
	}

	result, err = s.GetHolderCountsInRange(context.Background(), mint, query.IntervalRaw, counts[0].Time, counts[49].Time, query.Ascending)
	require.NoError(t, err)
	assert.Equal(t, len(result), 50)
	for i, item := range result {
		assert.Equal(t, counts[i].Time.Unix(), item.Time.Unix())
		assert.EqualValues(t, counts[i].HolderCount, item.HolderCount)
	}

	result, err = s.GetHolderCountsInRange(context.Background(), mint, query.IntervalRaw, counts[0].Time, counts[99].Time, query.Descending)
	require.NoError(t, err)
	assert.Equal(t, len(result), 100)
	for i, item := range result {
		assert.Equal(t, counts[99-i].Time.Unix(), item.Time.Unix())
		assert.EqualValues(t, counts[99-i].HolderCount, item.HolderCount)
	}

	for _, interval := range query.AllIntervals {
		_, err = s.GetHolderCountsInRange(context.Background(), mint, interval, counts[0].Time, counts[99].Time, query.Ascending)
		require.NoError(t, err)
	}
}

func testLiveHolderCountRoundTrip(t *testing.T, s holder.Store) {
	ctx := context.Background()
	mint := "live-holder-mint"

	_, err := s.GetLiveHolderCount(ctx, mint)
	assert.Equal(t, currency.ErrNotFound, err)

	t1 := time.Date(2022, 03, 01, 10, 0, 0, 0, time.UTC)
	require.NoError(t, s.PutLiveHolderCount(ctx, &currency.HolderCountRecord{
		Mint:        mint,
		HolderCount: 10,
		Time:        t1,
	}))

	actual, err := s.GetLiveHolderCount(ctx, mint)
	require.NoError(t, err)
	assert.Equal(t, mint, actual.Mint)
	assert.EqualValues(t, 10, actual.HolderCount)

	// Later timestamp advances.
	t2 := t1.Add(time.Hour)
	require.NoError(t, s.PutLiveHolderCount(ctx, &currency.HolderCountRecord{
		Mint:        mint,
		HolderCount: 20,
		Time:        t2,
	}))

	actual, err = s.GetLiveHolderCount(ctx, mint)
	require.NoError(t, err)
	assert.EqualValues(t, 20, actual.HolderCount)

	// Equal timestamp is stale.
	assert.Equal(t, currency.ErrStaleHolderState, s.PutLiveHolderCount(ctx, &currency.HolderCountRecord{
		Mint:        mint,
		HolderCount: 30,
		Time:        t2,
	}))

	// Earlier timestamp is stale.
	assert.Equal(t, currency.ErrStaleHolderState, s.PutLiveHolderCount(ctx, &currency.HolderCountRecord{
		Mint:        mint,
		HolderCount: 30,
		Time:        t1,
	}))

	// Unchanged after stale attempts.
	actual, err = s.GetLiveHolderCount(ctx, mint)
	require.NoError(t, err)
	assert.EqualValues(t, 20, actual.HolderCount)
}

func testGetAllLiveHolderCounts(t *testing.T, s holder.Store) {
	ctx := context.Background()

	_, err := s.GetAllLiveHolderCounts(ctx)
	assert.Equal(t, currency.ErrNotFound, err)

	now := time.Now().UTC()
	require.NoError(t, s.PutLiveHolderCount(ctx, &currency.HolderCountRecord{
		Mint:        "mint-all-live-1",
		HolderCount: 100,
		Time:        now,
	}))

	counts, err := s.GetAllLiveHolderCounts(ctx)
	require.NoError(t, err)
	assert.Len(t, counts, 1)
	assert.EqualValues(t, 100, counts["mint-all-live-1"].HolderCount)

	require.NoError(t, s.PutLiveHolderCount(ctx, &currency.HolderCountRecord{
		Mint:        "mint-all-live-2",
		HolderCount: 200,
		Time:        now,
	}))

	counts, err = s.GetAllLiveHolderCounts(ctx)
	require.NoError(t, err)
	assert.Len(t, counts, 2)
	assert.EqualValues(t, 100, counts["mint-all-live-1"].HolderCount)
	assert.EqualValues(t, 200, counts["mint-all-live-2"].HolderCount)

	// Updating one mint is reflected.
	require.NoError(t, s.PutLiveHolderCount(ctx, &currency.HolderCountRecord{
		Mint:        "mint-all-live-1",
		HolderCount: 150,
		Time:        now.Add(time.Hour),
	}))

	counts, err = s.GetAllLiveHolderCounts(ctx)
	require.NoError(t, err)
	assert.Len(t, counts, 2)
	assert.EqualValues(t, 150, counts["mint-all-live-1"].HolderCount)
	assert.EqualValues(t, 200, counts["mint-all-live-2"].HolderCount)
}
