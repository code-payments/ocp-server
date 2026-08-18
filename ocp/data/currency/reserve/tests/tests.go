// Package tests holds the shared conformance suite run against every
// reserve.Store implementation.
package tests

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/code-payments/ocp-server/database/query"
	"github.com/code-payments/ocp-server/ocp/data/currency"
	"github.com/code-payments/ocp-server/ocp/data/currency/reserve"
)

func RunStoreTests(t *testing.T, s reserve.Store, teardown func()) {
	for _, tf := range []func(t *testing.T, s reserve.Store){
		testReserveRoundTrip,
		testGetReservesForDay,
		testGetReservesInRange,
		testLiveReserveRoundTrip,
		testGetAllLiveReserves,
	} {
		tf(t, s)
		teardown()
	}
}

func testReserveRoundTrip(t *testing.T, s reserve.Store) {
	now := time.Date(2021, 01, 29, 13, 0, 5, 0, time.UTC)
	mint := "mint"

	record, err := s.GetReserveAtTime(context.Background(), mint, now)
	assert.Nil(t, record)
	assert.Equal(t, currency.ErrNotFound, err)

	expected := &currency.ReserveRecord{
		Mint:              mint,
		SupplyFromBonding: 1,
		Time:              now,
	}
	require.NoError(t, s.PutHistoricalReserve(context.Background(), expected))

	// Duplicate timestamp for the mint fails.
	assert.Equal(t, currency.ErrExists, s.PutHistoricalReserve(context.Background(), expected))

	actual, err := s.GetReserveAtTime(context.Background(), mint, now)
	require.NoError(t, err)
	assert.Equal(t, now.Unix(), actual.Time.Unix())
	assert.EqualValues(t, expected.SupplyFromBonding, actual.SupplyFromBonding)

	// A later time returns the most recent record at or before it.
	actual, err = s.GetReserveAtTime(context.Background(), mint, time.Date(2021, 01, 29, 14, 0, 5, 0, time.UTC))
	require.NoError(t, err)
	assert.Equal(t, now.Unix(), actual.Time.Unix())
	assert.EqualValues(t, expected.SupplyFromBonding, actual.SupplyFromBonding)

	// A later day still returns it (no same-day requirement).
	tomorrow := time.Date(2021, 01, 30, 0, 0, 0, 0, time.UTC)
	actual, err = s.GetReserveAtTime(context.Background(), mint, tomorrow)
	require.NoError(t, err)
	assert.Equal(t, now.Unix(), actual.Time.Unix())

	// A time before any record exists is not found.
	before := time.Date(2021, 01, 28, 0, 0, 0, 0, time.UTC)
	actual, err = s.GetReserveAtTime(context.Background(), mint, before)
	assert.Nil(t, actual)
	assert.Equal(t, currency.ErrNotFound, err)

	// A different mint is independent.
	_, err = s.GetReserveAtTime(context.Background(), "other-mint", now)
	assert.Equal(t, currency.ErrNotFound, err)
}

func testGetReservesForDay(t *testing.T, s reserve.Store) {
	ctx := context.Background()
	day := time.Date(2022, 05, 10, 0, 0, 0, 0, time.UTC)

	// mintA: a prior-day record plus two on `day` (close = 800 at 20:00).
	require.NoError(t, s.PutHistoricalReserve(ctx, &currency.ReserveRecord{Mint: "mintA", SupplyFromBonding: 100, Time: day.Add(-15 * time.Hour)}))
	require.NoError(t, s.PutHistoricalReserve(ctx, &currency.ReserveRecord{Mint: "mintA", SupplyFromBonding: 500, Time: day.Add(10 * time.Hour)}))
	require.NoError(t, s.PutHistoricalReserve(ctx, &currency.ReserveRecord{Mint: "mintA", SupplyFromBonding: 800, Time: day.Add(20 * time.Hour)}))
	// mintB: a single record on `day`.
	require.NoError(t, s.PutHistoricalReserve(ctx, &currency.ReserveRecord{Mint: "mintB", SupplyFromBonding: 10000, Time: day.Add(12 * time.Hour)}))
	// mintC: a record only on the next day — should be omitted for a `day` query.
	require.NoError(t, s.PutHistoricalReserve(ctx, &currency.ReserveRecord{Mint: "mintC", SupplyFromBonding: 5000, Time: day.AddDate(0, 0, 1).Add(8 * time.Hour)}))

	queryT := time.Date(2022, 05, 10, 23, 59, 59, 0, time.UTC)
	res, err := s.GetReservesForDay(ctx, []string{"mintA", "mintB", "mintC", "mintD"}, queryT)
	require.NoError(t, err)
	require.Len(t, res, 2)

	// mintA: close of the day = 800 at 20:00.
	require.Contains(t, res, "mintA")
	assert.EqualValues(t, 800, res["mintA"].SupplyFromBonding)
	assert.Equal(t, day.Add(20*time.Hour).Unix(), res["mintA"].Time.Unix())

	// mintB: its single same-day record.
	require.Contains(t, res, "mintB")
	assert.EqualValues(t, 10000, res["mintB"].SupplyFromBonding)

	// mintC has no record on `day`; mintD has none at all — both omitted.
	assert.NotContains(t, res, "mintC")
	assert.NotContains(t, res, "mintD")

	// Empty input yields an empty map, not an error.
	empty, err := s.GetReservesForDay(ctx, nil, queryT)
	require.NoError(t, err)
	assert.Empty(t, empty)
}

func testGetReservesInRange(t *testing.T, s reserve.Store) {
	var reserves []currency.ReserveRecord

	now := time.Now().UTC()
	mint := "test-mint"

	for i := 0; i < 100; i++ {
		reserves = append(reserves, currency.ReserveRecord{
			Mint:              mint,
			SupplyFromBonding: uint64(1000 + i),
			Time:              now.Add(time.Duration(i) * time.Hour),
		})
	}

	record, err := s.GetReserveAtTime(context.Background(), mint, reserves[0].Time)
	assert.Nil(t, record)
	assert.Equal(t, currency.ErrNotFound, err)

	for _, item := range reserves {
		itemCopy := item
		require.NoError(t, s.PutHistoricalReserve(context.Background(), &itemCopy))
	}

	result, err := s.GetReservesInRange(context.Background(), mint, query.IntervalRaw, reserves[0].Time, reserves[99].Time, query.Ascending)
	require.NoError(t, err)
	assert.Equal(t, len(result), 100)
	for i, item := range result {
		assert.Equal(t, reserves[i].Time.Unix(), item.Time.Unix())
		assert.EqualValues(t, reserves[i].SupplyFromBonding, item.SupplyFromBonding)
	}

	result, err = s.GetReservesInRange(context.Background(), mint, query.IntervalRaw, reserves[0].Time, reserves[49].Time, query.Ascending)
	require.NoError(t, err)
	assert.Equal(t, len(result), 50)
	for i, item := range result {
		assert.Equal(t, reserves[i].Time.Unix(), item.Time.Unix())
		assert.EqualValues(t, reserves[i].SupplyFromBonding, item.SupplyFromBonding)
	}

	result, err = s.GetReservesInRange(context.Background(), mint, query.IntervalRaw, reserves[0].Time, reserves[99].Time, query.Descending)
	require.NoError(t, err)
	assert.Equal(t, len(result), 100)
	for i, item := range result {
		assert.Equal(t, reserves[99-i].Time.Unix(), item.Time.Unix())
		assert.EqualValues(t, reserves[99-i].SupplyFromBonding, item.SupplyFromBonding)
	}

	for _, interval := range query.AllIntervals {
		_, err = s.GetReservesInRange(context.Background(), mint, interval, reserves[0].Time, reserves[99].Time, query.Ascending)
		require.NoError(t, err)
	}
}

func testLiveReserveRoundTrip(t *testing.T, s reserve.Store) {
	ctx := context.Background()
	mint := "live-reserve-mint"

	_, err := s.GetLiveReserve(ctx, mint)
	assert.Equal(t, currency.ErrNotFound, err)

	require.NoError(t, s.PutLiveReserve(ctx, &currency.ReserveRecord{
		Mint:              mint,
		SupplyFromBonding: 1000,
		Slot:              100,
		Time:              time.Now(),
	}))

	actual, err := s.GetLiveReserve(ctx, mint)
	require.NoError(t, err)
	assert.Equal(t, mint, actual.Mint)
	assert.EqualValues(t, 1000, actual.SupplyFromBonding)
	assert.EqualValues(t, 100, actual.Slot)

	// Higher slot advances.
	require.NoError(t, s.PutLiveReserve(ctx, &currency.ReserveRecord{
		Mint:              mint,
		SupplyFromBonding: 2000,
		Slot:              200,
		Time:              time.Now(),
	}))

	actual, err = s.GetLiveReserve(ctx, mint)
	require.NoError(t, err)
	assert.EqualValues(t, 2000, actual.SupplyFromBonding)
	assert.EqualValues(t, 200, actual.Slot)

	// Equal slot is stale.
	assert.Equal(t, currency.ErrStaleReserveState, s.PutLiveReserve(ctx, &currency.ReserveRecord{
		Mint:              mint,
		SupplyFromBonding: 3000,
		Slot:              200,
		Time:              time.Now(),
	}))

	// Lower slot is stale.
	assert.Equal(t, currency.ErrStaleReserveState, s.PutLiveReserve(ctx, &currency.ReserveRecord{
		Mint:              mint,
		SupplyFromBonding: 3000,
		Slot:              50,
		Time:              time.Now(),
	}))

	// Unchanged after stale attempts.
	actual, err = s.GetLiveReserve(ctx, mint)
	require.NoError(t, err)
	assert.EqualValues(t, 2000, actual.SupplyFromBonding)
	assert.EqualValues(t, 200, actual.Slot)
}

func testGetAllLiveReserves(t *testing.T, s reserve.Store) {
	ctx := context.Background()

	_, err := s.GetAllLiveReserves(ctx)
	assert.Equal(t, currency.ErrNotFound, err)

	require.NoError(t, s.PutLiveReserve(ctx, &currency.ReserveRecord{
		Mint:              "mint-all-live-1",
		SupplyFromBonding: 1000,
		Slot:              100,
		Time:              time.Now(),
	}))

	reserves, err := s.GetAllLiveReserves(ctx)
	require.NoError(t, err)
	assert.Len(t, reserves, 1)
	assert.EqualValues(t, 1000, reserves["mint-all-live-1"].SupplyFromBonding)
	assert.EqualValues(t, 100, reserves["mint-all-live-1"].Slot)

	require.NoError(t, s.PutLiveReserve(ctx, &currency.ReserveRecord{
		Mint:              "mint-all-live-2",
		SupplyFromBonding: 2000,
		Slot:              200,
		Time:              time.Now(),
	}))

	reserves, err = s.GetAllLiveReserves(ctx)
	require.NoError(t, err)
	assert.Len(t, reserves, 2)
	assert.EqualValues(t, 1000, reserves["mint-all-live-1"].SupplyFromBonding)
	assert.EqualValues(t, 2000, reserves["mint-all-live-2"].SupplyFromBonding)
	assert.EqualValues(t, 200, reserves["mint-all-live-2"].Slot)

	// Updating one mint is reflected.
	require.NoError(t, s.PutLiveReserve(ctx, &currency.ReserveRecord{
		Mint:              "mint-all-live-1",
		SupplyFromBonding: 1500,
		Slot:              150,
		Time:              time.Now(),
	}))

	reserves, err = s.GetAllLiveReserves(ctx)
	require.NoError(t, err)
	assert.Len(t, reserves, 2)
	assert.EqualValues(t, 1500, reserves["mint-all-live-1"].SupplyFromBonding)
	assert.EqualValues(t, 150, reserves["mint-all-live-1"].Slot)
}
