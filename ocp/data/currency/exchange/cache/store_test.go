package cache

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/code-payments/ocp-server/ocp/data/currency"
	"github.com/code-payments/ocp-server/ocp/data/currency/exchange/memory"
	"github.com/code-payments/ocp-server/ocp/data/currency/exchange/tests"
)

func TestExchange_CachedStore(t *testing.T) {
	testStore := New(memory.New()).(*store)
	teardown := func() {
		testStore.backing = memory.New()
		testStore.cache.Clear()
	}
	tests.RunStoreTests(t, testStore, teardown)
}

// TestExchange_CachedReadsServedFromCache verifies that once a rate is read, a
// later read in the same time bucket is served from the cache even after the
// backing store no longer holds it, while a read in a different bucket falls
// through to the (now empty) backing store.
func TestExchange_CachedReadsServedFromCache(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2021, 01, 29, 13, 0, 5, 0, time.UTC)

	s := New(memory.New()).(*store)
	require.NoError(t, s.PutExchangeRates(ctx, &currency.MultiRateRecord{
		Time:  now,
		Rates: map[string]float64{"usd": 0.000055, "cad": 0.00007},
	}))

	// Prime the cache with a single- and all-symbol read.
	single, err := s.GetExchangeRate(ctx, "usd", now)
	require.NoError(t, err)
	assert.EqualValues(t, 0.000055, single.Rate)

	all, err := s.GetAllExchangeRates(ctx, now)
	require.NoError(t, err)
	assert.Len(t, all.Rates, 2)

	// Drop the backing data. Reads in the same bucket are still served.
	s.backing = memory.New()

	single, err = s.GetExchangeRate(ctx, "usd", now)
	require.NoError(t, err)
	assert.EqualValues(t, 0.000055, single.Rate)

	all, err = s.GetAllExchangeRates(ctx, now)
	require.NoError(t, err)
	assert.Len(t, all.Rates, 2)

	// A read truncating to a different bucket misses the cache and falls
	// through to the empty backing store.
	otherBucket := now.Add(cacheBucket)
	_, err = s.GetExchangeRate(ctx, "usd", otherBucket)
	assert.Equal(t, currency.ErrNotFound, err)

	_, err = s.GetAllExchangeRates(ctx, otherBucket)
	assert.Equal(t, currency.ErrNotFound, err)
}
