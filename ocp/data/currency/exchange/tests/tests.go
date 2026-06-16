// Package tests holds the shared conformance suite run against every
// exchange.Store implementation.
package tests

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/code-payments/ocp-server/database/query"
	"github.com/code-payments/ocp-server/ocp/data/currency"
	"github.com/code-payments/ocp-server/ocp/data/currency/exchange"
)

func RunStoreTests(t *testing.T, s exchange.Store, teardown func()) {
	for _, tf := range []func(t *testing.T, s exchange.Store){
		testExchangeRateRoundTrip,
		testGetExchangeRatesInRange,
	} {
		tf(t, s)
		teardown()
	}
}

func testExchangeRateRoundTrip(t *testing.T, s exchange.Store) {
	now := time.Date(2021, 01, 29, 13, 0, 5, 0, time.UTC)

	record, err := s.GetAllExchangeRates(context.Background(), now)
	assert.Nil(t, record)
	assert.Equal(t, currency.ErrNotFound, err)

	rates := map[string]float64{
		"usd": 0.000055,
		"cad": 0.00007,
	}
	require.NoError(t, s.PutExchangeRates(context.Background(), &currency.MultiRateRecord{
		Time:  now,
		Rates: rates,
	}))

	// Overwrite should fail
	assert.Equal(t, currency.ErrExists, s.PutExchangeRates(context.Background(), &currency.MultiRateRecord{
		Time:  now,
		Rates: rates,
	}))

	// Test GetExchangeRate(), it should return the USD record
	single, err := s.GetExchangeRate(context.Background(), "usd", now)
	require.NoError(t, err)
	assert.Equal(t, now.Unix(), single.Time.Unix())
	assert.EqualValues(t, rates["usd"], single.Rate)

	// Test GetAllExchangeRates(), it should return all recent rates
	record, err = s.GetAllExchangeRates(context.Background(), now)
	require.NoError(t, err)

	assert.Equal(t, now.Unix(), record.Time.Unix())
	assert.EqualValues(t, rates, record.Rates)

	// a later time the same day returns the most recent entry
	record, err = s.GetAllExchangeRates(context.Background(), time.Date(2021, 01, 29, 14, 0, 5, 0, time.UTC))
	require.NoError(t, err)

	assert.Equal(t, now.Unix(), record.Time.Unix())
	assert.EqualValues(t, rates, record.Rates)

	// a later day still returns the most recent entry at or before the timestamp
	tomorrow := time.Date(2021, 01, 30, 0, 0, 0, 0, time.UTC)
	record, err = s.GetAllExchangeRates(context.Background(), tomorrow)
	require.NoError(t, err)
	assert.Equal(t, now.Unix(), record.Time.Unix())
	assert.EqualValues(t, rates, record.Rates)

	// a time before any record exists is not found
	before := time.Date(2021, 01, 28, 0, 0, 0, 0, time.UTC)
	record, err = s.GetAllExchangeRates(context.Background(), before)
	assert.Nil(t, record)
	assert.Equal(t, currency.ErrNotFound, err)
}

func testGetExchangeRatesInRange(t *testing.T, s exchange.Store) {
	var rates []currency.MultiRateRecord

	now := time.Now().UTC()

	for i := 0; i < 100; i++ {
		rates = append(rates, currency.MultiRateRecord{
			Time: now.Add(time.Duration(i) * time.Hour),
			Rates: map[string]float64{
				"usd": (0.000058 + float64(i/10000)),
				"cad": (0.00008 + float64(i/10000)),
			},
		})
	}

	record, err := s.GetAllExchangeRates(context.Background(), rates[0].Time)
	assert.Nil(t, record)
	assert.Equal(t, currency.ErrNotFound, err)

	for _, item := range rates {
		require.NoError(t, s.PutExchangeRates(context.Background(), &item))
	}

	result, err := s.GetExchangeRatesInRange(context.Background(), "usd", query.IntervalRaw, rates[0].Time, rates[99].Time, query.Ascending)
	require.NoError(t, err)
	assert.Equal(t, len(result), 100)
	for i, item := range result {
		assert.Equal(t, rates[i].Time.Unix(), item.Time.Unix())
		assert.EqualValues(t, rates[i].Rates["usd"], item.Rate)
	}

	result, err = s.GetExchangeRatesInRange(context.Background(), "usd", query.IntervalRaw, rates[0].Time, rates[49].Time, query.Ascending)
	require.NoError(t, err)
	assert.Equal(t, len(result), 50)
	for i, item := range result {
		assert.Equal(t, rates[i].Time.Unix(), item.Time.Unix())
		assert.EqualValues(t, rates[i].Rates["usd"], item.Rate)
	}

	result, err = s.GetExchangeRatesInRange(context.Background(), "usd", query.IntervalRaw, rates[0].Time, rates[99].Time, query.Descending)
	require.NoError(t, err)
	assert.Equal(t, len(result), 100)
	for i, item := range result {
		assert.Equal(t, rates[99-i].Time.Unix(), item.Time.Unix())
		assert.EqualValues(t, rates[99-i].Rates["usd"], item.Rate)
	}

	_, err = s.GetExchangeRatesInRange(context.Background(), "usd", query.IntervalSecond, rates[0].Time, rates[99].Time, query.Ascending)
	require.NoError(t, err)
	_, err = s.GetExchangeRatesInRange(context.Background(), "usd", query.IntervalMinute, rates[0].Time, rates[99].Time, query.Ascending)
	require.NoError(t, err)
	_, err = s.GetExchangeRatesInRange(context.Background(), "usd", query.IntervalHour, rates[0].Time, rates[99].Time, query.Ascending)
	require.NoError(t, err)
	_, err = s.GetExchangeRatesInRange(context.Background(), "usd", query.IntervalDay, rates[0].Time, rates[99].Time, query.Ascending)
	require.NoError(t, err)
	_, err = s.GetExchangeRatesInRange(context.Background(), "usd", query.IntervalWeek, rates[0].Time, rates[99].Time, query.Ascending)
	require.NoError(t, err)
	_, err = s.GetExchangeRatesInRange(context.Background(), "usd", query.IntervalMonth, rates[0].Time, rates[99].Time, query.Ascending)
	require.NoError(t, err)
}
