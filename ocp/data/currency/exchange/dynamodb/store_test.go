package dynamodb

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	dynamotest "github.com/code-payments/ocp-server/database/dynamodb/test"
	"github.com/code-payments/ocp-server/database/query"
	"github.com/code-payments/ocp-server/ocp/data/currency"
	"github.com/code-payments/ocp-server/ocp/data/currency/exchange/tests"
)

const exchangeRateTable = "exchange_rate_test"

var testEnv *dynamotest.TestEnv

func TestMain(m *testing.M) {
	log := zap.Must(zap.NewDevelopment())

	env, err := dynamotest.NewTestEnv()
	if err != nil {
		log.With(zap.Error(err)).Error("Error creating dynamodb test environment")
		os.Exit(1)
	}

	testEnv = env

	os.Exit(m.Run())
}

func newTestStore(t *testing.T) *store {
	require.NoError(t, CreateTables(context.Background(), testEnv.Client, exchangeRateTable))
	s := New(testEnv.Client, exchangeRateTable).(*store)
	s.reset()
	return s
}

func TestExchange_DynamoDBStore(t *testing.T) {
	testStore := newTestStore(t)
	teardown := func() {
		testStore.reset()
	}
	tests.RunStoreTests(t, testStore, teardown)
}

// TestExchange_DynamoDBRollups verifies that a coarse interval reads from the
// matching rollup partition and that each bucket holds the close (the most
// recent sample within the bucket).
func TestExchange_DynamoDBRollups(t *testing.T) {
	s := newTestStore(t)
	defer s.reset()
	ctx := context.Background()

	// Two samples in hour 10 (close = 2.0 at 10:45), one in hour 11.
	samples := []struct {
		at   time.Time
		rate float64
	}{
		{time.Date(2021, 06, 01, 10, 05, 0, 0, time.UTC), 1.0},
		{time.Date(2021, 06, 01, 10, 45, 0, 0, time.UTC), 2.0},
		{time.Date(2021, 06, 01, 11, 30, 0, 0, time.UTC), 3.0},
	}
	for _, sample := range samples {
		require.NoError(t, s.PutExchangeRates(ctx, &currency.MultiRateRecord{
			Time:  sample.at,
			Rates: map[string]float64{"usd": sample.rate},
		}))
	}

	start := time.Date(2021, 06, 01, 10, 0, 0, 0, time.UTC)
	end := time.Date(2021, 06, 01, 12, 0, 0, 0, time.UTC)

	// Hourly buckets: hour 10 -> close 2.0 @ 10:45, hour 11 -> 3.0 @ 11:30.
	hourly, err := s.GetExchangeRatesInRange(ctx, "usd", query.IntervalHour, start, end, query.Ascending)
	require.NoError(t, err)
	require.Len(t, hourly, 2)
	assert.Equal(t, samples[1].at.Unix(), hourly[0].Time.Unix())
	assert.EqualValues(t, 2.0, hourly[0].Rate)
	assert.Equal(t, samples[2].at.Unix(), hourly[1].Time.Unix())
	assert.EqualValues(t, 3.0, hourly[1].Rate)

	// The raw partition still holds every sample.
	raw, err := s.GetExchangeRatesInRange(ctx, "usd", query.IntervalRaw, start, end, query.Ascending)
	require.NoError(t, err)
	assert.Len(t, raw, len(samples))
}

// TestExchange_DynamoDBResolutionForInterval verifies the requested interval is
// honored directly, with sub-hour intervals served from raw.
func TestExchange_DynamoDBResolutionForInterval(t *testing.T) {
	assert.Equal(t, resRaw, resolutionForInterval(query.IntervalRaw))
	assert.Equal(t, resRaw, resolutionForInterval(query.IntervalSecond))
	assert.Equal(t, resRaw, resolutionForInterval(query.IntervalMinute))
	assert.Equal(t, resHour, resolutionForInterval(query.IntervalHour))
	assert.Equal(t, resDay, resolutionForInterval(query.IntervalDay))
	assert.Equal(t, resWeek, resolutionForInterval(query.IntervalWeek))
	assert.Equal(t, resMonth, resolutionForInterval(query.IntervalMonth))
}

// TestExchange_DynamoDBSymbolProjection verifies single-symbol reads return the
// requested symbol's rate and treat an absent symbol as not found.
func TestExchange_DynamoDBSymbolProjection(t *testing.T) {
	s := newTestStore(t)
	defer s.reset()
	ctx := context.Background()

	now := time.Date(2022, 01, 02, 12, 0, 0, 0, time.UTC)
	require.NoError(t, s.PutExchangeRates(ctx, &currency.MultiRateRecord{
		Time:  now,
		Rates: map[string]float64{"usd": 1.5, "cad": 2.5},
	}))

	// A present symbol returns its own rate.
	rec, err := s.GetExchangeRate(ctx, "cad", now)
	require.NoError(t, err)
	assert.EqualValues(t, 2.5, rec.Rate)
	assert.Equal(t, now.Unix(), rec.Time.Unix())

	// A symbol absent from the record is not found.
	_, err = s.GetExchangeRate(ctx, "eur", now)
	assert.Equal(t, currency.ErrNotFound, err)

	// The range query returns only the requested symbol.
	got, err := s.GetExchangeRatesInRange(ctx, "usd", query.IntervalRaw, now.Add(-time.Hour), now.Add(time.Hour), query.Ascending)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, "usd", got[0].Symbol)
	assert.EqualValues(t, 1.5, got[0].Rate)

	// A range query for an absent symbol finds nothing.
	_, err = s.GetExchangeRatesInRange(ctx, "eur", query.IntervalRaw, now.Add(-time.Hour), now.Add(time.Hour), query.Ascending)
	assert.Equal(t, currency.ErrNotFound, err)
}

// TestExchange_BucketStartWeek verifies weekly buckets start on Sunday.
func TestExchange_BucketStartWeek(t *testing.T) {
	sunday := time.Date(2021, 05, 30, 0, 0, 0, 0, time.UTC)

	// 2021-06-01 is a Tuesday; its week starts Sunday 2021-05-30.
	tuesday := time.Date(2021, 06, 01, 15, 30, 0, 0, time.UTC)
	assert.Equal(t, sunday, bucketStart(tuesday, resWeek))

	// A Sunday maps to itself (midnight).
	assert.Equal(t, sunday, bucketStart(sunday.Add(9*time.Hour), resWeek))
}
