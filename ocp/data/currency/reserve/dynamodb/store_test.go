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
	"github.com/code-payments/ocp-server/ocp/data/currency/reserve/tests"
)

const (
	historyTable = "reserve_history_test"
	liveTable    = "reserve_live_test"
)

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
	require.NoError(t, CreateTables(context.Background(), testEnv.Client, historyTable, liveTable))
	s := New(testEnv.Client, historyTable, liveTable).(*store)
	s.reset()
	return s
}

func TestReserve_DynamoDBStore(t *testing.T) {
	testStore := newTestStore(t)
	teardown := func() {
		testStore.reset()
	}
	tests.RunStoreTests(t, testStore, teardown)
}

// TestReserve_DynamoDBRollups verifies that a coarse interval reads from the
// matching rollup partition and that each bucket holds the close (the most
// recent sample within the bucket).
func TestReserve_DynamoDBRollups(t *testing.T) {
	s := newTestStore(t)
	defer s.reset()
	ctx := context.Background()
	mint := "rollup-mint"

	// Two samples in hour 10 (close = 200 at 10:45), one in hour 11.
	samples := []struct {
		at     time.Time
		supply uint64
	}{
		{time.Date(2021, 06, 01, 10, 05, 0, 0, time.UTC), 100},
		{time.Date(2021, 06, 01, 10, 45, 0, 0, time.UTC), 200},
		{time.Date(2021, 06, 01, 11, 30, 0, 0, time.UTC), 300},
	}
	for _, sample := range samples {
		require.NoError(t, s.PutHistoricalReserve(ctx, &currency.ReserveRecord{
			Mint:              mint,
			SupplyFromBonding: sample.supply,
			Time:              sample.at,
		}))
	}

	start := time.Date(2021, 06, 01, 10, 0, 0, 0, time.UTC)
	end := time.Date(2021, 06, 01, 12, 0, 0, 0, time.UTC)

	hourly, err := s.GetReservesInRange(ctx, mint, query.IntervalHour, start, end, query.Ascending)
	require.NoError(t, err)
	require.Len(t, hourly, 2)
	assert.Equal(t, samples[1].at.Unix(), hourly[0].Time.Unix())
	assert.EqualValues(t, 200, hourly[0].SupplyFromBonding)
	assert.Equal(t, samples[2].at.Unix(), hourly[1].Time.Unix())
	assert.EqualValues(t, 300, hourly[1].SupplyFromBonding)

	// Raw still has every sample.
	raw, err := s.GetReservesInRange(ctx, mint, query.IntervalRaw, start, end, query.Ascending)
	require.NoError(t, err)
	assert.Len(t, raw, len(samples))
}

func TestReserve_DynamoDBResolutionForInterval(t *testing.T) {
	assert.Equal(t, resRaw, resolutionForInterval(query.IntervalRaw))
	assert.Equal(t, resRaw, resolutionForInterval(query.IntervalSecond))
	assert.Equal(t, resRaw, resolutionForInterval(query.IntervalMinute))
	assert.Equal(t, resHour, resolutionForInterval(query.IntervalHour))
	assert.Equal(t, resDay, resolutionForInterval(query.IntervalDay))
	assert.Equal(t, resWeek, resolutionForInterval(query.IntervalWeek))
	assert.Equal(t, resMonth, resolutionForInterval(query.IntervalMonth))
}
