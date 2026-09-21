package watch

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/code-payments/ocp-server/currency"
	"github.com/code-payments/ocp-server/ocp/common"
	watch_data "github.com/code-payments/ocp-server/ocp/data/balance/watch"
)

var (
	coreMint  = common.CoreMintAccount.PublicKey().ToBase58()
	otherMint = "otherMint"

	rates = map[string]float64{"usd": 1, "eur": 0.5, "jpy": 150}
)

func TestEvaluate_CoreMintThreshold(t *testing.T) {
	now := time.Now()

	for _, tc := range []struct {
		name     string
		holdings map[string]uint64
		mints    []string
		state    watch_data.State
		exposed  bool
		headroom float64
	}{
		{"core mint alone clears", map[string]uint64{coreMint: 150}, nil, watch_data.StateAbove, false, 1.5},
		{"exactly at threshold", map[string]uint64{coreMint: 100}, nil, watch_data.StateAbove, false, 1},
		{"one quark short", map[string]uint64{coreMint: 99}, nil, watch_data.StateBelow, true, 0.99},
		{"cleared only with another mint", map[string]uint64{coreMint: 60, otherMint: 50}, nil, watch_data.StateAbove, true, 1.1},
		{"holds nothing", map[string]uint64{}, nil, watch_data.StateBelow, true, 0},
		{"filter excludes what clears it", map[string]uint64{coreMint: 60, otherMint: 50}, []string{otherMint}, watch_data.StateBelow, true, 0.5},
		{"filter includes the core mint", map[string]uint64{coreMint: 120, otherMint: 50}, []string{coreMint}, watch_data.StateAbove, false, 1.2},
		{"duplicate filter entries count once", map[string]uint64{otherMint: 60}, []string{otherMint, otherMint}, watch_data.StateBelow, true, 0.6},
	} {
		t.Run(tc.name, func(t *testing.T) {
			record := newRecord()
			record.Mints = tc.mints

			outcome, err := Evaluate(record, tc.holdings, rates, now)
			require.NoError(t, err)

			assert.Equal(t, tc.state, outcome.Record.State)
			assert.Equal(t, tc.exposed, outcome.Exposed)
			assert.InDelta(t, tc.headroom, outcome.Headroom, 1e-9)
			assert.Equal(t, watch_data.StateUnknown, outcome.Previous)
			assert.False(t, outcome.Transitioned, "creation is not a transition")
			assert.True(t, outcome.Record.StateSince.Equal(now))
			assert.True(t, outcome.Record.LastValuation.EvaluatedAt.Equal(now))
			assert.Nil(t, outcome.Record.LastValuation.FiatValue)

			// The input is untouched.
			assert.Equal(t, watch_data.StateUnknown, record.State)
		})
	}
}

func TestEvaluate_FiatThreshold(t *testing.T) {
	now := time.Now()
	unit := uint64(common.CoreMintQuarksPerUnit)

	for _, tc := range []struct {
		name      string
		currency  string
		amount    float64
		holdings  map[string]uint64
		state     watch_data.State
		exposed   bool
		fiatValue float64
	}{
		// 1 USD per core unit
		{"usd clears", "usd", 10, map[string]uint64{coreMint: 12 * unit}, watch_data.StateAbove, false, 12},
		{"usd within half a cent", "usd", 10, map[string]uint64{coreMint: 10*unit - 4_000}, watch_data.StateAbove, false, 9.996},
		{"usd beyond half a cent", "usd", 10, map[string]uint64{coreMint: 10*unit - 6_000}, watch_data.StateBelow, true, 9.994},
		{"usd cleared by another mint is exposed", "usd", 10, map[string]uint64{coreMint: 5 * unit, otherMint: 6 * unit}, watch_data.StateAbove, true, 11},
		// 0.5 EUR per core unit: always exposed to the rate
		{"eur clears", "eur", 5, map[string]uint64{coreMint: 12 * unit}, watch_data.StateAbove, true, 6},
		{"eur short", "eur", 5, map[string]uint64{coreMint: 9 * unit}, watch_data.StateBelow, true, 4.5},
		// JPY has no minor unit: half a yen of slack
		{"jpy within half a yen", "jpy", 1500, map[string]uint64{coreMint: 10*unit - 3_000}, watch_data.StateAbove, true, 1499.55},
		{"jpy beyond half a yen", "jpy", 1500, map[string]uint64{coreMint: 10*unit - 4_000}, watch_data.StateBelow, true, 1499.4},
	} {
		t.Run(tc.name, func(t *testing.T) {
			record := newRecord()
			record.Threshold = watch_data.Threshold{Kind: watch_data.ThresholdKindFiat, Currency: currency.Code(tc.currency), NativeAmount: tc.amount}

			outcome, err := Evaluate(record, tc.holdings, rates, now)
			require.NoError(t, err)

			assert.Equal(t, tc.state, outcome.Record.State)
			assert.Equal(t, tc.exposed, outcome.Exposed)
			require.NotNil(t, outcome.Record.LastValuation.FiatValue)
			assert.InDelta(t, tc.fiatValue, *outcome.Record.LastValuation.FiatValue, 1e-6)
			assert.InDelta(t, tc.fiatValue/tc.amount, outcome.Headroom, 1e-9)
		})
	}

	t.Run("currency is matched case-insensitively", func(t *testing.T) {
		record := newRecord()
		record.Threshold = watch_data.Threshold{Kind: watch_data.ThresholdKindFiat, Currency: "EUR", NativeAmount: 5}
		outcome, err := Evaluate(record, map[string]uint64{coreMint: 12 * unit}, rates, now)
		require.NoError(t, err)
		assert.Equal(t, watch_data.StateAbove, outcome.Record.State)
	})

	t.Run("unsupported currency", func(t *testing.T) {
		record := newRecord()
		record.Threshold = watch_data.Threshold{Kind: watch_data.ThresholdKindFiat, Currency: "xyz", NativeAmount: 5}
		_, err := Evaluate(record, map[string]uint64{coreMint: 12 * unit}, rates, now)
		assert.ErrorIs(t, err, ErrUnsupportedCurrency)
	})
}

func TestEvaluate_StateMachine(t *testing.T) {
	start := time.Now()
	grace := time.Hour
	above := map[string]uint64{coreMint: 150}
	below := map[string]uint64{coreMint: 50}

	// Created above, stays above: no transition and the since time holds.
	record := newRecord()
	record.Grace = grace
	outcome, err := Evaluate(record, above, rates, start)
	require.NoError(t, err)
	record = outcome.Record
	require.Equal(t, watch_data.StateAbove, record.State)

	outcome, err = Evaluate(record, above, rates, start.Add(time.Minute))
	require.NoError(t, err)
	assert.False(t, outcome.Transitioned)
	assert.True(t, outcome.Record.StateSince.Equal(start))

	// Falls under: ABOVE -> BELOW, since = now, grace starts.
	fell := start.Add(2 * time.Minute)
	outcome, err = Evaluate(record, below, rates, fell)
	require.NoError(t, err)
	record = outcome.Record
	assert.True(t, outcome.Transitioned)
	assert.Equal(t, watch_data.StateAbove, outcome.Previous)
	assert.Equal(t, watch_data.StateBelow, record.State)
	assert.True(t, record.StateSince.Equal(fell))

	// Still under, grace running: unchanged.
	outcome, err = Evaluate(record, below, rates, fell.Add(grace-time.Second))
	require.NoError(t, err)
	assert.False(t, outcome.Transitioned)
	assert.Equal(t, watch_data.StateBelow, outcome.Record.State)
	assert.True(t, outcome.Record.StateSince.Equal(fell))

	// Still under at the deadline: BELOW -> BELOW_SUSTAINED, since kept.
	outcome, err = Evaluate(record, below, rates, fell.Add(grace))
	require.NoError(t, err)
	record = outcome.Record
	assert.True(t, outcome.Transitioned)
	assert.Equal(t, watch_data.StateBelow, outcome.Previous)
	assert.Equal(t, watch_data.StateBelowSustained, record.State)
	assert.True(t, record.StateSince.Equal(fell), "sustained keeps the time the balance fell under")

	// Still under later: unchanged.
	outcome, err = Evaluate(record, below, rates, fell.Add(2*grace))
	require.NoError(t, err)
	assert.False(t, outcome.Transitioned)

	// Recovers: BELOW_SUSTAINED -> ABOVE, since = now.
	recovered := fell.Add(3 * grace)
	outcome, err = Evaluate(record, above, rates, recovered)
	require.NoError(t, err)
	record = outcome.Record
	assert.True(t, outcome.Transitioned)
	assert.Equal(t, watch_data.StateBelowSustained, outcome.Previous)
	assert.Equal(t, watch_data.StateAbove, record.State)
	assert.True(t, record.StateSince.Equal(recovered))

	// Falls and recovers within the grace: BELOW -> ABOVE.
	outcome, err = Evaluate(record, below, rates, recovered.Add(time.Minute))
	require.NoError(t, err)
	record = outcome.Record
	require.Equal(t, watch_data.StateBelow, record.State)
	outcome, err = Evaluate(record, above, rates, recovered.Add(2*time.Minute))
	require.NoError(t, err)
	assert.True(t, outcome.Transitioned)
	assert.Equal(t, watch_data.StateBelow, outcome.Previous)
	assert.Equal(t, watch_data.StateAbove, outcome.Record.State)

	t.Run("zero grace skips BELOW", func(t *testing.T) {
		record := newRecord()
		record.Grace = 0
		outcome, err := Evaluate(record, above, rates, start)
		require.NoError(t, err)
		outcome, err = Evaluate(outcome.Record, below, rates, start.Add(time.Minute))
		require.NoError(t, err)
		assert.True(t, outcome.Transitioned)
		assert.Equal(t, watch_data.StateBelowSustained, outcome.Record.State)

		fresh := newRecord()
		fresh.Grace = 0
		created, err := Evaluate(fresh, below, rates, start)
		require.NoError(t, err)
		assert.Equal(t, watch_data.StateBelowSustained, created.Record.State)
		assert.False(t, created.Transitioned)
	})

	t.Run("a replacement keeps agreeing history", func(t *testing.T) {
		// A watch in BELOW whose threshold is lowered to something the balance
		// still fails keeps its since time; one lowered under the balance
		// recovers.
		record := newRecord()
		record.Grace = grace
		outcome, err := Evaluate(record, above, rates, start)
		require.NoError(t, err)
		outcome, err = Evaluate(outcome.Record, below, rates, fell)
		require.NoError(t, err)
		record = outcome.Record

		lowered := record.Clone()
		lowered.Threshold.Quarks = 60
		outcome, err = Evaluate(&lowered, below, rates, fell.Add(time.Minute))
		require.NoError(t, err)
		assert.False(t, outcome.Transitioned)
		assert.True(t, outcome.Record.StateSince.Equal(fell))

		lowered.Threshold.Quarks = 40
		outcome, err = Evaluate(&lowered, below, rates, fell.Add(time.Minute))
		require.NoError(t, err)
		assert.True(t, outcome.Transitioned)
		assert.Equal(t, watch_data.StateAbove, outcome.Record.State)
	})
}

func TestTiers_NextEvaluation(t *testing.T) {
	now := time.Now()
	tiers := DefaultTiers

	outcome := func(state watch_data.State, headroom float64, exposed bool, since time.Time, grace time.Duration) *Outcome {
		record := newRecord()
		record.State = state
		record.StateSince = since
		record.Grace = grace
		return &Outcome{Record: record, Headroom: headroom, Exposed: exposed}
	}

	for _, tc := range []struct {
		name     string
		outcomes []*Outcome
		expected time.Time
	}{
		{"no watches", nil, now.Add(tiers.Safe)},
		{"not exposed, however thin", []*Outcome{outcome(watch_data.StateAbove, 1.01, false, now, 0)}, now.Add(tiers.Safe)},
		{"exposed and thin", []*Outcome{outcome(watch_data.StateAbove, 1.2, true, now, 0)}, now.Add(tiers.Thin)},
		{"exposed and moderate", []*Outcome{outcome(watch_data.StateAbove, 1.5, true, now, 0)}, now.Add(tiers.Moderate)},
		{"exposed and comfortable", []*Outcome{outcome(watch_data.StateAbove, 5, true, now, 0)}, now.Add(tiers.Comfortable)},
		{"exposed and under: recovery is thin", []*Outcome{outcome(watch_data.StateBelowSustained, 0.5, true, now, 0)}, now.Add(tiers.Thin)},
		{"not exposed and under: the ledger will say", []*Outcome{outcome(watch_data.StateBelowSustained, 0.5, false, now, 0)}, now.Add(tiers.Safe)},
		{"grace deadline caps a safe watch", []*Outcome{outcome(watch_data.StateBelow, 0.5, false, now.Add(-time.Hour), 2*time.Hour)}, now.Add(time.Hour)},
		{"grace deadline caps a thin watch", []*Outcome{outcome(watch_data.StateBelow, 0.5, true, now.Add(-time.Second), 10*time.Second)}, now.Add(9 * time.Second)},
		{"an overdue deadline is now", []*Outcome{outcome(watch_data.StateBelow, 0.5, false, now.Add(-3*time.Hour), time.Hour)}, now},
		{"the soonest watch wins", []*Outcome{
			outcome(watch_data.StateAbove, 5, true, now, 0),
			outcome(watch_data.StateAbove, 1.1, true, now, 0),
			outcome(watch_data.StateAbove, 1.01, false, now, 0),
		}, now.Add(tiers.Thin)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.True(t, tc.expected.Equal(tiers.NextEvaluation(tc.outcomes, now)), "expected %v, got %v", tc.expected, tiers.NextEvaluation(tc.outcomes, now))
		})
	}
}

// newRecord is a watch being created: a core mint threshold of 100 quarks, an
// hour of grace, no state yet.
func newRecord() *watch_data.Record {
	return &watch_data.Record{
		Subscriber: "subscriber",
		Owner:      "owner",
		Key:        []byte("key"),
		Threshold: watch_data.Threshold{
			Kind:   watch_data.ThresholdKindCoreMintQuarks,
			Quarks: 100,
		},
		Grace:      time.Hour,
		Generation: 1,
	}
}
