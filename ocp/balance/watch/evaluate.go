// Package watch decides balance watches: given what an owner holds, which of
// the watches on the owner are above their threshold, which have fallen
// under, and when the owner should be looked at again. It is pure — no store,
// no clock — so that the RPC that creates a watch and the worker that
// re-evaluates owners share one definition, and so that the rules can be
// tested as a table.
//
// The inputs are what one evaluation of an owner reads once, however many
// watches the owner has: the core mint value of the owner's holding in each
// mint (balance.Valuer.ValueHoldings), the live exchange rates, and the time.
// Each watch then reads its own slice of that — its mint filter, its currency
// — and is decided independently. The store's data package holds what is
// decided (watch_data.Record); this package holds how.
package watch

import (
	"math"
	"strings"
	"time"

	"github.com/pkg/errors"

	"github.com/code-payments/ocp-server/currency"
	"github.com/code-payments/ocp-server/ocp/balance"
	"github.com/code-payments/ocp-server/ocp/common"
	currency_util "github.com/code-payments/ocp-server/ocp/currency"
	watch_data "github.com/code-payments/ocp-server/ocp/data/balance/watch"
)

// ErrUnsupportedCurrency is returned when a fiat threshold's currency has no
// live exchange rate. A watch in that currency cannot be decided, and must
// not be treated as either above or below.
var ErrUnsupportedCurrency = errors.New("no exchange rate for currency")

// Outcome is one watch as an evaluation decided it.
type Outcome struct {
	// Record is the watch after the evaluation: State, StateSince and
	// LastValuation as decided, everything else as it came in. It is a copy;
	// the input record is not modified.
	Record *watch_data.Record

	// Previous is the state the watch was in before, or StateUnknown for a
	// watch being created.
	Previous watch_data.State

	// Transitioned reports whether State differs from Previous, which is what
	// earns an event. A watch being created never transitions: its first
	// state is where it starts, not something that happened to it.
	Transitioned bool

	// Headroom is the watched value as a multiple of the threshold: 1 is the
	// threshold exactly, below 1 is under. It paces the next evaluation.
	Headroom float64

	// Exposed reports whether something other than the owner's own activity
	// can move the watch under its threshold: a launchpad currency price, or
	// an exchange rate. A watch whose core mint holdings alone clear a core
	// mint or USD threshold is not exposed, and only the owner's own ledger
	// changes — which re-evaluate the owner on their own — can move it.
	Exposed bool
}

// Evaluate decides one watch at now, given the core mint value of the owner's
// holding in each mint and the live exchange rates (fiat per core mint unit,
// keyed by lower-case code). The record's current State and StateSince are
// the state the decision starts from; a record with StateUnknown is being
// created.
//
// The comparison is the Balance service's: a core mint threshold is met when
// the watched quarks reach it, exactly; a fiat threshold is met when the
// watched value at the current rate, plus half a minor unit of the currency
// for rounding, reaches the amount. The state machine is:
//
//	above threshold                          -> StateAbove
//	under, from StateAbove or being created  -> StateBelow, or StateBelowSustained when grace is zero
//	under, in StateBelow past the grace      -> StateBelowSustained, StateSince kept
//	under, otherwise                         -> unchanged
//
// StateSince is when the current state was entered; StateBelowSustained keeps
// the StateSince of the StateBelow it grew out of, so it says when the balance
// fell under, not when the grace ran out.
func Evaluate(record *watch_data.Record, coreMintValueByMint map[string]uint64, rates map[string]float64, now time.Time) (*Outcome, error) {
	decided := record.Clone()
	outcome := &Outcome{
		Record:   &decided,
		Previous: record.State,
	}

	watched := balance.SumCoreMintValue(coreMintValueByMint, record.Mints)
	coreOnly := coreMintOnly(coreMintValueByMint, record.Mints)

	var meets, coreOnlyMeets bool
	switch record.Threshold.Kind {
	case watch_data.ThresholdKindCoreMintQuarks:
		threshold := record.Threshold.Quarks
		meets = watched >= threshold
		coreOnlyMeets = coreOnly >= threshold
		outcome.Headroom = float64(watched) / float64(threshold)
		outcome.Exposed = !coreOnlyMeets
		decided.LastValuation = watch_data.Valuation{CoreMintValue: watched, EvaluatedAt: now}

	case watch_data.ThresholdKindFiat:
		code := strings.ToLower(string(record.Threshold.Currency))
		rate, ok := rates[code]
		if !ok {
			return nil, errors.Wrap(ErrUnsupportedCurrency, code)
		}
		threshold := record.Threshold.NativeAmount
		slack := fiatSlack(currency.Code(code))
		fiatValue := currency_util.CalculateFiatValueFromCoreMintQuarks(watched, rate)
		meets = fiatValue+slack >= threshold
		coreOnlyMeets = currency_util.CalculateFiatValueFromCoreMintQuarks(coreOnly, rate)+slack >= threshold
		outcome.Headroom = fiatValue / threshold
		// A rate other than the core mint's own moves on its own, so the watch
		// is exposed to it whatever the holdings are.
		outcome.Exposed = !coreOnlyMeets || code != coreMintCurrency
		decided.LastValuation = watch_data.Valuation{CoreMintValue: watched, FiatValue: &fiatValue, EvaluatedAt: now}

	default:
		return nil, errors.Errorf("unsupported threshold kind %d", record.Threshold.Kind)
	}

	decided.State, decided.StateSince = nextState(record.State, record.StateSince, record.Grace, meets, now)
	outcome.Transitioned = record.State != watch_data.StateUnknown && decided.State != record.State
	return outcome, nil
}

// nextState runs the state machine (see Evaluate) from the current state.
func nextState(current watch_data.State, since time.Time, grace time.Duration, meets bool, now time.Time) (watch_data.State, time.Time) {
	if meets {
		if current == watch_data.StateAbove {
			return current, since
		}
		return watch_data.StateAbove, now
	}

	switch current {
	case watch_data.StateBelow:
		if !now.Before(since.Add(grace)) {
			return watch_data.StateBelowSustained, since
		}
		return current, since
	case watch_data.StateBelowSustained:
		return current, since
	default: // StateAbove, or being created
		if grace <= 0 {
			return watch_data.StateBelowSustained, now
		}
		return watch_data.StateBelow, now
	}
}

// coreMintCurrency is the currency the core mint is pegged to. A fiat
// threshold in it is a core mint threshold in different units: its rate is
// fixed, so it is not exposed to exchange rate movement.
const coreMintCurrency = "usd"

// coreMintOnly is the owner's core mint holding as the watch sees it: the
// whole of it when the watch has no mint filter or the filter includes the
// core mint, nothing otherwise.
func coreMintOnly(coreMintValueByMint map[string]uint64, mints []string) uint64 {
	coreMint := common.CoreMintAccount.PublicKey().ToBase58()
	if len(mints) == 0 {
		return coreMintValueByMint[coreMint]
	}
	for _, mint := range mints {
		if mint == coreMint {
			return coreMintValueByMint[coreMint]
		}
	}
	return 0
}

// fiatSlack is half of one unit at the currency's last decimal place: 0.005
// for USD, 0.5 for a currency with no minor unit. A fiat value is a quoted
// rate applied to a quark total, so a comparison against it allows this much
// rounding, the same allowance every other fiat comparison in the protocol
// makes.
func fiatSlack(code currency.Code) float64 {
	return math.Pow10(-currency.GetDecimals(code)) / 2
}
