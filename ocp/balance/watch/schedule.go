package watch

import (
	"time"

	watch_data "github.com/code-payments/ocp-server/ocp/data/balance/watch"
)

// Tiers paces an owner's re-evaluation by how close their watches are to
// their thresholds and by whether anything but the owner's own activity can
// move them (Outcome.Exposed). An owner's next evaluation is the soonest any
// of their watches asks for.
//
// The pacing exists for what an evaluation cannot be told about: launchpad
// currency prices and exchange rates moving. The owner's own ledger changes
// re-evaluate the owner on their own, so a watch that only those can move
// needs nothing but the Safe tier as a backstop, however thin its headroom.
type Tiers struct {
	// Thin is the interval for an exposed watch within ThinHeadroom of its
	// threshold, and for an exposed watch that is under it and could recover
	// on a price move.
	Thin time.Duration
	// Moderate is the interval for an exposed watch within ModerateHeadroom.
	Moderate time.Duration
	// Comfortable is the interval for any other exposed watch.
	Comfortable time.Duration
	// Safe is the interval for a watch that is not exposed: a backstop, not
	// a detector.
	Safe time.Duration

	ThinHeadroom     float64
	ModerateHeadroom float64
}

// DefaultTiers is the production pacing.
var DefaultTiers = Tiers{
	Thin:             time.Minute,
	Moderate:         30 * time.Minute,
	Comfortable:      4 * time.Hour,
	Safe:             24 * time.Hour,
	ThinHeadroom:     1.25,
	ModerateHeadroom: 2,
}

// NextEvaluation is when an owner with these outcomes should be evaluated
// again: the earliest of every watch's own ask, or now plus Safe for an owner
// with no watches (the schedule is kept for a put that may arrive at any
// time).
//
// A watch in StateBelow is asked for again at its grace deadline at the
// latest, so that StateBelowSustained is reached on time whatever else is
// going on.
func (t Tiers) NextEvaluation(outcomes []*Outcome, now time.Time) time.Time {
	next := now.Add(t.Safe)
	for _, outcome := range outcomes {
		if candidate := t.nextFor(outcome, now); candidate.Before(next) {
			next = candidate
		}
	}
	return next
}

func (t Tiers) nextFor(outcome *Outcome, now time.Time) time.Time {
	record := outcome.Record

	var interval time.Duration
	switch {
	case !outcome.Exposed:
		interval = t.Safe
	case record.State != watch_data.StateAbove:
		// Under the threshold, the question is recovery, which a price move
		// can bring at any time.
		interval = t.Thin
	case outcome.Headroom < t.ThinHeadroom:
		interval = t.Thin
	case outcome.Headroom < t.ModerateHeadroom:
		interval = t.Moderate
	default:
		interval = t.Comfortable
	}
	next := now.Add(interval)

	if record.State == watch_data.StateBelow {
		if deadline := record.StateSince.Add(record.Grace); deadline.Before(next) {
			next = deadline
		}
	}
	if next.Before(now) {
		next = now
	}
	return next
}
