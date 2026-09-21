// Package watch defines the store behind the Balance service's watches: a
// subscriber's standing request to be told when an owner's balance crosses a
// threshold (see ocp.balance.v1.Balance, "Section: Watches").
//
// The store holds three kinds of thing, all keyed so that the owner is the
// unit of work:
//
//   - A watch (Record) belongs to a subscriber, names an owner and carries the
//     subscriber's own key for it. Every watch on an owner is a function of the
//     same inputs — the owner's ledger records, the reserve state of the mints
//     they hold, the exchange rates — so an evaluation reads those once and
//     decides every watch on the owner in a loop. State and grace are still
//     per watch: a $5 rule and a $100 rule on the same owner cross at different
//     times.
//
//   - A schedule (Schedule) is the owner's single entry in the evaluation
//     queue: when the owner is next due, and a version that a worker claims
//     before evaluating so that two workers never evaluate one owner at once.
//     There is one per owner with any watch, never one per watch, so an owner
//     with a thousand watches is one claim, one read and one pass.
//
//   - An event (Event) announces the transitions one evaluation produced for
//     one owner to one subscriber, and lives in that subscriber's queue until
//     acked. Delivery is lease-based so several processes of one subscriber can
//     drain the queue concurrently.
//
// Atomicity is per commit, never per owner: a watch's transition and the event
// that announces it land together (CommitTransitions), a schedule's claim is a
// compare-and-set on its version, and nothing spans more watches than one
// commit carries (MaxTransitionsPerCommit). An owner with more transitions
// than that is committed in several chunks, each consistent on its own; a
// crash between chunks leaves the later watches un-transitioned, which the
// next evaluation finds still crossing. That is what lets the number of
// watches on an owner be unbounded.
package watch

import (
	"time"

	"github.com/google/uuid"
	"github.com/pkg/errors"

	"github.com/code-payments/ocp-server/currency"
)

// State is where an owner's balance stands against a watch's threshold. The
// values are persisted; the enum is append-only.
type State uint8

const (
	StateUnknown State = iota
	// StateAbove means the balance meets the threshold.
	StateAbove
	// StateBelow means the balance fell under the threshold and the grace
	// period is running.
	StateBelow
	// StateBelowSustained means the balance has stayed under the threshold for
	// the whole grace period.
	StateBelowSustained
)

// ThresholdKind says what a threshold is denominated in. The values are
// persisted; the enum is append-only.
type ThresholdKind uint8

const (
	ThresholdKindUnknown ThresholdKind = iota
	// ThresholdKindCoreMintQuarks is a core mint value in quarks, compared
	// exactly with no exchange rate involved.
	ThresholdKindCoreMintQuarks
	// ThresholdKindFiat is a fiat value, compared against the balance valued
	// at the current exchange rate.
	ThresholdKindFiat
)

// Threshold is the balance a watched owner must hold to be StateAbove.
type Threshold struct {
	Kind ThresholdKind

	// Set for ThresholdKindCoreMintQuarks
	Quarks uint64

	// Set for ThresholdKindFiat
	Currency     currency.Code
	NativeAmount float64
}

// Valuation is what a watched balance was worth when a watch was evaluated,
// restricted to the watch's mints.
type Valuation struct {
	CoreMintValue uint64

	// The value in the threshold's currency, at the rate the evaluation used.
	// Set only for a fiat threshold.
	FiatValue *float64

	EvaluatedAt time.Time
}

// Record is one watch.
type Record struct {
	Subscriber string
	Owner      string
	Key        []byte

	Threshold Threshold
	Mints     []string
	Grace     time.Duration

	State      State
	StateSince time.Time

	// Generation counts the puts of this (subscriber, owner, key). It is the
	// fence DeleteWatch checks and is visible to the subscriber.
	Generation uint64

	LastValuation Valuation

	// Version is the store's own optimistic concurrency stamp, moved by every
	// write of the record — puts and transitions alike — and never shown to a
	// subscriber. A record that has not been written yet has Version 0.
	Version uint64
}

// Schedule is an owner's entry in the evaluation queue.
type Schedule struct {
	Owner string

	NextEvaluationAt time.Time

	// Version moves on every write of the schedule: a claim, a reschedule, and
	// every put of a watch on the owner (which pulls the owner forward). A
	// worker that claimed the schedule at one version and finds another at the
	// end of its pass knows a put landed meanwhile and leaves the owner due.
	Version uint64
}

// Transition is one watch's change of state, as an event carries it.
type Transition struct {
	// The watch after the transition
	Watch *Record

	PreviousState State
}

// Event announces the transitions one evaluation produced for one owner, to
// the subscriber whose watches they are.
type Event struct {
	// ID orders the subscriber's events and is the handle they are acked by.
	// It is issued by NewEventID, never chosen elsewhere.
	ID uuid.UUID

	Subscriber string
	Owner      string

	Transitions []Transition

	Timestamp time.Time

	// LeasedUntil is when the current lease on the event ends, or zero when it
	// is not leased. It is set by the store on LeaseEvents and ignored on
	// write.
	LeasedUntil time.Time
}

// MaxKeyLen bounds a watch's key, matching the proto.
const MaxKeyLen = 64

// Validate checks that the record can be written.
func (r *Record) Validate() error {
	if len(r.Subscriber) == 0 {
		return errors.New("subscriber is required")
	}
	if len(r.Owner) == 0 {
		return errors.New("owner is required")
	}
	if len(r.Key) == 0 {
		return errors.New("key is required")
	}
	if len(r.Key) > MaxKeyLen {
		return errors.Errorf("key exceeds %d bytes", MaxKeyLen)
	}
	if err := r.Threshold.Validate(); err != nil {
		return err
	}
	for _, mint := range r.Mints {
		if len(mint) == 0 {
			return errors.New("mint is empty")
		}
	}
	if r.Grace < 0 {
		return errors.New("grace is negative")
	}
	switch r.State {
	case StateAbove, StateBelow, StateBelowSustained:
	default:
		return errors.New("state is required")
	}
	if r.StateSince.IsZero() {
		return errors.New("state since is required")
	}
	if r.Generation == 0 {
		return errors.New("generation is required")
	}
	if r.LastValuation.EvaluatedAt.IsZero() {
		return errors.New("last valuation is required")
	}
	if (r.Threshold.Kind == ThresholdKindFiat) != (r.LastValuation.FiatValue != nil) {
		return errors.New("fiat value is set exactly for a fiat threshold")
	}
	return nil
}

// Validate checks that the threshold is one the store can hold.
func (t *Threshold) Validate() error {
	switch t.Kind {
	case ThresholdKindCoreMintQuarks:
		if t.Quarks == 0 {
			return errors.New("threshold quarks is required")
		}
		if len(t.Currency) != 0 || t.NativeAmount != 0 {
			return errors.New("core mint threshold carries no fiat amount")
		}
	case ThresholdKindFiat:
		if len(t.Currency) == 0 {
			return errors.New("threshold currency is required")
		}
		if !(t.NativeAmount > 0) {
			return errors.New("threshold native amount must be positive")
		}
		if t.Quarks != 0 {
			return errors.New("fiat threshold carries no quarks")
		}
	default:
		return errors.New("threshold kind is required")
	}
	return nil
}

// Validate checks that the schedule can be written.
func (s *Schedule) Validate() error {
	if len(s.Owner) == 0 {
		return errors.New("owner is required")
	}
	if s.NextEvaluationAt.IsZero() {
		return errors.New("next evaluation time is required")
	}
	return nil
}

// Validate checks that the event can be written: it is well formed and every
// transition it carries is a watch of its subscriber on its owner.
func (e *Event) Validate() error {
	if e.ID == uuid.Nil {
		return errors.New("id is required")
	}
	if len(e.Subscriber) == 0 {
		return errors.New("subscriber is required")
	}
	if len(e.Owner) == 0 {
		return errors.New("owner is required")
	}
	if len(e.Transitions) == 0 {
		return errors.New("transitions are required")
	}
	if e.Timestamp.IsZero() {
		return errors.New("timestamp is required")
	}
	for _, transition := range e.Transitions {
		if transition.Watch == nil {
			return errors.New("transition watch is required")
		}
		if err := transition.Watch.Validate(); err != nil {
			return err
		}
		if transition.Watch.Subscriber != e.Subscriber || transition.Watch.Owner != e.Owner {
			return errors.New("transition watch belongs to another subscriber or owner")
		}
		switch transition.PreviousState {
		case StateAbove, StateBelow, StateBelowSustained:
		default:
			return errors.New("transition previous state is required")
		}
	}
	return nil
}

// ValidateCommit checks the arguments of Store.CommitTransitions: at least one
// and at most MaxTransitionsPerCommit valid records, every one already written
// once (a transition is never a creation) and belonging to the event's
// subscriber and owner, and a valid event.
func ValidateCommit(records []*Record, event *Event) error {
	if len(records) == 0 {
		return errors.New("records are required")
	}
	if len(records) > MaxTransitionsPerCommit {
		return ErrTooManyTransitions
	}
	if event == nil {
		return errors.New("event is required")
	}
	if err := event.Validate(); err != nil {
		return err
	}
	seen := make(map[string]struct{}, len(records))
	for _, record := range records {
		if err := record.Validate(); err != nil {
			return err
		}
		if record.Version == 0 {
			return errors.New("transition of an unwritten watch")
		}
		if record.Subscriber != event.Subscriber || record.Owner != event.Owner {
			return errors.New("record belongs to another subscriber or owner than the event")
		}
		if _, ok := seen[string(record.Key)]; ok {
			return errors.New("duplicate record in commit")
		}
		seen[string(record.Key)] = struct{}{}
	}
	return nil
}

// Clone returns a deep copy of the record.
func (r *Record) Clone() Record {
	cloned := *r
	cloned.Key = append([]byte(nil), r.Key...)
	if r.Mints != nil {
		cloned.Mints = append([]string(nil), r.Mints...)
	}
	if r.LastValuation.FiatValue != nil {
		value := *r.LastValuation.FiatValue
		cloned.LastValuation.FiatValue = &value
	}
	return cloned
}

// Clone returns a copy of the schedule.
func (s *Schedule) Clone() Schedule {
	return *s
}

// Clone returns a deep copy of the event.
func (e *Event) Clone() Event {
	cloned := *e
	cloned.Transitions = make([]Transition, len(e.Transitions))
	for i, transition := range e.Transitions {
		watch := transition.Watch.Clone()
		cloned.Transitions[i] = Transition{
			Watch:         &watch,
			PreviousState: transition.PreviousState,
		}
	}
	return cloned
}

func (s State) String() string {
	switch s {
	case StateAbove:
		return "above"
	case StateBelow:
		return "below"
	case StateBelowSustained:
		return "below_sustained"
	}
	return "unknown"
}

func (k ThresholdKind) String() string {
	switch k {
	case ThresholdKindCoreMintQuarks:
		return "core_mint_quarks"
	case ThresholdKindFiat:
		return "fiat"
	}
	return "unknown"
}
