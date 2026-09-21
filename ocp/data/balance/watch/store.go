package watch

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/pkg/errors"

	"github.com/code-payments/ocp-server/database/query"
)

var (
	ErrWatchNotFound = errors.New("watch not found")
	ErrWatchExists   = errors.New("watch already exists")
	ErrOwnerNotFound = errors.New("owner has no watches")

	// ErrStaleVersion is returned when a write's expected version is not the
	// record's current one: another writer got there first.
	ErrStaleVersion = errors.New("version is stale")

	// ErrStaleGeneration is returned by DeleteWatch when the watch exists at a
	// generation other than the one the caller last saw.
	ErrStaleGeneration = errors.New("generation is stale")

	ErrInvalidCursor = errors.New("cursor is invalid")

	// ErrTooManyTransitions is returned by CommitTransitions for more records
	// than one commit can carry.
	ErrTooManyTransitions = errors.New("too many transitions for one commit")
)

// MaxTransitionsPerCommit is the most watches CommitTransitions writes at
// once: DynamoDB's 100-item transaction limit, less the event that goes with
// them and a margin. A caller with more transitions for one owner commits
// them in chunks, one event each.
const MaxTransitionsPerCommit = 98

// Store persists watches, the per-owner evaluation schedule, and the
// per-subscriber event queues. See the package comment for how the three fit
// together.
//
// Every write that can race is conditional and monotonic: a record write
// carries the version the caller read and is refused with ErrStaleVersion if
// the record has moved, a schedule claim likewise, and a lease is taken only
// on an event nobody else holds. Callers retry from a fresh read. Nothing here
// takes a lock.
type Store interface {
	// GetWatch returns one watch.
	//
	// ErrWatchNotFound is returned if there is none.
	GetWatch(ctx context.Context, subscriber, owner string, key []byte) (*Record, error)

	// GetOwner returns an owner's schedule and every watch on the owner across
	// all subscribers, in one strongly consistent read: what an evaluation
	// needs. Watches are in no particular order.
	//
	// ErrOwnerNotFound is returned if no watch has ever been put on the owner.
	// An owner whose watches were all deleted still has a schedule and is
	// returned with no watches; the evaluation that finds it so is expected to
	// leave it scheduled far out rather than delete it, since a put may arrive
	// at any time.
	GetOwner(ctx context.Context, owner string) (*Schedule, []*Record, error)

	// GetWatchesBySubscriber pages through a subscriber's watches on one
	// owner, in key order, each page one strongly consistent read. The cursor
	// is the one the previous page returned, or query.EmptyCursor for the
	// first page; the returned cursor is empty on the last page. A subscriber
	// with none gets an empty result, not an error.
	//
	// ErrInvalidCursor is returned for a cursor this read did not issue.
	GetWatchesBySubscriber(ctx context.Context, subscriber, owner string, cursor query.Cursor, limit uint64) ([]*Record, query.Cursor, error)

	// ListWatches pages through every watch a subscriber holds, in an order
	// that is stable across pages but otherwise unspecified. The cursor is
	// the one the previous page returned, or query.EmptyCursor for the first
	// page; the returned cursor is empty on the last page.
	//
	// ErrInvalidCursor is returned for a cursor this listing did not issue.
	ListWatches(ctx context.Context, subscriber string, cursor query.Cursor, limit uint64) ([]*Record, query.Cursor, error)

	// PutWatch writes a watch and pulls its owner's schedule forward to now,
	// creating the schedule if this is the owner's first watch, atomically. A
	// record with Version 0 is created and must not exist (ErrWatchExists);
	// any other Version must be the stored one (ErrStaleVersion). The caller
	// sets Generation to one more than the record it read, or 1. An event,
	// when given, is written in the same transaction.
	//
	// The schedule is pulled forward because the new watch may need the owner
	// evaluated sooner than the owner's other watches did. Its version moves
	// too, so a worker mid-pass on the owner sees the put when it comes to
	// reschedule (see Schedule.Version).
	PutWatch(ctx context.Context, record *Record, event *Event) error

	// DeleteWatch removes a watch if it is at the given generation.
	//
	// ErrStaleGeneration is returned if the watch exists at another
	// generation. Deleting a watch that does not exist is a no-op.
	DeleteWatch(ctx context.Context, subscriber, owner string, key []byte, generation uint64) error

	// CommitTransitions writes the records — every one at the Version the
	// caller read, with State, StateSince and LastValuation as the evaluation
	// decided them — and the event announcing them, atomically. All records
	// belong to the event's subscriber and owner. Up to
	// MaxTransitionsPerCommit records go in one commit.
	//
	// ErrStaleVersion is returned, and nothing written, if any record has
	// moved; ErrTooManyTransitions for more records than one commit carries.
	CommitTransitions(ctx context.Context, records []*Record, event *Event) error

	// GetSchedulesDue returns up to limit schedules in shard due at or before
	// asOf, earliest first. The read may be eventually consistent: a schedule
	// just pulled forward may show up a moment late, and one just claimed may
	// still show up, which the claim's version check turns away.
	GetSchedulesDue(ctx context.Context, shard int, asOf time.Time, limit int) ([]*Schedule, error)

	// ClaimSchedule takes an owner for evaluation: it moves the schedule's
	// version and pushes NextEvaluationAt out to until, so that no other
	// worker claims the owner before then. The schedule is updated in place
	// with the version the claim produced. A worker that crashes mid-pass
	// leaves the owner to be claimed again at until.
	//
	// ErrStaleVersion is returned if the schedule is not at the version given:
	// another worker claimed it, or a put pulled it forward.
	ClaimSchedule(ctx context.Context, schedule *Schedule, until time.Time) error

	// SetSchedule writes NextEvaluationAt at the end of an evaluation, at the
	// version the claim produced, and updates the schedule in place with the
	// new version.
	//
	// ErrStaleVersion is returned if the schedule has moved since the claim.
	// The usual cause is a put on the owner, which left the owner due now;
	// the worker leaves it so.
	SetSchedule(ctx context.Context, schedule *Schedule) error

	// LeaseEvents takes up to limit of a subscriber's events that no one
	// holds a lease on as of asOf — never leased, or with a lease ending at
	// or before asOf — and leases them until the given time. Events about one
	// owner come back in the order they were issued.
	LeaseEvents(ctx context.Context, subscriber string, asOf, until time.Time, limit int) ([]*Event, error)

	// AckEvents removes events from a subscriber's queue. An ID that is not in
	// the queue is ignored.
	AckEvents(ctx context.Context, subscriber string, ids []uuid.UUID) error
}
