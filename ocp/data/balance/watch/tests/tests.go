package tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/code-payments/ocp-server/database/query"
	"github.com/code-payments/ocp-server/ocp/data/balance/watch"
)

func RunTests(t *testing.T, s watch.Store, teardown func()) {
	for _, tf := range []func(t *testing.T, s watch.Store){
		testPutAndGetWatch,
		testPutWatchConcurrency,
		testPutWatchValidation,
		testGetOwner,
		testGetWatchesBySubscriber,
		testListWatches,
		testDeleteWatch,
		testCommitTransitions,
		testCommitTransitionsAtomicity,
		testSchedules,
		testScheduleClaimRaces,
		testEvents,
		testEventOrdering,
	} {
		tf(t, s)
		teardown()
	}
}

func testPutAndGetWatch(t *testing.T, s watch.Store) {
	t.Run("testPutAndGetWatch", func(t *testing.T) {
		ctx := context.Background()

		_, err := s.GetWatch(ctx, "subscriber", "owner", []byte("key"))
		assert.Equal(t, watch.ErrWatchNotFound, err)

		record := newRecord("subscriber", "owner", "key")
		require.NoError(t, s.PutWatch(ctx, record, nil))
		assert.EqualValues(t, 1, record.Version)

		actual, err := s.GetWatch(ctx, "subscriber", "owner", []byte("key"))
		require.NoError(t, err)
		assertEquivalentRecords(t, record, actual)

		// A fiat threshold round-trips with its currency and fiat valuation.
		fiat := newRecord("subscriber", "owner", "fiat")
		fiat.Threshold = watch.Threshold{Kind: watch.ThresholdKindFiat, Currency: "eur", NativeAmount: 12.5}
		fiat.Mints = []string{"mint1", "mint2"}
		fiat.Grace = time.Hour
		fiat.State = watch.StateBelow
		fiat.LastValuation.FiatValue = float64Ptr(11.25)
		require.NoError(t, s.PutWatch(ctx, fiat, nil))

		actual, err = s.GetWatch(ctx, "subscriber", "owner", []byte("fiat"))
		require.NoError(t, err)
		assertEquivalentRecords(t, fiat, actual)

		// Replacing at the current version moves the version and keeps the
		// caller's generation.
		replaced := actual.Clone()
		replaced.Generation++
		replaced.Threshold.Quarks = 0
		replaced.Threshold = watch.Threshold{Kind: watch.ThresholdKindCoreMintQuarks, Quarks: 500}
		replaced.LastValuation.FiatValue = nil
		replaced.State = watch.StateAbove
		require.NoError(t, s.PutWatch(ctx, &replaced, nil))
		assert.EqualValues(t, 2, replaced.Version)

		actual, err = s.GetWatch(ctx, "subscriber", "owner", []byte("fiat"))
		require.NoError(t, err)
		assertEquivalentRecords(t, &replaced, actual)
		assert.EqualValues(t, 2, actual.Generation)

		// Keys are bytes, compared exactly, and scoped to the subscriber.
		_, err = s.GetWatch(ctx, "subscriber", "owner", []byte("KEY"))
		assert.Equal(t, watch.ErrWatchNotFound, err)
		_, err = s.GetWatch(ctx, "other", "owner", []byte("key"))
		assert.Equal(t, watch.ErrWatchNotFound, err)
	})
}

func testPutWatchConcurrency(t *testing.T, s watch.Store) {
	t.Run("testPutWatchConcurrency", func(t *testing.T) {
		ctx := context.Background()

		record := newRecord("subscriber", "owner", "key")
		require.NoError(t, s.PutWatch(ctx, record, nil))

		// A second creation of the same key loses to the first.
		duplicate := newRecord("subscriber", "owner", "key")
		assert.Equal(t, watch.ErrWatchExists, s.PutWatch(ctx, duplicate, nil))

		// A replacement at a version other than the stored one is refused.
		stale := record.Clone()
		stale.Version = 7
		stale.Generation++
		assert.Equal(t, watch.ErrStaleVersion, s.PutWatch(ctx, &stale, nil))

		// A replacement of a watch that was deleted meanwhile is refused too:
		// the caller must re-read and create.
		require.NoError(t, s.DeleteWatch(ctx, "subscriber", "owner", []byte("key"), record.Generation))
		replacement := record.Clone()
		replacement.Generation++
		assert.Equal(t, watch.ErrStaleVersion, s.PutWatch(ctx, &replacement, nil))

		actual, err := s.GetWatch(ctx, "subscriber", "owner", []byte("key"))
		assert.Equal(t, watch.ErrWatchNotFound, err)
		assert.Nil(t, actual)
	})
}

func testPutWatchValidation(t *testing.T, s watch.Store) {
	t.Run("testPutWatchValidation", func(t *testing.T) {
		ctx := context.Background()

		for _, tc := range []struct {
			name   string
			mutate func(r *watch.Record)
		}{
			{"no subscriber", func(r *watch.Record) { r.Subscriber = "" }},
			{"no owner", func(r *watch.Record) { r.Owner = "" }},
			{"no key", func(r *watch.Record) { r.Key = nil }},
			{"long key", func(r *watch.Record) { r.Key = make([]byte, watch.MaxKeyLen+1) }},
			{"no threshold", func(r *watch.Record) { r.Threshold = watch.Threshold{} }},
			{"zero quarks", func(r *watch.Record) { r.Threshold.Quarks = 0 }},
			{"fiat without currency", func(r *watch.Record) {
				r.Threshold = watch.Threshold{Kind: watch.ThresholdKindFiat, NativeAmount: 1}
			}},
			{"fiat without amount", func(r *watch.Record) {
				r.Threshold = watch.Threshold{Kind: watch.ThresholdKindFiat, Currency: "usd"}
			}},
			{"fiat without fiat valuation", func(r *watch.Record) {
				r.Threshold = watch.Threshold{Kind: watch.ThresholdKindFiat, Currency: "usd", NativeAmount: 1}
			}},
			{"core mint with fiat valuation", func(r *watch.Record) { r.LastValuation.FiatValue = float64Ptr(1) }},
			{"negative grace", func(r *watch.Record) { r.Grace = -time.Second }},
			{"no state", func(r *watch.Record) { r.State = watch.StateUnknown }},
			{"no state since", func(r *watch.Record) { r.StateSince = time.Time{} }},
			{"no generation", func(r *watch.Record) { r.Generation = 0 }},
			{"no valuation", func(r *watch.Record) { r.LastValuation.EvaluatedAt = time.Time{} }},
		} {
			record := newRecord("subscriber", "owner", "key")
			tc.mutate(record)
			assert.Error(t, s.PutWatch(ctx, record, nil), tc.name)
		}

		_, err := s.GetWatch(ctx, "subscriber", "owner", []byte("key"))
		assert.Equal(t, watch.ErrWatchNotFound, err)
	})
}

func testGetOwner(t *testing.T, s watch.Store) {
	t.Run("testGetOwner", func(t *testing.T) {
		ctx := context.Background()

		_, _, err := s.GetOwner(ctx, "owner")
		assert.Equal(t, watch.ErrOwnerNotFound, err)

		before := time.Now()
		records := []*watch.Record{
			newRecord("subscriber1", "owner", "a"),
			newRecord("subscriber1", "owner", "b"),
			newRecord("subscriber2", "owner", "a"),
			newRecord("subscriber1", "other", "a"),
		}
		for _, record := range records {
			require.NoError(t, s.PutWatch(ctx, record, nil))
		}

		// Every subscriber's watches on the owner come back with the owner's
		// schedule, which the puts left due now.
		schedule, actual, err := s.GetOwner(ctx, "owner")
		require.NoError(t, err)
		assert.Equal(t, "owner", schedule.Owner)
		assert.False(t, schedule.NextEvaluationAt.Before(before))
		assert.False(t, schedule.NextEvaluationAt.After(time.Now()))
		assert.EqualValues(t, 3, schedule.Version, "one version per put")
		assertSameRecords(t, records[:3], actual)

		// An owner whose watches are all gone keeps its schedule.
		require.NoError(t, s.DeleteWatch(ctx, "subscriber1", "other", []byte("a"), 1))
		schedule, actual, err = s.GetOwner(ctx, "other")
		require.NoError(t, err)
		assert.Equal(t, "other", schedule.Owner)
		assert.Empty(t, actual)
	})
}

func testGetWatchesBySubscriber(t *testing.T, s watch.Store) {
	t.Run("testGetWatchesBySubscriber", func(t *testing.T) {
		ctx := context.Background()

		actual, next, err := s.GetWatchesBySubscriber(ctx, "subscriber1", "owner", query.EmptyCursor, 10)
		require.NoError(t, err)
		assert.Empty(t, actual)
		assert.Empty(t, next)

		// Seven watches for subscriber1 on the owner, keyed out of order, plus
		// noise on another subscriber and another owner.
		var records []*watch.Record
		for _, key := range []string{"e", "b", "g", "a", "f", "c", "d"} {
			record := newRecord("subscriber1", "owner", key)
			require.NoError(t, s.PutWatch(ctx, record, nil))
			records = append(records, record)
		}
		other := newRecord("subscriber2", "owner", "a")
		require.NoError(t, s.PutWatch(ctx, other, nil))
		require.NoError(t, s.PutWatch(ctx, newRecord("subscriber1", "other", "a"), nil))

		// Pages of three walk every watch exactly once, in key order, and end
		// with an empty cursor.
		var listed []*watch.Record
		cursor := query.EmptyCursor
		for page := 0; ; page++ {
			require.Less(t, page, 10, "paging did not terminate")
			actual, next, err := s.GetWatchesBySubscriber(ctx, "subscriber1", "owner", cursor, 3)
			require.NoError(t, err)
			listed = append(listed, actual...)
			if len(next) == 0 {
				break
			}
			assert.Len(t, actual, 3)
			cursor = next
		}
		assertSameRecords(t, records, listed)
		for i := 1; i < len(listed); i++ {
			assert.Less(t, string(listed[i-1].Key), string(listed[i].Key), "pages are in key order")
		}

		// A page that exactly holds the rest ends the paging.
		actual, next, err = s.GetWatchesBySubscriber(ctx, "subscriber1", "owner", query.EmptyCursor, 7)
		require.NoError(t, err)
		assert.Len(t, actual, 7)
		assert.Empty(t, next)

		actual, next, err = s.GetWatchesBySubscriber(ctx, "subscriber2", "owner", query.EmptyCursor, 10)
		require.NoError(t, err)
		assertSameRecords(t, []*watch.Record{other}, actual)
		assert.Empty(t, next)

		// A subscriber whose name prefixes another's never sees its watches.
		actual, _, err = s.GetWatchesBySubscriber(ctx, "subscriber", "owner", query.EmptyCursor, 10)
		require.NoError(t, err)
		assert.Empty(t, actual)

		_, _, err = s.GetWatchesBySubscriber(ctx, "subscriber1", "owner", query.Cursor("not hex"), 3)
		assert.Equal(t, watch.ErrInvalidCursor, err)
	})
}

func testListWatches(t *testing.T, s watch.Store) {
	t.Run("testListWatches", func(t *testing.T) {
		ctx := context.Background()

		actual, next, err := s.ListWatches(ctx, "subscriber", query.EmptyCursor, 10)
		require.NoError(t, err)
		assert.Empty(t, actual)
		assert.Empty(t, next)

		var records []*watch.Record
		for i := 0; i < 7; i++ {
			record := newRecord("subscriber", fmt.Sprintf("owner%d", i%3), fmt.Sprintf("key%d", i))
			require.NoError(t, s.PutWatch(ctx, record, nil))
			records = append(records, record)
		}
		require.NoError(t, s.PutWatch(ctx, newRecord("other", "owner0", "key0"), nil))

		// Pages of three walk every watch exactly once and end with an empty
		// cursor.
		var listed []*watch.Record
		cursor := query.EmptyCursor
		for page := 0; ; page++ {
			require.Less(t, page, 10, "listing did not terminate")
			actual, next, err := s.ListWatches(ctx, "subscriber", cursor, 3)
			require.NoError(t, err)
			listed = append(listed, actual...)
			if len(next) == 0 {
				break
			}
			assert.Len(t, actual, 3)
			cursor = next
		}
		assertSameRecords(t, records, listed)

		// A page that exactly holds the rest ends the listing.
		actual, next, err = s.ListWatches(ctx, "subscriber", query.EmptyCursor, 7)
		require.NoError(t, err)
		assert.Len(t, actual, 7)
		assert.Empty(t, next)

		_, _, err = s.ListWatches(ctx, "subscriber", query.Cursor("garbage"), 3)
		assert.Equal(t, watch.ErrInvalidCursor, err)
	})
}

func testDeleteWatch(t *testing.T, s watch.Store) {
	t.Run("testDeleteWatch", func(t *testing.T) {
		ctx := context.Background()

		require.NoError(t, s.DeleteWatch(ctx, "subscriber", "owner", []byte("key"), 1))

		record := newRecord("subscriber", "owner", "key")
		require.NoError(t, s.PutWatch(ctx, record, nil))

		replaced := record.Clone()
		replaced.Generation = 2
		require.NoError(t, s.PutWatch(ctx, &replaced, nil))

		// A delete carrying the generation of an earlier put is refused: the
		// watch was re-armed after that generation was read.
		assert.Equal(t, watch.ErrStaleGeneration, s.DeleteWatch(ctx, "subscriber", "owner", []byte("key"), 1))
		_, err := s.GetWatch(ctx, "subscriber", "owner", []byte("key"))
		require.NoError(t, err)

		require.NoError(t, s.DeleteWatch(ctx, "subscriber", "owner", []byte("key"), 2))
		_, err = s.GetWatch(ctx, "subscriber", "owner", []byte("key"))
		assert.Equal(t, watch.ErrWatchNotFound, err)

		// Deleting again is a no-op.
		require.NoError(t, s.DeleteWatch(ctx, "subscriber", "owner", []byte("key"), 2))

		// The key can be created again, at generation 1 and version 1.
		recreated := newRecord("subscriber", "owner", "key")
		require.NoError(t, s.PutWatch(ctx, recreated, nil))
		assert.EqualValues(t, 1, recreated.Version)
	})
}

func testCommitTransitions(t *testing.T, s watch.Store) {
	t.Run("testCommitTransitions", func(t *testing.T) {
		ctx := context.Background()

		a := newRecord("subscriber", "owner", "a")
		b := newRecord("subscriber", "owner", "b")
		untouched := newRecord("subscriber", "owner", "c")
		for _, record := range []*watch.Record{a, b, untouched} {
			require.NoError(t, s.PutWatch(ctx, record, nil))
		}

		now := time.Now()
		a.State, a.StateSince = watch.StateBelow, now
		a.LastValuation = watch.Valuation{CoreMintValue: 50, EvaluatedAt: now}
		b.State, b.StateSince = watch.StateBelowSustained, now
		b.LastValuation = watch.Valuation{CoreMintValue: 10, EvaluatedAt: now}

		event := newEvent("subscriber", "owner", now,
			watch.Transition{Watch: a, PreviousState: watch.StateAbove},
			watch.Transition{Watch: b, PreviousState: watch.StateBelow},
		)
		require.NoError(t, s.CommitTransitions(ctx, []*watch.Record{a, b}, event))
		assert.EqualValues(t, 2, a.Version)
		assert.EqualValues(t, 2, b.Version)

		for _, expected := range []*watch.Record{a, b, untouched} {
			actual, err := s.GetWatch(ctx, "subscriber", "owner", expected.Key)
			require.NoError(t, err)
			assertEquivalentRecords(t, expected, actual)
		}

		// The event is in the subscriber's queue with the transitions as
		// committed, and nowhere else.
		events, err := s.LeaseEvents(ctx, "subscriber", now, now.Add(time.Minute), 10)
		require.NoError(t, err)
		require.Len(t, events, 1)
		assertEquivalentEvents(t, event, events[0])
		assert.True(t, events[0].LeasedUntil.Equal(now.Add(time.Minute)))

		events, err = s.LeaseEvents(ctx, "other", now, now.Add(time.Minute), 10)
		require.NoError(t, err)
		assert.Empty(t, events)

		// Argument checks
		assert.Error(t, s.CommitTransitions(ctx, nil, event))
		assert.Error(t, s.CommitTransitions(ctx, []*watch.Record{a}, nil))
		tooMany := make([]*watch.Record, watch.MaxTransitionsPerCommit+1)
		for i := range tooMany {
			tooMany[i] = a
		}
		assert.Equal(t, watch.ErrTooManyTransitions, s.CommitTransitions(ctx, tooMany, event))
		foreign := newRecord("other", "owner", "a")
		foreign.Version = 1
		assert.Error(t, s.CommitTransitions(ctx, []*watch.Record{foreign}, event))
	})
}

func testCommitTransitionsAtomicity(t *testing.T, s watch.Store) {
	t.Run("testCommitTransitionsAtomicity", func(t *testing.T) {
		ctx := context.Background()

		a := newRecord("subscriber", "owner", "a")
		b := newRecord("subscriber", "owner", "b")
		for _, record := range []*watch.Record{a, b} {
			require.NoError(t, s.PutWatch(ctx, record, nil))
		}
		original := a.Clone()

		// Someone replaces b after the evaluation read it.
		replaced := b.Clone()
		replaced.Generation++
		require.NoError(t, s.PutWatch(ctx, &replaced, nil))

		now := time.Now()
		a.State, a.StateSince = watch.StateBelow, now
		b.State, b.StateSince = watch.StateBelow, now
		event := newEvent("subscriber", "owner", now,
			watch.Transition{Watch: a, PreviousState: watch.StateAbove},
			watch.Transition{Watch: b, PreviousState: watch.StateAbove},
		)
		assert.Equal(t, watch.ErrStaleVersion, s.CommitTransitions(ctx, []*watch.Record{a, b}, event))

		// Nothing moved: not a, whose version was current, and no event.
		actual, err := s.GetWatch(ctx, "subscriber", "owner", []byte("a"))
		require.NoError(t, err)
		assertEquivalentRecords(t, &original, actual)
		assert.EqualValues(t, 1, a.Version)

		actual, err = s.GetWatch(ctx, "subscriber", "owner", []byte("b"))
		require.NoError(t, err)
		assertEquivalentRecords(t, &replaced, actual)

		events, err := s.LeaseEvents(ctx, "subscriber", now, now.Add(time.Minute), 10)
		require.NoError(t, err)
		assert.Empty(t, events)

		// A deleted watch cannot be transitioned either.
		require.NoError(t, s.DeleteWatch(ctx, "subscriber", "owner", []byte("a"), a.Generation))
		event = newEvent("subscriber", "owner", now, watch.Transition{Watch: a, PreviousState: watch.StateAbove})
		assert.Equal(t, watch.ErrStaleVersion, s.CommitTransitions(ctx, []*watch.Record{a}, event))
	})
}

func testSchedules(t *testing.T, s watch.Store) {
	t.Run("testSchedules", func(t *testing.T) {
		ctx := context.Background()

		// Owners spread over shards; pick three in one shard and one outside.
		var inShard []string
		var outOfShard string
		shard := watch.ScheduleShard("owner0")
		for i := 0; len(inShard) < 3 || outOfShard == ""; i++ {
			owner := fmt.Sprintf("owner%d", i)
			if watch.ScheduleShard(owner) == shard {
				if len(inShard) < 3 {
					inShard = append(inShard, owner)
				}
			} else if outOfShard == "" {
				outOfShard = owner
			}
		}

		start := time.Now()
		for _, owner := range append(inShard, outOfShard) {
			require.NoError(t, s.PutWatch(ctx, newRecord("subscriber", owner, "key"), nil))
		}

		// Everything is due now, in the shard it hashes to.
		due, err := s.GetSchedulesDue(ctx, shard, time.Now(), 10)
		require.NoError(t, err)
		assertOwners(t, inShard, due)
		for _, schedule := range due {
			assert.False(t, schedule.NextEvaluationAt.Before(start))
		}

		due, err = s.GetSchedulesDue(ctx, shard, start.Add(-time.Second), 10)
		require.NoError(t, err)
		assert.Empty(t, due, "nothing was due before the puts")

		due, err = s.GetSchedulesDue(ctx, shard, time.Now(), 2)
		require.NoError(t, err)
		assert.Len(t, due, 2, "limit is honored")

		// Claiming an owner pushes it out until the claim ends.
		schedule, _, err := s.GetOwner(ctx, inShard[0])
		require.NoError(t, err)
		claimVersion := schedule.Version
		until := time.Now().Add(time.Minute)
		require.NoError(t, s.ClaimSchedule(ctx, schedule, until))
		assert.Equal(t, claimVersion+1, schedule.Version)
		assert.True(t, schedule.NextEvaluationAt.Equal(until))

		due, err = s.GetSchedulesDue(ctx, shard, time.Now(), 10)
		require.NoError(t, err)
		assertOwners(t, inShard[1:], due)

		due, err = s.GetSchedulesDue(ctx, shard, until, 10)
		require.NoError(t, err)
		assertOwners(t, inShard, due)
		assert.Equal(t, inShard[0], due[len(due)-1].Owner, "earliest first")

		// Rescheduling at the end of the pass lands at the claim's version.
		schedule.NextEvaluationAt = time.Now().Add(time.Hour)
		require.NoError(t, s.SetSchedule(ctx, schedule))
		assert.Equal(t, claimVersion+2, schedule.Version)

		actual, _, err := s.GetOwner(ctx, inShard[0])
		require.NoError(t, err)
		assert.True(t, actual.NextEvaluationAt.Equal(schedule.NextEvaluationAt))
		assert.Equal(t, schedule.Version, actual.Version)

		due, err = s.GetSchedulesDue(ctx, shard, time.Now().Add(30*time.Minute), 10)
		require.NoError(t, err)
		assertOwners(t, inShard[1:], due)

		// Validation
		assert.Error(t, s.SetSchedule(ctx, &watch.Schedule{Owner: inShard[0], Version: schedule.Version}))
		assert.Error(t, s.ClaimSchedule(ctx, &watch.Schedule{}, until))
	})
}

func testScheduleClaimRaces(t *testing.T, s watch.Store) {
	t.Run("testScheduleClaimRaces", func(t *testing.T) {
		ctx := context.Background()

		require.NoError(t, s.PutWatch(ctx, newRecord("subscriber", "owner", "key"), nil))

		// Two workers read the same schedule; one claim wins.
		first, _, err := s.GetOwner(ctx, "owner")
		require.NoError(t, err)
		second := first.Clone()

		until := time.Now().Add(time.Minute)
		require.NoError(t, s.ClaimSchedule(ctx, first, until))
		assert.Equal(t, watch.ErrStaleVersion, s.ClaimSchedule(ctx, &second, until))

		// A put on the owner during the pass pulls the owner forward and
		// moves the version, so the worker's reschedule is refused and the
		// owner stays due.
		require.NoError(t, s.PutWatch(ctx, newRecord("subscriber", "owner", "another"), nil))

		first.NextEvaluationAt = time.Now().Add(time.Hour)
		assert.Equal(t, watch.ErrStaleVersion, s.SetSchedule(ctx, first))

		actual, _, err := s.GetOwner(ctx, "owner")
		require.NoError(t, err)
		assert.False(t, actual.NextEvaluationAt.After(time.Now()))
		assert.Equal(t, first.Version+1, actual.Version)

		// Nobody can claim or reschedule an owner that has no schedule.
		assert.Equal(t, watch.ErrStaleVersion, s.ClaimSchedule(ctx, &watch.Schedule{Owner: "nobody", NextEvaluationAt: until}, until))
		assert.Equal(t, watch.ErrStaleVersion, s.SetSchedule(ctx, &watch.Schedule{Owner: "nobody", NextEvaluationAt: until}))
	})
}

func testEvents(t *testing.T, s watch.Store) {
	t.Run("testEvents", func(t *testing.T) {
		ctx := context.Background()

		now := time.Now()
		events, err := s.LeaseEvents(ctx, "subscriber", now, now.Add(time.Minute), 10)
		require.NoError(t, err)
		assert.Empty(t, events)

		// A put can carry an event.
		record := newRecord("subscriber", "owner", "key")
		record.State = watch.StateBelow
		putEvent := newEvent("subscriber", "owner", now, watch.Transition{Watch: record, PreviousState: watch.StateAbove})
		require.NoError(t, s.PutWatch(ctx, record, putEvent))

		other := newRecord("subscriber", "other", "key")
		require.NoError(t, s.PutWatch(ctx, other, nil))
		other.State = watch.StateBelow
		commitEvent := newEvent("subscriber", "other", now, watch.Transition{Watch: other, PreviousState: watch.StateAbove})
		require.NoError(t, s.CommitTransitions(ctx, []*watch.Record{other}, commitEvent))

		// A lease hands out each event once until it lapses.
		leaseEnd := now.Add(time.Minute)
		events, err = s.LeaseEvents(ctx, "subscriber", now, leaseEnd, 1)
		require.NoError(t, err)
		require.Len(t, events, 1)
		firstID := events[0].ID

		events, err = s.LeaseEvents(ctx, "subscriber", now, leaseEnd, 10)
		require.NoError(t, err)
		require.Len(t, events, 1)
		assert.NotEqual(t, firstID, events[0].ID)

		events, err = s.LeaseEvents(ctx, "subscriber", now, leaseEnd, 10)
		require.NoError(t, err)
		assert.Empty(t, events, "both are leased")

		events, err = s.LeaseEvents(ctx, "subscriber", leaseEnd, leaseEnd.Add(time.Minute), 10)
		require.NoError(t, err)
		require.Len(t, events, 2, "leases lapsed")
		assertSameEvents(t, []*watch.Event{putEvent, commitEvent}, events)

		// An ack removes an event for good; unknown IDs are ignored.
		require.NoError(t, s.AckEvents(ctx, "subscriber", []uuid.UUID{putEvent.ID, uuid.New()}))
		events, err = s.LeaseEvents(ctx, "subscriber", leaseEnd.Add(time.Hour), leaseEnd.Add(2*time.Hour), 10)
		require.NoError(t, err)
		assertSameEvents(t, []*watch.Event{commitEvent}, events)

		// An ack by another subscriber does nothing.
		require.NoError(t, s.AckEvents(ctx, "other", []uuid.UUID{commitEvent.ID}))
		events, err = s.LeaseEvents(ctx, "subscriber", leaseEnd.Add(3*time.Hour), leaseEnd.Add(4*time.Hour), 10)
		require.NoError(t, err)
		assertSameEvents(t, []*watch.Event{commitEvent}, events)

		require.NoError(t, s.AckEvents(ctx, "subscriber", []uuid.UUID{commitEvent.ID}))
		events, err = s.LeaseEvents(ctx, "subscriber", leaseEnd.Add(5*time.Hour), leaseEnd.Add(6*time.Hour), 10)
		require.NoError(t, err)
		assert.Empty(t, events)
	})
}

func testEventOrdering(t *testing.T, s watch.Store) {
	t.Run("testEventOrdering", func(t *testing.T) {
		ctx := context.Background()

		record := newRecord("subscriber", "owner", "key")
		require.NoError(t, s.PutWatch(ctx, record, nil))

		// A run of transitions on one owner is leased in the order it happened,
		// whatever the page size.
		var expected []*watch.Event
		states := []watch.State{watch.StateBelow, watch.StateBelowSustained, watch.StateAbove, watch.StateBelow, watch.StateAbove}
		for i, state := range states {
			previous := record.State
			record.State = state
			record.StateSince = time.Now()
			event := newEvent("subscriber", "owner", record.StateSince, watch.Transition{Watch: record, PreviousState: previous})
			require.NoError(t, s.CommitTransitions(ctx, []*watch.Record{record}, event), i)
			expected = append(expected, event)
		}

		now := time.Now()
		var leased []*watch.Event
		for len(leased) < len(expected) {
			events, err := s.LeaseEvents(ctx, "subscriber", now, now.Add(time.Minute), 2)
			require.NoError(t, err)
			require.NotEmpty(t, events)
			leased = append(leased, events...)
		}
		require.Len(t, leased, len(expected))
		for i := range expected {
			assertEquivalentEvents(t, expected[i], leased[i])
		}
	})
}

func newRecord(subscriber, owner, key string) *watch.Record {
	now := time.Now()
	return &watch.Record{
		Subscriber: subscriber,
		Owner:      owner,
		Key:        []byte(key),
		Threshold: watch.Threshold{
			Kind:   watch.ThresholdKindCoreMintQuarks,
			Quarks: 100,
		},
		State:      watch.StateAbove,
		StateSince: now,
		Generation: 1,
		LastValuation: watch.Valuation{
			CoreMintValue: 150,
			EvaluatedAt:   now,
		},
	}
}

// newEvent builds an event carrying a snapshot of each transition's watch as
// it is now, since the store writes transitions exactly as given.
func newEvent(subscriber, owner string, at time.Time, transitions ...watch.Transition) *watch.Event {
	id, err := watch.NewEventID(owner)
	if err != nil {
		panic(err)
	}
	for i := range transitions {
		snapshot := transitions[i].Watch.Clone()
		transitions[i].Watch = &snapshot
	}
	return &watch.Event{
		ID:          id,
		Subscriber:  subscriber,
		Owner:       owner,
		Transitions: transitions,
		Timestamp:   at,
	}
}

func assertEquivalentRecords(t *testing.T, expected, actual *watch.Record) {
	assert.Equal(t, expected.Subscriber, actual.Subscriber)
	assert.Equal(t, expected.Owner, actual.Owner)
	assert.Equal(t, expected.Key, actual.Key)
	assert.Equal(t, expected.Threshold, actual.Threshold)
	assert.ElementsMatch(t, expected.Mints, actual.Mints)
	assert.Equal(t, expected.Grace, actual.Grace)
	assert.Equal(t, expected.State, actual.State)
	assert.True(t, expected.StateSince.Equal(actual.StateSince), "state since %v != %v", expected.StateSince, actual.StateSince)
	assert.Equal(t, expected.Generation, actual.Generation)
	assert.Equal(t, expected.LastValuation.CoreMintValue, actual.LastValuation.CoreMintValue)
	assert.Equal(t, expected.LastValuation.FiatValue, actual.LastValuation.FiatValue)
	assert.True(t, expected.LastValuation.EvaluatedAt.Equal(actual.LastValuation.EvaluatedAt))
	assert.Equal(t, expected.Version, actual.Version)
}

func assertSameRecords(t *testing.T, expected, actual []*watch.Record) {
	require.Len(t, actual, len(expected))
	for _, e := range expected {
		var found bool
		for _, a := range actual {
			if a.Subscriber == e.Subscriber && a.Owner == e.Owner && string(a.Key) == string(e.Key) {
				assertEquivalentRecords(t, e, a)
				found = true
				break
			}
		}
		assert.True(t, found, "missing watch %s/%s/%s", e.Subscriber, e.Owner, e.Key)
	}
}

func assertEquivalentEvents(t *testing.T, expected, actual *watch.Event) {
	assert.Equal(t, expected.ID, actual.ID)
	assert.Equal(t, expected.Subscriber, actual.Subscriber)
	assert.Equal(t, expected.Owner, actual.Owner)
	assert.True(t, expected.Timestamp.Equal(actual.Timestamp))
	require.Len(t, actual.Transitions, len(expected.Transitions))
	for i := range expected.Transitions {
		assert.Equal(t, expected.Transitions[i].PreviousState, actual.Transitions[i].PreviousState)
		assertEquivalentRecords(t, expected.Transitions[i].Watch, actual.Transitions[i].Watch)
	}
}

func assertSameEvents(t *testing.T, expected, actual []*watch.Event) {
	require.Len(t, actual, len(expected))
	for _, e := range expected {
		var found bool
		for _, a := range actual {
			if a.ID == e.ID {
				assertEquivalentEvents(t, e, a)
				found = true
				break
			}
		}
		assert.True(t, found, "missing event %s", e.ID)
	}
}

func assertOwners(t *testing.T, expected []string, schedules []*watch.Schedule) {
	owners := make([]string, 0, len(schedules))
	for _, schedule := range schedules {
		owners = append(owners, schedule.Owner)
	}
	assert.ElementsMatch(t, expected, owners)
}

func float64Ptr(value float64) *float64 {
	return &value
}
