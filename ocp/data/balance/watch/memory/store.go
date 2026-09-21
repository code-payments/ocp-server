package memory

import (
	"bytes"
	"context"
	"encoding/hex"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/code-payments/ocp-server/database/query"
	"github.com/code-payments/ocp-server/ocp/data/balance/watch"
)

type store struct {
	mu        sync.Mutex
	watches   map[string]*watch.Record   // by watchKey
	schedules map[string]*watch.Schedule // by owner
	events    map[string][]*watch.Event  // by subscriber, sorted by ID
}

// New returns a memory backed watch.Store.
func New() watch.Store {
	return &store{
		watches:   make(map[string]*watch.Record),
		schedules: make(map[string]*watch.Schedule),
		events:    make(map[string][]*watch.Event),
	}
}

// GetWatch implements watch.Store.GetWatch.
func (s *store) GetWatch(_ context.Context, subscriber, owner string, key []byte) (*watch.Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	record, ok := s.watches[watchKey(subscriber, owner, key)]
	if !ok {
		return nil, watch.ErrWatchNotFound
	}
	cloned := record.Clone()
	return &cloned, nil
}

// GetOwner implements watch.Store.GetOwner.
func (s *store) GetOwner(_ context.Context, owner string) (*watch.Schedule, []*watch.Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	schedule, ok := s.schedules[owner]
	if !ok {
		return nil, nil, watch.ErrOwnerNotFound
	}
	clonedSchedule := schedule.Clone()

	var records []*watch.Record
	for _, record := range s.watches {
		if record.Owner == owner {
			cloned := record.Clone()
			records = append(records, &cloned)
		}
	}
	return &clonedSchedule, records, nil
}

// GetWatchesBySubscriber implements watch.Store.GetWatchesBySubscriber. Pages
// are ordered by key; the cursor is the hex of the last key returned.
func (s *store) GetWatchesBySubscriber(_ context.Context, subscriber, owner string, cursor query.Cursor, limit uint64) ([]*watch.Record, query.Cursor, error) {
	if limit == 0 {
		return nil, query.EmptyCursor, nil
	}

	var after string
	if len(cursor) > 0 {
		if _, err := hex.DecodeString(string(cursor)); err != nil {
			return nil, nil, watch.ErrInvalidCursor
		}
		after = string(cursor)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	var all []*watch.Record
	for _, record := range s.watches {
		if record.Subscriber == subscriber && record.Owner == owner && hex.EncodeToString(record.Key) > after {
			all = append(all, record)
		}
	}
	sort.Slice(all, func(i, j int) bool {
		return bytes.Compare(all[i].Key, all[j].Key) < 0
	})

	hasMore := uint64(len(all)) > limit
	if hasMore {
		all = all[:limit]
	}

	records := make([]*watch.Record, 0, len(all))
	for _, record := range all {
		cloned := record.Clone()
		records = append(records, &cloned)
	}

	next := query.EmptyCursor
	if hasMore {
		next = query.Cursor(hex.EncodeToString(all[len(all)-1].Key))
	}
	return records, next, nil
}

// ListWatches implements watch.Store.ListWatches. Pages are ordered by
// (owner, key); the cursor is the position of the last watch returned.
func (s *store) ListWatches(_ context.Context, subscriber string, cursor query.Cursor, limit uint64) ([]*watch.Record, query.Cursor, error) {
	if limit == 0 {
		return nil, query.EmptyCursor, nil
	}

	var after string
	if len(cursor) > 0 {
		after = string(cursor)
		if !strings.Contains(after, "#") {
			return nil, nil, watch.ErrInvalidCursor
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	var all []*watch.Record
	for _, record := range s.watches {
		if record.Subscriber == subscriber && listPosition(record) > after {
			all = append(all, record)
		}
	}
	sort.Slice(all, func(i, j int) bool {
		return listPosition(all[i]) < listPosition(all[j])
	})

	hasMore := uint64(len(all)) > limit
	if hasMore {
		all = all[:limit]
	}

	records := make([]*watch.Record, 0, len(all))
	for _, record := range all {
		cloned := record.Clone()
		records = append(records, &cloned)
	}

	next := query.EmptyCursor
	if hasMore {
		next = query.Cursor(listPosition(all[len(all)-1]))
	}
	return records, next, nil
}

// PutWatch implements watch.Store.PutWatch.
func (s *store) PutWatch(_ context.Context, record *watch.Record, event *watch.Event) error {
	if err := record.Validate(); err != nil {
		return err
	}
	if event != nil {
		if err := event.Validate(); err != nil {
			return err
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	key := watchKey(record.Subscriber, record.Owner, record.Key)
	existing, exists := s.watches[key]
	switch {
	case record.Version == 0 && exists:
		return watch.ErrWatchExists
	case record.Version != 0 && (!exists || existing.Version != record.Version):
		return watch.ErrStaleVersion
	}

	// The event is written as given, so copy it before the record's version
	// moves in place.
	var stored *watch.Event
	if event != nil {
		copied := event.Clone()
		stored = &copied
	}

	cloned := record.Clone()
	cloned.Version++
	s.watches[key] = &cloned
	record.Version = cloned.Version

	s.pullScheduleForward(record.Owner)

	if stored != nil {
		s.insertEvent(stored)
	}
	return nil
}

// DeleteWatch implements watch.Store.DeleteWatch.
func (s *store) DeleteWatch(_ context.Context, subscriber, owner string, key []byte, generation uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	k := watchKey(subscriber, owner, key)
	existing, ok := s.watches[k]
	if !ok {
		return nil
	}
	if existing.Generation != generation {
		return watch.ErrStaleGeneration
	}
	delete(s.watches, k)
	return nil
}

// CommitTransitions implements watch.Store.CommitTransitions.
func (s *store) CommitTransitions(_ context.Context, records []*watch.Record, event *watch.Event) error {
	if err := watch.ValidateCommit(records, event); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Check every version before writing any, so the commit is all or nothing.
	for _, record := range records {
		existing, ok := s.watches[watchKey(record.Subscriber, record.Owner, record.Key)]
		if !ok || existing.Version != record.Version {
			return watch.ErrStaleVersion
		}
	}

	stored := event.Clone()
	for _, record := range records {
		cloned := record.Clone()
		cloned.Version++
		s.watches[watchKey(record.Subscriber, record.Owner, record.Key)] = &cloned
		record.Version = cloned.Version
	}
	s.insertEvent(&stored)
	return nil
}

// GetSchedulesDue implements watch.Store.GetSchedulesDue.
func (s *store) GetSchedulesDue(_ context.Context, shard int, asOf time.Time, limit int) ([]*watch.Schedule, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var due []*watch.Schedule
	for _, schedule := range s.schedules {
		if watch.ScheduleShard(schedule.Owner) != shard || schedule.NextEvaluationAt.After(asOf) {
			continue
		}
		cloned := schedule.Clone()
		due = append(due, &cloned)
	}
	sort.Slice(due, func(i, j int) bool {
		if !due[i].NextEvaluationAt.Equal(due[j].NextEvaluationAt) {
			return due[i].NextEvaluationAt.Before(due[j].NextEvaluationAt)
		}
		return due[i].Owner < due[j].Owner
	})
	if len(due) > limit {
		due = due[:limit]
	}
	return due, nil
}

// ClaimSchedule implements watch.Store.ClaimSchedule.
func (s *store) ClaimSchedule(_ context.Context, schedule *watch.Schedule, until time.Time) error {
	if err := schedule.Validate(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	existing, ok := s.schedules[schedule.Owner]
	if !ok || existing.Version != schedule.Version {
		return watch.ErrStaleVersion
	}
	existing.NextEvaluationAt = until
	existing.Version++

	schedule.NextEvaluationAt = until
	schedule.Version = existing.Version
	return nil
}

// SetSchedule implements watch.Store.SetSchedule.
func (s *store) SetSchedule(_ context.Context, schedule *watch.Schedule) error {
	if err := schedule.Validate(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	existing, ok := s.schedules[schedule.Owner]
	if !ok || existing.Version != schedule.Version {
		return watch.ErrStaleVersion
	}
	existing.NextEvaluationAt = schedule.NextEvaluationAt
	existing.Version++

	schedule.Version = existing.Version
	return nil
}

// LeaseEvents implements watch.Store.LeaseEvents.
func (s *store) LeaseEvents(_ context.Context, subscriber string, asOf, until time.Time, limit int) ([]*watch.Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	leased := make([]*watch.Event, 0)
	for _, event := range s.events[subscriber] {
		if len(leased) >= limit {
			break
		}
		if !event.LeasedUntil.IsZero() && event.LeasedUntil.After(asOf) {
			continue
		}
		event.LeasedUntil = until
		cloned := event.Clone()
		leased = append(leased, &cloned)
	}
	return leased, nil
}

// AckEvents implements watch.Store.AckEvents.
func (s *store) AckEvents(_ context.Context, subscriber string, ids []uuid.UUID) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	acked := make(map[uuid.UUID]struct{}, len(ids))
	for _, id := range ids {
		acked[id] = struct{}{}
	}

	events := s.events[subscriber]
	kept := events[:0]
	for _, event := range events {
		if _, ok := acked[event.ID]; !ok {
			kept = append(kept, event)
		}
	}
	s.events[subscriber] = kept
	return nil
}

// pullScheduleForward makes the owner due now, creating the schedule if
// needed, and moves its version. The caller holds the lock.
func (s *store) pullScheduleForward(owner string) {
	schedule, ok := s.schedules[owner]
	if !ok {
		schedule = &watch.Schedule{Owner: owner}
		s.schedules[owner] = schedule
	}
	schedule.NextEvaluationAt = time.Now()
	schedule.Version++
}

// insertEvent adds the event, which the store now owns, to its subscriber's
// queue, keeping the queue in ID order. The caller holds the lock.
func (s *store) insertEvent(event *watch.Event) {
	event.LeasedUntil = time.Time{}

	events := s.events[event.Subscriber]
	i := sort.Search(len(events), func(i int) bool {
		return bytes.Compare(events[i].ID[:], event.ID[:]) > 0
	})
	events = append(events, nil)
	copy(events[i+1:], events[i:])
	events[i] = event
	s.events[event.Subscriber] = events
}

func (s *store) reset() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.watches = make(map[string]*watch.Record)
	s.schedules = make(map[string]*watch.Schedule)
	s.events = make(map[string][]*watch.Event)
}

func watchKey(subscriber, owner string, key []byte) string {
	return subscriber + "|" + owner + "|" + hex.EncodeToString(key)
}

// listPosition is a watch's place in its subscriber's listing.
func listPosition(record *watch.Record) string {
	return record.Owner + "#" + hex.EncodeToString(record.Key)
}
