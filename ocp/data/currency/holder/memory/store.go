// Package memory provides an in-memory holder.Store implementation for fast
// unit tests.
package memory

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/code-payments/ocp-server/database/query"
	"github.com/code-payments/ocp-server/ocp/data/currency"
	"github.com/code-payments/ocp-server/ocp/data/currency/holder"
)

type store struct {
	mu         sync.Mutex
	historical []*currency.HolderCountRecord
	lastID     uint64
	live       map[string]*currency.HolderCountRecord
}

type holderCountByTime []*currency.HolderCountRecord

func (a holderCountByTime) Len() int      { return len(a) }
func (a holderCountByTime) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a holderCountByTime) Less(i, j int) bool {
	// DESC order (most recent first)
	return a[i].Time.Unix() > a[j].Time.Unix()
}

func New() holder.Store {
	return &store{
		historical: make([]*currency.HolderCountRecord, 0),
		lastID:     1,
		live:       make(map[string]*currency.HolderCountRecord),
	}
}

func (s *store) reset() {
	s.mu.Lock()
	s.historical = make([]*currency.HolderCountRecord, 0)
	s.lastID = 1
	s.live = make(map[string]*currency.HolderCountRecord)
	s.mu.Unlock()
}

func (s *store) PutHistoricalHolderCount(ctx context.Context, record *currency.HolderCountRecord) error {
	if err := record.Validate(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	for _, item := range s.historical {
		if item.Mint == record.Mint && item.Time.Unix() == record.Time.Unix() {
			return currency.ErrExists
		}
	}

	cloned := record.Clone()
	cloned.Id = s.lastID
	s.historical = append(s.historical, cloned)
	s.lastID++

	return nil
}

func (s *store) GetHolderCountAtTime(ctx context.Context, mint string, t time.Time) (*currency.HolderCountRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var results []*currency.HolderCountRecord
	for _, item := range s.historical {
		if item.Mint == mint && item.Time.Unix() <= t.Unix() {
			results = append(results, item)
		}
	}

	if len(results) == 0 {
		return nil, currency.ErrNotFound
	}

	sort.Sort(holderCountByTime(results))

	return results[0].Clone(), nil
}

func (s *store) GetHolderCountsForDay(ctx context.Context, mints []string, t time.Time) (map[string]*currency.HolderCountRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	res := make(map[string]*currency.HolderCountRecord, len(mints))
	for _, mint := range mints {
		// The close of t's UTC day for this mint: its most recent record that day.
		var latest *currency.HolderCountRecord
		for _, item := range s.historical {
			if item.Mint != mint || !sameUTCDay(item.Time, t) {
				continue
			}
			if latest == nil || item.Time.After(latest.Time) {
				latest = item
			}
		}
		if latest != nil {
			res[mint] = latest.Clone()
		}
	}
	return res, nil
}

func sameUTCDay(a, b time.Time) bool {
	ay, am, ad := a.UTC().Date()
	by, bm, bd := b.UTC().Date()
	return ay == by && am == bm && ad == bd
}

func (s *store) GetHolderCountsInRange(ctx context.Context, mint string, interval query.Interval, start time.Time, end time.Time, ordering query.Ordering) ([]*currency.HolderCountRecord, error) {
	if interval > query.IntervalMonth {
		return nil, currency.ErrInvalidInterval
	}
	if start.IsZero() || end.IsZero() {
		return nil, currency.ErrInvalidRange
	}

	actualStart, actualEnd := start, end
	if start.Unix() > end.Unix() {
		actualStart, actualEnd = end, start
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	var all []*currency.HolderCountRecord
	for _, item := range s.historical {
		if item.Mint == mint && item.Time.Unix() >= actualStart.Unix() && item.Time.Unix() <= actualEnd.Unix() {
			all = append(all, item.Clone())
		}
	}

	// TODO: handle the interval

	if len(all) == 0 {
		return nil, currency.ErrNotFound
	}

	sort.Sort(holderCountByTime(all)) // DESC
	if ordering == query.Ascending {
		for i, j := 0, len(all)-1; i < j; i, j = i+1, j-1 {
			all[i], all[j] = all[j], all[i]
		}
	}

	return all, nil
}

func (s *store) PutLiveHolderCount(ctx context.Context, record *currency.HolderCountRecord) error {
	if err := record.Validate(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if existing, ok := s.live[record.Mint]; ok && !record.Time.After(existing.Time) {
		return currency.ErrStaleHolderState
	}

	s.live[record.Mint] = record.Clone()
	return nil
}

func (s *store) GetLiveHolderCount(ctx context.Context, mint string) (*currency.HolderCountRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	record, ok := s.live[mint]
	if !ok {
		return nil, currency.ErrNotFound
	}
	return record.Clone(), nil
}

func (s *store) GetAllLiveHolderCounts(ctx context.Context) (map[string]*currency.HolderCountRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.live) == 0 {
		return nil, currency.ErrNotFound
	}

	res := make(map[string]*currency.HolderCountRecord, len(s.live))
	for mint, record := range s.live {
		res[mint] = record.Clone()
	}
	return res, nil
}
