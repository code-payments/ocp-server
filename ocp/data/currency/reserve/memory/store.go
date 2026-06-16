// Package memory provides an in-memory reserve.Store implementation for fast
// unit tests.
package memory

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/code-payments/ocp-server/database/query"
	"github.com/code-payments/ocp-server/ocp/data/currency"
	"github.com/code-payments/ocp-server/ocp/data/currency/reserve"
)

type store struct {
	mu         sync.Mutex
	historical []*currency.ReserveRecord
	lastID     uint64
	live       map[string]*currency.ReserveRecord
}

type reserveByTime []*currency.ReserveRecord

func (a reserveByTime) Len() int      { return len(a) }
func (a reserveByTime) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a reserveByTime) Less(i, j int) bool {
	// DESC order (most recent first)
	return a[i].Time.Unix() > a[j].Time.Unix()
}

func New() reserve.Store {
	return &store{
		historical: make([]*currency.ReserveRecord, 0),
		lastID:     1,
		live:       make(map[string]*currency.ReserveRecord),
	}
}

func (s *store) reset() {
	s.mu.Lock()
	s.historical = make([]*currency.ReserveRecord, 0)
	s.lastID = 1
	s.live = make(map[string]*currency.ReserveRecord)
	s.mu.Unlock()
}

func (s *store) PutHistoricalReserve(ctx context.Context, record *currency.ReserveRecord) error {
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

func (s *store) GetReserveAtTime(ctx context.Context, mint string, t time.Time) (*currency.ReserveRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var results []*currency.ReserveRecord
	for _, item := range s.historical {
		if item.Mint == mint && item.Time.Unix() <= t.Unix() {
			results = append(results, item)
		}
	}

	if len(results) == 0 {
		return nil, currency.ErrNotFound
	}

	sort.Sort(reserveByTime(results))

	return results[0].Clone(), nil
}

func (s *store) GetReservesInRange(ctx context.Context, mint string, interval query.Interval, start time.Time, end time.Time, ordering query.Ordering) ([]*currency.ReserveRecord, error) {
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

	var all []*currency.ReserveRecord
	for _, item := range s.historical {
		if item.Mint == mint && item.Time.Unix() >= actualStart.Unix() && item.Time.Unix() <= actualEnd.Unix() {
			all = append(all, item.Clone())
		}
	}

	// TODO: handle the interval

	if len(all) == 0 {
		return nil, currency.ErrNotFound
	}

	sort.Sort(reserveByTime(all)) // DESC
	if ordering == query.Ascending {
		for i, j := 0, len(all)-1; i < j; i, j = i+1, j-1 {
			all[i], all[j] = all[j], all[i]
		}
	}

	return all, nil
}

func (s *store) PutLiveReserve(ctx context.Context, record *currency.ReserveRecord) error {
	if err := record.Validate(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if existing, ok := s.live[record.Mint]; ok && record.Slot <= existing.Slot {
		return currency.ErrStaleReserveState
	}

	s.live[record.Mint] = record.Clone()
	return nil
}

func (s *store) GetLiveReserve(ctx context.Context, mint string) (*currency.ReserveRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	record, ok := s.live[mint]
	if !ok {
		return nil, currency.ErrNotFound
	}
	return record.Clone(), nil
}

func (s *store) GetAllLiveReserves(ctx context.Context) (map[string]*currency.ReserveRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.live) == 0 {
		return nil, currency.ErrNotFound
	}

	res := make(map[string]*currency.ReserveRecord, len(s.live))
	for mint, record := range s.live {
		res[mint] = record.Clone()
	}
	return res, nil
}
