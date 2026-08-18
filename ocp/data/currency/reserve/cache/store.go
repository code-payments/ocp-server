// Package cache provides a reserve.Store decorator that caches point-in-time
// reserve lookups in front of a wrapped store.
//
// Point-in-time reads are keyed by mint and a coarse time bucket that doubles as
// the freshness window. Range, day and live reads pass straight through. Live writes
// are guarded against the last successfully saved slot per mint: a write whose
// slot is not greater is rejected with currency.ErrStaleReserveState without a
// round-trip to the backing store. Everything else passes straight through.
package cache

import (
	"context"
	"fmt"
	"sync"
	"time"

	lrucache "github.com/code-payments/ocp-server/cache"
	"github.com/code-payments/ocp-server/database/query"
	"github.com/code-payments/ocp-server/ocp/data/currency"
	"github.com/code-payments/ocp-server/ocp/data/currency/reserve"
)

const (
	// maxCacheBudget bounds the weighted size of the reserve cache before the
	// least-recently-used entries are evicted.
	maxCacheBudget = 100_000

	// reserveWeight weights each cached entry against the budget. A cached entry
	// is a single per-mint, point-in-time record.
	reserveWeight = 1

	// cacheBucket is the time granularity used to build cache keys. Lookups that
	// truncate to the same bucket share a cached result, which doubles as the
	// effective freshness window for cached reserves.
	cacheBucket = 5 * time.Minute
)

type store struct {
	backing reserve.Store
	cache   lrucache.Cache

	// liveSlots tracks the slot of the last live reserve successfully saved per
	// mint, so stale writes can be rejected without a round-trip to the backing
	// store.
	liveSlotsMu sync.Mutex
	liveSlots   map[string]uint64
}

// New returns a reserve.Store that caches reads in front of backing.
func New(backing reserve.Store) reserve.Store {
	return &store{
		backing:   backing,
		cache:     lrucache.NewCache(maxCacheBudget),
		liveSlots: make(map[string]uint64),
	}
}

func (s *store) PutHistoricalReserve(ctx context.Context, record *currency.ReserveRecord) error {
	return s.backing.PutHistoricalReserve(ctx, record)
}

func (s *store) GetReserveAtTime(ctx context.Context, mint string, t time.Time) (*currency.ReserveRecord, error) {
	key := fmt.Sprintf("%s:%s", mint, t.Truncate(cacheBucket).Format(time.RFC3339))
	if cached, ok := s.cache.Retrieve(key); ok {
		return cached.(*currency.ReserveRecord), nil
	}

	record, err := s.backing.GetReserveAtTime(ctx, mint, t)
	if err != nil {
		return nil, err
	}

	s.cache.Insert(key, record, reserveWeight)

	return record, nil
}

func (s *store) GetReservesForDay(ctx context.Context, mints []string, t time.Time) (map[string]*currency.ReserveRecord, error) {
	return s.backing.GetReservesForDay(ctx, mints, t)
}

func (s *store) GetReservesInRange(ctx context.Context, mint string, interval query.Interval, start time.Time, end time.Time, ordering query.Ordering) ([]*currency.ReserveRecord, error) {
	return s.backing.GetReservesInRange(ctx, mint, interval, start, end, ordering)
}

func (s *store) PutLiveReserve(ctx context.Context, record *currency.ReserveRecord) error {
	s.liveSlotsMu.Lock()
	lastSlot, tracked := s.liveSlots[record.Mint]
	s.liveSlotsMu.Unlock()

	// Reject stale writes locally. The backing store remains the source of truth
	// and performs the same check atomically, so a race here at worst lets a
	// stale write through to be rejected there.
	if tracked && record.Slot <= lastSlot {
		return currency.ErrStaleReserveState
	}

	if err := s.backing.PutLiveReserve(ctx, record); err != nil {
		return err
	}

	s.liveSlotsMu.Lock()
	if cur, ok := s.liveSlots[record.Mint]; !ok || record.Slot > cur {
		s.liveSlots[record.Mint] = record.Slot
	}
	s.liveSlotsMu.Unlock()

	return nil
}

func (s *store) GetLiveReserve(ctx context.Context, mint string) (*currency.ReserveRecord, error) {
	return s.backing.GetLiveReserve(ctx, mint)
}

func (s *store) GetAllLiveReserves(ctx context.Context) (map[string]*currency.ReserveRecord, error) {
	return s.backing.GetAllLiveReserves(ctx)
}
