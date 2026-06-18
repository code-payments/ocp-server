// Package cache provides an exchange.Store decorator that caches single- and
// all-symbol rate lookups in front of a wrapped store.
//
// Reads are keyed by a coarse time bucket that doubles as the freshness window,
// and a full set of rates is weighted more heavily than a single-symbol entry.
// Range and history reads, and all writes, pass straight through to the wrapped
// store.
package cache

import (
	"context"
	"fmt"
	"time"

	lrucache "github.com/code-payments/ocp-server/cache"
	"github.com/code-payments/ocp-server/database/query"
	"github.com/code-payments/ocp-server/ocp/data/currency"
	"github.com/code-payments/ocp-server/ocp/data/currency/exchange"
)

const (
	// maxCacheBudget bounds the weighted size of the rate cache before the
	// least-recently-used entries are evicted.
	maxCacheBudget = 100_000

	// singleRateWeight and multiRateWeight weight cached entries against the
	// budget. A full set of rates spans many symbols, so it costs
	// proportionally more than a single-symbol entry.
	singleRateWeight = 1
	multiRateWeight  = 100

	// cacheBucket is the time granularity used to build cache keys. Lookups that
	// truncate to the same bucket share a cached result, which doubles as the
	// effective freshness window for cached rates.
	cacheBucket = 5 * time.Minute
)

type store struct {
	backing exchange.Store
	cache   lrucache.Cache
}

// New returns an exchange.Store that caches reads in front of backing.
func New(backing exchange.Store) exchange.Store {
	return &store{
		backing: backing,
		cache:   lrucache.NewCache(maxCacheBudget),
	}
}

func (s *store) PutExchangeRates(ctx context.Context, record *currency.MultiRateRecord) error {
	return s.backing.PutExchangeRates(ctx, record)
}

func (s *store) GetExchangeRate(ctx context.Context, symbol string, t time.Time) (*currency.ExchangeRateRecord, error) {
	key := fmt.Sprintf("%s:%s", symbol, t.Truncate(cacheBucket).Format(time.RFC3339))
	if cached, ok := s.cache.Retrieve(key); ok {
		return cached.(*currency.ExchangeRateRecord), nil
	}

	rate, err := s.backing.GetExchangeRate(ctx, symbol, t)
	if err != nil {
		return nil, err
	}

	s.cache.Insert(key, rate, singleRateWeight)

	return rate, nil
}

func (s *store) GetAllExchangeRates(ctx context.Context, t time.Time) (*currency.MultiRateRecord, error) {
	key := fmt.Sprintf("everything:%s", t.Truncate(cacheBucket).Format(time.RFC3339))
	if cached, ok := s.cache.Retrieve(key); ok {
		return cached.(*currency.MultiRateRecord), nil
	}

	rates, err := s.backing.GetAllExchangeRates(ctx, t)
	if err != nil {
		return nil, err
	}

	s.cache.Insert(key, rates, multiRateWeight)

	return rates, nil
}

func (s *store) GetExchangeRatesInRange(ctx context.Context, symbol string, interval query.Interval, start time.Time, end time.Time, ordering query.Ordering) ([]*currency.ExchangeRateRecord, error) {
	return s.backing.GetExchangeRatesInRange(ctx, symbol, interval, start, end, ordering)
}
