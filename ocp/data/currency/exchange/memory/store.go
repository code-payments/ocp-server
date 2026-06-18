// Package memory provides an in-memory exchange.Store implementation for fast
// unit tests. It mirrors the exchange-rate behavior of the in-memory currency
// store.
package memory

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/code-payments/ocp-server/database/query"
	"github.com/code-payments/ocp-server/ocp/data/currency"
	"github.com/code-payments/ocp-server/ocp/data/currency/exchange"
)

type store struct {
	mu      sync.Mutex
	records []*currency.ExchangeRateRecord
	lastID  uint64
}

type rateByTime []*currency.ExchangeRateRecord

func (a rateByTime) Len() int      { return len(a) }
func (a rateByTime) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a rateByTime) Less(i, j int) bool {
	// DESC order (most recent first)
	return a[i].Time.Unix() > a[j].Time.Unix()
}

func New() exchange.Store {
	return &store{
		records: make([]*currency.ExchangeRateRecord, 0),
		lastID:  1,
	}
}

func (s *store) reset() {
	s.mu.Lock()
	s.records = make([]*currency.ExchangeRateRecord, 0)
	s.lastID = 1
	s.mu.Unlock()
}

func (s *store) PutExchangeRates(ctx context.Context, data *currency.MultiRateRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, item := range s.records {
		if item.Time.Unix() == data.Time.Unix() {
			return currency.ErrExists
		}
	}

	for symbol, rate := range data.Rates {
		s.records = append(s.records, &currency.ExchangeRateRecord{
			Id:     s.lastID,
			Rate:   rate,
			Time:   data.Time,
			Symbol: symbol,
		})
		s.lastID++
	}

	return nil
}

func (s *store) GetExchangeRate(ctx context.Context, symbol string, t time.Time) (*currency.ExchangeRateRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var results []*currency.ExchangeRateRecord
	for _, item := range s.records {
		if item.Symbol == symbol && item.Time.Unix() <= t.Unix() {
			results = append(results, item)
		}
	}

	if len(results) == 0 {
		return nil, currency.ErrNotFound
	}

	sort.Sort(rateByTime(results))

	return results[0], nil
}

func (s *store) GetAllExchangeRates(ctx context.Context, t time.Time) (*currency.MultiRateRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	sort.Sort(rateByTime(s.records)) // most recent first

	result := currency.MultiRateRecord{
		Rates: make(map[string]float64),
	}
	for _, item := range s.records {
		if item.Time.Unix() > t.Unix() {
			continue
		}
		// The first record at or before t (descending) is the most recent;
		// collect every symbol recorded at that same instant, then stop.
		if len(result.Rates) == 0 {
			result.Time = item.Time
		} else if !item.Time.Equal(result.Time) {
			break
		}
		result.Rates[item.Symbol] = item.Rate
	}

	if len(result.Rates) == 0 {
		return nil, currency.ErrNotFound
	}

	return &result, nil
}

func (s *store) GetExchangeRatesInRange(ctx context.Context, symbol string, interval query.Interval, start time.Time, end time.Time, ordering query.Ordering) ([]*currency.ExchangeRateRecord, error) {
	if interval > query.IntervalMonth {
		return nil, currency.ErrInvalidInterval
	}
	if start.IsZero() || end.IsZero() {
		return nil, currency.ErrInvalidRange
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	sort.Sort(rateByTime(s.records))

	var all []*currency.ExchangeRateRecord
	for _, item := range s.records {
		if item.Symbol == symbol && item.Time.Unix() >= start.Unix() && item.Time.Unix() <= end.Unix() {
			all = append(all, item)
		}
	}

	// TODO: handle the interval

	if len(all) == 0 {
		return nil, currency.ErrNotFound
	}

	if ordering == query.Ascending {
		for i, j := 0, len(all)-1; i < j; i, j = i+1, j-1 {
			all[i], all[j] = all[j], all[i]
		}
	}

	return all, nil
}
