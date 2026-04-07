package memory

import (
	"context"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/code-payments/ocp-server/database/query"
	"github.com/code-payments/ocp-server/ocp/data/currency"
)

const (
	dateFormat = "2006-01-02"
)

type store struct {
	mu                              sync.Mutex
	exchangeRateRecords             []*currency.ExchangeRateRecord
	lastExchangeRateIndex           uint64
	metadataRecords                 []*currency.MetadataRecord
	lastMetadataIndex               uint64
	historicalReserveRecords        []*currency.ReserveRecord
	lastHistoricalReserveIndex      uint64
	liveReserveRecords              map[string]*currency.ReserveRecord
	lastLiveReserveIndex            uint64
	historicalHolderCountRecords    []*currency.HolderCountRecord
	lastHistoricalHolderCountIndex  uint64
	liveHolderCountRecords          map[string]*currency.HolderCountRecord
	lastLiveHolderCountIndex        uint64
}

type RateByTime []*currency.ExchangeRateRecord

func (a RateByTime) Len() int      { return len(a) }
func (a RateByTime) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a RateByTime) Less(i, j int) bool {
	// DESC order (most recent first)
	return a[i].Time.Unix() > a[j].Time.Unix()
}

type ReserveByTime []*currency.ReserveRecord

func (a ReserveByTime) Len() int      { return len(a) }
func (a ReserveByTime) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a ReserveByTime) Less(i, j int) bool {
	// DESC order (most recent first)
	return a[i].Time.Unix() > a[j].Time.Unix()
}

type HolderCountByTime []*currency.HolderCountRecord

func (a HolderCountByTime) Len() int      { return len(a) }
func (a HolderCountByTime) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a HolderCountByTime) Less(i, j int) bool {
	// DESC order (most recent first)
	return a[i].Time.Unix() > a[j].Time.Unix()
}

func New() currency.Store {
	return &store{
		exchangeRateRecords:    make([]*currency.ExchangeRateRecord, 0),
		lastExchangeRateIndex:  1,
		liveReserveRecords:     make(map[string]*currency.ReserveRecord),
		lastLiveReserveIndex:   1,
		liveHolderCountRecords: make(map[string]*currency.HolderCountRecord),
		lastLiveHolderCountIndex: 1,
	}
}

func (s *store) reset() {
	s.mu.Lock()
	s.exchangeRateRecords = make([]*currency.ExchangeRateRecord, 0)
	s.lastExchangeRateIndex = 1
	s.metadataRecords = make([]*currency.MetadataRecord, 0)
	s.lastMetadataIndex = 1
	s.historicalReserveRecords = make([]*currency.ReserveRecord, 0)
	s.lastHistoricalReserveIndex = 1
	s.liveReserveRecords = make(map[string]*currency.ReserveRecord)
	s.lastLiveReserveIndex = 1
	s.historicalHolderCountRecords = make([]*currency.HolderCountRecord, 0)
	s.lastHistoricalHolderCountIndex = 1
	s.liveHolderCountRecords = make(map[string]*currency.HolderCountRecord)
	s.lastLiveHolderCountIndex = 1
	s.mu.Unlock()
}

func (s *store) PutExchangeRates(ctx context.Context, data *currency.MultiRateRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Not ideal but fine for testing the currency store
	for _, item := range s.exchangeRateRecords {
		if item.Time.Unix() == data.Time.Unix() {
			return currency.ErrExists
		}
	}

	for symbol, item := range data.Rates {
		s.exchangeRateRecords = append(s.exchangeRateRecords, &currency.ExchangeRateRecord{
			Id:     s.lastExchangeRateIndex,
			Rate:   item,
			Time:   data.Time,
			Symbol: symbol,
		})
		s.lastExchangeRateIndex = s.lastExchangeRateIndex + 1
	}

	return nil
}

func (s *store) GetExchangeRate(ctx context.Context, symbol string, t time.Time) (*currency.ExchangeRateRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Not ideal but fine for testing the currency store
	var results []*currency.ExchangeRateRecord
	for _, item := range s.exchangeRateRecords {
		if item.Symbol == symbol && item.Time.Unix() <= t.Unix() && item.Time.Format(dateFormat) == t.Format(dateFormat) {
			results = append(results, item)
		}
	}

	if len(results) == 0 {
		return nil, currency.ErrNotFound
	}

	sort.Sort(RateByTime(results))

	return results[0], nil
}

func (s *store) GetAllExchangeRates(ctx context.Context, t time.Time) (*currency.MultiRateRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Not ideal but fine for testing the currency store
	sort.Sort(RateByTime(s.exchangeRateRecords))

	result := currency.MultiRateRecord{
		Rates: make(map[string]float64),
	}
	for _, item := range s.exchangeRateRecords {
		if item.Time.Unix() <= t.Unix() && item.Time.Format(dateFormat) == t.Format(dateFormat) {
			result.Rates[item.Symbol] = item.Rate
			result.Time = item.Time
		}
	}

	if len(result.Rates) == 0 {
		return nil, currency.ErrNotFound
	}

	return &result, nil
}

func (s *store) GetExchangeRatesInRange(ctx context.Context, symbol string, interval query.Interval, start time.Time, end time.Time, ordering query.Ordering) ([]*currency.ExchangeRateRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	sort.Sort(RateByTime(s.exchangeRateRecords))

	// Not ideal but fine for testing the currency store
	var all []*currency.ExchangeRateRecord
	for _, item := range s.exchangeRateRecords {
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

func (s *store) SaveMetadata(ctx context.Context, data *currency.MetadataRecord) error {
	if err := data.Validate(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	for i, item := range s.metadataRecords {
		if item.Mint == data.Mint {
			if item.Version != data.Version {
				return currency.ErrStaleMetadataVersion
			}

			cloned := item.Clone()
			cloned.Description = data.Description
			cloned.ImageUrl = data.ImageUrl
			cloned.BillColors = append([]string(nil), data.BillColors...)
			cloned.SocialLinks = append([]currency.SocialLink(nil), data.SocialLinks...)
			cloned.Alt = data.Alt
			cloned.State = data.State
			cloned.Version = item.Version + 1

			s.metadataRecords[i] = cloned
			cloned.CopyTo(data)
			return nil
		}
	}

	for _, item := range s.metadataRecords {
		if strings.EqualFold(item.Name, data.Name) && item.State != currency.MetadataStateAbandoned {
			return currency.ErrDuplicateCurrency
		}
	}

	data.Version = 1
	data.Id = s.lastMetadataIndex
	s.metadataRecords = append(s.metadataRecords, data.Clone())
	s.lastMetadataIndex = s.lastMetadataIndex + 1

	return nil
}

func (s *store) GetMetadata(ctx context.Context, mint string) (*currency.MetadataRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, item := range s.metadataRecords {
		if item.Mint == mint {
			return item.Clone(), nil
		}
	}

	return nil, currency.ErrNotFound
}

type metadataById []*currency.MetadataRecord

func (a metadataById) Len() int           { return len(a) }
func (a metadataById) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a metadataById) Less(i, j int) bool { return a[i].Id < a[j].Id }

func (s *store) GetAllMetadataByState(_ context.Context, state currency.MetadataState, cursor query.Cursor, limit uint64, direction query.Ordering) ([]*currency.MetadataRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var items []*currency.MetadataRecord
	for _, item := range s.metadataRecords {
		if item.State == state {
			items = append(items, item)
		}
	}

	if len(items) == 0 {
		return nil, currency.ErrNotFound
	}

	var start uint64
	start = 0
	if direction == query.Descending {
		start = s.lastMetadataIndex + 1
	}
	if len(cursor) > 0 {
		start = cursor.ToUint64()
	}

	var res []*currency.MetadataRecord
	for _, item := range items {
		if item.Id > start && direction == query.Ascending {
			res = append(res, item.Clone())
		}
		if item.Id < start && direction == query.Descending {
			res = append(res, item.Clone())
		}
	}

	if len(res) == 0 {
		return nil, currency.ErrNotFound
	}

	if direction == query.Descending {
		sort.Sort(sort.Reverse(metadataById(res)))
	}

	if uint64(len(res)) > limit {
		res = res[:limit]
	}

	return res, nil
}

func (s *store) GetAllMints(ctx context.Context) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.metadataRecords) == 0 {
		return nil, currency.ErrNotFound
	}

	var mints []string
	for _, item := range s.metadataRecords {
		mints = append(mints, item.Mint)
	}

	return mints, nil
}

func (s *store) CountMetadataByState(_ context.Context, state currency.MetadataState) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var count uint64
	for _, item := range s.metadataRecords {
		if item.State == state {
			count++
		}
	}
	return count, nil
}

func (s *store) CountMints(ctx context.Context) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return uint64(len(s.metadataRecords)), nil
}

func (s *store) IsNameAvailable(_ context.Context, name string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, item := range s.metadataRecords {
		if strings.EqualFold(item.Name, name) && item.State != currency.MetadataStateAbandoned {
			return false, nil
		}
	}
	return true, nil
}

func (s *store) PutHistoricalReserveRecord(ctx context.Context, data *currency.ReserveRecord) error {
	if err := data.Validate(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Not ideal but fine for testing the currency store
	for _, item := range s.historicalReserveRecords {
		if item.Mint == data.Mint && item.Time.Unix() == data.Time.Unix() {
			return currency.ErrExists
		}
	}

	data.Id = s.lastHistoricalReserveIndex
	s.historicalReserveRecords = append(s.historicalReserveRecords, data.Clone())
	s.lastHistoricalReserveIndex = s.lastHistoricalReserveIndex + 1

	return nil
}

func (s *store) GetReserveAtTime(ctx context.Context, mint string, t time.Time) (*currency.ReserveRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Not ideal but fine for testing the currency store
	var results []*currency.ReserveRecord
	for _, item := range s.historicalReserveRecords {
		if item.Mint == mint && item.Time.Unix() <= t.Unix() && item.Time.Format(dateFormat) == t.Format(dateFormat) {
			results = append(results, item)
		}
	}

	if len(results) == 0 {
		return nil, currency.ErrNotFound
	}

	sort.Sort(ReserveByTime(results))

	return results[0].Clone(), nil
}

func (s *store) GetReservesInRange(ctx context.Context, mint string, interval query.Interval, start time.Time, end time.Time, ordering query.Ordering) ([]*currency.ReserveRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	sort.Sort(ReserveByTime(s.historicalReserveRecords))

	// Not ideal but fine for testing the currency store
	var all []*currency.ReserveRecord
	for _, item := range s.historicalReserveRecords {
		if item.Mint == mint && item.Time.Unix() >= start.Unix() && item.Time.Unix() <= end.Unix() {
			all = append(all, item.Clone())
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

func (s *store) PutLiveReserveRecord(ctx context.Context, data *currency.ReserveRecord) error {
	if err := data.Validate(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if existing, ok := s.liveReserveRecords[data.Mint]; ok {
		if data.Slot <= existing.Slot {
			return currency.ErrStaleReserveState
		}

		cloned := data.Clone()
		cloned.Id = existing.Id
		s.liveReserveRecords[data.Mint] = cloned
		cloned.CopyTo(data)
		return nil
	}

	data.Id = s.lastLiveReserveIndex
	s.liveReserveRecords[data.Mint] = data.Clone()
	s.lastLiveReserveIndex++

	return nil
}

func (s *store) GetLiveReserve(ctx context.Context, mint string) (*currency.ReserveRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	record, ok := s.liveReserveRecords[mint]
	if !ok {
		return nil, currency.ErrNotFound
	}

	return record.Clone(), nil
}

func (s *store) GetAllLiveReserves(ctx context.Context) (map[string]*currency.ReserveRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.liveReserveRecords) == 0 {
		return nil, currency.ErrNotFound
	}

	res := make(map[string]*currency.ReserveRecord, len(s.liveReserveRecords))
	for mint, record := range s.liveReserveRecords {
		res[mint] = record.Clone()
	}
	return res, nil
}

func (s *store) PutHistoricalHolderCountRecord(ctx context.Context, data *currency.HolderCountRecord) error {
	if err := data.Validate(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	for _, item := range s.historicalHolderCountRecords {
		if item.Mint == data.Mint && item.Time.Unix() == data.Time.Unix() {
			return currency.ErrExists
		}
	}

	data.Id = s.lastHistoricalHolderCountIndex
	s.historicalHolderCountRecords = append(s.historicalHolderCountRecords, data.Clone())
	s.lastHistoricalHolderCountIndex = s.lastHistoricalHolderCountIndex + 1

	return nil
}

func (s *store) GetHolderCountAtTime(ctx context.Context, mint string, t time.Time) (*currency.HolderCountRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var results []*currency.HolderCountRecord
	for _, item := range s.historicalHolderCountRecords {
		if item.Mint == mint && item.Time.Unix() <= t.Unix() && item.Time.Format(dateFormat) == t.Format(dateFormat) {
			results = append(results, item)
		}
	}

	if len(results) == 0 {
		return nil, currency.ErrNotFound
	}

	sort.Sort(HolderCountByTime(results))

	return results[0].Clone(), nil
}

func (s *store) GetAllHolderCountsAtTime(ctx context.Context, t time.Time) (map[string]*currency.HolderCountRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	sort.Sort(HolderCountByTime(s.historicalHolderCountRecords))

	result := make(map[string]*currency.HolderCountRecord)
	for _, item := range s.historicalHolderCountRecords {
		if item.Time.Unix() <= t.Unix() && item.Time.Format(dateFormat) == t.Format(dateFormat) {
			if _, exists := result[item.Mint]; !exists {
				result[item.Mint] = item.Clone()
			}
		}
	}

	if len(result) == 0 {
		return nil, currency.ErrNotFound
	}

	return result, nil
}

func (s *store) PutLiveHolderCountRecord(ctx context.Context, data *currency.HolderCountRecord) error {
	if err := data.Validate(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if existing, ok := s.liveHolderCountRecords[data.Mint]; ok {
		if !data.Time.After(existing.Time) {
			return currency.ErrStaleHolderState
		}

		cloned := data.Clone()
		cloned.Id = existing.Id
		s.liveHolderCountRecords[data.Mint] = cloned
		cloned.CopyTo(data)
		return nil
	}

	data.Id = s.lastLiveHolderCountIndex
	s.liveHolderCountRecords[data.Mint] = data.Clone()
	s.lastLiveHolderCountIndex++

	return nil
}

func (s *store) GetLiveHolderCount(ctx context.Context, mint string) (*currency.HolderCountRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	record, ok := s.liveHolderCountRecords[mint]
	if !ok {
		return nil, currency.ErrNotFound
	}

	return record.Clone(), nil
}

func (s *store) GetAllLiveHolderCounts(ctx context.Context) (map[string]*currency.HolderCountRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.liveHolderCountRecords) == 0 {
		return nil, currency.ErrNotFound
	}

	res := make(map[string]*currency.HolderCountRecord, len(s.liveHolderCountRecords))
	for mint, record := range s.liveHolderCountRecords {
		res[mint] = record.Clone()
	}
	return res, nil
}
