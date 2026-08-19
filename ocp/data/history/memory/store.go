package memory

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/code-payments/ocp-server/database/query"
	"github.com/code-payments/ocp-server/ocp/data/history"
	"github.com/code-payments/ocp-server/pointer"
)

// ByCreatedAt orders records the way a history is read: by event time, with the
// record ID breaking ties so the order is total.
type ByCreatedAt []*history.Record

func (a ByCreatedAt) Len() int      { return len(a) }
func (a ByCreatedAt) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a ByCreatedAt) Less(i, j int) bool {
	if !a[i].CreatedAt.Equal(a[j].CreatedAt) {
		return a[i].CreatedAt.Before(a[j].CreatedAt)
	}
	return a[i].Id < a[j].Id
}

type store struct {
	mu      sync.RWMutex
	records []*history.Record
	last    uint64
}

func New() history.Store {
	return &store{}
}

func (s *store) Save(_ context.Context, data *history.Record) error {
	if err := data.Validate(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if data.Id == 0 {
		if s.findByOwnerAndReference(data.OwnerAccount, data.ReferenceType, data.ReferenceId) != nil {
			return history.ErrExists
		}

		s.last++
		data.Id = s.last
		data.Version++
		data.UpdatedAt = data.CreatedAt

		cloned := data.Clone()
		s.records = append(s.records, &cloned)

		return nil
	}

	item := s.findById(data.Id)
	if item == nil {
		return history.ErrNotFound
	}
	if item.Version != data.Version {
		return history.ErrStaleVersion
	}

	// Only the mutable part of a record is applied. The caller then gets the
	// record back as stored, so an edit to an immutable field is neither
	// persisted nor left behind on the caller's copy.
	item.State = data.State
	item.DestinationQuantity = pointer.Uint64Copy(data.DestinationQuantity)
	item.Fees = cloneFees(data.Fees)
	item.Version++
	item.UpdatedAt = time.Now()

	item.CopyTo(data)

	return nil
}

func (s *store) GetAllByOwner(_ context.Context, owner string, cursor query.Cursor, limit uint64, direction query.Ordering) ([]*history.Record, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	items := s.findByOwner(owner)

	res, err := s.page(items, cursor, limit, direction)
	if err != nil {
		return nil, err
	}
	if len(res) == 0 {
		return nil, history.ErrNotFound
	}
	return res, nil
}

func (s *store) GetAllByOwnerMint(_ context.Context, owner, mint string, cursor query.Cursor, limit uint64, direction query.Ordering) ([]*history.Record, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	items := s.filterByMint(s.findByOwner(owner), mint)

	res, err := s.page(items, cursor, limit, direction)
	if err != nil {
		return nil, err
	}
	if len(res) == 0 {
		return nil, history.ErrNotFound
	}
	return res, nil
}

func (s *store) GetAllByIds(_ context.Context, ids []uint64) ([]*history.Record, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	wanted := make(map[uint64]struct{}, len(ids))
	for _, id := range ids {
		wanted[id] = struct{}{}
	}

	var items []*history.Record
	for _, item := range s.records {
		if _, ok := wanted[item.Id]; ok {
			items = append(items, item)
		}
	}

	sort.Slice(items, func(i, j int) bool { return items[i].Id < items[j].Id })

	if len(items) == 0 {
		return nil, history.ErrNotFound
	}
	return cloneRecords(items), nil
}

func (s *store) GetAllByReference(_ context.Context, referenceType history.ReferenceType, referenceId string) ([]*history.Record, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var items []*history.Record
	for _, item := range s.records {
		if item.ReferenceType == referenceType && item.ReferenceId == referenceId {
			items = append(items, item)
		}
	}

	if len(items) == 0 {
		return nil, history.ErrNotFound
	}
	return cloneRecords(items), nil
}

func (s *store) GetAllByGiftCardVault(_ context.Context, vault string) ([]*history.Record, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var items []*history.Record
	for _, item := range s.records {
		if item.GiftCardVault != nil && *item.GiftCardVault == vault {
			items = append(items, item)
		}
	}

	if len(items) == 0 {
		return nil, history.ErrNotFound
	}
	return cloneRecords(items), nil
}

func (s *store) findById(id uint64) *history.Record {
	for _, item := range s.records {
		if item.Id == id {
			return item
		}
	}
	return nil
}

func (s *store) findByOwnerAndReference(owner string, referenceType history.ReferenceType, referenceId string) *history.Record {
	for _, item := range s.records {
		if item.OwnerAccount == owner && item.ReferenceType == referenceType && item.ReferenceId == referenceId {
			return item
		}
	}
	return nil
}

func (s *store) findByOwner(owner string) []*history.Record {
	var res []*history.Record
	for _, item := range s.records {
		if item.OwnerAccount == owner {
			res = append(res, item)
		}
	}
	return res
}

func (s *store) filterByMint(items []*history.Record, mint string) []*history.Record {
	var res []*history.Record
	for _, item := range items {
		if item.MintAccount == mint {
			res = append(res, item)
			continue
		}
		if item.DestinationMintAccount != nil && *item.DestinationMintAccount == mint {
			res = append(res, item)
		}
	}
	return res
}

func (s *store) page(items []*history.Record, cursor query.Cursor, limit uint64, direction query.Ordering) ([]*history.Record, error) {
	var res []*history.Record
	if len(cursor) == 0 {
		res = append(res, items...)
	} else {
		createdAt, id, ok := history.FromCursor(cursor)
		if !ok {
			return nil, history.ErrInvalidCursor
		}

		for _, item := range items {
			cmp := compareToCursor(item, createdAt, id)
			if direction == query.Ascending && cmp > 0 {
				res = append(res, item)
			}
			if direction == query.Descending && cmp < 0 {
				res = append(res, item)
			}
		}
	}

	if direction == query.Descending {
		sort.Sort(sort.Reverse(ByCreatedAt(res)))
	} else {
		sort.Sort(ByCreatedAt(res))
	}

	if limit > 0 && uint64(len(res)) > limit {
		res = res[:limit]
	}

	return cloneRecords(res), nil
}

// compareToCursor orders a record against a cursor position on the same
// (event time, ID) terms the history itself is ordered by.
func compareToCursor(item *history.Record, createdAt time.Time, id uint64) int {
	if !item.CreatedAt.Equal(createdAt) {
		if item.CreatedAt.Before(createdAt) {
			return -1
		}
		return 1
	}

	switch {
	case item.Id < id:
		return -1
	case item.Id > id:
		return 1
	default:
		return 0
	}
}

func cloneRecords(items []*history.Record) []*history.Record {
	res := make([]*history.Record, 0, len(items))
	for _, item := range items {
		cloned := item.Clone()
		res = append(res, &cloned)
	}
	return res
}

func cloneFees(fees []history.Fee) []history.Fee {
	if fees == nil {
		return nil
	}
	cloned := make([]history.Fee, len(fees))
	copy(cloned, fees)
	return cloned
}

func (s *store) reset() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.records = nil
	s.last = 0
}
