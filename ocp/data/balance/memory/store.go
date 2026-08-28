package memory

import (
	"context"
	"sync"
	"time"

	"github.com/code-payments/ocp-server/database/query"
	"github.com/code-payments/ocp-server/ocp/data/balance"
)

type store struct {
	mu                           sync.Mutex
	balanceRecords               []*balance.Record
	balanceRecordsByTokenAccount map[string]*balance.Record

	cachedBalanceVersionsByAccount map[string]uint64
	closedAccounts                 map[string]any
	externalCheckpointRecords      []*balance.ExternalCheckpointRecord

	last uint64
}

// New returns a new in memory balance.Store
func New() balance.Store {
	return &store{
		balanceRecordsByTokenAccount:   make(map[string]*balance.Record),
		cachedBalanceVersionsByAccount: make(map[string]uint64),
		closedAccounts:                 make(map[string]any),
	}
}

// Create implements balance.Store.Create
func (s *store) Create(_ context.Context, record *balance.Record) error {
	if err := record.Validate(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.balanceRecordsByTokenAccount[record.TokenAccount]; ok {
		return balance.ErrRecordExists
	}

	s.last++
	record.Id = s.last
	record.UpdatedAt = time.Now()

	cloned := record.Clone()
	s.balanceRecordsByTokenAccount[record.TokenAccount] = &cloned
	s.balanceRecords = append(s.balanceRecords, &cloned)

	return nil
}

// Get implements balance.Store.Get
func (s *store) Get(_ context.Context, tokenAccount string) (*balance.Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	item, ok := s.balanceRecordsByTokenAccount[tokenAccount]
	if !ok {
		return nil, balance.ErrRecordNotFound
	}
	cloned := item.Clone()
	return &cloned, nil
}

// GetBatch implements balance.Store.GetBatch
func (s *store) GetBatch(_ context.Context, tokenAccounts ...string) (map[string]*balance.Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	res := make(map[string]*balance.Record)
	for _, tokenAccount := range tokenAccounts {
		item, ok := s.balanceRecordsByTokenAccount[tokenAccount]
		if !ok {
			continue
		}
		cloned := item.Clone()
		res[tokenAccount] = &cloned
	}
	return res, nil
}

// GetAllByOwner implements balance.Store.GetAllByOwner
func (s *store) GetAllByOwner(_ context.Context, owner string) ([]*balance.Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.filter(func(item *balance.Record) bool {
		return item.OwnerAccount == owner
	})
}

// GetAllByOwnerAndMint implements balance.Store.GetAllByOwnerAndMint
func (s *store) GetAllByOwnerAndMint(_ context.Context, owner, mint string) ([]*balance.Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.filter(func(item *balance.Record) bool {
		return item.OwnerAccount == owner && item.MintAccount == mint
	})
}

// GetAllByMint implements balance.Store.GetAllByMint
func (s *store) GetAllByMint(_ context.Context, mint string, minQuarks int64, cursor query.Cursor, limit uint64, direction query.Ordering) ([]*balance.Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	res, err := s.filter(func(item *balance.Record) bool {
		if item.MintAccount != mint || item.Quarks < minQuarks {
			return false
		}
		if len(cursor) > 0 {
			if direction == query.Ascending && item.Id <= cursor.ToUint64() {
				return false
			}
			if direction == query.Descending && item.Id >= cursor.ToUint64() {
				return false
			}
		}
		return true
	})
	if err != nil {
		return nil, err
	}

	if direction == query.Descending {
		for i, j := 0, len(res)-1; i < j; i, j = i+1, j-1 {
			res[i], res[j] = res[j], res[i]
		}
	}

	if limit > 0 && uint64(len(res)) > limit {
		res = res[:limit]
	}
	return res, nil
}

// CountByMint implements balance.Store.CountByMint
func (s *store) CountByMint(_ context.Context, mint string, minQuarks int64) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var res uint64
	for _, item := range s.balanceRecordsByTokenAccount {
		if item.MintAccount == mint && item.Quarks >= minQuarks && item.IsBackfilled {
			res++
		}
	}
	return res, nil
}

// ApplyDeltas implements balance.Store.ApplyDeltas
func (s *store) ApplyDeltas(_ context.Context, deltas ...*balance.Delta) error {
	for _, delta := range deltas {
		if err := delta.Validate(); err != nil {
			return err
		}
	}

	merged := balance.MergeDeltas(deltas)

	s.mu.Lock()
	defer s.mu.Unlock()

	// Apply to copies first so a failure part way through leaves the store
	// untouched, matching the transactional behaviour of the DB store.
	updated := make(map[string]*balance.Record)
	for _, delta := range merged {
		item, ok := updated[delta.TokenAccount]
		if !ok {
			original, ok := s.balanceRecordsByTokenAccount[delta.TokenAccount]
			if !ok {
				return balance.ErrRecordNotFound
			}
			cloned := original.Clone()
			item = &cloned
			updated[delta.TokenAccount] = item
		}

		if err := applyDelta(item, delta); err != nil {
			return err
		}
	}

	now := time.Now()
	for tokenAccount, item := range updated {
		item.UpdatedAt = now
		item.CopyTo(s.balanceRecordsByTokenAccount[tokenAccount])
	}
	return nil
}

func applyDelta(item *balance.Record, delta *balance.Delta) error {
	enforce := item.IsBackfilled

	switch delta.Kind {
	case balance.DeltaCredit:
		if enforce && !item.IsOpen {
			return balance.ErrAccountClosed
		}
		item.Quarks += int64(delta.Quarks)
		item.UsdCostBasis += delta.UsdCostBasis
	case balance.DeltaDebit:
		if enforce && item.Quarks < int64(delta.Quarks) {
			return balance.ErrInsufficientBalance
		}
		item.Quarks -= int64(delta.Quarks)
		item.UsdCostBasis -= delta.UsdCostBasis
	case balance.DeltaDrain:
		if enforce {
			if !item.IsOpen {
				return balance.ErrAccountClosed
			}
			if item.Quarks != int64(delta.Quarks) {
				return balance.ErrBalanceChanged
			}
			item.Quarks = 0
			item.UsdCostBasis = 0
		} else {
			item.Quarks -= int64(delta.Quarks)
			item.UsdCostBasis -= delta.UsdCostBasis
		}
		item.IsOpen = false
	case balance.DeltaClose:
		if enforce {
			if !item.IsOpen {
				return balance.ErrAccountClosed
			}
			if item.Quarks != 0 {
				return balance.ErrBalanceChanged
			}
		}
		item.IsOpen = false
	}
	return nil
}

// Backfill implements balance.Store.Backfill
//
// Note: The lock is released while fn runs, since fn reads from other stores
// sharing the provider. Unlike the DB store, this doesn't block concurrent
// deltas, which tests don't exercise against a backfill.
func (s *store) Backfill(ctx context.Context, tokenAccount string, fn balance.BackfillFunc) error {
	s.mu.Lock()
	item, ok := s.balanceRecordsByTokenAccount[tokenAccount]
	if !ok {
		s.mu.Unlock()
		return balance.ErrRecordNotFound
	}
	if item.IsBackfilled {
		s.mu.Unlock()
		return balance.ErrAlreadyBackfilled
	}
	s.mu.Unlock()

	result, err := fn(ctx)
	if err != nil {
		return err
	}
	if result.Quarks < 0 {
		return balance.ErrNegativeBalance
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	item.Quarks = result.Quarks
	item.UsdCostBasis = result.UsdCostBasis
	item.IsOpen = result.IsOpen
	item.IsBackfilled = true
	item.UpdatedAt = time.Now()
	return nil
}

func (s *store) filter(fn func(*balance.Record) bool) ([]*balance.Record, error) {
	var res []*balance.Record
	for _, item := range s.balanceRecords {
		if !fn(item) {
			continue
		}
		cloned := item.Clone()
		res = append(res, &cloned)
	}
	if len(res) == 0 {
		return nil, balance.ErrRecordNotFound
	}
	return res, nil
}

func (s *store) reset() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.balanceRecords = nil
	s.balanceRecordsByTokenAccount = make(map[string]*balance.Record)
	s.cachedBalanceVersionsByAccount = make(map[string]uint64)
	s.closedAccounts = make(map[string]any)
	s.externalCheckpointRecords = nil
	s.last = 0
}

// SaveExternalCheckpoint implements balance.Store.SaveExternalCheckpoint
func (s *store) SaveExternalCheckpoint(_ context.Context, data *balance.ExternalCheckpointRecord) error {
	if err := data.Validate(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.last++
	if item := s.findExternalCheckpoint(data); item != nil {
		if data.SlotCheckpoint <= item.SlotCheckpoint {
			return balance.ErrStaleCheckpoint
		}

		item.SlotCheckpoint = data.SlotCheckpoint
		item.Quarks = data.Quarks
		item.LastUpdatedAt = time.Now()
		item.CopyTo(data)
	} else {
		if data.Id == 0 {
			data.Id = s.last
		}
		data.LastUpdatedAt = time.Now()
		c := data.Clone()
		s.externalCheckpointRecords = append(s.externalCheckpointRecords, &c)
	}

	return nil
}

// GetExternalCheckpoint implements balance.Store.GetExternalCheckpoint
func (s *store) GetExternalCheckpoint(_ context.Context, account string) (*balance.ExternalCheckpointRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if item := s.findExternalCheckpointByTokenAccount(account); item != nil {
		cloned := item.Clone()
		return &cloned, nil
	}
	return nil, balance.ErrCheckpointNotFound
}

func (s *store) findExternalCheckpoint(data *balance.ExternalCheckpointRecord) *balance.ExternalCheckpointRecord {
	for _, item := range s.externalCheckpointRecords {
		if item.Id == data.Id {
			return item
		}
		if data.TokenAccount == item.TokenAccount {
			return item
		}
	}
	return nil
}

func (s *store) findExternalCheckpointByTokenAccount(account string) *balance.ExternalCheckpointRecord {
	for _, item := range s.externalCheckpointRecords {
		if account == item.TokenAccount {
			return item
		}
	}
	return nil
}
