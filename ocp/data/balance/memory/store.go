package memory

import (
	"context"
	"sync"
	"time"

	commonpb "github.com/code-payments/ocp-protobuf-api/generated/go/common/v1"

	"github.com/code-payments/ocp-server/ocp/data/balance"
)

type store struct {
	mu                             sync.Mutex
	cachedBalanceVersionsByAccount map[string]uint64
	closedAccounts                 map[string]any
	externalCheckpointRecords      []*balance.ExternalCheckpointRecord
	balances                       map[string]balance.AccountBalanceRecord
	last                           uint64
}

// New returns a new in memory balance.Store
func New() balance.Store {
	return &store{
		cachedBalanceVersionsByAccount: make(map[string]uint64),
		closedAccounts:                 make(map[string]any),
		balances:                       make(map[string]balance.AccountBalanceRecord),
	}
}

// GetCachedVersion implements balance.Store.GetCachedVersion
func (s *store) GetCachedVersion(_ context.Context, account string) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	current, ok := s.cachedBalanceVersionsByAccount[account]
	if !ok {
		return 0, nil
	}
	return current, nil
}

// AdvanceCachedVersion implements balance.Store.AdvanceCachedVersion
func (s *store) AdvanceCachedVersion(_ context.Context, account string, currentVersion uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	actualVersion, ok := s.cachedBalanceVersionsByAccount[account]
	if !ok {
		if currentVersion != 0 {
			return balance.ErrStaleCachedBalanceVersion
		}

		s.cachedBalanceVersionsByAccount[account] = 1

		return nil
	}

	if actualVersion != currentVersion {
		return balance.ErrStaleCachedBalanceVersion
	}

	s.cachedBalanceVersionsByAccount[account]++

	return nil
}

// CheckNotClosed implements balance.Store.CheckNotClosed
func (s *store) CheckNotClosed(ctx context.Context, account string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.closedAccounts[account]; ok {
		return balance.ErrAccountClosed
	}

	return nil
}

// MarkAsClosed implements balance.Store.MarkAsClosed
func (s *store) MarkAsClosed(ctx context.Context, account string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.closedAccounts[account] = true

	return nil
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

// GetBalance implements balance.Store.GetBalance
func (s *store) GetBalance(_ context.Context, account string) (*balance.AccountBalanceRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	record, ok := s.balances[account]
	if !ok {
		return nil, balance.ErrBalanceNotFound
	}
	cloned := record.Clone()
	return &cloned, nil
}

// GetBalanceBatch implements balance.Store.GetBalanceBatch
func (s *store) GetBalanceBatch(_ context.Context, accounts ...string) (map[string]*balance.AccountBalanceRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	res := make(map[string]*balance.AccountBalanceRecord)
	for _, account := range accounts {
		if record, ok := s.balances[account]; ok {
			cloned := record.Clone()
			res[account] = &cloned
		} else {
			res[account] = &balance.AccountBalanceRecord{}
		}
	}
	return res, nil
}

// AdjustBalance implements balance.Store.AdjustBalance
func (s *store) AdjustBalance(_ context.Context, account string, quarks int64, usdCostBasis float64, _ string, _ string, _ commonpb.AccountType) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	record := s.balances[account]
	newQuarks := int64(record.Quarks) + quarks
	if newQuarks < 0 {
		return balance.ErrNegativeBalance
	}
	record.Quarks = uint64(newQuarks)
	record.UsdCostBasis += usdCostBasis
	record.Version++
	s.balances[account] = record
	return nil
}

func (s *store) reset() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.cachedBalanceVersionsByAccount = make(map[string]uint64)
	s.closedAccounts = make(map[string]any)
	s.externalCheckpointRecords = nil
	s.balances = make(map[string]balance.AccountBalanceRecord)
	s.last = 0
}
