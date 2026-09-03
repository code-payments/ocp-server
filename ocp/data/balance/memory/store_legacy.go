package memory

import (
	"context"

	"github.com/code-payments/ocp-server/ocp/data/balance"
)

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
