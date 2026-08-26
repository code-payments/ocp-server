package postgres

import (
	"context"
)

// GetCachedVersion implements balance.Store.GetCachedVersion
func (s *store) GetCachedVersion(ctx context.Context, account string) (uint64, error) {
	return dbGetCachedVersion(ctx, s.db, account)
}

// AdvanceCachedVersion implements balance.Store.AdvanceCachedVersion
func (s *store) AdvanceCachedVersion(ctx context.Context, account string, currentVersion uint64) error {
	return dbAdvanceCachedVersion(ctx, s.db, account, currentVersion)
}

// CheckNotClosed implements balance.Store.CheckNotClosed
func (s *store) CheckNotClosed(ctx context.Context, account string) error {
	return dbCheckNotClosed(ctx, s.db, account)
}

// MarkAsClosed implements balance.Store.MarkAsClosed
func (s *store) MarkAsClosed(ctx context.Context, account string) error {
	return dbMarkAsClosed(ctx, s.db, account)
}
