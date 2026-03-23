package postgres

import (
	"context"
	"database/sql"

	"github.com/jmoiron/sqlx"

	commonpb "github.com/code-payments/ocp-protobuf-api/generated/go/common/v1"

	"github.com/code-payments/ocp-server/ocp/data/balance"
)

type store struct {
	db *sqlx.DB
}

// New returns a new postgres balance.Store
func New(db *sql.DB) balance.Store {
	return &store{
		db: sqlx.NewDb(db, "pgx"),
	}
}

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

// SaveExternalCheckpoint implements balance.Store.SaveExternalCheckpoint
func (s *store) SaveExternalCheckpoint(ctx context.Context, record *balance.ExternalCheckpointRecord) error {
	model, err := toExternalCheckpointModel(record)
	if err != nil {
		return err
	}

	if err := model.dbSave(ctx, s.db); err != nil {
		return err
	}

	res := fromExternalCheckpoingModel(model)
	res.CopyTo(record)

	return nil
}

// GetExternalCheckpoint implements balance.Store.GetExternalCheckpoint
func (s *store) GetExternalCheckpoint(ctx context.Context, account string) (*balance.ExternalCheckpointRecord, error) {
	model, err := dbGetExternalCheckpoint(ctx, s.db, account)
	if err != nil {
		return nil, err
	}
	return fromExternalCheckpoingModel(model), nil
}

// GetBalance implements balance.Store.GetBalance
func (s *store) GetBalance(ctx context.Context, account string) (*balance.AccountBalanceRecord, error) {
	return dbGetBalance(ctx, s.db, account)
}

// GetBalanceBatch implements balance.Store.GetBalanceBatch
func (s *store) GetBalanceBatch(ctx context.Context, accounts ...string) (map[string]*balance.AccountBalanceRecord, error) {
	return dbGetBalanceBatch(ctx, s.db, accounts...)
}

// AdjustBalance implements balance.Store.AdjustBalance
func (s *store) AdjustBalance(ctx context.Context, account string, quarks int64, usdCostBasis float64, owner string, mint string, accountType commonpb.AccountType) error {
	return dbAdjustBalance(ctx, s.db, account, quarks, usdCostBasis, owner, mint, int(accountType))
}
