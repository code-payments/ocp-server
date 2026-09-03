package postgres

import (
	"context"
	"database/sql"

	"github.com/jmoiron/sqlx"

	"github.com/code-payments/ocp-server/database/query"
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

// Create implements balance.Store.Create
func (s *store) Create(ctx context.Context, record *balance.Record) error {
	model, err := toModel(record)
	if err != nil {
		return err
	}

	if err := model.dbCreate(ctx, s.db); err != nil {
		return err
	}

	fromModel(model).CopyTo(record)
	return nil
}

// Get implements balance.Store.Get
func (s *store) Get(ctx context.Context, tokenAccount string) (*balance.Record, error) {
	model, err := dbGet(ctx, s.db, tokenAccount)
	if err != nil {
		return nil, err
	}
	return fromModel(model), nil
}

// GetBatch implements balance.Store.GetBatch
func (s *store) GetBatch(ctx context.Context, tokenAccounts ...string) (map[string]*balance.Record, error) {
	models, err := dbGetBatch(ctx, s.db, tokenAccounts...)
	if err != nil {
		return nil, err
	}

	res := make(map[string]*balance.Record, len(models))
	for _, model := range models {
		res[model.TokenAccount] = fromModel(model)
	}
	return res, nil
}

// GetAllByOwner implements balance.Store.GetAllByOwner
func (s *store) GetAllByOwner(ctx context.Context, owner string) ([]*balance.Record, error) {
	models, err := dbGetAllByOwner(ctx, s.db, owner, nil)
	if err != nil {
		return nil, err
	}
	return fromModels(models), nil
}

// GetAllByOwnerAndMint implements balance.Store.GetAllByOwnerAndMint
func (s *store) GetAllByOwnerAndMint(ctx context.Context, owner, mint string) ([]*balance.Record, error) {
	models, err := dbGetAllByOwner(ctx, s.db, owner, &mint)
	if err != nil {
		return nil, err
	}
	return fromModels(models), nil
}

// GetAllLockedByMint implements balance.Store.GetAllLockedByMint
func (s *store) GetAllLockedByMint(ctx context.Context, mint string, minQuarks int64, cursor query.Cursor, limit uint64, direction query.Ordering) ([]*balance.Record, error) {
	models, err := dbGetAllLockedByMint(ctx, s.db, mint, minQuarks, cursor, limit, direction)
	if err != nil {
		return nil, err
	}
	return fromModels(models), nil
}

// CountLockedByMint implements balance.Store.CountLockedByMint
func (s *store) CountLockedByMint(ctx context.Context, mint string, minQuarks int64) (uint64, error) {
	return dbCountLockedByMint(ctx, s.db, mint, minQuarks)
}

// MarkAsUnlocked implements balance.Store.MarkAsUnlocked
func (s *store) MarkAsUnlocked(ctx context.Context, tokenAccount string) error {
	return dbMarkAsUnlocked(ctx, s.db, tokenAccount)
}

// ApplyDeltas implements balance.Store.ApplyDeltas
func (s *store) ApplyDeltas(ctx context.Context, deltas ...*balance.Delta) error {
	for _, delta := range deltas {
		if err := delta.Validate(); err != nil {
			return err
		}
	}

	return dbApplyDeltas(ctx, s.db, balance.MergeDeltas(deltas))
}

// Backfill implements balance.Store.Backfill
func (s *store) Backfill(ctx context.Context, tokenAccount string, fn balance.BackfillFunc) error {
	return dbBackfill(ctx, s.db, tokenAccount, fn)
}

func fromModels(models []*model) []*balance.Record {
	res := make([]*balance.Record, len(models))
	for i, model := range models {
		res[i] = fromModel(model)
	}
	return res
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
