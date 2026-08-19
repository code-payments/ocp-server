package postgres

import (
	"context"
	"database/sql"

	"github.com/jmoiron/sqlx"

	"github.com/code-payments/ocp-server/database/query"
	"github.com/code-payments/ocp-server/ocp/data/history"
)

type store struct {
	db *sqlx.DB
}

func New(db *sql.DB) history.Store {
	return &store{
		db: sqlx.NewDb(db, "pgx"),
	}
}

func (s *store) Save(ctx context.Context, record *history.Record) error {
	obj, err := toModel(record)
	if err != nil {
		return err
	}

	if err := obj.dbSave(ctx, s.db); err != nil {
		return err
	}

	res, err := fromModel(obj)
	if err != nil {
		return err
	}
	res.CopyTo(record)

	return nil
}

func (s *store) GetAllByOwner(ctx context.Context, owner string, cursor query.Cursor, limit uint64, direction query.Ordering) ([]*history.Record, error) {
	models, err := dbGetAllByOwner(ctx, s.db, owner, cursor, limit, direction)
	if err != nil {
		return nil, err
	}
	return fromModels(models)
}

func (s *store) GetAllByOwnerMint(ctx context.Context, owner, mint string, cursor query.Cursor, limit uint64, direction query.Ordering) ([]*history.Record, error) {
	models, err := dbGetAllByOwnerMint(ctx, s.db, owner, mint, cursor, limit, direction)
	if err != nil {
		return nil, err
	}
	return fromModels(models)
}

func (s *store) GetAllByIds(ctx context.Context, ids []uint64) ([]*history.Record, error) {
	models, err := dbGetAllByIds(ctx, s.db, ids)
	if err != nil {
		return nil, err
	}
	return fromModels(models)
}

func (s *store) GetAllByReference(ctx context.Context, referenceType history.ReferenceType, referenceId string) ([]*history.Record, error) {
	models, err := dbGetAllByReference(ctx, s.db, referenceType, referenceId)
	if err != nil {
		return nil, err
	}
	return fromModels(models)
}

func (s *store) GetAllByGiftCardVault(ctx context.Context, vault string) ([]*history.Record, error) {
	models, err := dbGetAllByGiftCardVault(ctx, s.db, vault)
	if err != nil {
		return nil, err
	}
	return fromModels(models)
}

func fromModels(models []*model) ([]*history.Record, error) {
	records := make([]*history.Record, len(models))
	for i, m := range models {
		record, err := fromModel(m)
		if err != nil {
			return nil, err
		}
		records[i] = record
	}
	return records, nil
}
