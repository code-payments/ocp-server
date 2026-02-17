package postgres

import (
	"context"
	"database/sql"

	"github.com/jmoiron/sqlx"

	"github.com/code-payments/ocp-server/ocp/data/vm/metadata"
)

type store struct {
	db *sqlx.DB
}

// New returns a new postgres vm.metadata.Store
func New(db *sql.DB) metadata.Store {
	return &store{
		db: sqlx.NewDb(db, "pgx"),
	}
}

// Put implements vm.metadata.Store.Put
func (s *store) Put(ctx context.Context, record *metadata.Record) error {
	obj, err := toModel(record)
	if err != nil {
		return err
	}

	err = obj.dbPut(ctx, s.db)
	if err != nil {
		return err
	}

	fromModel(obj).CopyTo(record)

	return nil
}

// GetByMint implements vm.metadata.Store.GetByMint
func (s *store) GetByMint(ctx context.Context, mint string) (*metadata.Record, error) {
	obj, err := dbGetByMint(ctx, s.db, mint)
	if err != nil {
		return nil, err
	}
	return fromModel(obj), nil
}
