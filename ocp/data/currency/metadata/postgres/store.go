package postgres

import (
	"context"
	"database/sql"

	"github.com/jmoiron/sqlx"

	"github.com/code-payments/ocp-server/database/query"
	"github.com/code-payments/ocp-server/ocp/data/currency"
	"github.com/code-payments/ocp-server/ocp/data/currency/metadata"
)

type store struct {
	db *sqlx.DB
}

func New(db *sql.DB) metadata.Store {
	return &store{
		db: sqlx.NewDb(db, "pgx"),
	}
}

func (s *store) SaveMetadata(ctx context.Context, record *currency.MetadataRecord) error {
	model, err := toModel(record)
	if err != nil {
		return err
	}

	err = model.dbSave(ctx, s.db)
	if err != nil {
		return err
	}

	fromModel(model).CopyTo(record)

	return nil
}

func (s *store) GetMetadata(ctx context.Context, mint string) (*currency.MetadataRecord, error) {
	model, err := dbGetMetadataByMint(ctx, s.db, mint)
	if err != nil {
		return nil, err
	}
	return fromModel(model), nil
}

func (s *store) GetAllMetadataByState(ctx context.Context, state currency.MetadataState, cursor query.Cursor, limit uint64, direction query.Ordering) ([]*currency.MetadataRecord, error) {
	models, err := dbGetAllMetadataByState(ctx, s.db, state, cursor, limit, direction)
	if err != nil {
		return nil, err
	}

	res := make([]*currency.MetadataRecord, len(models))
	for i, model := range models {
		res[i] = fromModel(model)
	}
	return res, nil
}

func (s *store) GetAllMints(ctx context.Context) ([]string, error) {
	return dbGetAllMints(ctx, s.db)
}

func (s *store) CountMints(ctx context.Context) (uint64, error) {
	return dbCountMints(ctx, s.db)
}

func (s *store) CountMetadataByState(ctx context.Context, state currency.MetadataState) (uint64, error) {
	return dbCountMetadataByState(ctx, s.db, state)
}

func (s *store) IsNameAvailable(ctx context.Context, name string) (bool, error) {
	return dbIsNameAvailable(ctx, s.db, name)
}
