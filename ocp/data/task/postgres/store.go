package postgres

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/jmoiron/sqlx"

	pgutil "github.com/code-payments/ocp-server/database/postgres"
	"github.com/code-payments/ocp-server/database/query"
	"github.com/code-payments/ocp-server/ocp/data/task"
)

type store struct {
	db *sqlx.DB
}

func New(db *sql.DB) task.Store {
	return &store{
		db: sqlx.NewDb(db, "pgx"),
	}
}

// PutAll implements task.Store.PutAll
func (s *store) PutAll(ctx context.Context, records ...*task.Record) error {
	if len(records) == 0 {
		return errors.New("empty task set")
	}

	models := make([]*model, len(records))
	for i, record := range records {
		if record.Id > 0 {
			return task.ErrExists
		}

		model, err := toModel(record)
		if err != nil {
			return err
		}

		models[i] = model
	}

	return pgutil.ExecuteInTx(ctx, s.db, sql.LevelDefault, func(tx *sqlx.Tx) error {
		updated, err := dbPutAllInTx(ctx, tx, models)
		if err != nil {
			return err
		}

		if len(updated) != len(records) {
			return errors.New("unexpected count of task models returned")
		}

		// Don't assume postgres properly orders things
		updatedByTaskId := make(map[string]*model)
		for _, model := range updated {
			updatedByTaskId[model.TaskId] = model
		}

		for _, record := range records {
			model, ok := updatedByTaskId[record.TaskId]
			if !ok {
				return errors.New("task model not returned")
			}

			fromModel(model).CopyTo(record)
		}

		return nil
	})
}

// Update implements task.Store.Update
func (s *store) Update(ctx context.Context, record *task.Record) error {
	obj, err := toModel(record)
	if err != nil {
		return err
	}

	err = obj.dbUpdate(ctx, s.db)
	if err != nil {
		return err
	}

	fromModel(obj).CopyTo(record)

	return nil
}

// GetByTaskId implements task.Store.GetByTaskId
func (s *store) GetByTaskId(ctx context.Context, taskId string) (*task.Record, error) {
	obj, err := dbGetByTaskId(ctx, s.db, taskId)
	if err != nil {
		return nil, err
	}
	return fromModel(obj), nil
}

// GetAllReadyByState implements task.Store.GetAllReadyByState
func (s *store) GetAllReadyByState(ctx context.Context, state task.State, asOf time.Time, cursor query.Cursor, limit uint64, direction query.Ordering) ([]*task.Record, error) {
	models, err := dbGetAllReadyByState(ctx, s.db, state, asOf, cursor, limit, direction)
	if err != nil {
		return nil, err
	}

	res := make([]*task.Record, len(models))
	for i, model := range models {
		res[i] = fromModel(model)
	}
	return res, nil
}

// CountByState implements task.Store.CountByState
func (s *store) CountByState(ctx context.Context, state task.State) (uint64, error) {
	return dbCountByState(ctx, s.db, state)
}
