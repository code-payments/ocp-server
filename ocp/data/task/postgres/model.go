package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"

	pgutil "github.com/code-payments/ocp-server/database/postgres"
	q "github.com/code-payments/ocp-server/database/query"
	"github.com/code-payments/ocp-server/ocp/data/task"
)

const (
	tableName = "ocp__core_task"
)

type model struct {
	Id             sql.NullInt64  `db:"id"`
	TaskId         string         `db:"task_id"`
	TaskType       uint32         `db:"task_type"`
	Data           []byte         `db:"data"`
	ReferenceId    sql.NullString `db:"reference_id"`
	State          uint8          `db:"state"`
	FailedAttempts uint32         `db:"failed_attempts"`
	NextAttemptAt  time.Time      `db:"next_attempt_at"`
	Version        uint64         `db:"version"`
	CreatedAt      time.Time      `db:"created_at"`
}

func toModel(obj *task.Record) (*model, error) {
	if err := obj.Validate(); err != nil {
		return nil, err
	}

	if obj.CreatedAt.IsZero() {
		obj.CreatedAt = time.Now().UTC()
	}

	if obj.NextAttemptAt.IsZero() {
		obj.NextAttemptAt = obj.CreatedAt
	}

	var referenceId sql.NullString
	if obj.ReferenceId != nil {
		referenceId = sql.NullString{String: *obj.ReferenceId, Valid: true}
	}

	return &model{
		Id:             sql.NullInt64{Int64: int64(obj.Id), Valid: true},
		TaskId:         obj.TaskId,
		TaskType:       obj.Type,
		Data:           obj.Data,
		ReferenceId:    referenceId,
		State:          uint8(obj.State),
		FailedAttempts: obj.FailedAttempts,
		NextAttemptAt:  obj.NextAttemptAt,
		Version:        obj.Version,
		CreatedAt:      obj.CreatedAt,
	}, nil
}

func fromModel(m *model) *task.Record {
	var referenceId *string
	if m.ReferenceId.Valid {
		value := m.ReferenceId.String
		referenceId = &value
	}

	return &task.Record{
		Id:             uint64(m.Id.Int64),
		TaskId:         m.TaskId,
		Type:           m.TaskType,
		Data:           m.Data,
		ReferenceId:    referenceId,
		State:          task.State(m.State),
		FailedAttempts: m.FailedAttempts,
		NextAttemptAt:  m.NextAttemptAt,
		Version:        m.Version,
		CreatedAt:      m.CreatedAt,
	}
}

func dbPutAllInTx(ctx context.Context, tx *sqlx.Tx, models []*model) ([]*model, error) {
	var res []*model

	query := `INSERT INTO ` + tableName + ` (task_id, task_type, data, reference_id, state, failed_attempts, next_attempt_at, version, created_at) VALUES `

	var parameters []interface{}
	for i, model := range models {
		baseIndex := len(parameters)
		query += fmt.Sprintf(
			`($%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d + 1, $%d)`,
			baseIndex+1, baseIndex+2, baseIndex+3, baseIndex+4, baseIndex+5, baseIndex+6, baseIndex+7, baseIndex+8, baseIndex+9,
		)

		if i != len(models)-1 {
			query += ","
		}

		parameters = append(
			parameters,
			model.TaskId,
			model.TaskType,
			model.Data,
			model.ReferenceId,
			model.State,
			model.FailedAttempts,
			model.NextAttemptAt,
			model.Version,
			model.CreatedAt,
		)
	}

	query += ` RETURNING id, task_id, task_type, data, reference_id, state, failed_attempts, next_attempt_at, version, created_at`

	err := tx.SelectContext(
		ctx,
		&res,
		query,
		parameters...,
	)
	if err != nil {
		return nil, pgutil.CheckUniqueViolation(err, task.ErrExists)
	}

	return res, nil
}

func (m *model) dbUpdate(ctx context.Context, db *sqlx.DB) error {
	return pgutil.ExecuteInTx(ctx, db, sql.LevelDefault, func(tx *sqlx.Tx) error {
		query := `UPDATE ` + tableName + `
			SET state = $3, failed_attempts = $4, next_attempt_at = $5, version = version + 1
			WHERE task_id = $1 AND version = $2
			RETURNING id, task_id, task_type, data, reference_id, state, failed_attempts, next_attempt_at, version, created_at`

		err := tx.QueryRowxContext(
			ctx,
			query,
			m.TaskId,
			m.Version,
			m.State,
			m.FailedAttempts,
			m.NextAttemptAt,
		).StructScan(m)
		if err != nil {
			return pgutil.CheckNoRows(err, task.ErrStaleVersion)
		}

		return nil
	})
}

func dbGetByTaskId(ctx context.Context, db *sqlx.DB, taskId string) (*model, error) {
	res := &model{}

	query := `SELECT id, task_id, task_type, data, reference_id, state, failed_attempts, next_attempt_at, version, created_at
		FROM ` + tableName + `
		WHERE task_id = $1
		LIMIT 1`

	err := db.GetContext(ctx, res, query, taskId)
	if err != nil {
		return nil, pgutil.CheckNoRows(err, task.ErrNotFound)
	}
	return res, nil
}

func dbGetAllReadyByState(ctx context.Context, db *sqlx.DB, state task.State, asOf time.Time, cursor q.Cursor, limit uint64, direction q.Ordering) ([]*model, error) {
	res := []*model{}

	query := `SELECT
		id, task_id, task_type, data, reference_id, state, failed_attempts, next_attempt_at, version, created_at
		FROM ` + tableName + `
		WHERE state = $1 AND next_attempt_at <= $2`

	opts := []interface{}{state, asOf}
	query, opts = q.PaginateQuery(query, opts, cursor, limit, direction)

	err := db.SelectContext(ctx, &res, query, opts...)
	if err != nil {
		return nil, pgutil.CheckNoRows(err, task.ErrNotFound)
	}

	if len(res) == 0 {
		return nil, task.ErrNotFound
	}
	return res, nil
}

func dbCountByState(ctx context.Context, db *sqlx.DB, state task.State) (uint64, error) {
	var res uint64
	query := `SELECT COUNT(*) FROM ` + tableName + ` WHERE state = $1`
	err := db.GetContext(ctx, &res, query, state)
	if err != nil {
		return 0, err
	}
	return res, nil
}
