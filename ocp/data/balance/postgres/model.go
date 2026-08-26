package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"

	pgutil "github.com/code-payments/ocp-server/database/postgres"
	q "github.com/code-payments/ocp-server/database/query"
	"github.com/code-payments/ocp-server/ocp/data/balance"
)

const (
	tableName = "ocp__core_balance"

	allColumns = "id, token_account, owner_account, mint_account, quarks, usd_cost_basis, is_open, is_backfilled, updated_at"
)

type model struct {
	Id sql.NullInt64 `db:"id"`

	TokenAccount string `db:"token_account"`
	OwnerAccount string `db:"owner_account"`
	MintAccount  string `db:"mint_account"`

	Quarks       int64 `db:"quarks"`
	UsdCostBasis int64 `db:"usd_cost_basis"`

	IsOpen       bool `db:"is_open"`
	IsBackfilled bool `db:"is_backfilled"`

	UpdatedAt time.Time `db:"updated_at"`
}

func toModel(obj *balance.Record) (*model, error) {
	if err := obj.Validate(); err != nil {
		return nil, err
	}

	return &model{
		TokenAccount: obj.TokenAccount,
		OwnerAccount: obj.OwnerAccount,
		MintAccount:  obj.MintAccount,

		Quarks:       obj.Quarks,
		UsdCostBasis: obj.UsdCostBasis,

		IsOpen:       obj.IsOpen,
		IsBackfilled: obj.IsBackfilled,

		UpdatedAt: obj.UpdatedAt,
	}, nil
}

func fromModel(obj *model) *balance.Record {
	return &balance.Record{
		Id: uint64(obj.Id.Int64),

		TokenAccount: obj.TokenAccount,
		OwnerAccount: obj.OwnerAccount,
		MintAccount:  obj.MintAccount,

		Quarks:       obj.Quarks,
		UsdCostBasis: obj.UsdCostBasis,

		IsOpen:       obj.IsOpen,
		IsBackfilled: obj.IsBackfilled,

		UpdatedAt: obj.UpdatedAt,
	}
}

func (m *model) dbCreate(ctx context.Context, db *sqlx.DB) error {
	return pgutil.ExecuteInTx(ctx, db, sql.LevelDefault, func(tx *sqlx.Tx) error {
		query := `INSERT INTO ` + tableName + `
			(token_account, owner_account, mint_account, quarks, usd_cost_basis, is_open, is_backfilled, updated_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
			RETURNING ` + allColumns

		m.UpdatedAt = time.Now()

		err := tx.QueryRowxContext(
			ctx,
			query,
			m.TokenAccount,
			m.OwnerAccount,
			m.MintAccount,
			m.Quarks,
			m.UsdCostBasis,
			m.IsOpen,
			m.IsBackfilled,
			m.UpdatedAt.UTC(),
		).StructScan(m)

		return pgutil.CheckUniqueViolation(err, balance.ErrRecordExists)
	})
}

func dbGet(ctx context.Context, db *sqlx.DB, tokenAccount string) (*model, error) {
	res := &model{}

	query := `SELECT ` + allColumns + ` FROM ` + tableName + `
		WHERE token_account = $1`

	err := pgutil.ExecuteInTx(ctx, db, sql.LevelDefault, func(tx *sqlx.Tx) error {
		return tx.GetContext(ctx, res, query, tokenAccount)
	})
	if err != nil {
		return nil, pgutil.CheckNoRows(err, balance.ErrRecordNotFound)
	}
	return res, nil
}

func dbGetBatch(ctx context.Context, db *sqlx.DB, tokenAccounts ...string) ([]*model, error) {
	res := []*model{}
	if len(tokenAccounts) == 0 {
		return res, nil
	}

	query := `SELECT ` + allColumns + ` FROM ` + tableName + `
		WHERE token_account = ANY($1)`

	err := pgutil.ExecuteInTx(ctx, db, sql.LevelDefault, func(tx *sqlx.Tx) error {
		return tx.SelectContext(ctx, &res, query, tokenAccounts)
	})
	if err != nil && !pgutil.IsNoRows(err) {
		return nil, err
	}
	return res, nil
}

func dbGetAllByOwner(ctx context.Context, db *sqlx.DB, owner string, mint *string) ([]*model, error) {
	res := []*model{}

	query := `SELECT ` + allColumns + ` FROM ` + tableName + `
		WHERE owner_account = $1`
	args := []any{owner}
	if mint != nil {
		query += ` AND mint_account = $2`
		args = append(args, *mint)
	}
	query += ` ORDER BY id ASC`

	err := pgutil.ExecuteInTx(ctx, db, sql.LevelDefault, func(tx *sqlx.Tx) error {
		return tx.SelectContext(ctx, &res, query, args...)
	})
	if err != nil {
		return nil, pgutil.CheckNoRows(err, balance.ErrRecordNotFound)
	}
	if len(res) == 0 {
		return nil, balance.ErrRecordNotFound
	}
	return res, nil
}

func dbGetAllByMint(ctx context.Context, db *sqlx.DB, mint string, minQuarks int64, cursor q.Cursor, limit uint64, direction q.Ordering) ([]*model, error) {
	res := []*model{}

	query := `SELECT ` + allColumns + ` FROM ` + tableName + `
		WHERE (mint_account = $1 AND quarks >= $2)`
	query, args := q.PaginateQuery(query, []any{mint, minQuarks}, cursor, limit, direction)

	err := pgutil.ExecuteInTx(ctx, db, sql.LevelDefault, func(tx *sqlx.Tx) error {
		return tx.SelectContext(ctx, &res, query, args...)
	})
	if err != nil {
		return nil, pgutil.CheckNoRows(err, balance.ErrRecordNotFound)
	}
	if len(res) == 0 {
		return nil, balance.ErrRecordNotFound
	}
	return res, nil
}

// dbApplyDeltas applies every delta in a single transaction. Each delta is one
// conditional UPDATE, so its predicate is evaluated against the row after the
// row lock is acquired. Predicates only apply to backfilled rows.
func dbApplyDeltas(ctx context.Context, db *sqlx.DB, deltas []*balance.Delta) error {
	return pgutil.ExecuteInTx(ctx, db, sql.LevelDefault, func(tx *sqlx.Tx) error {
		for _, delta := range deltas {
			var query string
			var args []any
			switch delta.Kind {
			case balance.DeltaCredit:
				query = `UPDATE ` + tableName + `
					SET quarks = quarks + $2, usd_cost_basis = usd_cost_basis + $3, updated_at = $4
					WHERE token_account = $1 AND (NOT is_backfilled OR is_open)`
				args = []any{delta.TokenAccount, int64(delta.Quarks), delta.UsdCostBasis, time.Now().UTC()}
			case balance.DeltaDebit:
				query = `UPDATE ` + tableName + `
					SET quarks = quarks - $2, usd_cost_basis = usd_cost_basis - $3, updated_at = $4
					WHERE token_account = $1 AND (NOT is_backfilled OR quarks >= $2)`
				args = []any{delta.TokenAccount, int64(delta.Quarks), delta.UsdCostBasis, time.Now().UTC()}
			case balance.DeltaDrain:
				query = `UPDATE ` + tableName + `
					SET quarks = CASE WHEN is_backfilled THEN 0 ELSE quarks - $2 END,
					    usd_cost_basis = CASE WHEN is_backfilled THEN 0 ELSE usd_cost_basis - $3 END,
					    is_open = FALSE,
					    updated_at = $4
					WHERE token_account = $1 AND (NOT is_backfilled OR (is_open AND quarks = $2))`
				args = []any{delta.TokenAccount, int64(delta.Quarks), delta.UsdCostBasis, time.Now().UTC()}
			case balance.DeltaClose:
				query = `UPDATE ` + tableName + `
					SET is_open = FALSE, updated_at = $2
					WHERE token_account = $1 AND (NOT is_backfilled OR (is_open AND quarks = 0))`
				args = []any{delta.TokenAccount, time.Now().UTC()}
			default:
				return fmt.Errorf("unsupported delta kind: %s", delta.Kind)
			}

			sqlResult, err := tx.ExecContext(ctx, query, args...)
			if err != nil {
				return err
			}
			rowsAffected, err := sqlResult.RowsAffected()
			if err != nil {
				return err
			}
			if rowsAffected == 1 {
				continue
			}

			// Either the predicate failed or there is no record. Classify which.
			var current model
			err = tx.GetContext(ctx, &current, `SELECT `+allColumns+` FROM `+tableName+` WHERE token_account = $1`, delta.TokenAccount)
			if pgutil.IsNoRows(err) {
				continue // Not an account we track
			} else if err != nil {
				return err
			}
			return classifyFailedDelta(delta, fromModel(&current))
		}
		return nil
	})
}

func classifyFailedDelta(delta *balance.Delta, current *balance.Record) error {
	switch delta.Kind {
	case balance.DeltaCredit:
		return balance.ErrAccountClosed
	case balance.DeltaDebit:
		return balance.ErrInsufficientBalance
	case balance.DeltaDrain, balance.DeltaClose:
		if !current.IsOpen {
			return balance.ErrAccountClosed
		}
		return balance.ErrBalanceChanged
	}
	return fmt.Errorf("unsupported delta kind: %s", delta.Kind)
}

func dbBackfill(ctx context.Context, db *sqlx.DB, tokenAccount string, fn balance.BackfillFunc) error {
	return executeTxWithinCtxOrJoin(ctx, db, func(ctx context.Context) error {
		return pgutil.ExecuteInTx(ctx, db, sql.LevelDefault, func(tx *sqlx.Tx) error {
			var current model
			err := tx.GetContext(ctx, &current, `SELECT `+allColumns+` FROM `+tableName+` WHERE token_account = $1 FOR UPDATE`, tokenAccount)
			if err != nil {
				return pgutil.CheckNoRows(err, balance.ErrRecordNotFound)
			}
			if current.IsBackfilled {
				return balance.ErrAlreadyBackfilled
			}

			// The row lock is held across fn, so it observes every committed
			// write to the account and blocks every in-flight one.
			result, err := fn(ctx)
			if err != nil {
				return err
			}
			if result.Quarks < 0 {
				return balance.ErrNegativeBalance
			}

			query := `UPDATE ` + tableName + `
				SET quarks = $2, usd_cost_basis = $3, is_open = $4, is_backfilled = TRUE, updated_at = $5
				WHERE token_account = $1`
			_, err = tx.ExecContext(ctx, query, tokenAccount, result.Quarks, result.UsdCostBasis, result.IsOpen, time.Now().UTC())
			return err
		})
	})
}

// executeTxWithinCtxOrJoin runs fn with a context carrying a DB transaction,
// starting one if the context doesn't already have one.
func executeTxWithinCtxOrJoin(ctx context.Context, db *sqlx.DB, fn func(ctx context.Context) error) error {
	err := pgutil.ExecuteTxWithinCtx(ctx, db, sql.LevelDefault, fn)
	if err == pgutil.ErrAlreadyInTx {
		return fn(ctx)
	}
	return err
}
