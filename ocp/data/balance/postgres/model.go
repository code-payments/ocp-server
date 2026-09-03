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
	tableName                   = "ocp__core_balance"
	externalCheckpointTableName = "ocp__core_externalbalancecheckpoint"

	allColumns = "id, token_account, owner_account, mint_account, quarks, usd_cost_basis, is_open, is_locked, is_backfilled, updated_at"
)

type model struct {
	Id sql.NullInt64 `db:"id"`

	TokenAccount string `db:"token_account"`
	OwnerAccount string `db:"owner_account"`
	MintAccount  string `db:"mint_account"`

	Quarks       int64 `db:"quarks"`
	UsdCostBasis int64 `db:"usd_cost_basis"`

	IsOpen       bool `db:"is_open"`
	IsLocked     bool `db:"is_locked"`
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
		IsLocked:     obj.IsLocked,
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
		IsLocked:     obj.IsLocked,
		IsBackfilled: obj.IsBackfilled,

		UpdatedAt: obj.UpdatedAt,
	}
}

func (m *model) dbCreate(ctx context.Context, db *sqlx.DB) error {
	return pgutil.ExecuteInTx(ctx, db, sql.LevelDefault, func(tx *sqlx.Tx) error {
		query := `INSERT INTO ` + tableName + `
			(token_account, owner_account, mint_account, quarks, usd_cost_basis, is_open, is_locked, is_backfilled, updated_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
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
			m.IsLocked,
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

func dbGetAllLockedByMint(ctx context.Context, db *sqlx.DB, mint string, minQuarks int64, cursor q.Cursor, limit uint64, direction q.Ordering) ([]*model, error) {
	res := []*model{}

	query := `SELECT ` + allColumns + ` FROM ` + tableName + `
		WHERE (mint_account = $1 AND quarks >= $2 AND is_locked)`
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

func dbMarkAsUnlocked(ctx context.Context, db *sqlx.DB, tokenAccount string) error {
	return pgutil.ExecuteInTx(ctx, db, sql.LevelDefault, func(tx *sqlx.Tx) error {
		query := `UPDATE ` + tableName + `
			SET is_locked = FALSE, updated_at = $2
			WHERE token_account = $1`
		sqlResult, err := tx.ExecContext(ctx, query, tokenAccount, time.Now().UTC())
		if err != nil {
			return err
		}
		rowsAffected, err := sqlResult.RowsAffected()
		if err != nil {
			return err
		}
		if rowsAffected == 0 {
			return balance.ErrRecordNotFound
		}
		return nil
	})
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
					WHERE token_account = $1 AND (NOT is_backfilled OR (is_locked AND quarks >= $2))`
				args = []any{delta.TokenAccount, int64(delta.Quarks), delta.UsdCostBasis, time.Now().UTC()}
			case balance.DeltaDrain:
				query = `UPDATE ` + tableName + `
					SET quarks = CASE WHEN is_backfilled THEN 0 ELSE quarks - $2 END,
					    usd_cost_basis = CASE WHEN is_backfilled THEN 0 ELSE usd_cost_basis - $3 END,
					    is_open = FALSE,
					    updated_at = $4
					WHERE token_account = $1 AND (NOT is_backfilled OR (is_open AND is_locked AND quarks = $2))`
				args = []any{delta.TokenAccount, int64(delta.Quarks), delta.UsdCostBasis, time.Now().UTC()}
			case balance.DeltaClose:
				query = `UPDATE ` + tableName + `
					SET is_open = FALSE, updated_at = $2
					WHERE token_account = $1 AND (NOT is_backfilled OR (is_open AND is_locked AND quarks = 0))`
				args = []any{delta.TokenAccount, time.Now().UTC()}
			case balance.DeltaAdjustUsdCostBasis:
				// No predicate: no quarks move, so nothing the other kinds
				// guard against can happen here
				query = `UPDATE ` + tableName + `
					SET usd_cost_basis = usd_cost_basis + $2, updated_at = $3
					WHERE token_account = $1`
				args = []any{delta.TokenAccount, delta.UsdCostBasis, time.Now().UTC()}
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
				return balance.ErrRecordNotFound
			} else if err != nil {
				return err
			}
			return classifyFailedDelta(delta, fromModel(&current))
		}
		return nil
	})
}

func classifyFailedDelta(delta *balance.Delta, current *balance.Record) error {
	// A credit doesn't require the vault to be locked, so a closed account is
	// the only thing that turns one away.
	if delta.Kind == balance.DeltaCredit {
		return balance.ErrAccountClosed
	}

	if !current.IsLocked {
		return balance.ErrAccountUnlocked
	}
	switch delta.Kind {
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
				SET quarks = $2, usd_cost_basis = $3, is_open = $4, is_locked = $5, is_backfilled = TRUE, updated_at = $6
				WHERE token_account = $1`
			_, err = tx.ExecContext(ctx, query, tokenAccount, result.Quarks, result.UsdCostBasis, result.IsOpen, result.IsLocked, time.Now().UTC())
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

type externalCheckpointModel struct {
	Id sql.NullInt64 `db:"id"`

	TokenAccount   string `db:"token_account"`
	Quarks         uint64 `db:"quarks"`
	SlotCheckpoint uint64 `db:"slot_checkpoint"`

	LastUpdatedAt time.Time `db:"last_updated_at"`
}

func toExternalCheckpointModel(obj *balance.ExternalCheckpointRecord) (*externalCheckpointModel, error) {
	if err := obj.Validate(); err != nil {
		return nil, err
	}

	return &externalCheckpointModel{
		TokenAccount:   obj.TokenAccount,
		Quarks:         obj.Quarks,
		SlotCheckpoint: obj.SlotCheckpoint,
		LastUpdatedAt:  obj.LastUpdatedAt,
	}, nil
}

func fromExternalCheckpoingModel(obj *externalCheckpointModel) *balance.ExternalCheckpointRecord {
	return &balance.ExternalCheckpointRecord{
		Id:             uint64(obj.Id.Int64),
		TokenAccount:   obj.TokenAccount,
		Quarks:         obj.Quarks,
		SlotCheckpoint: obj.SlotCheckpoint,
		LastUpdatedAt:  obj.LastUpdatedAt,
	}
}

func (m *externalCheckpointModel) dbSave(ctx context.Context, db *sqlx.DB) error {
	return pgutil.ExecuteInTx(ctx, db, sql.LevelDefault, func(tx *sqlx.Tx) error {
		query := `INSERT INTO ` + externalCheckpointTableName + `
			(token_account, quarks, slot_checkpoint, last_updated_at)
			VALUES ($1, $2, $3, $4)

			ON CONFLICT (token_account)
			DO UPDATE
				SET quarks = $2, slot_checkpoint = $3, last_updated_at = $4
				WHERE ` + externalCheckpointTableName + `.token_account = $1 AND ` + externalCheckpointTableName + `.slot_checkpoint < $3

			RETURNING
				id, token_account, quarks, slot_checkpoint, last_updated_at`

		m.LastUpdatedAt = time.Now()

		err := tx.QueryRowxContext(
			ctx,
			query,
			m.TokenAccount,
			m.Quarks,
			m.SlotCheckpoint,
			m.LastUpdatedAt.UTC(),
		).StructScan(m)

		return pgutil.CheckNoRows(err, balance.ErrStaleCheckpoint)
	})
}

func dbGetExternalCheckpoint(ctx context.Context, db *sqlx.DB, account string) (*externalCheckpointModel, error) {
	res := &externalCheckpointModel{}

	query := `SELECT id, token_account, quarks, slot_checkpoint, last_updated_at FROM ` + externalCheckpointTableName + `
		WHERE token_account = $1
		LIMIT 1`

	err := db.GetContext(ctx, res, query, account)
	if err != nil {
		return nil, pgutil.CheckNoRows(err, balance.ErrCheckpointNotFound)
	}
	return res, nil
}
