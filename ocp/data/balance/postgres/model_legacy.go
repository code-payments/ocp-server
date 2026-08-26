package postgres

import (
	"context"
	"database/sql"
	"errors"

	"github.com/jmoiron/sqlx"

	pgutil "github.com/code-payments/ocp-server/database/postgres"
	"github.com/code-payments/ocp-server/ocp/data/balance"
)

const (
	cachedBalanceVersionTableName = "ocp__core_cachedbalanceversion"
	openCloseLocksTableName       = "ocp__core_opencloselocks"
)

func dbGetCachedVersion(ctx context.Context, db *sqlx.DB, account string) (uint64, error) {
	var res uint64
	err := pgutil.ExecuteInTx(ctx, db, sql.LevelDefault, func(tx *sqlx.Tx) error {
		insertQuery := `INSERT INTO ` + cachedBalanceVersionTableName + `
			(token_account, version)
			VALUES($1, 0)
			ON CONFLICT DO NOTHING
		`
		sqlResult, err := tx.ExecContext(ctx, insertQuery, account)
		if err != nil {
			return err
		}
		rowsAffected, err := sqlResult.RowsAffected()
		if err != nil {
			return err
		}
		if rowsAffected == 1 {
			res = 0
			return nil
		}

		selectQuery := `SELECT version FROM ` + cachedBalanceVersionTableName + `
			WHERE token_account = $1
			FOR UPDATE`
		return db.GetContext(ctx, &res, selectQuery, account)
	})
	return res, err

}

func dbAdvanceCachedVersion(ctx context.Context, db *sqlx.DB, account string, currentVersion uint64) error {
	return pgutil.ExecuteInTx(ctx, db, sql.LevelDefault, func(tx *sqlx.Tx) error {
		var res uint64
		query := `UPDATE ` + cachedBalanceVersionTableName + `
			SET version = version + 1
			WHERE token_account = $1 AND version = $2
			RETURNING version
		`
		err := tx.GetContext(ctx, &res, query, account, currentVersion)
		if pgutil.IsNoRows(err) || pgutil.IsUniqueViolation(err) {
			return balance.ErrStaleCachedBalanceVersion
		}
		if err != nil {
			return err
		}
		return nil
	})

}

func dbCheckNotClosed(ctx context.Context, db *sqlx.DB, account string) error {
	return pgutil.ExecuteInTx(ctx, db, sql.LevelDefault, func(tx *sqlx.Tx) error {
		insertQuery := `INSERT INTO ` + openCloseLocksTableName + `
			(token_account, is_open)
			VALUES ($1, TRUE)
			ON CONFLICT DO NOTHING
		`

		_, err := tx.ExecContext(ctx, insertQuery, account)
		if err != nil {
			return err
		}

		selectQuery := `SELECT is_open FROM ` + openCloseLocksTableName + `
			WHERE token_account = $1
			FOR UPDATE
		`
		var isOpen bool
		err = tx.GetContext(ctx, &isOpen, selectQuery, account)
		if err != nil {
			return err
		}
		if !isOpen {
			return balance.ErrAccountClosed
		}
		return nil
	})
}

func dbMarkAsClosed(ctx context.Context, db *sqlx.DB, account string) error {
	return pgutil.ExecuteInTx(ctx, db, sql.LevelDefault, func(tx *sqlx.Tx) error {
		query := `INSERT INTO ` + openCloseLocksTableName + `
			(token_account, is_open)
			VALUES ($1, FALSE)

			ON CONFLICT (token_account)
			DO UPDATE
				SET is_open = FALSE
				WHERE ` + openCloseLocksTableName + `.token_account = $1 

			RETURNING is_open
		`
		var isOpen bool
		err := tx.GetContext(ctx, &isOpen, query, account)
		if err != nil {
			return err
		}
		if isOpen {
			return errors.New("unexpected state transition")
		}
		return nil
	})
}
