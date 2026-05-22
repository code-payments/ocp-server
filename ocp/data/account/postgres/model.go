package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"

	commonpb "github.com/code-payments/ocp-protobuf-api/generated/go/common/v1"

	"github.com/code-payments/ocp-server/ocp/data/account"
	pgutil "github.com/code-payments/ocp-server/database/postgres"
)

const (
	tableName = "ocp__core_accountinfo"
)

type model struct {
	Id sql.NullInt64 `db:"id"`

	OwnerAccount     string `db:"owner_account"`
	AuthorityAccount string `db:"authority_account"`
	TokenAccount     string `db:"token_account"`
	MintAccount      string `db:"mint_account"`

	AccountType int    `db:"account_type"`
	Index       uint64 `db:"index"`

	RequiresDepositSync  bool      `db:"requires_deposit_sync"`
	DepositsLastSyncedAt time.Time `db:"deposits_last_synced_at"`

	RequiresAutoReturnCheck bool `db:"requires_auto_return_check"`

	Balance sql.NullInt64 `db:"balance"`

	CreatedAt time.Time `db:"created_at"`
}

func toModel(obj *account.Record) (*model, error) {
	if err := obj.Validate(); err != nil {
		return nil, err
	}

	balance := sql.NullInt64{}
	if obj.Balance != nil {
		balance.Int64 = int64(*obj.Balance)
		balance.Valid = true
	}

	return &model{
		OwnerAccount:     obj.OwnerAccount,
		AuthorityAccount: obj.AuthorityAccount,
		TokenAccount:     obj.TokenAccount,
		MintAccount:      obj.MintAccount,

		AccountType: int(obj.AccountType),
		Index:       obj.Index,

		RequiresDepositSync:  obj.RequiresDepositSync,
		DepositsLastSyncedAt: obj.DepositsLastSyncedAt.UTC(),

		RequiresAutoReturnCheck: obj.RequiresAutoReturnCheck,

		Balance: balance,

		CreatedAt: obj.CreatedAt.UTC(),
	}, nil
}

func fromModel(obj *model) *account.Record {
	var balance *uint64
	if obj.Balance.Valid {
		quarks := uint64(obj.Balance.Int64)
		balance = &quarks
	}

	return &account.Record{
		Id: uint64(obj.Id.Int64),

		OwnerAccount:     obj.OwnerAccount,
		AuthorityAccount: obj.AuthorityAccount,
		TokenAccount:     obj.TokenAccount,
		MintAccount:      obj.MintAccount,

		AccountType: commonpb.AccountType(obj.AccountType),
		Index:       obj.Index,

		RequiresDepositSync:  obj.RequiresDepositSync,
		DepositsLastSyncedAt: obj.DepositsLastSyncedAt,

		RequiresAutoReturnCheck: obj.RequiresAutoReturnCheck,

		Balance: balance,

		CreatedAt: obj.CreatedAt,
	}
}

func (m *model) dbInsert(ctx context.Context, db *sqlx.DB) error {
	// A newly created account has no history, so its balance is always zero.
	// Reject any attempt to create one with a pre-existing balance, then default
	// the balance to zero.
	if m.Balance.Valid && m.Balance.Int64 != 0 {
		return account.ErrInvalidAccountInfo
	}
	m.Balance = sql.NullInt64{Int64: 0, Valid: true}

	return pgutil.ExecuteInTx(ctx, db, sql.LevelDefault, func(tx *sqlx.Tx) error {
		if m.CreatedAt.IsZero() {
			m.CreatedAt = time.Now()
		}

		query := `INSERT INTO ` + tableName + `
			(owner_account, authority_account, token_account, mint_account, account_type, index, requires_deposit_sync, deposits_last_synced_at, requires_auto_return_check, balance, created_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
			RETURNING id, owner_account, authority_account, token_account, mint_account, account_type, index, requires_deposit_sync, deposits_last_synced_at, requires_auto_return_check, balance, created_at
		`
		err := tx.QueryRowxContext(
			ctx,
			query,
			m.OwnerAccount,
			m.AuthorityAccount,
			m.TokenAccount,
			m.MintAccount,
			m.AccountType,
			m.Index,
			m.RequiresDepositSync,
			m.DepositsLastSyncedAt,
			m.RequiresAutoReturnCheck,
			m.Balance,
			m.CreatedAt,
		).StructScan(m)
		if err == nil {
			return nil
		}

		// There are multiple unique violations, which may indicate we have something
		// invalid or the record exists as is. We need to query it to see what's
		// up and return the right error code.
		if pgutil.IsUniqueViolation(err) {
			existingModel, err := dbGetByTokenAddress(ctx, db, m.TokenAccount)
			if err == account.ErrAccountInfoNotFound {
				return account.ErrInvalidAccountInfo
			} else if err != nil {
				return err
			}

			if equivalentModels(existingModel, m) {
				return account.ErrAccountInfoExists
			}
			return account.ErrInvalidAccountInfo
		}

		return err
	})
}

func (m *model) dbUpdate(ctx context.Context, db *sqlx.DB) error {
	query := `UPDATE ` + tableName + `
		SET requires_deposit_sync = $2, deposits_last_synced_at = $3, requires_auto_return_check = $4
		WHERE token_account = $1
		RETURNING id, owner_account, authority_account, token_account, mint_account, account_type, index, requires_deposit_sync, deposits_last_synced_at, requires_auto_return_check, balance, created_at
	`

	err := db.QueryRowxContext(
		ctx,
		query,
		m.TokenAccount,
		m.RequiresDepositSync,
		m.DepositsLastSyncedAt,
		m.RequiresAutoReturnCheck,
	).StructScan(m)

	if err != nil {
		return pgutil.CheckNoRows(err, account.ErrAccountInfoNotFound)
	}
	return nil
}

func dbGetByTokenAddress(ctx context.Context, db *sqlx.DB, address string) (*model, error) {
	res := &model{}

	query := `SELECT id, owner_account, authority_account, token_account, mint_account, account_type, index, requires_deposit_sync, deposits_last_synced_at, requires_auto_return_check, balance, created_at FROM ` + tableName + `
		WHERE token_account = $1
	`

	err := db.QueryRowxContext(
		ctx,
		query,
		address,
	).StructScan(res)

	if err != nil {
		return nil, pgutil.CheckNoRows(err, account.ErrAccountInfoNotFound)
	}
	return res, nil
}

func dbGetByTokenAddressBatch(ctx context.Context, db *sqlx.DB, addresses ...string) ([]*model, error) {
	res := []*model{}

	individualFilters := make([]string, len(addresses))
	for i, address := range addresses {
		individualFilters[i] = fmt.Sprintf("'%s'", address)
	}

	query := fmt.Sprintf(
		`SELECT id, owner_account, authority_account, token_account, mint_account, account_type, index, requires_deposit_sync, deposits_last_synced_at, requires_auto_return_check, balance, created_at FROM `+tableName+`
		WHERE token_account IN (%s)`,
		strings.Join(individualFilters, ", "),
	)

	err := db.SelectContext(ctx, &res, query)
	if err != nil {
		return nil, pgutil.CheckNoRows(err, account.ErrAccountInfoNotFound)
	}
	if len(res) != len(addresses) {
		return nil, account.ErrAccountInfoNotFound
	}
	return res, nil
}

func dbGetByAuthorityAddress(ctx context.Context, db *sqlx.DB, address string) ([]*model, error) {
	var res []*model

	query := `SELECT id, owner_account, authority_account, token_account, mint_account, account_type, index, requires_deposit_sync, deposits_last_synced_at, requires_auto_return_check, balance, created_at FROM ` + tableName + `
		WHERE authority_account = $1
	`

	err := db.SelectContext(
		ctx,
		&res,
		query,
		address,
	)
	if err != nil {
		return nil, pgutil.CheckNoRows(err, account.ErrAccountInfoNotFound)
	}
	if len(res) == 0 {
		return nil, account.ErrAccountInfoNotFound
	}
	return res, nil
}

func dbGetLatestByOwnerAddress(ctx context.Context, db *sqlx.DB, address string) ([]*model, error) {
	var res1 []*model

	query1 := `SELECT DISTINCT ON (mint_account, account_type) id, owner_account, authority_account, token_account, mint_account, account_type, index, requires_deposit_sync, deposits_last_synced_at, requires_auto_return_check, balance, created_at FROM ` + tableName + `
		WHERE owner_account = $1 AND account_type NOT IN ($2)
		ORDER BY mint_account, account_type, index DESC
	`

	err := db.SelectContext(
		ctx,
		&res1,
		query1,
		address,
		commonpb.AccountType_POOL,
	)
	if err != nil && !pgutil.IsNoRows(err) {
		return nil, err
	}

	var res2 []*model

	query2 := `SELECT id, owner_account, authority_account, token_account, mint_account, account_type, index, requires_deposit_sync, deposits_last_synced_at, requires_auto_return_check, balance, created_at FROM ` + tableName + `
		WHERE owner_account = $1 AND account_type IN ($2)
		ORDER BY index ASC
	`
	err = db.SelectContext(
		ctx,
		&res2,
		query2,
		address,
		commonpb.AccountType_POOL,
	)
	if err != nil && !pgutil.IsNoRows(err) {
		return nil, err
	}

	var res []*model
	res = append(res, res1...)
	res = append(res, res2...)
	if len(res) == 0 {
		return nil, account.ErrAccountInfoNotFound
	}
	return res, nil
}

func dbGetLatestByOwnerAddressAndType(ctx context.Context, db *sqlx.DB, address string, accountType commonpb.AccountType) ([]*model, error) {
	var res []*model

	query := `SELECT DISTINCT ON (mint_account) id, owner_account, authority_account, token_account, mint_account, account_type, index, requires_deposit_sync, deposits_last_synced_at, requires_auto_return_check, balance, created_at FROM ` + tableName + `
		WHERE owner_account = $1 AND account_type = $2
		ORDER BY mint_account, index DESC
	`

	err := db.SelectContext(
		ctx,
		&res,
		query,
		address,
		accountType,
	)
	if err != nil {
		return nil, pgutil.CheckNoRows(err, account.ErrAccountInfoNotFound)
	}
	if len(res) == 0 {
		return nil, account.ErrAccountInfoNotFound
	}
	return res, nil
}

func dbGetByMintAndType(ctx context.Context, db *sqlx.DB, mint string, accountType commonpb.AccountType) ([]*model, error) {
	var res []*model

	query := `SELECT id, owner_account, authority_account, token_account, mint_account, account_type, index, requires_deposit_sync, deposits_last_synced_at, requires_auto_return_check, balance, created_at FROM ` + tableName + `
		WHERE mint_account = $1 AND account_type = $2
		ORDER BY index ASC
	`

	err := db.SelectContext(
		ctx,
		&res,
		query,
		mint,
		accountType,
	)
	if err != nil {
		return nil, pgutil.CheckNoRows(err, account.ErrAccountInfoNotFound)
	}
	if len(res) == 0 {
		return nil, account.ErrAccountInfoNotFound
	}
	return res, nil
}

func dbGetPrioritizedRequiringDepositSync(ctx context.Context, db *sqlx.DB, limit uint64) ([]*model, error) {
	var res []*model

	query := `SELECT id, owner_account, authority_account, token_account, mint_account, account_type, index, requires_deposit_sync, deposits_last_synced_at, requires_auto_return_check, balance, created_at FROM ` + tableName + `
		WHERE requires_deposit_sync = TRUE
		ORDER BY deposits_last_synced_at ASC
		LIMIT $1
	`
	err := db.SelectContext(
		ctx,
		&res,
		query,
		limit,
	)
	if err != nil {
		return nil, pgutil.CheckNoRows(err, account.ErrAccountInfoNotFound)
	}

	if len(res) == 0 {
		return nil, account.ErrAccountInfoNotFound
	}
	return res, nil
}

func dbCountRequiringDepositSync(ctx context.Context, db *sqlx.DB) (uint64, error) {
	var res uint64

	query := `SELECT COUNT(*) FROM ` + tableName + `
		WHERE requires_deposit_sync = TRUE
	`

	err := db.GetContext(ctx, &res, query)
	if err != nil {
		return 0, err
	}

	return res, nil
}

func dbGetPrioritizedRequiringAutoReturnChecks(ctx context.Context, db *sqlx.DB, minAge time.Duration, limit uint64) ([]*model, error) {
	var res []*model

	query := `SELECT id, owner_account, authority_account, token_account, mint_account, account_type, index, requires_deposit_sync, deposits_last_synced_at, requires_auto_return_check, balance, created_at FROM ` + tableName + `
		WHERE requires_auto_return_check = TRUE AND created_at <= $1
		ORDER BY created_at ASC
		LIMIT $2
	`
	err := db.SelectContext(
		ctx,
		&res,
		query,
		time.Now().Add(-minAge),
		limit,
	)
	if err != nil {
		return nil, pgutil.CheckNoRows(err, account.ErrAccountInfoNotFound)
	}

	if len(res) == 0 {
		return nil, account.ErrAccountInfoNotFound
	}
	return res, nil
}

func dbCountRequiringAutoReturnCheck(ctx context.Context, db *sqlx.DB) (uint64, error) {
	var res uint64

	query := `SELECT COUNT(*) FROM ` + tableName + `
		WHERE requires_auto_return_check = TRUE
	`

	err := db.GetContext(ctx, &res, query)
	if err != nil {
		return 0, err
	}

	return res, nil
}

func dbGetBalanceForUpdate(ctx context.Context, db *sqlx.DB, tokenAccount string) (*uint64, error) {
	var balance sql.NullInt64
	var found bool
	err := pgutil.ExecuteInTx(ctx, db, sql.LevelDefault, func(tx *sqlx.Tx) error {
		query := `SELECT balance FROM ` + tableName + `
			WHERE token_account = $1
			FOR UPDATE
		`
		err := tx.QueryRowxContext(ctx, query, tokenAccount).Scan(&balance)
		if pgutil.IsNoRows(err) {
			// Not an error: the account simply has no row. Returning nil here
			// keeps pgutil.ExecuteInTx from treating it as a failure and rolling
			// back a transaction it owns.
			return nil
		}
		if err != nil {
			return err
		}
		found = true
		return nil
	})
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, account.ErrAccountInfoNotFound
	}
	if !balance.Valid {
		return nil, nil
	}
	quarks := uint64(balance.Int64)
	return &quarks, nil
}

func dbApplyBalanceDelta(ctx context.Context, db *sqlx.DB, tokenAccount string, delta int64) error {
	return pgutil.ExecuteInTx(ctx, db, sql.LevelDefault, func(tx *sqlx.Tx) error {
		query := `UPDATE ` + tableName + `
			SET balance = balance + $2
			WHERE token_account = $1 AND balance IS NOT NULL
		`
		res, err := tx.ExecContext(ctx, query, tokenAccount, delta)
		if err != nil {
			if pgutil.IsCheckViolation(err) {
				return account.ErrNegativeBalance
			}
			return err
		}

		rowsAffected, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if rowsAffected == 0 {
			return account.ErrBalanceNotInitialized
		}
		return nil
	})
}

func dbInitializeBalance(ctx context.Context, db *sqlx.DB, tokenAccount string, balance uint64) error {
	return pgutil.ExecuteInTx(ctx, db, sql.LevelDefault, func(tx *sqlx.Tx) error {
		query := `UPDATE ` + tableName + `
			SET balance = $2
			WHERE token_account = $1 AND balance IS NULL
		`
		res, err := tx.ExecContext(ctx, query, tokenAccount, int64(balance))
		if err != nil {
			return err
		}

		rowsAffected, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if rowsAffected == 0 {
			return account.ErrBalanceAlreadyInitialized
		}
		return nil
	})
}

func dbGetRequiringBalanceInitialization(ctx context.Context, db *sqlx.DB, limit uint64) ([]*model, error) {
	var res []*model

	query := `SELECT id, owner_account, authority_account, token_account, mint_account, account_type, index, requires_deposit_sync, deposits_last_synced_at, requires_auto_return_check, balance, created_at FROM ` + tableName + `
		WHERE balance IS NULL
		ORDER BY id ASC
		LIMIT $1
	`
	err := db.SelectContext(
		ctx,
		&res,
		query,
		limit,
	)
	if err != nil {
		return nil, pgutil.CheckNoRows(err, account.ErrAccountInfoNotFound)
	}

	if len(res) == 0 {
		return nil, account.ErrAccountInfoNotFound
	}
	return res, nil
}

func equivalentModels(obj1, obj2 *model) bool {
	if obj1.OwnerAccount != obj2.OwnerAccount {
		return false
	}

	if obj1.AuthorityAccount != obj2.AuthorityAccount {
		return false
	}

	if obj1.TokenAccount != obj2.TokenAccount {
		return false
	}

	if obj1.MintAccount != obj2.MintAccount {
		return false
	}

	if obj1.Index != obj2.Index {
		return false
	}

	if obj1.AccountType != obj2.AccountType {
		return false
	}

	return true
}
