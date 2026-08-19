package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"

	"github.com/code-payments/ocp-server/currency"
	pgutil "github.com/code-payments/ocp-server/database/postgres"
	q "github.com/code-payments/ocp-server/database/query"
	"github.com/code-payments/ocp-server/ocp/data/history"
	"github.com/code-payments/ocp-server/pointer"
)

const (
	tableName = "ocp__core_transactionhistory"

	allColumns = `id, reference_id, reference_type, type, owner_account, counterparty_owner_account, exchange_currency, native_amount, fees, mint_account, quantity, destination_mint_account, destination_quantity, gift_card_vault, app_metadata, version, state, created_at, updated_at`
)

type model struct {
	Id                       sql.NullInt64  `db:"id"`
	ReferenceId              string         `db:"reference_id"`
	ReferenceType            uint8          `db:"reference_type"`
	Type                     uint8          `db:"type"`
	OwnerAccount             string         `db:"owner_account"`
	CounterpartyOwnerAccount sql.NullString `db:"counterparty_owner_account"`
	ExchangeCurrency         string         `db:"exchange_currency"`
	NativeAmount             float64        `db:"native_amount"`
	Fees                     string         `db:"fees"`
	MintAccount              string         `db:"mint_account"`
	Quantity                 uint64         `db:"quantity"`
	DestinationMintAccount   sql.NullString `db:"destination_mint_account"`
	DestinationQuantity      sql.NullInt64  `db:"destination_quantity"`
	GiftCardVault            sql.NullString `db:"gift_card_vault"`
	AppMetadata              []byte         `db:"app_metadata"`
	Version                  uint64         `db:"version"`
	State                    uint8          `db:"state"`
	CreatedAt                time.Time      `db:"created_at"`
	UpdatedAt                time.Time      `db:"updated_at"`
}

func toModel(obj *history.Record) (*model, error) {
	if err := obj.Validate(); err != nil {
		return nil, err
	}

	fees, err := marshalFees(obj.Fees)
	if err != nil {
		return nil, err
	}

	return &model{
		Id:                       sql.NullInt64{Int64: int64(obj.Id), Valid: true},
		ReferenceId:              obj.ReferenceId,
		ReferenceType:            uint8(obj.ReferenceType),
		Type:                     uint8(obj.Type),
		OwnerAccount:             obj.OwnerAccount,
		CounterpartyOwnerAccount: toNullString(obj.CounterpartyOwnerAccount),
		ExchangeCurrency:         string(obj.ExchangeCurrency),
		NativeAmount:             obj.NativeAmount,
		Fees:                     fees,
		MintAccount:              obj.MintAccount,
		Quantity:                 obj.Quantity,
		DestinationMintAccount:   toNullString(obj.DestinationMintAccount),
		DestinationQuantity:      toNullInt64(obj.DestinationQuantity),
		GiftCardVault:            toNullString(obj.GiftCardVault),
		AppMetadata:              obj.AppMetadata,
		Version:                  obj.Version,
		State:                    uint8(obj.State),
		CreatedAt:                obj.CreatedAt,
		UpdatedAt:                obj.UpdatedAt,
	}, nil
}

func fromModel(m *model) (*history.Record, error) {
	fees, err := unmarshalFees(m.Fees)
	if err != nil {
		return nil, err
	}

	return &history.Record{
		Id:                       uint64(m.Id.Int64),
		ReferenceId:              m.ReferenceId,
		ReferenceType:            history.ReferenceType(m.ReferenceType),
		Type:                     history.Type(m.Type),
		OwnerAccount:             m.OwnerAccount,
		CounterpartyOwnerAccount: fromNullString(m.CounterpartyOwnerAccount),
		ExchangeCurrency:         currency.Code(m.ExchangeCurrency),
		NativeAmount:             m.NativeAmount,
		Fees:                     fees,
		MintAccount:              m.MintAccount,
		Quantity:                 m.Quantity,
		DestinationMintAccount:   fromNullString(m.DestinationMintAccount),
		DestinationQuantity:      fromNullInt64(m.DestinationQuantity),
		GiftCardVault:            fromNullString(m.GiftCardVault),
		AppMetadata:              m.AppMetadata,
		Version:                  m.Version,
		State:                    history.State(m.State),
		CreatedAt:                m.CreatedAt,
		UpdatedAt:                m.UpdatedAt,
	}, nil
}

// dbSave inserts a new record or applies an update to an existing one. Which of
// the two is decided by whether the record carries an ID, so that a write with
// no ID can never silently land on top of a record already stored: an owner
// already holding a record for the reference is reported as history.ErrExists,
// which is what makes a retried write a no-op rather than a double entry.
func (m *model) dbSave(ctx context.Context, db *sqlx.DB) error {
	if m.Id.Int64 == 0 {
		return m.dbInsert(ctx, db)
	}
	return m.dbUpdate(ctx, db)
}

func (m *model) dbInsert(ctx context.Context, db *sqlx.DB) error {
	return pgutil.ExecuteInTx(ctx, db, sql.LevelDefault, func(tx *sqlx.Tx) error {
		query := `INSERT INTO ` + tableName + `
			(reference_id, reference_type, type, owner_account, counterparty_owner_account, exchange_currency, native_amount, fees, mint_account, quantity, destination_mint_account, destination_quantity, gift_card_vault, app_metadata, version, state, created_at, updated_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15 + 1, $16, $17, $17)

			ON CONFLICT (owner_account, reference_type, reference_id) DO NOTHING

			RETURNING ` + allColumns

		err := tx.QueryRowxContext(
			ctx,
			query,
			m.ReferenceId,
			m.ReferenceType,
			m.Type,
			m.OwnerAccount,
			m.CounterpartyOwnerAccount,
			m.ExchangeCurrency,
			m.NativeAmount,
			m.Fees,
			m.MintAccount,
			m.Quantity,
			m.DestinationMintAccount,
			m.DestinationQuantity,
			m.GiftCardVault,
			m.AppMetadata,
			m.Version,
			m.State,
			m.CreatedAt,
		).StructScan(m)
		if err != nil {
			return pgutil.CheckNoRows(err, history.ErrExists)
		}
		return nil
	})
}

// dbUpdate applies the mutable part of a record: the state it has reached, the
// destination leg a swap only learns on finalizing, and the fees only known by
// then. Everything else is settled when the record is written and is returned
// as stored, so a caller that edited an immutable field does not persist it.
//
// The write time is stamped here rather than taken from the caller, since the
// caller's copy holds the time of the write it read the record from. Taking it
// would leave updated_at frozen at the creation time for a record's whole life.
func (m *model) dbUpdate(ctx context.Context, db *sqlx.DB) error {
	return pgutil.ExecuteInTx(ctx, db, sql.LevelDefault, func(tx *sqlx.Tx) error {
		query := `UPDATE ` + tableName + `
			SET state = $3, destination_quantity = $4, fees = $5, version = version + 1, updated_at = NOW()
			WHERE id = $1 AND version = $2
			RETURNING ` + allColumns

		err := tx.QueryRowxContext(
			ctx,
			query,
			m.Id,
			m.Version,
			m.State,
			m.DestinationQuantity,
			m.Fees,
		).StructScan(m)
		if err == nil {
			return nil
		}
		if err != sql.ErrNoRows {
			return err
		}

		// The update matched nothing, which is either a record that never existed
		// or one that has since moved on. Distinguishing them costs a query only
		// on this path, and callers act differently on each.
		var exists bool
		if err := tx.GetContext(ctx, &exists, `SELECT EXISTS (SELECT 1 FROM `+tableName+` WHERE id = $1)`, m.Id); err != nil {
			return err
		}
		if !exists {
			return history.ErrNotFound
		}
		return history.ErrStaleVersion
	})
}

// paginate applies the history's ordering to a query: by event time, then by
// ID to break ties. q.PaginateQuery cannot be used because it orders on a
// single column, and time alone is not a total order.
//
// The cursor predicate is a row comparison rather than a pair of conditions on
// each column, so it stays a single range scan over the (created_at, id) part
// of an index rather than a filter over everything sharing a timestamp.
func paginate(query string, opts []any, cursor q.Cursor, limit uint64, direction q.Ordering) (string, []any, error) {
	if len(cursor) > 0 {
		createdAt, id, ok := history.FromCursor(cursor)
		if !ok {
			return "", nil, history.ErrInvalidCursor
		}

		comparison := "<"
		if direction == q.Ascending {
			comparison = ">"
		}

		query += fmt.Sprintf(" AND (created_at, id) %s ($%d, $%d)", comparison, len(opts)+1, len(opts)+2)
		opts = append(opts, createdAt, id)
	}

	if direction == q.Ascending {
		query += " ORDER BY created_at ASC, id ASC"
	} else {
		query += " ORDER BY created_at DESC, id DESC"
	}

	if limit > 0 {
		query += fmt.Sprintf(" LIMIT $%d", len(opts)+1)
		opts = append(opts, limit)
	}

	return query, opts, nil
}

func dbGetAllByOwner(ctx context.Context, db *sqlx.DB, owner string, cursor q.Cursor, limit uint64, direction q.Ordering) ([]*model, error) {
	res := []*model{}

	query := `SELECT ` + allColumns + `
		FROM ` + tableName + `
		WHERE owner_account = $1`

	query, opts, err := paginate(query, []any{owner}, cursor, limit, direction)
	if err != nil {
		return nil, err
	}

	err = db.SelectContext(ctx, &res, query, opts...)
	if err != nil {
		return nil, pgutil.CheckNoRows(err, history.ErrNotFound)
	}
	if len(res) == 0 {
		return nil, history.ErrNotFound
	}
	return res, nil
}

// dbGetAllByOwnerMint matches a mint on either leg, so that a mint's history
// holds what was traded into it as well as out of it.
func dbGetAllByOwnerMint(ctx context.Context, db *sqlx.DB, owner, mint string, cursor q.Cursor, limit uint64, direction q.Ordering) ([]*model, error) {
	res := []*model{}

	query := `SELECT ` + allColumns + `
		FROM ` + tableName + `
		WHERE owner_account = $1 AND (mint_account = $2 OR destination_mint_account = $2)`

	query, opts, err := paginate(query, []any{owner, mint}, cursor, limit, direction)
	if err != nil {
		return nil, err
	}

	err = db.SelectContext(ctx, &res, query, opts...)
	if err != nil {
		return nil, pgutil.CheckNoRows(err, history.ErrNotFound)
	}
	if len(res) == 0 {
		return nil, history.ErrNotFound
	}
	return res, nil
}

func dbGetAllByIds(ctx context.Context, db *sqlx.DB, ids []uint64) ([]*model, error) {
	if len(ids) == 0 {
		return nil, history.ErrNotFound
	}

	res := []*model{}

	query, opts, err := sqlx.In(`SELECT `+allColumns+`
		FROM `+tableName+`
		WHERE id IN (?)
		ORDER BY id ASC`, ids)
	if err != nil {
		return nil, err
	}

	err = db.SelectContext(ctx, &res, db.Rebind(query), opts...)
	if err != nil {
		return nil, pgutil.CheckNoRows(err, history.ErrNotFound)
	}
	if len(res) == 0 {
		return nil, history.ErrNotFound
	}
	return res, nil
}

func dbGetAllByReference(ctx context.Context, db *sqlx.DB, referenceType history.ReferenceType, referenceId string) ([]*model, error) {
	res := []*model{}

	query := `SELECT ` + allColumns + `
		FROM ` + tableName + `
		WHERE reference_type = $1 AND reference_id = $2
		ORDER BY id ASC`

	err := db.SelectContext(ctx, &res, query, uint8(referenceType), referenceId)
	if err != nil {
		return nil, pgutil.CheckNoRows(err, history.ErrNotFound)
	}
	if len(res) == 0 {
		return nil, history.ErrNotFound
	}
	return res, nil
}

func dbGetAllByGiftCardVault(ctx context.Context, db *sqlx.DB, vault string) ([]*model, error) {
	res := []*model{}

	query := `SELECT ` + allColumns + `
		FROM ` + tableName + `
		WHERE gift_card_vault = $1
		ORDER BY id ASC`

	err := db.SelectContext(ctx, &res, query, vault)
	if err != nil {
		return nil, pgutil.CheckNoRows(err, history.ErrNotFound)
	}
	if len(res) == 0 {
		return nil, history.ErrNotFound
	}
	return res, nil
}

func marshalFees(fees []history.Fee) (string, error) {
	if len(fees) == 0 {
		return "[]", nil
	}

	data, err := json.Marshal(fees)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// unmarshalFees reports a blob it cannot decode rather than returning no fees.
// The difference matters because a record is updated by reading it, changing
// the state it has reached, and writing the whole thing back, so fees that
// decoded to nothing would be written back as nothing and the stored blob would
// be gone rather than merely misread.
func unmarshalFees(data string) ([]history.Fee, error) {
	if data == "[]" {
		return nil, nil
	}

	var fees []history.Fee
	if err := json.Unmarshal([]byte(data), &fees); err != nil {
		return nil, err
	}
	return fees, nil
}

func toNullString(val *string) sql.NullString {
	if val == nil {
		return sql.NullString{}
	}
	return sql.NullString{String: *val, Valid: true}
}

func fromNullString(val sql.NullString) *string {
	if !val.Valid {
		return nil
	}
	return pointer.String(val.String)
}

func toNullInt64(val *uint64) sql.NullInt64 {
	if val == nil {
		return sql.NullInt64{}
	}
	return sql.NullInt64{Int64: int64(*val), Valid: true}
}

func fromNullInt64(val sql.NullInt64) *uint64 {
	if !val.Valid {
		return nil
	}
	return pointer.Uint64(uint64(val.Int64))
}
