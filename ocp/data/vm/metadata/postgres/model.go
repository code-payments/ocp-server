package postgres

import (
	"context"
	"database/sql"
	"time"

	"github.com/jmoiron/sqlx"

	pgutil "github.com/code-payments/ocp-server/database/postgres"
	"github.com/code-payments/ocp-server/ocp/data/vm/metadata"
)

const (
	tableName = "ocp__core_vmmetadata"
)

type model struct {
	Id sql.NullInt64 `db:"id"`

	Mint        string `db:"mint"`
	Authority   string `db:"authority"`
	Vm          string `db:"vm"`
	VmBump      uint8  `db:"vm_bump"`
	Omnibus     string `db:"omnibus"`
	OmnibusBump uint8  `db:"omnibus_bump"`
	DaysLocked  uint8  `db:"days_locked"`

	CreatedAt time.Time `db:"created_at"`
}

func toModel(obj *metadata.Record) (*model, error) {
	if err := obj.Validate(); err != nil {
		return nil, err
	}

	return &model{
		Mint:        obj.Mint,
		Authority:   obj.Authority,
		Vm:          obj.Vm,
		VmBump:      obj.VmBump,
		Omnibus:     obj.Omnibus,
		OmnibusBump: obj.OmnibusBump,
		DaysLocked:  obj.DaysLocked,

		CreatedAt: obj.CreatedAt,
	}, nil
}

func fromModel(obj *model) *metadata.Record {
	return &metadata.Record{
		Id: uint64(obj.Id.Int64),

		Mint:        obj.Mint,
		Authority:   obj.Authority,
		Vm:          obj.Vm,
		VmBump:      obj.VmBump,
		Omnibus:     obj.Omnibus,
		OmnibusBump: obj.OmnibusBump,
		DaysLocked:  obj.DaysLocked,

		CreatedAt: obj.CreatedAt,
	}
}

func (m *model) dbPut(ctx context.Context, db *sqlx.DB) error {
	query := `INSERT INTO ` + tableName + `
		(mint, authority, vm, vm_bump, omnibus, omnibus_bump, days_locked, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		RETURNING id, mint, authority, vm, vm_bump, omnibus, omnibus_bump, days_locked, created_at
	`

	if m.CreatedAt.IsZero() {
		m.CreatedAt = time.Now()
	}

	err := db.QueryRowxContext(
		ctx,
		query,
		m.Mint,
		m.Authority,
		m.Vm,
		m.VmBump,
		m.Omnibus,
		m.OmnibusBump,
		m.DaysLocked,
		m.CreatedAt,
	).StructScan(m)

	return pgutil.CheckUniqueViolation(err, metadata.ErrAlreadyExists)
}

func dbGetByMint(ctx context.Context, db *sqlx.DB, mint string) (*model, error) {
	var res model
	query := `SELECT id, mint, authority, vm, vm_bump, omnibus, omnibus_bump, days_locked, created_at FROM ` + tableName + `
		WHERE mint = $1
	`

	err := db.GetContext(ctx, &res, query, mint)
	if err != nil {
		return nil, pgutil.CheckNoRows(err, metadata.ErrNotFound)
	}
	return &res, nil
}
