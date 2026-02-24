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

	State   uint8  `db:"state"`
	Version uint64 `db:"version"`

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

		State:   uint8(obj.State),
		Version: obj.Version,

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

		State:   metadata.State(obj.State),
		Version: obj.Version,

		CreatedAt: obj.CreatedAt,
	}
}

func (m *model) dbSave(ctx context.Context, db *sqlx.DB) error {
	return pgutil.ExecuteInTx(ctx, db, sql.LevelDefault, func(tx *sqlx.Tx) error {
		if m.CreatedAt.IsZero() {
			m.CreatedAt = time.Now()
		}

		err := db.QueryRowxContext(ctx,
			`INSERT INTO `+tableName+`
		(mint, authority, vm, vm_bump, omnibus, omnibus_bump, days_locked, state, version, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9 + 1, $10)

		ON CONFLICT (mint)
		DO UPDATE
			SET state = $8, version = `+tableName+`.version + 1
			WHERE `+tableName+`.mint = $1 AND `+tableName+`.version = $9

		RETURNING id, mint, authority, vm, vm_bump, omnibus, omnibus_bump, days_locked, state, version, created_at`,
			m.Mint,
			m.Authority,
			m.Vm,
			m.VmBump,
			m.Omnibus,
			m.OmnibusBump,
			m.DaysLocked,
			m.State,
			m.Version,
			m.CreatedAt,
		).StructScan(m)

		return pgutil.CheckNoRows(err, metadata.ErrStaleVersion)
	})
}

func dbGetAllVms(ctx context.Context, db *sqlx.DB) ([]string, error) {
	var res []string
	query := `SELECT DISTINCT vm FROM ` + tableName

	err := db.SelectContext(ctx, &res, query)
	if err != nil {
		return nil, err
	}
	if len(res) == 0 {
		return nil, metadata.ErrNotFound
	}
	return res, nil
}

func dbGetByMint(ctx context.Context, db *sqlx.DB, mint string) (*model, error) {
	var res model
	query := `SELECT id, mint, authority, vm, vm_bump, omnibus, omnibus_bump, days_locked, state, version, created_at FROM ` + tableName + `
		WHERE mint = $1
	`

	err := db.GetContext(ctx, &res, query, mint)
	if err != nil {
		return nil, pgutil.CheckNoRows(err, metadata.ErrNotFound)
	}
	return &res, nil
}
