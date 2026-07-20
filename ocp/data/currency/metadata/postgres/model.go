package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"

	q "github.com/code-payments/ocp-server/database/query"
	"github.com/code-payments/ocp-server/ocp/data/currency"

	pgutil "github.com/code-payments/ocp-server/database/postgres"
)

const (
	tableName = "ocp__core_currencymetadata"
)

type model struct {
	Id sql.NullInt64 `db:"id"`

	Name        string `db:"name"`
	Symbol      string `db:"symbol"`
	Description string `db:"description"`
	ImageUrl    string `db:"image_url"`
	BillColors  string `db:"bill_colors"`
	SocialLinks string `db:"social_links"`

	Seed string `db:"seed"`

	Authority string `db:"authority"`

	Mint     string `db:"mint"`
	MintBump uint8  `db:"mint_bump"`
	Decimals uint8  `db:"decimals"`

	CurrencyConfig     string `db:"currency_config"`
	CurrencyConfigBump uint8  `db:"currency_config_bump"`

	LiquidityPool     string `db:"liquidity_pool"`
	LiquidityPoolBump uint8  `db:"liquidity_pool_bump"`

	VaultMint     string `db:"vault_mint"`
	VaultMintBump uint8  `db:"vault_mint_bump"`

	VaultCore     string `db:"vault_core"`
	VaultCoreBump uint8  `db:"vault_core_bump"`

	SellFeeBps uint16 `db:"sell_fee_bps"`

	Alt string `db:"alt"`

	IsDiscoverable bool `db:"is_discoverable"`

	State   uint8  `db:"state"`
	Version uint64 `db:"version"`

	CreatedBy string    `db:"created_by"`
	CreatedAt time.Time `db:"created_at"`
}

func toModel(obj *currency.MetadataRecord) (*model, error) {
	if err := obj.Validate(); err != nil {
		return nil, err
	}

	return &model{
		Id: sql.NullInt64{Int64: int64(obj.Id), Valid: obj.Id > 0},

		Name:        obj.Name,
		Symbol:      obj.Symbol,
		Description: obj.Description,
		ImageUrl:    obj.ImageUrl,
		BillColors:  strings.Join(obj.BillColors, ","),
		SocialLinks: marshalSocialLinks(obj.SocialLinks),

		Seed: obj.Seed,

		Authority: obj.Authority,

		Mint:     obj.Mint,
		MintBump: obj.MintBump,
		Decimals: obj.Decimals,

		CurrencyConfig:     obj.CurrencyConfig,
		CurrencyConfigBump: obj.CurrencyConfigBump,

		LiquidityPool:     obj.LiquidityPool,
		LiquidityPoolBump: obj.LiquidityPoolBump,

		VaultMint:     obj.VaultMint,
		VaultMintBump: obj.VaultMintBump,

		VaultCore:     obj.VaultCore,
		VaultCoreBump: obj.VaultCoreBump,

		SellFeeBps: obj.SellFeeBps,

		Alt: obj.Alt,

		IsDiscoverable: obj.IsDiscoverable,

		State:   uint8(obj.State),
		Version: obj.Version,

		CreatedBy: obj.CreatedBy,
		CreatedAt: obj.CreatedAt,
	}, nil
}

func fromModel(obj *model) *currency.MetadataRecord {
	var billColors []string
	if obj.BillColors != "" {
		billColors = strings.Split(obj.BillColors, ",")
	}

	return &currency.MetadataRecord{
		Id: uint64(obj.Id.Int64),

		Name:        obj.Name,
		Symbol:      obj.Symbol,
		Description: obj.Description,
		ImageUrl:    obj.ImageUrl,
		BillColors:  billColors,
		SocialLinks: unmarshalSocialLinks(obj.SocialLinks),

		Seed: obj.Seed,

		Authority: obj.Authority,

		Mint:     obj.Mint,
		MintBump: obj.MintBump,
		Decimals: obj.Decimals,

		CurrencyConfig:     obj.CurrencyConfig,
		CurrencyConfigBump: obj.CurrencyConfigBump,

		LiquidityPool:     obj.LiquidityPool,
		LiquidityPoolBump: obj.LiquidityPoolBump,

		VaultMint:     obj.VaultMint,
		VaultMintBump: obj.VaultMintBump,

		VaultCore:     obj.VaultCore,
		VaultCoreBump: obj.VaultCoreBump,

		SellFeeBps: obj.SellFeeBps,

		Alt: obj.Alt,

		IsDiscoverable: obj.IsDiscoverable,

		State:   currency.MetadataState(obj.State),
		Version: obj.Version,

		CreatedBy: obj.CreatedBy,
		CreatedAt: obj.CreatedAt,
	}
}

func marshalSocialLinks(links []currency.SocialLink) string {
	if len(links) == 0 {
		return "[]"
	}
	data, _ := json.Marshal(links)
	return string(data)
}

func unmarshalSocialLinks(data string) []currency.SocialLink {
	if data == "" || data == "[]" {
		return nil
	}
	var links []currency.SocialLink
	_ = json.Unmarshal([]byte(data), &links)
	return links
}

func (m *model) dbSave(ctx context.Context, db *sqlx.DB) error {
	return pgutil.ExecuteInTx(ctx, db, sql.LevelDefault, func(tx *sqlx.Tx) error {
		err := tx.QueryRowxContext(ctx,
			`INSERT INTO `+tableName+`
			(name, symbol, description, image_url, bill_colors, social_links, seed, authority, mint, mint_bump, decimals, currency_config, currency_config_bump, liquidity_pool, liquidity_pool_bump, vault_mint, vault_mint_bump, vault_core, vault_core_bump, sell_fee_bps, alt, is_discoverable, state, version, created_by, created_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24 + 1, $25, $26)

			ON CONFLICT (mint)
			DO UPDATE
				SET description = $3, image_url = $4, bill_colors = $5, social_links = $6, alt = $21, is_discoverable = $22, state = $23, version = `+tableName+`.version + 1
				WHERE `+tableName+`.mint = $9 AND `+tableName+`.version = $24

			RETURNING id, name, symbol, description, image_url, bill_colors, social_links, seed, authority, mint, mint_bump, decimals, currency_config, currency_config_bump, liquidity_pool, liquidity_pool_bump, vault_mint, vault_mint_bump, vault_core, vault_core_bump, sell_fee_bps, alt, is_discoverable, state, version, created_by, created_at`,
			m.Name,
			m.Symbol,
			m.Description,
			m.ImageUrl,
			m.BillColors,
			m.SocialLinks,
			m.Seed,
			m.Authority,
			m.Mint,
			m.MintBump,
			m.Decimals,
			m.CurrencyConfig,
			m.CurrencyConfigBump,
			m.LiquidityPool,
			m.LiquidityPoolBump,
			m.VaultMint,
			m.VaultMintBump,
			m.VaultCore,
			m.VaultCoreBump,
			m.SellFeeBps,
			m.Alt,
			m.IsDiscoverable,
			m.State,
			m.Version,
			m.CreatedBy,
			m.CreatedAt,
		).StructScan(m)

		err = pgutil.CheckUniqueViolation(err, currency.ErrDuplicateCurrency)
		if err == currency.ErrDuplicateCurrency {
			return err
		}

		return pgutil.CheckNoRows(err, currency.ErrStaleMetadataVersion)
	})
}

func dbGetAllMetadataByState(ctx context.Context, db *sqlx.DB, state currency.MetadataState, cursor q.Cursor, limit uint64, direction q.Ordering) ([]*model, error) {
	res := []*model{}

	query := `SELECT
		id, name, symbol, description, image_url, bill_colors, social_links, seed, authority, mint, mint_bump, decimals, currency_config, currency_config_bump, liquidity_pool, liquidity_pool_bump, vault_mint, vault_mint_bump, vault_core, vault_core_bump, sell_fee_bps, alt, is_discoverable, state, version, created_by, created_at
		FROM ` + tableName + `
		WHERE state = $1`

	opts := []interface{}{state}
	query, opts = q.PaginateQuery(query, opts, cursor, limit, direction)

	err := db.SelectContext(ctx, &res, query, opts...)
	if err != nil {
		return nil, pgutil.CheckNoRows(err, currency.ErrNotFound)
	}

	if len(res) == 0 {
		return nil, currency.ErrNotFound
	}
	return res, nil
}

func dbGetMetadataByMint(ctx context.Context, db *sqlx.DB, mint string) (*model, error) {
	res := &model{}
	err := db.GetContext(ctx, res,
		`SELECT id, name, symbol, description, image_url, bill_colors, social_links, seed, authority, mint, mint_bump, decimals, currency_config, currency_config_bump, liquidity_pool, liquidity_pool_bump, vault_mint, vault_mint_bump, vault_core, vault_core_bump, sell_fee_bps, alt, is_discoverable, state, version, created_by, created_at
		FROM `+tableName+`
		WHERE mint = $1`,
		mint,
	)
	return res, pgutil.CheckNoRows(err, currency.ErrNotFound)
}

func dbGetAllMints(ctx context.Context, db *sqlx.DB) ([]string, error) {
	var res []string
	err := db.SelectContext(ctx, &res,
		`SELECT mint FROM `+tableName,
	)

	if err != nil {
		return nil, pgutil.CheckNoRows(err, currency.ErrNotFound)
	}
	if len(res) == 0 {
		return nil, currency.ErrNotFound
	}

	return res, nil
}

func dbCountMints(ctx context.Context, db *sqlx.DB) (uint64, error) {
	var count uint64
	err := db.GetContext(ctx, &count,
		`SELECT COUNT(*) FROM `+tableName+` WHERE state != $1`,
		currency.MetadataStateAbandoned,
	)
	if err != nil {
		return 0, err
	}
	return count, nil
}

func dbCountMetadataByState(ctx context.Context, db *sqlx.DB, state currency.MetadataState) (uint64, error) {
	var res uint64
	query := `SELECT COUNT(*) FROM ` + tableName + ` WHERE state = $1`
	err := db.GetContext(ctx, &res, query, state)
	if err != nil {
		return 0, err
	}
	return res, nil
}

func dbIsNameAvailable(ctx context.Context, db *sqlx.DB, name string) (bool, error) {
	var count uint64
	err := db.GetContext(ctx, &count,
		`SELECT COUNT(*) FROM `+tableName+` WHERE LOWER(name) = LOWER($1) AND state != $2`,
		name,
		currency.MetadataStateAbandoned,
	)
	if err != nil {
		return false, err
	}
	return count == 0, nil
}
