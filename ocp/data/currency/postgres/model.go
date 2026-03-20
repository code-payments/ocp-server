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
	exchangeRateTableName    = "ocp__core_exchangerate"
	metadataTableName        = "ocp__core_currencymetadata"
	reserveTableName         = "ocp__core_currencyreserve"
	liveReserveTableName     = "ocp__core_currencyreserve2"
	holderCountTableName     = "ocp__core_currencyholdercount"
	liveHolderCountTableName = "ocp__core_currencyholdercount2"

	dateFormat = "2006-01-02"
)

type exchangeRateModel struct {
	Id           sql.NullInt64 `db:"id"`
	ForDate      string        `db:"for_date"`
	ForTimestamp time.Time     `db:"for_timestamp"`
	CurrencyCode string        `db:"currency_code"`
	CurrencyRate float64       `db:"currency_rate"`
}

func toExchangeRateModel(obj *currency.ExchangeRateRecord) *exchangeRateModel {
	return &exchangeRateModel{
		Id:           sql.NullInt64{Int64: int64(obj.Id), Valid: obj.Id > 0},
		ForDate:      obj.Time.UTC().Format(dateFormat),
		ForTimestamp: obj.Time.UTC(),
		CurrencyCode: obj.Symbol,
		CurrencyRate: obj.Rate,
	}
}

func fromExchangeRateModel(obj *exchangeRateModel) *currency.ExchangeRateRecord {
	return &currency.ExchangeRateRecord{
		Id:     uint64(obj.Id.Int64),
		Time:   obj.ForTimestamp.UTC(),
		Symbol: obj.CurrencyCode,
		Rate:   obj.CurrencyRate,
	}
}

type metadataModel struct {
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

	State   uint8  `db:"state"`
	Version uint64 `db:"version"`

	CreatedBy string    `db:"created_by"`
	CreatedAt time.Time `db:"created_at"`
}

func toMetadataModel(obj *currency.MetadataRecord) (*metadataModel, error) {
	if err := obj.Validate(); err != nil {
		return nil, err
	}

	return &metadataModel{
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

		State:   uint8(obj.State),
		Version: obj.Version,

		CreatedBy: obj.CreatedBy,
		CreatedAt: obj.CreatedAt,
	}, nil
}

func fromMetadataModel(obj *metadataModel) *currency.MetadataRecord {
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

		State:   currency.MetadataState(obj.State),
		Version: obj.Version,

		CreatedBy: obj.CreatedBy,
		CreatedAt: obj.CreatedAt,
	}
}

type historicalReserveModel struct {
	Id                sql.NullInt64 `db:"id"`
	ForDate           string        `db:"for_date"`
	ForTimestamp      time.Time     `db:"for_timestamp"`
	Mint              string        `db:"mint"`
	SupplyFromBonding uint64        `db:"supply_from_bonding"`
}

func toHistoricalReserveModel(obj *currency.ReserveRecord) (*historicalReserveModel, error) {
	if err := obj.Validate(); err != nil {
		return nil, err
	}

	return &historicalReserveModel{
		Id:                sql.NullInt64{Int64: int64(obj.Id), Valid: obj.Id > 0},
		ForDate:           obj.Time.UTC().Format(dateFormat),
		ForTimestamp:      obj.Time.UTC(),
		Mint:              obj.Mint,
		SupplyFromBonding: obj.SupplyFromBonding,
	}, nil
}

func fromHistoricalReserveModel(obj *historicalReserveModel) *currency.ReserveRecord {
	return &currency.ReserveRecord{
		Id:                uint64(obj.Id.Int64),
		Time:              obj.ForTimestamp.UTC(),
		Mint:              obj.Mint,
		SupplyFromBonding: obj.SupplyFromBonding,
	}
}

type liveReserveModel struct {
	Id                sql.NullInt64 `db:"id"`
	Mint              string        `db:"mint"`
	SupplyFromBonding uint64        `db:"supply_from_bonding"`
	Slot              uint64        `db:"slot"`
	LastUpdatedAt     time.Time     `db:"last_updated_at"`
}

func toLiveReserveModel(obj *currency.ReserveRecord) *liveReserveModel {
	return &liveReserveModel{
		Id:                sql.NullInt64{Int64: int64(obj.Id), Valid: obj.Id > 0},
		Mint:              obj.Mint,
		SupplyFromBonding: obj.SupplyFromBonding,
		Slot:              obj.Slot,
		LastUpdatedAt:     obj.Time.UTC(),
	}
}

func fromLiveReserveModel(obj *liveReserveModel) *currency.ReserveRecord {
	return &currency.ReserveRecord{
		Id:                uint64(obj.Id.Int64),
		Mint:              obj.Mint,
		SupplyFromBonding: obj.SupplyFromBonding,
		Slot:              obj.Slot,
		Time:              obj.LastUpdatedAt.UTC(),
	}
}

type historicalHolderCountModel struct {
	Id           sql.NullInt64 `db:"id"`
	ForDate      string        `db:"for_date"`
	ForTimestamp time.Time     `db:"for_timestamp"`
	Mint         string        `db:"mint"`
	HolderCount  uint64        `db:"holder_count"`
}

func toHistoricalHolderCountModel(obj *currency.HolderCountRecord) (*historicalHolderCountModel, error) {
	if err := obj.Validate(); err != nil {
		return nil, err
	}

	return &historicalHolderCountModel{
		Id:           sql.NullInt64{Int64: int64(obj.Id), Valid: obj.Id > 0},
		ForDate:      obj.Time.UTC().Format(dateFormat),
		ForTimestamp: obj.Time.UTC(),
		Mint:         obj.Mint,
		HolderCount:  obj.HolderCount,
	}, nil
}

func fromHistoricalHolderCountModel(obj *historicalHolderCountModel) *currency.HolderCountRecord {
	return &currency.HolderCountRecord{
		Id:          uint64(obj.Id.Int64),
		Time:        obj.ForTimestamp.UTC(),
		Mint:        obj.Mint,
		HolderCount: obj.HolderCount,
	}
}

type liveHolderCountModel struct {
	Id            sql.NullInt64 `db:"id"`
	Mint          string        `db:"mint"`
	HolderCount   uint64        `db:"holder_count"`
	LastUpdatedAt time.Time     `db:"last_updated_at"`
}

func toLiveHolderCountModel(obj *currency.HolderCountRecord) *liveHolderCountModel {
	return &liveHolderCountModel{
		Id:            sql.NullInt64{Int64: int64(obj.Id), Valid: obj.Id > 0},
		Mint:          obj.Mint,
		HolderCount:   obj.HolderCount,
		LastUpdatedAt: obj.Time.UTC(),
	}
}

func fromLiveHolderCountModel(obj *liveHolderCountModel) *currency.HolderCountRecord {
	return &currency.HolderCountRecord{
		Id:          uint64(obj.Id.Int64),
		Mint:        obj.Mint,
		HolderCount: obj.HolderCount,
		Time:        obj.LastUpdatedAt.UTC(),
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

func makeTimeBasedSelectQuery(table, condition string, ordering q.Ordering) string {
	return `SELECT * FROM ` + table + ` WHERE ` + condition + ` ORDER BY for_timestamp ` + q.FromOrderingWithFallback(ordering, "asc")
}

func makeTimeBasedGetQuery(table, condition string, ordering q.Ordering) string {
	return makeTimeBasedSelectQuery(table, condition, ordering) + ` LIMIT 1`
}

func makeTimeBasedRangeQuery(table, condition string, ordering q.Ordering, interval q.Interval) string {
	var query, bucket string

	if interval == q.IntervalRaw {
		query = `SELECT *`
	} else {
		bucket = `date_trunc('` + q.FromIntervalWithFallback(interval, "hour") + `', for_timestamp)`
		query = `SELECT DISTINCT ON (` + bucket + `) *`
	}

	query = query + ` FROM ` + table + ` WHERE ` + condition

	if interval == q.IntervalRaw {
		query = query + ` ORDER BY for_timestamp ` + q.FromOrderingWithFallback(ordering, "asc")
	} else {
		query = query + ` ORDER BY ` + bucket + `, for_timestamp DESC` // keep only the latest record for each bucket
	}

	return query
}

func (m *exchangeRateModel) txSave(ctx context.Context, tx *sqlx.Tx) error {
	err := tx.QueryRowxContext(ctx,
		`INSERT INTO `+exchangeRateTableName+`
		(for_date, for_timestamp, currency_code, currency_rate)
		VALUES ($1, $2, $3, $4)
		RETURNING id, for_date, for_timestamp, currency_code, currency_rate`,
		m.ForDate,
		m.ForTimestamp,
		m.CurrencyCode,
		m.CurrencyRate,
	).StructScan(m)

	return pgutil.CheckUniqueViolation(err, currency.ErrExists)
}

func (m *metadataModel) dbSave(ctx context.Context, db *sqlx.DB) error {
	return pgutil.ExecuteInTx(ctx, db, sql.LevelDefault, func(tx *sqlx.Tx) error {
		err := tx.QueryRowxContext(ctx,
			`INSERT INTO `+metadataTableName+`
			(name, symbol, description, image_url, bill_colors, social_links, seed, authority, mint, mint_bump, decimals, currency_config, currency_config_bump, liquidity_pool, liquidity_pool_bump, vault_mint, vault_mint_bump, vault_core, vault_core_bump, sell_fee_bps, alt, state, version, created_by, created_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23 + 1, $24, $25)

			ON CONFLICT (mint)
			DO UPDATE
				SET description = $3, image_url = $4, bill_colors = $5, social_links = $6, alt = $21, state = $22, version = `+metadataTableName+`.version + 1
				WHERE `+metadataTableName+`.mint = $9 AND `+metadataTableName+`.version = $23

			RETURNING id, name, symbol, description, image_url, bill_colors, social_links, seed, authority, mint, mint_bump, decimals, currency_config, currency_config_bump, liquidity_pool, liquidity_pool_bump, vault_mint, vault_mint_bump, vault_core, vault_core_bump, sell_fee_bps, alt, state, version, created_by, created_at`,
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

func (m *historicalReserveModel) dbSave(ctx context.Context, db *sqlx.DB) error {
	return pgutil.ExecuteInTx(ctx, db, sql.LevelDefault, func(tx *sqlx.Tx) error {
		err := tx.QueryRowxContext(ctx,
			`INSERT INTO `+reserveTableName+`
			(for_date, for_timestamp, mint, supply_from_bonding)
			VALUES ($1, $2, $3, $4)
			RETURNING id, for_date, for_timestamp, mint, supply_from_bonding`,
			m.ForDate,
			m.ForTimestamp,
			m.Mint,
			m.SupplyFromBonding,
		).StructScan(m)

		return pgutil.CheckUniqueViolation(err, currency.ErrExists)
	})
}

func dbGetExchangeRateBySymbolAndTime(ctx context.Context, db *sqlx.DB, symbol string, t time.Time, ordering q.Ordering) (*exchangeRateModel, error) {
	res := &exchangeRateModel{}
	err := db.GetContext(ctx, res,
		makeTimeBasedGetQuery(exchangeRateTableName, "currency_code = $1 AND for_date = $2 AND for_timestamp <= $3", ordering),
		symbol,
		t.UTC().Format(dateFormat),
		t.UTC(),
	)
	return res, pgutil.CheckNoRows(err, currency.ErrNotFound)
}

func dbGetAllExchangeRatesByTime(ctx context.Context, db *sqlx.DB, t time.Time, ordering q.Ordering) ([]*exchangeRateModel, error) {
	query := `SELECT DISTINCT ON (currency_code) *
		FROM ` + exchangeRateTableName + `
		WHERE for_date = $1 AND for_timestamp <= $2
		ORDER BY currency_code, for_timestamp ` + q.FromOrderingWithFallback(ordering, "asc")

	res := []*exchangeRateModel{}
	err := db.SelectContext(ctx, &res, query, t.UTC().Format(dateFormat), t.UTC())

	if err != nil {
		return nil, pgutil.CheckNoRows(err, currency.ErrNotFound)
	}
	if res == nil {
		return nil, currency.ErrNotFound
	}
	if len(res) == 0 {
		return nil, currency.ErrNotFound
	}

	return res, nil
}

func dbGetAllExchangeRatesForRange(ctx context.Context, db *sqlx.DB, symbol string, interval q.Interval, start time.Time, end time.Time, ordering q.Ordering) ([]*exchangeRateModel, error) {
	res := []*exchangeRateModel{}
	err := db.SelectContext(ctx, &res,
		makeTimeBasedRangeQuery(exchangeRateTableName, "currency_code = $1 AND for_timestamp >= $2 AND for_timestamp <= $3", ordering, interval),
		symbol, start.UTC(), end.UTC(),
	)

	if err != nil {
		return nil, pgutil.CheckNoRows(err, currency.ErrNotFound)
	}
	if len(res) == 0 {
		return nil, currency.ErrNotFound
	}

	return res, nil
}

func dbGetAllMetadataByState(ctx context.Context, db *sqlx.DB, state currency.MetadataState, cursor q.Cursor, limit uint64, direction q.Ordering) ([]*metadataModel, error) {
	res := []*metadataModel{}

	query := `SELECT
		id, name, symbol, description, image_url, bill_colors, social_links, seed, authority, mint, mint_bump, decimals, currency_config, currency_config_bump, liquidity_pool, liquidity_pool_bump, vault_mint, vault_mint_bump, vault_core, vault_core_bump, sell_fee_bps, alt, state, version, created_by, created_at
		FROM ` + metadataTableName + `
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

func dbGetMetadataByMint(ctx context.Context, db *sqlx.DB, mint string) (*metadataModel, error) {
	res := &metadataModel{}
	err := db.GetContext(ctx, res,
		`SELECT id, name, symbol, description, image_url, bill_colors, social_links, seed, authority, mint, mint_bump, decimals, currency_config, currency_config_bump, liquidity_pool, liquidity_pool_bump, vault_mint, vault_mint_bump, vault_core, vault_core_bump, sell_fee_bps, alt, state, version, created_by, created_at
		FROM `+metadataTableName+`
		WHERE mint = $1`,
		mint,
	)
	return res, pgutil.CheckNoRows(err, currency.ErrNotFound)
}

func dbGetAllMints(ctx context.Context, db *sqlx.DB) ([]string, error) {
	var res []string
	err := db.SelectContext(ctx, &res,
		`SELECT mint FROM `+metadataTableName,
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
	err := db.GetContext(ctx, &count, `SELECT COUNT(*) FROM `+metadataTableName)
	if err != nil {
		return 0, err
	}
	return count, nil
}

func dbCountMetadataByState(ctx context.Context, db *sqlx.DB, state currency.MetadataState) (uint64, error) {
	var res uint64
	query := `SELECT COUNT(*) FROM ` + metadataTableName + ` WHERE state = $1`
	err := db.GetContext(ctx, &res, query, state)
	if err != nil {
		return 0, err
	}
	return res, nil
}

func dbGetReserveByMintAndTime(ctx context.Context, db *sqlx.DB, mint string, t time.Time, ordering q.Ordering) (*historicalReserveModel, error) {
	res := &historicalReserveModel{}
	err := db.GetContext(ctx, res,
		makeTimeBasedGetQuery(reserveTableName, "mint = $1 AND for_date = $2 AND for_timestamp <= $3", ordering),
		mint,
		t.UTC().Format(dateFormat),
		t.UTC(),
	)
	return res, pgutil.CheckNoRows(err, currency.ErrNotFound)
}

func dbGetAllReservesForRange(ctx context.Context, db *sqlx.DB, mint string, interval q.Interval, start time.Time, end time.Time, ordering q.Ordering) ([]*historicalReserveModel, error) {
	res := []*historicalReserveModel{}
	err := db.SelectContext(ctx, &res,
		makeTimeBasedRangeQuery(reserveTableName, "mint = $1 AND for_timestamp >= $2 AND for_timestamp <= $3", ordering, interval),
		mint, start.UTC(), end.UTC(),
	)

	if err != nil {
		return nil, pgutil.CheckNoRows(err, currency.ErrNotFound)
	}
	if len(res) == 0 {
		return nil, currency.ErrNotFound
	}

	return res, nil
}

func (m *liveReserveModel) dbSave(ctx context.Context, db *sqlx.DB) error {
	return pgutil.ExecuteInTx(ctx, db, sql.LevelDefault, func(tx *sqlx.Tx) error {
		err := tx.QueryRowxContext(ctx,
			`INSERT INTO `+liveReserveTableName+`
			(mint, supply_from_bonding, slot, last_updated_at)
			VALUES ($1, $2, $3, $4)

			ON CONFLICT (mint)
			DO UPDATE SET supply_from_bonding = $2, slot = $3, last_updated_at = $4
				WHERE `+liveReserveTableName+`.slot < $3

			RETURNING id, mint, supply_from_bonding, slot, last_updated_at`,
			m.Mint,
			m.SupplyFromBonding,
			m.Slot,
			m.LastUpdatedAt,
		).StructScan(m)

		return pgutil.CheckNoRows(err, currency.ErrStaleReserveState)
	})
}

func dbGetLiveReserveByMint(ctx context.Context, db *sqlx.DB, mint string) (*liveReserveModel, error) {
	res := &liveReserveModel{}
	err := db.GetContext(ctx, res,
		`SELECT id, mint, supply_from_bonding, slot, last_updated_at
		FROM `+liveReserveTableName+`
		WHERE mint = $1`,
		mint,
	)
	return res, pgutil.CheckNoRows(err, currency.ErrNotFound)
}

func dbGetAllLiveReserves(ctx context.Context, db *sqlx.DB) ([]*liveReserveModel, error) {
	var res []*liveReserveModel
	err := db.SelectContext(ctx, &res,
		`SELECT id, mint, supply_from_bonding, slot, last_updated_at
		FROM `+liveReserveTableName,
	)
	if err != nil {
		return nil, pgutil.CheckNoRows(err, currency.ErrNotFound)
	}
	if len(res) == 0 {
		return nil, currency.ErrNotFound
	}
	return res, nil
}

func (m *historicalHolderCountModel) dbSave(ctx context.Context, db *sqlx.DB) error {
	return pgutil.ExecuteInTx(ctx, db, sql.LevelDefault, func(tx *sqlx.Tx) error {
		err := tx.QueryRowxContext(ctx,
			`INSERT INTO `+holderCountTableName+`
			(for_date, for_timestamp, mint, holder_count)
			VALUES ($1, $2, $3, $4)
			RETURNING id, for_date, for_timestamp, mint, holder_count`,
			m.ForDate,
			m.ForTimestamp,
			m.Mint,
			m.HolderCount,
		).StructScan(m)

		return pgutil.CheckUniqueViolation(err, currency.ErrExists)
	})
}

func dbGetHolderCountByMintAndTime(ctx context.Context, db *sqlx.DB, mint string, t time.Time, ordering q.Ordering) (*historicalHolderCountModel, error) {
	res := &historicalHolderCountModel{}
	err := db.GetContext(ctx, res,
		makeTimeBasedGetQuery(holderCountTableName, "mint = $1 AND for_date = $2 AND for_timestamp <= $3", ordering),
		mint,
		t.UTC().Format(dateFormat),
		t.UTC(),
	)
	return res, pgutil.CheckNoRows(err, currency.ErrNotFound)
}

func (m *liveHolderCountModel) dbSave(ctx context.Context, db *sqlx.DB) error {
	return pgutil.ExecuteInTx(ctx, db, sql.LevelDefault, func(tx *sqlx.Tx) error {
		err := tx.QueryRowxContext(ctx,
			`INSERT INTO `+liveHolderCountTableName+`
			(mint, holder_count, last_updated_at)
			VALUES ($1, $2, $3)

			ON CONFLICT (mint)
			DO UPDATE SET holder_count = $2, last_updated_at = $3
				WHERE `+liveHolderCountTableName+`.last_updated_at < $3

			RETURNING id, mint, holder_count, last_updated_at`,
			m.Mint,
			m.HolderCount,
			m.LastUpdatedAt,
		).StructScan(m)

		return pgutil.CheckNoRows(err, currency.ErrStaleHolderState)
	})
}

func dbGetLiveHolderCountByMint(ctx context.Context, db *sqlx.DB, mint string) (*liveHolderCountModel, error) {
	res := &liveHolderCountModel{}
	err := db.GetContext(ctx, res,
		`SELECT id, mint, holder_count, last_updated_at
		FROM `+liveHolderCountTableName+`
		WHERE mint = $1`,
		mint,
	)
	return res, pgutil.CheckNoRows(err, currency.ErrNotFound)
}

func dbGetAllLiveHolderCounts(ctx context.Context, db *sqlx.DB) ([]*liveHolderCountModel, error) {
	var res []*liveHolderCountModel
	err := db.SelectContext(ctx, &res,
		`SELECT id, mint, holder_count, last_updated_at
		FROM `+liveHolderCountTableName,
	)
	if err != nil {
		return nil, pgutil.CheckNoRows(err, currency.ErrNotFound)
	}
	if len(res) == 0 {
		return nil, currency.ErrNotFound
	}
	return res, nil
}
