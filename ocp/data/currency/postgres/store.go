package postgres

import (
	"context"
	"database/sql"
	"time"

	"github.com/jmoiron/sqlx"

	"github.com/code-payments/ocp-server/database/query"
	"github.com/code-payments/ocp-server/ocp/data/currency"

	pg "github.com/code-payments/ocp-server/database/postgres"
)

type store struct {
	db *sqlx.DB
}

func New(db *sql.DB) currency.Store {
	return &store{
		db: sqlx.NewDb(db, "pgx"),
	}
}

func (s *store) PutExchangeRates(ctx context.Context, obj *currency.MultiRateRecord) error {
	return pg.ExecuteInTx(ctx, s.db, sql.LevelDefault, func(tx *sqlx.Tx) error {
		// Loop through all rates and save individual records (within a transaction)
		for symbol, item := range obj.Rates {
			err := toExchangeRateModel(&currency.ExchangeRateRecord{
				Time:   obj.Time,
				Rate:   item,
				Symbol: symbol,
			}).txSave(ctx, tx)

			if err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *store) GetExchangeRate(ctx context.Context, symbol string, t time.Time) (*currency.ExchangeRateRecord, error) {
	obj, err := dbGetExchangeRateBySymbolAndTime(ctx, s.db, symbol, t, query.Descending)
	if err != nil {
		return nil, err
	}

	return fromExchangeRateModel(obj), nil
}

func (s *store) GetAllExchangeRates(ctx context.Context, t time.Time) (*currency.MultiRateRecord, error) {
	list, err := dbGetAllExchangeRatesByTime(ctx, s.db, t, query.Descending)
	if err != nil {
		return nil, err
	}

	res := &currency.MultiRateRecord{
		Time:  list[0].ForTimestamp,
		Rates: map[string]float64{},
	}
	for _, item := range list {
		res.Rates[item.CurrencyCode] = item.CurrencyRate
	}

	return res, nil
}

func (s *store) GetExchangeRatesInRange(ctx context.Context, symbol string, interval query.Interval, start time.Time, end time.Time, ordering query.Ordering) ([]*currency.ExchangeRateRecord, error) {
	if interval > query.IntervalMonth {
		return nil, currency.ErrInvalidInterval
	}

	if start.IsZero() || end.IsZero() {
		return nil, currency.ErrInvalidRange
	}

	var actualStart, actualEnd time.Time
	if start.Unix() > end.Unix() {
		actualStart = end
		actualEnd = start
	} else {
		actualStart = start
		actualEnd = end
	}

	// TODO: check that the range is reasonable

	list, err := dbGetAllExchangeRatesForRange(ctx, s.db, symbol, interval, actualStart, actualEnd, ordering)
	if err != nil {
		return nil, err
	}

	res := []*currency.ExchangeRateRecord{}
	for _, item := range list {
		res = append(res, fromExchangeRateModel(item))
	}

	return res, nil
}

func (s *store) SaveMetadata(ctx context.Context, record *currency.MetadataRecord) error {
	model, err := toMetadataModel(record)
	if err != nil {
		return err
	}

	err = model.dbSave(ctx, s.db)
	if err != nil {
		return err
	}

	fromMetadataModel(model).CopyTo(record)

	return nil
}

func (s *store) GetMetadata(ctx context.Context, mint string) (*currency.MetadataRecord, error) {
	model, err := dbGetMetadataByMint(ctx, s.db, mint)
	if err != nil {
		return nil, err
	}
	return fromMetadataModel(model), nil
}

func (s *store) GetAllMetadataByState(ctx context.Context, state currency.MetadataState, cursor query.Cursor, limit uint64, direction query.Ordering) ([]*currency.MetadataRecord, error) {
	models, err := dbGetAllMetadataByState(ctx, s.db, state, cursor, limit, direction)
	if err != nil {
		return nil, err
	}

	res := make([]*currency.MetadataRecord, len(models))
	for i, model := range models {
		res[i] = fromMetadataModel(model)
	}
	return res, nil
}

func (s *store) GetAllMints(ctx context.Context) ([]string, error) {
	return dbGetAllMints(ctx, s.db)
}

func (s *store) CountMints(ctx context.Context) (uint64, error) {
	return dbCountMints(ctx, s.db)
}

func (s *store) CountMetadataByState(ctx context.Context, state currency.MetadataState) (uint64, error) {
	return dbCountMetadataByState(ctx, s.db, state)
}

func (s *store) PutHistoricalReserveRecord(ctx context.Context, record *currency.ReserveRecord) error {
	model, err := toHistoricalReserveModel(record)
	if err != nil {
		return err
	}

	err = model.dbSave(ctx, s.db)
	if err != nil {
		return err
	}

	fromHistoricalReserveModel(model).CopyTo(record)

	return nil
}

func (s *store) GetReserveAtTime(ctx context.Context, mint string, t time.Time) (*currency.ReserveRecord, error) {
	model, err := dbGetReserveByMintAndTime(ctx, s.db, mint, t, query.Descending)
	if err != nil {
		return nil, err
	}
	return fromHistoricalReserveModel(model), nil
}

func (s *store) GetReservesInRange(ctx context.Context, mint string, interval query.Interval, start time.Time, end time.Time, ordering query.Ordering) ([]*currency.ReserveRecord, error) {
	if interval > query.IntervalMonth {
		return nil, currency.ErrInvalidInterval
	}

	if start.IsZero() || end.IsZero() {
		return nil, currency.ErrInvalidRange
	}

	var actualStart, actualEnd time.Time
	if start.Unix() > end.Unix() {
		actualStart = end
		actualEnd = start
	} else {
		actualStart = start
		actualEnd = end
	}

	list, err := dbGetAllReservesForRange(ctx, s.db, mint, interval, actualStart, actualEnd, ordering)
	if err != nil {
		return nil, err
	}

	res := []*currency.ReserveRecord{}
	for _, item := range list {
		res = append(res, fromHistoricalReserveModel(item))
	}

	return res, nil
}

func (s *store) PutLiveReserveRecord(ctx context.Context, record *currency.ReserveRecord) error {
	model := toLiveReserveModel(record)

	err := model.dbSave(ctx, s.db)
	if err != nil {
		return err
	}

	fromLiveReserveModel(model).CopyTo(record)

	return nil
}

func (s *store) GetLiveReserve(ctx context.Context, mint string) (*currency.ReserveRecord, error) {
	model, err := dbGetLiveReserveByMint(ctx, s.db, mint)
	if err != nil {
		return nil, err
	}
	return fromLiveReserveModel(model), nil
}

func (s *store) GetAllLiveReserves(ctx context.Context) (map[string]*currency.ReserveRecord, error) {
	models, err := dbGetAllLiveReserves(ctx, s.db)
	if err != nil {
		return nil, err
	}

	res := make(map[string]*currency.ReserveRecord, len(models))
	for _, model := range models {
		record := fromLiveReserveModel(model)
		res[record.Mint] = record
	}
	return res, nil
}

func (s *store) PutHistoricalHolderCountRecord(ctx context.Context, record *currency.HolderCountRecord) error {
	model, err := toHistoricalHolderCountModel(record)
	if err != nil {
		return err
	}

	err = model.dbSave(ctx, s.db)
	if err != nil {
		return err
	}

	fromHistoricalHolderCountModel(model).CopyTo(record)

	return nil
}

func (s *store) GetHolderCountAtTime(ctx context.Context, mint string, t time.Time) (*currency.HolderCountRecord, error) {
	model, err := dbGetHolderCountByMintAndTime(ctx, s.db, mint, t, query.Descending)
	if err != nil {
		return nil, err
	}
	return fromHistoricalHolderCountModel(model), nil
}

func (s *store) GetAllHolderCountsAtTime(ctx context.Context, t time.Time) (map[string]*currency.HolderCountRecord, error) {
	list, err := dbGetAllHolderCountsByTime(ctx, s.db, t, query.Descending)
	if err != nil {
		return nil, err
	}

	res := make(map[string]*currency.HolderCountRecord, len(list))
	for _, item := range list {
		record := fromHistoricalHolderCountModel(item)
		res[record.Mint] = record
	}

	return res, nil
}

func (s *store) PutLiveHolderCountRecord(ctx context.Context, record *currency.HolderCountRecord) error {
	model := toLiveHolderCountModel(record)

	err := model.dbSave(ctx, s.db)
	if err != nil {
		return err
	}

	fromLiveHolderCountModel(model).CopyTo(record)

	return nil
}

func (s *store) GetLiveHolderCount(ctx context.Context, mint string) (*currency.HolderCountRecord, error) {
	model, err := dbGetLiveHolderCountByMint(ctx, s.db, mint)
	if err != nil {
		return nil, err
	}
	return fromLiveHolderCountModel(model), nil
}

func (s *store) GetAllLiveHolderCounts(ctx context.Context) (map[string]*currency.HolderCountRecord, error) {
	models, err := dbGetAllLiveHolderCounts(ctx, s.db)
	if err != nil {
		return nil, err
	}

	res := make(map[string]*currency.HolderCountRecord, len(models))
	for _, model := range models {
		record := fromLiveHolderCountModel(model)
		res[record.Mint] = record
	}
	return res, nil
}
