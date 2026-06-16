// Package exchange defines a focused store for core-mint exchange rate records.
//
// It mirrors the exchange-rate portion of the larger ocp/data/currency store,
// reusing that package's record types (currency.ExchangeRateRecord,
// currency.MultiRateRecord) and error sentinels (currency.ErrNotFound,
// currency.ErrExists, currency.ErrInvalidRange, currency.ErrInvalidInterval) so
// callers see identical semantics. A DynamoDB-backed implementation lives in the
// dynamodb subpackage and an in-memory implementation lives in memory.
package exchange

import (
	"context"
	"time"

	"github.com/code-payments/ocp-server/database/query"
	"github.com/code-payments/ocp-server/ocp/data/currency"
)

type Store interface {
	// PutExchangeRates puts exchange rate records for the core mint into the store.
	//
	// currency.ErrExists is returned if records already exist for the provided time.
	PutExchangeRates(ctx context.Context, record *currency.MultiRateRecord) error

	// GetExchangeRate gets price information given a certain time and currency symbol
	// for the core mint. The most recent record at or before the requested time is
	// returned.
	//
	// currency.ErrNotFound is returned if no price data exists at or before the provided time.
	GetExchangeRate(ctx context.Context, symbol string, t time.Time) (*currency.ExchangeRateRecord, error)

	// GetAllExchangeRates gets price information given a certain time for the core mint.
	// The most recent record at or before the requested time is returned.
	//
	// currency.ErrNotFound is returned if no price data exists at or before the provided time.
	GetAllExchangeRates(ctx context.Context, t time.Time) (*currency.MultiRateRecord, error)

	// GetExchangeRatesInRange gets the price information for a range of time given a currency
	// symbol and interval for the core mint. The start and end timestamps are provided along
	// with the interval.
	//
	// currency.ErrNotFound is returned if the symbol or the exchange rates for the symbol cannot be found
	// currency.ErrInvalidRange is returned if the range is not valid
	// currency.ErrInvalidInterval is returned if the interval is not valid
	GetExchangeRatesInRange(ctx context.Context, symbol string, interval query.Interval, start time.Time, end time.Time, ordering query.Ordering) ([]*currency.ExchangeRateRecord, error)
}
