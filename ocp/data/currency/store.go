package currency

import (
	"context"
	"errors"
	"time"

	"github.com/code-payments/ocp-server/database/query"
)

var (
	ErrNotFound             = errors.New("record not found")
	ErrInvalidRange         = errors.New("the provided range is not valid")
	ErrInvalidInterval      = errors.New("the provided interval is not valid")
	ErrExists               = errors.New("record exists")
	ErrStaleMetadataVersion = errors.New("metadata version is stale")
	ErrDuplicateCurrency    = errors.New("duplicate currency detected")
)

type Store interface {
	// PutExchangeRates puts exchange rate records for the core mint into the store.
	PutExchangeRates(ctx context.Context, record *MultiRateRecord) error

	// GetExchangeRate gets price information given a certain time and currency symbol
	// for the core mint. If the exact time is not available, the most recent data prior
	// to the requested date within the same day will get returned, if available.
	//
	// ErrNotFound is returned if no price data was found for the provided Timestamp.
	GetExchangeRate(ctx context.Context, symbol string, t time.Time) (*ExchangeRateRecord, error)

	// GetAllExchangeRates gets price information given a certain time for the core mint.
	// If the exact time is not available, the most recent data prior to the requested date
	// within the same day will get returned, if available.
	//
	// ErrNotFound is returned if no price data was found for the provided Timestamp.
	GetAllExchangeRates(ctx context.Context, t time.Time) (*MultiRateRecord, error)

	// GetExchangeRatesInRange gets the price information for a range of time given a currency
	// symbol and interval for the core mint. The start and end timestamps are provided along
	// with the interval. If the raw data is not available at the sampling frequency requested,
	// it will be linearly interpolated between available points.
	//
	// ErrNotFound is returned if the symbol or the exchange rates for the symbol cannot be found
	// ErrInvalidRange is returned if the range is not valid
	// ErrInvalidInterval is returned if the interval is not valid
	GetExchangeRatesInRange(ctx context.Context, symbol string, interval query.Interval, start time.Time, end time.Time, ordering query.Ordering) ([]*ExchangeRateRecord, error)

	// SaveMetadata creates or updates currency creator metadata in the store.
	// On insert, Version is set to 1. On update, only mutable fields (Description,
	// ImageUrl, BillColors, SocialLinks, Alt, State) are updated and Version is incremented.
	// ErrStaleMetadataVersion is returned when the provided version doesn't match.
	SaveMetadata(ctx context.Context, record *MetadataRecord) error

	// GetMetadata gets currency creator mint metadata by the mint address
	GetMetadata(ctx context.Context, mint string) (*MetadataRecord, error)

	// GetAllMints returns the public keys of all currency creator mints
	//
	// ErrNotFound is returned if no mints exist
	GetAllMints(ctx context.Context) ([]string, error)

	// CountMints returns the total number of currency creator mints
	CountMints(ctx context.Context) (uint64, error)

	// PutReserveRecord puts a currency creator mint reserve records into the store.
	PutReserveRecord(ctx context.Context, record *ReserveRecord) error

	// GetReserveAtTime gets reserve state for a given currency creator mint at a point
	// in time. If the exact time is not available, the most recent data prior to the
	// requested date within the same day will get returned, if available.
	//
	// ErrNotFound is returned if no reserve data was found for the provided Timestamp.
	GetReserveAtTime(ctx context.Context, mint string, t time.Time) (*ReserveRecord, error)

	// GetReservesInRange gets the reserve records for a range of time given a currency
	// creator mint and interval. The start and end timestamps are provided along with
	// the interval.
	//
	// ErrNotFound is returned if the mint or the reserves for the mint cannot be found
	// ErrInvalidRange is returned if the range is not valid
	// ErrInvalidInterval is returned if the interval is not valid
	GetReservesInRange(ctx context.Context, mint string, interval query.Interval, start time.Time, end time.Time, ordering query.Ordering) ([]*ReserveRecord, error)
}
