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
	ErrStaleReserveState    = errors.New("reserve state is stale")
	ErrStaleHolderState     = errors.New("holder count state is stale")
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

	// GetAllMetadataByState returns all currency metadata records in a given state
	//
	// ErrNotFound is returned if no metadata records exist for the given state
	GetAllMetadataByState(ctx context.Context, state MetadataState, cursor query.Cursor, limit uint64, direction query.Ordering) ([]*MetadataRecord, error)

	// CountMetadataByState returns the count of currency metadata records in the requested state
	CountMetadataByState(ctx context.Context, state MetadataState) (uint64, error)

	// GetAllMints returns the public keys of all currency creator mints
	//
	// ErrNotFound is returned if no mints exist
	GetAllMints(ctx context.Context) ([]string, error)

	// CountMints returns the total number of currency creator mints,
	// excluding those in the abandoned state.
	CountMints(ctx context.Context) (uint64, error)

	// IsNameAvailable checks whether a currency name is available for use.
	// The check is case-insensitive.
	IsNameAvailable(ctx context.Context, name string) (bool, error)

	// PutHistoricalReserveRecord puts a currency creator mint reserve records into the store.
	PutHistoricalReserveRecord(ctx context.Context, record *ReserveRecord) error

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

	// PutLiveReserveRecord upserts the latest reserve record for a currency creator
	// mint. An upsert is only performed if the provided slot is greater than the slot
	// currently stored.
	//
	// ErrStaleReserveState is returned if the provided slot is not greater than the
	// stored slot.
	PutLiveReserveRecord(ctx context.Context, record *ReserveRecord) error

	// GetLiveReserve gets the latest live reserve record for a currency creator mint.
	//
	// ErrNotFound is returned if no live reserve record exists for the provided mint.
	GetLiveReserve(ctx context.Context, mint string) (*ReserveRecord, error)

	// GetAllLiveReserves gets the latest live reserve records for all currency
	// creator mints.
	//
	// ErrNotFound is returned if no live reserve records exist.
	GetAllLiveReserves(ctx context.Context) (map[string]*ReserveRecord, error)

	// PutHistoricalHolderCountRecord puts a currency creator mint holder count
	// record into the store.
	PutHistoricalHolderCountRecord(ctx context.Context, record *HolderCountRecord) error

	// GetHolderCountAtTime gets the holder count for a given currency creator
	// mint at a point in time. If the exact time is not available, the most
	// recent data prior to the requested date within the same day will get
	// returned, if available.
	//
	// ErrNotFound is returned if no holder count data was found for the
	// provided Timestamp.
	GetHolderCountAtTime(ctx context.Context, mint string, t time.Time) (*HolderCountRecord, error)

	// GetAllHolderCountsAtTime gets all holder counts at a specific point in
	// time. For each mint, the most recent record prior to the requested time
	// within the same day is returned.
	//
	// ErrNotFound is returned if no holder count data was found for the
	// provided Timestamp.
	GetAllHolderCountsAtTime(ctx context.Context, t time.Time) (map[string]*HolderCountRecord, error)

	// PutLiveHolderCountRecord upserts the latest holder count record for a
	// currency creator mint. An upsert is only performed if the provided
	// timestamp is greater than the timestamp currently stored.
	//
	// ErrStaleHolderState is returned if the provided timestamp is not greater
	// than the stored timestamp.
	PutLiveHolderCountRecord(ctx context.Context, record *HolderCountRecord) error

	// GetLiveHolderCount gets the latest live holder count record for a
	// currency creator mint.
	//
	// ErrNotFound is returned if no live holder count record exists for the
	// provided mint.
	GetLiveHolderCount(ctx context.Context, mint string) (*HolderCountRecord, error)

	// GetAllLiveHolderCounts gets the latest live holder count records for all
	// currency creator mints.
	//
	// ErrNotFound is returned if no live holder count records exist.
	GetAllLiveHolderCounts(ctx context.Context) (map[string]*HolderCountRecord, error)
}
