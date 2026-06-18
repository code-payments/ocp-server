// Package holder defines a focused store for currency creator mint holder
// counts.
//
// Records are keyed per mint (the number of mints is unbounded), so every record
// is a single item — there is no map-of-all-mints row.
package holder

import (
	"context"
	"time"

	"github.com/code-payments/ocp-server/database/query"
	"github.com/code-payments/ocp-server/ocp/data/currency"
)

type Store interface {
	// PutHistoricalHolderCount puts a currency creator mint holder count record
	// into the store.
	//
	// currency.ErrExists is returned if a record already exists for the mint at the
	// provided time.
	PutHistoricalHolderCount(ctx context.Context, record *currency.HolderCountRecord) error

	// GetHolderCountAtTime gets the holder count for a given currency creator mint
	// at a point in time. The most recent record at or before the requested time is
	// returned.
	//
	// currency.ErrNotFound is returned if no holder count data exists at or before
	// the provided time.
	GetHolderCountAtTime(ctx context.Context, mint string, t time.Time) (*currency.HolderCountRecord, error)

	// GetHolderCountsForDay gets the holder count for each of the given currency
	// creator mints as of the UTC day of t — the close of that day (the mint's most
	// recent record within the day), keyed by mint. Mints with no record on that day
	// are omitted from the result rather than reported as an error.
	//
	// Unlike GetHolderCountAtTime this is day-granularity: it does not fall back to an
	// earlier day, and for a mid-day t the returned record may be later than t (the
	// day's close). It is served as a single batched key get against the day rollups.
	GetHolderCountsForDay(ctx context.Context, mints []string, t time.Time) (map[string]*currency.HolderCountRecord, error)

	// GetHolderCountsInRange gets the holder count records for a range of time given
	// a currency creator mint and interval.
	//
	// currency.ErrNotFound is returned if the mint or the holder counts for the mint cannot be found
	// currency.ErrInvalidRange is returned if the range is not valid
	// currency.ErrInvalidInterval is returned if the interval is not valid
	GetHolderCountsInRange(ctx context.Context, mint string, interval query.Interval, start time.Time, end time.Time, ordering query.Ordering) ([]*currency.HolderCountRecord, error)

	// PutLiveHolderCount upserts the latest holder count record for a currency
	// creator mint. An upsert is only performed if the provided timestamp is greater
	// than the timestamp currently stored.
	//
	// currency.ErrStaleHolderState is returned if the provided timestamp is not
	// greater than the stored timestamp.
	PutLiveHolderCount(ctx context.Context, record *currency.HolderCountRecord) error

	// GetLiveHolderCount gets the latest live holder count record for a currency
	// creator mint.
	//
	// currency.ErrNotFound is returned if no live holder count record exists for the provided mint.
	GetLiveHolderCount(ctx context.Context, mint string) (*currency.HolderCountRecord, error)

	// GetAllLiveHolderCounts gets the latest live holder count records for all
	// currency creator mints.
	//
	// currency.ErrNotFound is returned if no live holder count records exist.
	GetAllLiveHolderCounts(ctx context.Context) (map[string]*currency.HolderCountRecord, error)
}
