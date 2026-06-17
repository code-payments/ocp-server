// Package reserve defines a focused store for currency creator mint reserve
// states.
//
// Records are keyed per mint (the number of mints is unbounded), so every record
// is a single item — there is no map-of-all-mints row.
package reserve

import (
	"context"
	"time"

	"github.com/code-payments/ocp-server/database/query"
	"github.com/code-payments/ocp-server/ocp/data/currency"
)

type Store interface {
	// PutHistoricalReserve puts a currency creator mint reserve record into the
	// store.
	//
	// currency.ErrExists is returned if a record already exists for the mint at the
	// provided time.
	PutHistoricalReserve(ctx context.Context, record *currency.ReserveRecord) error

	// GetReserveAtTime gets reserve state for a given currency creator mint at a
	// point in time. The most recent record at or before the requested time is
	// returned.
	//
	// currency.ErrNotFound is returned if no reserve data exists at or before the
	// provided time.
	GetReserveAtTime(ctx context.Context, mint string, t time.Time) (*currency.ReserveRecord, error)

	// GetReservesInRange gets the reserve records for a range of time given a
	// currency creator mint and interval.
	//
	// currency.ErrNotFound is returned if the mint or the reserves for the mint cannot be found
	// currency.ErrInvalidRange is returned if the range is not valid
	// currency.ErrInvalidInterval is returned if the interval is not valid
	GetReservesInRange(ctx context.Context, mint string, interval query.Interval, start time.Time, end time.Time, ordering query.Ordering) ([]*currency.ReserveRecord, error)

	// PutLiveReserve upserts the latest reserve record for a currency creator mint.
	// An upsert is only performed if the provided slot is greater than the slot
	// currently stored.
	//
	// currency.ErrStaleReserveState is returned if the provided slot is not greater
	// than the stored slot.
	PutLiveReserve(ctx context.Context, record *currency.ReserveRecord) error

	// GetLiveReserve gets the latest live reserve record for a currency creator mint.
	//
	// currency.ErrNotFound is returned if no live reserve record exists for the provided mint.
	GetLiveReserve(ctx context.Context, mint string) (*currency.ReserveRecord, error)

	// GetAllLiveReserves gets the latest live reserve records for all currency
	// creator mints.
	//
	// currency.ErrNotFound is returned if no live reserve records exist.
	GetAllLiveReserves(ctx context.Context) (map[string]*currency.ReserveRecord, error)
}
