package history

import (
	"context"
	"encoding/binary"
	"errors"
	"time"

	"github.com/code-payments/ocp-server/database/query"
)

var (
	ErrNotFound      = errors.New("no records could be found")
	ErrExists        = errors.New("history record already exists")
	ErrStaleVersion  = errors.New("history record version is stale")
	ErrInvalidCursor = errors.New("cursor is invalid")
)

// cursorSize is the byte length of an encoded cursor: the event time as
// big-endian unix nanoseconds, followed by the record ID.
const cursorSize = 16

// ToCursor encodes a record's position in a history. A history is ordered by
// event time, which is not unique, so a position is the time paired with the
// record ID that breaks ties. Ordering on the time alone would let a page skip
// or repeat the records sharing a boundary timestamp.
func ToCursor(createdAt time.Time, id uint64) query.Cursor {
	b := make([]byte, cursorSize)
	binary.BigEndian.PutUint64(b[0:8], uint64(createdAt.UnixNano()))
	binary.BigEndian.PutUint64(b[8:16], id)
	return b
}

// FromCursor reverses ToCursor. It reports ok false for anything that is not a
// cursor this package produced.
func FromCursor(cursor query.Cursor) (createdAt time.Time, id uint64, ok bool) {
	if len(cursor) != cursorSize {
		return time.Time{}, 0, false
	}
	createdAt = time.Unix(0, int64(binary.BigEndian.Uint64(cursor[0:8]))).UTC()
	id = binary.BigEndian.Uint64(cursor[8:16])
	return createdAt, id, true
}

// Store stores a per-owner history of ledger events. A record is one owner's
// view of one event, so an event involving two owners is two records.
type Store interface {
	// Save creates or updates a record.
	//
	// Returns ErrExists if the owner already has a record for the reference, and
	// ErrStaleVersion if the stored record has moved on.
	Save(ctx context.Context, record *Record) error

	// GetAllByOwner gets a page of an owner's history across all mints, ordered
	// by event time and then by ID, from the position named by cursor. A limit
	// of zero is unbounded.
	//
	// The order is the one a history is read in, so it is the event time rather
	// than the order records happened to be written. The two differ whenever an
	// event is recorded late — a backfill, or a deposit noticed after the fact —
	// and ordering by the write would put those records somewhere their own
	// timestamps do not explain. The cost is that such a record lands behind a
	// cursor a caller has already passed and is seen on a later read from the
	// start, rather than never.
	//
	// Returns ErrInvalidCursor for a cursor this package did not produce, and
	// ErrNotFound if no records are found.
	GetAllByOwner(ctx context.Context, owner string, cursor query.Cursor, limit uint64, direction query.Ordering) ([]*Record, error)

	// GetAllByOwnerMint gets a page of an owner's history for records involving a
	// mint, as either the source or the destination, so that a mint's history
	// holds what was traded into it as well as out of it. It is otherwise
	// GetAllByOwner.
	//
	// Returns ErrNotFound if no records are found.
	GetAllByOwnerMint(ctx context.Context, owner, mint string, cursor query.Cursor, limit uint64, direction query.Ordering) ([]*Record, error)

	// GetAllByIds gets a set of records by ID in one query, ordered by ID. An ID
	// with no record is omitted rather than reported, so a partial result is
	// normal and a caller should not read anything into the count.
	//
	// It is not scoped to an owner, so a caller serving a request on an owner's
	// behalf has to check the records it gets back belong to that owner.
	//
	// Returns ErrNotFound if no records are found.
	GetAllByIds(ctx context.Context, ids []uint64) ([]*Record, error)

	// GetAllByReference gets every owner's records for a reference. It is how an
	// outcome that arrives naming the intent or swap it concerns, rather than any
	// record, finds the records to transition.
	//
	// The reference is qualified by its type, since an ID is only unique within
	// its own kind, so a caller gets back only the records the thing it named
	// produced.
	//
	// Returns ErrNotFound if no records are found.
	GetAllByReference(ctx context.Context, referenceType ReferenceType, referenceId string) ([]*Record, error)

	// GetAllByGiftCardVault gets the records for a gift card: the issuer's
	// IndirectlySent record and, once claimed, the claimant's IndirectlyReceived
	// record. A card being claimed, voided, or returned is reported by vault, so
	// it cannot reach those records by reference.
	//
	// Returns ErrNotFound if no records are found.
	GetAllByGiftCardVault(ctx context.Context, vault string) ([]*Record, error)
}
