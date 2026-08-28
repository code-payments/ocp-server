package balance

import (
	"context"
	"errors"

	"github.com/code-payments/ocp-server/database/query"
)

var (
	ErrRecordNotFound = errors.New("balance record not found")
	ErrRecordExists   = errors.New("balance record already exists")

	// ErrInsufficientBalance is returned when a debit exceeds the balance.
	ErrInsufficientBalance = errors.New("insufficient balance")

	// ErrBalanceChanged is returned when a drain or close expected a different
	// balance than the one on record.
	ErrBalanceChanged = errors.New("balance is not the expected value")

	ErrAlreadyBackfilled = errors.New("balance record is already backfilled")

	// ErrNegativeBalance is returned when a backfill computes a negative
	// balance, which indicates inconsistent historical data that must be
	// reviewed rather than recorded.
	ErrNegativeBalance = errors.New("backfilled balance is negative")

	ErrStaleCachedBalanceVersion = errors.New("cached balance version is stale")

	ErrAccountClosed = errors.New("account open state is stale")

	ErrCheckpointNotFound = errors.New("checkpoint not found")
	ErrStaleCheckpoint    = errors.New("checkpoint is stale")
)

// BackfillResult is the full historical state of a token account.
type BackfillResult struct {
	Quarks       int64
	UsdCostBasis int64

	// IsOpen is false for accounts that can no longer receive funds, such as
	// claimed gift cards and distributed pools.
	IsOpen bool
}

// BackfillFunc computes the full historical state of a token account. It
// is called while the record is locked, with a context that is part of the
// same DB transaction, so any store reads made through it observe every
// committed change and block every in-flight one.
type BackfillFunc func(ctx context.Context) (*BackfillResult, error)

type Store interface {
	// Create creates a new balance record.
	//
	// ErrRecordExists is returned if the token account already has a record.
	Create(ctx context.Context, record *Record) error

	// Get gets the balance record for a token account.
	//
	// ErrRecordNotFound is returned if no record exists.
	Get(ctx context.Context, tokenAccount string) (*Record, error)

	// GetBatch gets balance records for a set of token accounts. Accounts
	// without a record are omitted from the result.
	GetBatch(ctx context.Context, tokenAccounts ...string) (map[string]*Record, error)

	// GetAllByOwner gets all balance records for an owner.
	//
	// ErrRecordNotFound is returned if no records exist.
	GetAllByOwner(ctx context.Context, owner string) ([]*Record, error)

	// GetAllByOwnerAndMint gets all balance records for an owner and mint.
	//
	// ErrRecordNotFound is returned if no records exist.
	GetAllByOwnerAndMint(ctx context.Context, owner, mint string) ([]*Record, error)

	// GetAllByMint gets balance records for a mint with at least minQuarks,
	// paged by record ID.
	//
	// ErrRecordNotFound is returned if no records exist.
	GetAllByMint(ctx context.Context, mint string, minQuarks int64, cursor query.Cursor, limit uint64, direction query.Ordering) ([]*Record, error)

	// CountByMint counts backfilled records for a mint with at least
	// minQuarks. Records that are not backfilled are excluded, since their
	// balances are partial sums that can't be compared against a threshold.
	CountByMint(ctx context.Context, mint string, minQuarks int64) (uint64, error)

	// ApplyDeltas atomically applies a set of deltas. Either every delta is
	// applied or none are. Deltas are applied in SortDeltas order.
	//
	// Predicates are enforced only on backfilled records; records that are
	// not backfilled simply accumulate the change.
	//
	// Every delta must target an account with a record, otherwise
	// ErrRecordNotFound is returned and nothing is applied. Callers are
	// responsible for not producing deltas for accounts the ledger doesn't
	// track, like external wallets.
	//
	// ErrInsufficientBalance is returned when a debit exceeds the balance.
	// ErrBalanceChanged is returned when a drain or close doesn't match the
	// balance. ErrAccountClosed is returned when a credit, drain or close
	// targets a closed account.
	ApplyDeltas(ctx context.Context, deltas ...*Delta) error

	// Backfill locks a record that is not yet backfilled, calls fn to compute
	// its full historical balance, and overwrites the record with the result,
	// marking it as backfilled. Deltas recorded before the backfill are
	// intentionally discarded, since fn observes them.
	//
	// ErrRecordNotFound is returned if no record exists. ErrAlreadyBackfilled
	// is returned if the record is already backfilled, in which case fn is
	// not called. ErrNegativeBalance is returned if fn computes a negative
	// balance, leaving the record untouched.
	Backfill(ctx context.Context, tokenAccount string, fn BackfillFunc) error

	// SaveExternalCheckpoint saves an external balance at a checkpoint.
	//
	// ErrStaleCheckpoint is returned if the checkpoint is outdated
	SaveExternalCheckpoint(ctx context.Context, record *ExternalCheckpointRecord) error

	// GetExternalCheckpoint gets an exeternal balance checkpoint for a
	// given account.
	//
	// ErrCheckpointNotFound is returend if no DB record exists.
	GetExternalCheckpoint(ctx context.Context, account string) (*ExternalCheckpointRecord, error)

	// GetCachedVersion gets the current cached balance version, which can be used
	// for optimistic locking cached balances for operations with outgoing transfers.
	//
	// Note: Use ApplyDeltas, whose predicates replace the version check.
	// Retained for accounts that are not yet backfilled.
	GetCachedVersion(ctx context.Context, account string) (uint64, error)

	// AdvanceCachedVersion advances an account's cached balance version.
	//
	// ErrStaleCachedBalanceVersion is returned if the currentVersion is out of date.
	//
	// Note: Use ApplyDeltas, whose predicates replace the version check.
	// Retained for accounts that are not yet backfilled.
	AdvanceCachedVersion(ctx context.Context, account string, currentVersion uint64) error

	// CheckNotClosed checks whether an account is closed under a lock to guarantee
	// payments to a closeable destination with cached balances are made to an open
	// account.
	//
	// ErrAccountClosed is returned if the account has been closed.
	//
	// Note: Use ApplyDeltas with DeltaCredit. Retained for accounts that
	// are not yet backfilled.
	CheckNotClosed(ctx context.Context, account string) error

	// MarkAsClosed marks an account as being closed and unable to receive payments
	// as a destination.
	//
	// Note: Use ApplyDeltas with DeltaDrain or DeltaClose. Retained for
	// accounts that are not yet backfilled.
	MarkAsClosed(ctx context.Context, account string) error
}
