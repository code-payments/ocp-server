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

	ErrAccountClosed = errors.New("account open state is stale")

	// ErrAccountUnlocked is returned when a delta other than a credit targets
	// an account whose timelock vault has unlocked. The ledger stops
	// maintaining the record at unlock, so nothing may leave it. Credits are
	// still applied, since an unlocked record is excluded from every read and
	// turning one away only blocks the flow recording it.
	ErrAccountUnlocked = errors.New("account is unlocked")

	ErrCheckpointNotFound = errors.New("checkpoint not found")
	ErrStaleCheckpoint    = errors.New("checkpoint is stale")
)

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

	// GetAllLockedByMint gets locked balance records for a mint with at
	// least minQuarks, paged by record ID. Unlocked records are excluded,
	// since funds can move on chain without an intent once a vault unlocks,
	// making their balances stale.
	//
	// ErrRecordNotFound is returned if no records exist.
	GetAllLockedByMint(ctx context.Context, mint string, minQuarks int64, cursor query.Cursor, limit uint64, direction query.Ordering) ([]*Record, error)

	// CountLockedByMint counts locked records for a mint with at least
	// minQuarks. Unlocked records are excluded, since their balances are
	// stale.
	CountLockedByMint(ctx context.Context, mint string, minQuarks int64) (uint64, error)

	// ApplyDeltas atomically applies a set of deltas. Either every delta is
	// applied or none are. Deltas are applied in SortDeltas order.
	//
	// Every delta must target an account with a record, otherwise
	// ErrRecordNotFound is returned and nothing is applied. Callers are
	// responsible for not producing deltas for accounts the ledger doesn't
	// track, like external wallets.
	//
	// ErrInsufficientBalance is returned when a debit exceeds the balance.
	// ErrBalanceChanged is returned when a drain or close doesn't match the
	// balance. ErrAccountClosed is returned when a credit, debit, drain or
	// close targets a closed account, which is frozen. ErrAccountUnlocked is
	// returned when a delta other than a credit targets an unlocked account,
	// whose record is no longer maintained. DeltaAdjustUsdCostBasis carries
	// no predicate: it moves no quarks, so it applies to closed and unlocked
	// accounts alike and only fails when the record is missing.
	ApplyDeltas(ctx context.Context, deltas ...*Delta) error

	// MarkAsUnlocked marks an account's timelock vault as unlocked, which is
	// one-way and idempotent. It is called in the same transaction that
	// commits the timelock record's transition out of the locked state, so
	// the flag cannot disagree with the timelock record it mirrors.
	//
	// ErrRecordNotFound is returned if no record exists.
	MarkAsUnlocked(ctx context.Context, tokenAccount string) error

	// SaveExternalCheckpoint saves an external balance at a checkpoint.
	//
	// ErrStaleCheckpoint is returned if the checkpoint is outdated
	SaveExternalCheckpoint(ctx context.Context, record *ExternalCheckpointRecord) error

	// GetExternalCheckpoint gets an exeternal balance checkpoint for a
	// given account.
	//
	// ErrCheckpointNotFound is returend if no DB record exists.
	GetExternalCheckpoint(ctx context.Context, account string) (*ExternalCheckpointRecord, error)
}
