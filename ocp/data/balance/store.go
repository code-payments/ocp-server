package balance

import (
	"context"
	"errors"

	commonpb "github.com/code-payments/ocp-protobuf-api/generated/go/common/v1"
)

var (
	ErrStaleCachedBalanceVersion = errors.New("cached balance version is stale")

	ErrAccountClosed = errors.New("account open state is stale")

	ErrCheckpointNotFound = errors.New("checkpoint not found")
	ErrStaleCheckpoint    = errors.New("checkpoint is stale")

	ErrBalanceNotFound = errors.New("balance not found")
	ErrNegativeBalance = errors.New("balance would go negative")
)

type Store interface {
	// GetCachedVersion gets the current cached balance version, which can be used
	// for optimistic locking cached balances for operations with outgoing transfers.
	GetCachedVersion(ctx context.Context, account string) (uint64, error)

	// AdvanceCachedVersion advances an account's cached balance version.
	//
	// ErrStaleCachedBalanceVersion is returned if the currentVersion is out of date.
	AdvanceCachedVersion(ctx context.Context, account string, currentVersion uint64) error

	// CheckNotClosed checks whether an account is closed under a lock to guarantee
	// payments to a closeable destination with cached balances are made to an open
	// account.
	//
	// ErrAccountClosed is returned if the account has been closed.
	CheckNotClosed(ctx context.Context, account string) error

	// MarkAsClosed marks an account as being closed and unable to receive payments
	// as a destination.
	MarkAsClosed(ctx context.Context, account string) error

	// SaveExternalCheckpoint saves an external balance at a checkpoint.
	//
	// ErrStaleCheckpoint is returned if the checkpoint is outdated
	SaveExternalCheckpoint(ctx context.Context, record *ExternalCheckpointRecord) error

	// GetExternalCheckpoint gets an exeternal balance checkpoint for a
	// given account.
	//
	// ErrCheckpointNotFound is returend if no DB record exists.
	GetExternalCheckpoint(ctx context.Context, account string) (*ExternalCheckpointRecord, error)

	// GetBalance gets the current balance for a token account from the
	// materialized balance table.
	//
	// ErrBalanceNotFound is returned if no balance record exists.
	GetBalance(ctx context.Context, account string) (*AccountBalanceRecord, error)

	// GetBalanceBatch gets balances for a batch of token accounts. Accounts
	// without a balance record are returned with a zero-valued record.
	GetBalanceBatch(ctx context.Context, accounts ...string) (map[string]*AccountBalanceRecord, error)

	// AdjustBalance adjusts a token account's balance by a relative delta.
	// The row is created if it doesn't exist. The CHECK constraint on the
	// underlying table prevents the balance from going negative.
	AdjustBalance(ctx context.Context, account string, quarks int64, usdCostBasis float64, owner string, mint string, accountType commonpb.AccountType) error
}
