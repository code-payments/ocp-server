package swap

import (
	"context"
	"errors"

	"github.com/code-payments/ocp-server/database/query"
)

var (
	ErrNotFound     = errors.New("swap not found")
	ErrExists       = errors.New("swap already exists")
	ErrStaleVersion = errors.New("swap version is stale")
)

type Store interface {
	// Save creates or updates a swap
	Save(ctx context.Context, record *Record) error

	// GetById gets a swap by ID
	GetById(ctx context.Context, id string) (*Record, error)

	// GetByFundingId gets a swap by the funding ID
	GetByFundingId(ctx context.Context, fundingId string) (*Record, error)

	// GetAllByOwnerAndStates gets all swaps for an owner in any of the provided states
	GetAllByOwnerAndStates(ctx context.Context, owner string, states ...State) ([]*Record, error)

	// GetAllByState gets all swaps by state
	GetAllByState(ctx context.Context, state State, cursor query.Cursor, limit uint64, direction query.Ordering) ([]*Record, error)

	// CountByState returns the count of swaps in the requested state
	CountByState(ctx context.Context, state State) (uint64, error)
}
