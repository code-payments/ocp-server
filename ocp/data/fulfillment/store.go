package fulfillment

import (
	"context"

	"github.com/pkg/errors"

	"github.com/code-payments/ocp-server/database/query"
)

var (
	ErrFulfillmentNotFound = errors.New("no fulfillment could be found")
	ErrFulfillmentExists   = errors.New("fulfillment exists")
	ErrStaleVersion        = errors.New("fulfillment version is stale")
)

type Store interface {
	// Count returns the total count of fulfillments in the provided state.
	CountByState(ctx context.Context, state State) (uint64, error)

	// CountForMetrics is like CountByStateGroupedByType for metrics. Partial data may be provided.
	CountForMetrics(ctx context.Context, state State) (map[Type]uint64, error)

	// CountByStateAndAddress returns the total count of fulfillments for the provided account and state.
	CountByStateAndAddress(ctx context.Context, state State, address string) (uint64, error)

	// Count returns the total count of fulfillments for the provided intent and state.
	CountByIntentAndState(ctx context.Context, intent string, state State) (uint64, error)

	// Count returns the total count of fulfillments for the provided intent.
	CountByIntent(ctx context.Context, intent string) (uint64, error)

	// CountPendingByType gets the count of pending transactions by type.
	// This is particularly useful for estimating fees that will be consumed
	// by our subsidizer.
	CountPendingByType(ctx context.Context) (map[Type]uint64, error)

	// PutAll creates all fulfillments in one transaction
	PutAll(ctx context.Context, records ...*Record) error

	// Update updates an existing fulfillment record
	//
	// Note 1: Updating pre-sorting metadata is allowed but limited to certain fulfillment types
	Update(ctx context.Context, record *Record) error

	// GetById find the fulfillment recofd for a given ID
	GetById(ctx context.Context, id uint64) (*Record, error)

	// GetBySignature finds the fulfillment record for a given signature.
	GetBySignature(ctx context.Context, signature string) (*Record, error)

	// GetByVirtualSignature finds the fulfillment record for a given virtual signature.
	GetByVirtualSignature(ctx context.Context, signature string) (*Record, error)

	// GetAllByState returns all fulfillment records for a given state.
	//
	// Returns ErrNotFound if no records are found.
	GetAllByState(ctx context.Context, state State, includeDisabledActiveScheduling bool, cursor query.Cursor, limit uint64, direction query.Ordering) ([]*Record, error)

	// GetAllByIntent returns all fulfillment records for a given intent.
	//
	// Returns ErrNotFound if no records are found.
	GetAllByIntent(ctx context.Context, intent string, cursor query.Cursor, limit uint64, direction query.Ordering) ([]*Record, error)

	// GetAllByAction returns all fulfillment records for a given action
	//
	// Returns ErrNotFound if no records are found.
	GetAllByAction(ctx context.Context, intentId string, actionId uint32) ([]*Record, error)

	// GetFirstSchedulableByAddressAsSource returns the earliest fulfillment
	// that can be scheduled for an account as a source given the total ordering
	// of all fulfillments.
	//
	// Returns ErrNotFound if no records are found.
	GetFirstSchedulableByAddressAsSource(ctx context.Context, address string) (*Record, error)

	// GetFirstSchedulableByAddressAsDestination returns the earliest fulfillment
	// that can be scheduled for an account as a destination given the total ordering
	// of all fulfillments.
	//
	// Returns ErrNotFound if no records are found.
	GetFirstSchedulableByAddressAsDestination(ctx context.Context, address string) (*Record, error)

	// GetNextSchedulableByAddress gets the next schedulable fulfillment for an account after
	// a point in time defined by ordering indices.
	//
	// Returns ErrNotFound if no records are found.
	GetNextSchedulableByAddress(ctx context.Context, address string, intentOrderingIndex uint64, actionOrderingIndex, fulfillmentOrderingIndex uint32) (*Record, error)
}
