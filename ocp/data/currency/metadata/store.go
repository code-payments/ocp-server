// Package metadata defines a focused store for currency creator mint metadata.
package metadata

import (
	"context"

	"github.com/code-payments/ocp-server/database/query"
	"github.com/code-payments/ocp-server/ocp/data/currency"
)

type Store interface {
	// SaveMetadata creates or updates currency creator metadata in the store.
	// On insert, Version is set to 1. On update, only mutable fields (Description,
	// ImageUrl, BillColors, SocialLinks, Alt, IsDiscoverable, State) are updated and Version is incremented.
	// ErrStaleMetadataVersion is returned when the provided version doesn't match.
	SaveMetadata(ctx context.Context, record *currency.MetadataRecord) error

	// GetMetadata gets currency creator mint metadata by the mint address
	GetMetadata(ctx context.Context, mint string) (*currency.MetadataRecord, error)

	// GetAllMetadataByState returns all currency metadata records in a given state
	//
	// ErrNotFound is returned if no metadata records exist for the given state
	GetAllMetadataByState(ctx context.Context, state currency.MetadataState, cursor query.Cursor, limit uint64, direction query.Ordering) ([]*currency.MetadataRecord, error)

	// CountMetadataByState returns the count of currency metadata records in the requested state
	CountMetadataByState(ctx context.Context, state currency.MetadataState) (uint64, error)

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
}
