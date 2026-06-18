package currency

import (
	"errors"
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
