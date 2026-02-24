package metadata

import (
	"context"
	"errors"
	"time"
)

type State uint8

const (
	StateUnknown State = iota
	StateAvailable
	// todo: define more states
)

var (
	ErrNotFound     = errors.New("vm metadata not found")
	ErrStaleVersion = errors.New("vm metadata version is stale")
)

type Record struct {
	Id uint64

	Mint        string
	Authority   string
	Vm          string
	VmBump      uint8
	Omnibus     string
	OmnibusBump uint8
	DaysLocked  uint8

	State   State
	Version uint64

	CreatedAt time.Time
}

type Store interface {
	// Save creates or updates a VM metadata record in the store.
	// On insert, Version is set to 1. On update, Version is incremented.
	// ErrStaleVersion is returned when the provided version doesn't match.
	Save(ctx context.Context, record *Record) error

	// GetByMint returns the VM metadata record for the given mint
	GetByMint(ctx context.Context, mint string) (*Record, error)

	// GetAllVms returns all VM public keys
	GetAllVms(ctx context.Context) ([]string, error)
}

func (r *Record) Validate() error {
	if len(r.Mint) == 0 {
		return errors.New("mint is required")
	}

	if len(r.Authority) == 0 {
		return errors.New("authority is required")
	}

	if len(r.Vm) == 0 {
		return errors.New("vm is required")
	}

	if len(r.Omnibus) == 0 {
		return errors.New("omnibus is required")
	}

	return nil
}

func (r *Record) Clone() Record {
	return Record{
		Id:          r.Id,
		Mint:        r.Mint,
		Authority:   r.Authority,
		Vm:          r.Vm,
		VmBump:      r.VmBump,
		Omnibus:     r.Omnibus,
		OmnibusBump: r.OmnibusBump,
		DaysLocked:  r.DaysLocked,
		State:       r.State,
		Version:     r.Version,
		CreatedAt:   r.CreatedAt,
	}
}

func (r *Record) CopyTo(dst *Record) {
	dst.Id = r.Id
	dst.Mint = r.Mint
	dst.Authority = r.Authority
	dst.Vm = r.Vm
	dst.VmBump = r.VmBump
	dst.Omnibus = r.Omnibus
	dst.OmnibusBump = r.OmnibusBump
	dst.DaysLocked = r.DaysLocked
	dst.State = r.State
	dst.Version = r.Version
	dst.CreatedAt = r.CreatedAt
}
