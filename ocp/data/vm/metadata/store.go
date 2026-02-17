package metadata

import (
	"context"
	"errors"
	"time"
)

var (
	ErrAlreadyExists = errors.New("vm metadata already exists")
	ErrNotFound      = errors.New("vm metadata not found")
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

	CreatedAt time.Time
}

type Store interface {
	// Put inserts or updates a VM metadata record
	Put(ctx context.Context, record *Record) error

	// GetByMint returns the VM metadata record for the given mint
	GetByMint(ctx context.Context, mint string) (*Record, error)
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
	dst.CreatedAt = r.CreatedAt
}
