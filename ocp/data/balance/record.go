package balance

import (
	"errors"
	"math"
	"sort"
	"time"
)

// UsdQuarksPerUnit is the scale of UsdCostBasis: 1 unit is $0.000001. It is
// deliberately equal to the core mint's quarks per unit, so a core mint account's
// USD cost basis is exactly its quark balance.
const UsdQuarksPerUnit = 1_000_000

// UsdCostBasisFromFloat converts a USD value into UsdQuarksPerUnit, rounding
// to the nearest unit. This is the single conversion point, so every caller
// rounds identically.
func UsdCostBasisFromFloat(usd float64) int64 {
	return int64(math.Round(usd * UsdQuarksPerUnit))
}

// UsdCostBasisToFloat converts a value in UsdQuarksPerUnit back into USD. Use
// it only at the edge, e.g. when populating a client-facing response.
func UsdCostBasisToFloat(usdCostBasis int64) float64 {
	return float64(usdCostBasis) / UsdQuarksPerUnit
}

// Record is the materialized balance of a token account managed by Code.
type Record struct {
	Id uint64

	TokenAccount string
	OwnerAccount string
	MintAccount  string

	// Quarks is signed because a record that has not been backfilled only
	// accumulates deltas, which may temporarily net negative. Backfilled
	// records are guaranteed to be non-negative.
	Quarks int64

	// UsdCostBasis is the account's USD cost basis, in UsdQuarksPerUnit.
	// A cost basis may legitimately be negative.
	UsdCostBasis int64

	IsOpen bool

	// IsBackfilled indicates the record reflects the full history of the
	// account. Until it does, deltas are recorded without enforcing any
	// balance predicates.
	IsBackfilled bool

	UpdatedAt time.Time
}

func (r *Record) Validate() error {
	if len(r.TokenAccount) == 0 {
		return errors.New("token account is required")
	}

	if len(r.OwnerAccount) == 0 {
		return errors.New("owner account is required")
	}

	if len(r.MintAccount) == 0 {
		return errors.New("mint account is required")
	}

	if r.IsBackfilled && r.Quarks < 0 {
		return errors.New("backfilled quarks cannot be negative")
	}

	return nil
}

func (r *Record) Clone() Record {
	return Record{
		Id: r.Id,

		TokenAccount: r.TokenAccount,
		OwnerAccount: r.OwnerAccount,
		MintAccount:  r.MintAccount,

		Quarks:       r.Quarks,
		UsdCostBasis: r.UsdCostBasis,

		IsOpen:       r.IsOpen,
		IsBackfilled: r.IsBackfilled,

		UpdatedAt: r.UpdatedAt,
	}
}

func (r *Record) CopyTo(dst *Record) {
	dst.Id = r.Id

	dst.TokenAccount = r.TokenAccount
	dst.OwnerAccount = r.OwnerAccount
	dst.MintAccount = r.MintAccount

	dst.Quarks = r.Quarks
	dst.UsdCostBasis = r.UsdCostBasis

	dst.IsOpen = r.IsOpen
	dst.IsBackfilled = r.IsBackfilled

	dst.UpdatedAt = r.UpdatedAt
}

// DeltaKind selects the predicate a Delta is applied under. Predicates are
// only enforced on backfilled records.
type DeltaKind uint8

const (
	// DeltaCredit adds funds to an open account.
	DeltaCredit DeltaKind = iota + 1

	// DeltaDebit removes funds from an account with sufficient balance.
	DeltaDebit

	// DeltaDrain removes exactly the account's full balance and closes it.
	DeltaDrain

	// DeltaClose closes an account with a zero balance.
	DeltaClose
)

// Delta is a single balance change to apply to a token account.
type Delta struct {
	TokenAccount string
	Kind         DeltaKind

	// Quarks is the amount credited, debited or drained. Ignored for DeltaClose.
	Quarks uint64

	// UsdCostBasis is added on credit and subtracted on debit. It is signed
	// so that a credit can also carry a downward reconciliation. Ignored for
	// DeltaDrain and DeltaClose on backfilled records, where the basis is
	// zeroed along with the balance.
	UsdCostBasis int64
}

func (d *Delta) Validate() error {
	if len(d.TokenAccount) == 0 {
		return errors.New("token account is required")
	}

	switch d.Kind {
	case DeltaCredit, DeltaDebit, DeltaDrain:
		if d.Quarks == 0 && d.UsdCostBasis == 0 {
			return errors.New("delta is a no-op")
		}
	case DeltaClose:
	default:
		return errors.New("invalid delta kind")
	}

	return nil
}

// SortDeltas orders deltas by token account, then by kind, so every store
// implementation acquires row locks in the same order and cannot deadlock
// against another transaction applying deltas to the same accounts.
func SortDeltas(deltas []*Delta) {
	sort.SliceStable(deltas, func(i, j int) bool {
		if deltas[i].TokenAccount != deltas[j].TokenAccount {
			return deltas[i].TokenAccount < deltas[j].TokenAccount
		}
		return deltas[i].Kind < deltas[j].Kind
	})
}

func (k DeltaKind) String() string {
	switch k {
	case DeltaCredit:
		return "credit"
	case DeltaDebit:
		return "debit"
	case DeltaDrain:
		return "drain"
	case DeltaClose:
		return "close"
	}
	return "unknown"
}
