package history

import (
	"errors"
	"fmt"
	"time"

	"github.com/code-payments/ocp-server/currency"
	"github.com/code-payments/ocp-server/pointer"
)

type Type uint8

const (
	UnknownType Type = iota
	DirectlySent
	DirectlyReceived
	IndirectlySent
	IndirectlyReceived
	Withdrawn
	Deposited
	Swap
)

// ReferenceType is the kind of thing a record's reference names. A reference
// is only unique within its own kind: intent IDs and swap IDs are both client
// supplied public keys drawn from the same space, and a transaction signature
// is a third space again. Pairing the two is what keeps one kind's reference
// from landing on another's.
type ReferenceType uint8

const (
	UnknownReferenceType ReferenceType = iota
	IntentReference
	SwapReference
	SignatureReference
)

type State uint8

const (
	StateUnknown State = iota
	StatePending
	StateCompleted
	StateFailed
	StateVoided
	StateReturned
)

// FeeType is persisted as its ordinal, inside the fees blob rather than in a
// column of its own, so this block is append-only. Inserting a value anywhere
// but the end re-labels every fee already stored, and nothing would report it.
type FeeType uint8

const (
	UnknownFeeType FeeType = iota
	ReserveBuyFee
	ReserveSellFee
	WithdrawalAccountCreationFee
	CurrencyLaunchFee
)

// The tags are what a stored fee is keyed by, so they are the storage schema
// and the field names are not. Renaming a field without them would leave every
// stored fee decoding to a zero value rather than failing.
//
// They are abbreviated because a key is repeated in full on every fee of every
// record, and the field names carry the meaning that the keys give up.
type Fee struct {
	Type         FeeType `json:"t"`
	NativeAmount float64 `json:"na"`
}

type Record struct {
	Id uint64

	ReferenceId   string
	ReferenceType ReferenceType

	Type Type

	OwnerAccount             string
	CounterpartyOwnerAccount *string

	ExchangeCurrency currency.Code
	NativeAmount     float64

	Fees []Fee

	MintAccount string
	Quantity    uint64

	DestinationMintAccount *string
	DestinationQuantity    *uint64

	GiftCardVault *string
	AppMetadata   []byte

	Version uint64

	State State

	CreatedAt time.Time
	UpdatedAt time.Time
}

func (r *Record) Validate() error {
	if len(r.ReferenceId) == 0 {
		return errors.New("reference id is required")
	}

	if r.ReferenceType == UnknownReferenceType {
		return errors.New("reference type is required")
	}

	if r.Type == UnknownType {
		return errors.New("type is required")
	}

	if len(r.OwnerAccount) == 0 {
		return errors.New("owner account is required")
	}

	if r.CounterpartyOwnerAccount != nil && len(*r.CounterpartyOwnerAccount) == 0 {
		return errors.New("counterparty owner account must not be empty")
	}

	if len(r.ExchangeCurrency) == 0 {
		return errors.New("exchange currency is required")
	}

	if r.NativeAmount == 0 {
		return errors.New("native amount is required")
	}

	for i, fee := range r.Fees {
		if err := fee.Validate(); err != nil {
			return fmt.Errorf("invalid fee at index %d: %w", i, err)
		}
	}

	if len(r.MintAccount) == 0 {
		return errors.New("mint account is required")
	}

	if r.Quantity == 0 {
		return errors.New("quantity is required")
	}

	switch r.Type {
	case Swap:
		if r.DestinationMintAccount == nil {
			return errors.New("destination mint account is required")
		}
	case Withdrawn, Deposited:
	default:
		if r.DestinationMintAccount != nil || r.DestinationQuantity != nil {
			return errors.New("destination leg must not be present")
		}
	}

	if r.DestinationMintAccount != nil {
		if len(*r.DestinationMintAccount) == 0 {
			return errors.New("destination mint account must not be empty")
		}

		if *r.DestinationMintAccount == r.MintAccount {
			return errors.New("source and destination mints must differ")
		}
	}

	if r.DestinationQuantity != nil {
		if *r.DestinationQuantity == 0 {
			return errors.New("destination quantity must not be zero")
		}

		if r.DestinationMintAccount == nil {
			return errors.New("destination quantity requires a destination mint account")
		}
	}

	switch r.Type {
	case IndirectlySent, IndirectlyReceived:
		if r.GiftCardVault == nil || len(*r.GiftCardVault) == 0 {
			return errors.New("gift card vault is required")
		}
	default:
		if r.GiftCardVault != nil {
			return errors.New("gift card vault must not be present")
		}
	}

	if r.State == StateUnknown {
		return errors.New("state is required")
	}

	switch r.State {
	case StateVoided, StateReturned:
		if r.Type != IndirectlySent {
			return fmt.Errorf("state %s is only valid for %s", r.State, IndirectlySent)
		}
	}

	if r.CreatedAt.IsZero() {
		return errors.New("creation time is required")
	}

	return nil
}

func (r *Record) Clone() Record {
	var cloned Record
	r.CopyTo(&cloned)
	return cloned
}

func (r *Record) CopyTo(dst *Record) {
	dst.Id = r.Id

	dst.ReferenceId = r.ReferenceId
	dst.ReferenceType = r.ReferenceType

	dst.Type = r.Type

	dst.OwnerAccount = r.OwnerAccount
	dst.CounterpartyOwnerAccount = pointer.StringCopy(r.CounterpartyOwnerAccount)

	dst.ExchangeCurrency = r.ExchangeCurrency
	dst.NativeAmount = r.NativeAmount

	if r.Fees != nil {
		dst.Fees = make([]Fee, len(r.Fees))
		copy(dst.Fees, r.Fees)
	} else {
		dst.Fees = nil
	}

	dst.MintAccount = r.MintAccount
	dst.Quantity = r.Quantity

	dst.DestinationMintAccount = pointer.StringCopy(r.DestinationMintAccount)
	dst.DestinationQuantity = pointer.Uint64Copy(r.DestinationQuantity)

	dst.GiftCardVault = pointer.StringCopy(r.GiftCardVault)

	if r.AppMetadata != nil {
		dst.AppMetadata = make([]byte, len(r.AppMetadata))
		copy(dst.AppMetadata, r.AppMetadata)
	} else {
		dst.AppMetadata = nil
	}

	dst.Version = r.Version

	dst.State = r.State

	dst.CreatedAt = r.CreatedAt
	dst.UpdatedAt = r.UpdatedAt
}

func (f *Fee) Validate() error {
	if f.Type == UnknownFeeType {
		return errors.New("fee type is required")
	}

	if f.NativeAmount == 0 {
		return errors.New("fee native amount is required")
	}

	return nil
}

func (t Type) String() string {
	switch t {
	case DirectlySent:
		return "directly_sent"
	case DirectlyReceived:
		return "directly_received"
	case IndirectlySent:
		return "indirectly_sent"
	case IndirectlyReceived:
		return "indirectly_received"
	case Withdrawn:
		return "withdrawn"
	case Deposited:
		return "deposited"
	case Swap:
		return "swap"
	}
	return "unknown"
}

func (r ReferenceType) String() string {
	switch r {
	case IntentReference:
		return "intent"
	case SwapReference:
		return "swap"
	case SignatureReference:
		return "signature"
	}
	return "unknown"
}

func (s State) String() string {
	switch s {
	case StatePending:
		return "pending"
	case StateCompleted:
		return "completed"
	case StateFailed:
		return "failed"
	case StateVoided:
		return "voided"
	case StateReturned:
		return "returned"
	}
	return "unknown"
}

func (f FeeType) String() string {
	switch f {
	case ReserveBuyFee:
		return "reserve_buy"
	case ReserveSellFee:
		return "reserve_sell"
	case WithdrawalAccountCreationFee:
		return "withdrawal_account_creation"
	case CurrencyLaunchFee:
		return "currency_launch"
	}
	return "unknown"
}
