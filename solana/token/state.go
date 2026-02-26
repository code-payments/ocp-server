package token

import (
	"crypto/ed25519"

	"github.com/code-payments/ocp-server/solana/binary"
)

type AccountState byte

const (
	AccountStateUninitialized AccountState = iota
	AccountStateInitialized
	AccountStateFrozen
)

// Reference: https://github.com/solana-labs/solana-program-library/blob/11b1e3eefdd4e523768d63f7c70a7aa391ea0d02/token/program/src/state.rs#L18
const MintSize = 82

// Reference: https://github.com/solana-labs/solana-program-library/blob/11b1e3eefdd4e523768d63f7c70a7aa391ea0d02/token/program/src/state.rs#L125
const AccountSize = 165

// Reference: https://github.com/solana-labs/solana-program-library/blob/8944f428fe693c3a4226bf766a79be9c75e8e520/token/program/src/state.rs#L214
const MultisigAccountSize = 355

const optionSize = 4

// Reference: https://github.com/solana-labs/solana-program-library/blob/11b1e3eefdd4e523768d63f7c70a7aa391ea0d02/token/program/src/state.rs#L18
type Mint struct {
	// Optional authority used to mint new tokens. The mint authority may only
	// be provided during mint creation. If no mint authority is present then
	// the mint has a fixed supply and no further tokens may be minted.
	MintAuthority ed25519.PublicKey
	// Total supply of tokens.
	Supply uint64
	// Number of base 10 digits to the right of the decimal place.
	Decimals uint8
	// Is true if this structure has been initialized.
	IsInitialized bool
	// Optional authority to freeze token accounts.
	FreezeAuthority ed25519.PublicKey
}

func (m *Mint) Marshal() []byte {
	b := make([]byte, MintSize)

	var offset int
	binary.PutOptionalKey32(b, m.MintAuthority, &offset, optionSize)
	binary.PutUint64(b[offset:], m.Supply, &offset)
	binary.PutUint8(b[offset:], m.Decimals, &offset)
	if m.IsInitialized {
		b[offset] = 1
	}
	offset++
	binary.PutOptionalKey32(b[offset:], m.FreezeAuthority, &offset, optionSize)

	return b
}

func (m *Mint) Unmarshal(b []byte) bool {
	if len(b) != MintSize {
		return false
	}

	var offset int
	binary.GetOptionalKey32(b, &m.MintAuthority, &offset, optionSize)
	binary.GetUint64(b[offset:], &m.Supply, &offset)
	binary.GetUint8(b[offset:], &m.Decimals, &offset)
	m.IsInitialized = b[offset] == 1
	offset++
	binary.GetOptionalKey32(b[offset:], &m.FreezeAuthority, &offset, optionSize)

	return true
}

type Account struct {
	// The mint associated with this account
	Mint ed25519.PublicKey
	// The owner of this account.
	Owner ed25519.PublicKey
	// The amount of tokens this account holds.
	Amount uint64
	// If set, then the 'DelegatedAmount' represents the amount
	// authorized by the delegate.
	Delegate ed25519.PublicKey
	/// The account's state
	State AccountState
	// If set, this is a native token, and the value logs the rent-exempt reserve. An Account
	// is required to be rent-exempt, so the value is used by the Processor to ensure that wrapped
	// SOL accounts do not drop below this threshold.
	IsNative *uint64
	// The amount delegated
	DelegatedAmount uint64
	// Optional authority to close the account.
	CloseAuthority ed25519.PublicKey
}

func (a *Account) Marshal() []byte {
	b := make([]byte, AccountSize)

	var offset int
	binary.PutKey32(b, a.Mint, &offset)
	binary.PutKey32(b[offset:], a.Owner, &offset)
	binary.PutUint64(b[offset:], a.Amount, &offset)
	binary.PutOptionalKey32(b[offset:], a.Delegate, &offset, optionSize)
	b[offset] = byte(a.State)
	offset++
	binary.PutOptionalUint64(b[offset:], a.IsNative, &offset, optionSize)
	binary.PutUint64(b[offset:], a.DelegatedAmount, &offset)
	binary.PutOptionalKey32(b[offset:], a.CloseAuthority, &offset, optionSize)

	return b
}

func (a *Account) Unmarshal(b []byte) bool {
	if len(b) != AccountSize {
		return false
	}

	var offset int
	binary.GetKey32(b, &a.Mint, &offset)
	binary.GetKey32(b[offset:], &a.Owner, &offset)
	binary.GetUint64(b[offset:], &a.Amount, &offset)
	binary.GetOptionalKey32(b[offset:], &a.Delegate, &offset, optionSize)
	a.State = AccountState(b[offset])
	offset++
	binary.GetOptionalUint64(b[offset:], &a.IsNative, &offset, optionSize)
	binary.GetUint64(b[offset:], &a.DelegatedAmount, &offset)
	binary.GetOptionalKey32(b[offset:], &a.CloseAuthority, &offset, optionSize)

	return true
}
