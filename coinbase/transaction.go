// Package coinbase provides a client for Coinbase Developer Platform APIs,
// scoped to the surfaces this repo currently consumes (Onramp transaction
// status). Authentication uses CDP API keys signed with Ed25519 (EdDSA),
// regenerated per request — see auth.go.
package coinbase

import (
	"errors"
	"time"
)

type TransactionStatus string

const (
	TransactionStatusUnknown    TransactionStatus = ""
	TransactionStatusCreated    TransactionStatus = "ONRAMP_TRANSACTION_STATUS_CREATED"
	TransactionStatusInProgress TransactionStatus = "ONRAMP_TRANSACTION_STATUS_IN_PROGRESS"
	TransactionStatusSuccess    TransactionStatus = "ONRAMP_TRANSACTION_STATUS_SUCCESS"
	TransactionStatusFailed     TransactionStatus = "ONRAMP_TRANSACTION_STATUS_FAILED"
)

// Transaction is the subset of the Coinbase Onramp transaction record this
// repo cares about. The full payload is larger; fields not used by callers
// are intentionally omitted.
type Transaction struct {
	OrderID         string // UUID assigned by Coinbase
	PartnerUserRef  string // Stable per-user reference supplied by us at widget init
	Status          TransactionStatus
	TxHash          string // Empty until on-chain settlement
	WalletAddress   string // Destination wallet on the target chain
	PurchaseAmount  Amount // Amount in the purchased asset
	PurchaseAssetID string // Coinbase asset identifier (e.g. "USDC")
	CreatedAt       time.Time
}

// Amount mirrors Coinbase's value/currency object.
type Amount struct {
	Value    string // Decimal string — preserve precision
	Currency string
}

// ErrTransactionNotFound is returned when an order ID isn't present under the
// given partnerUserRef (either it doesn't exist in our project, or the
// partnerUserRef is wrong).
var ErrTransactionNotFound = errors.New("coinbase transaction not found")
