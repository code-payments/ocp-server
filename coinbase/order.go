// Package coinbase provides a client for Coinbase Developer Platform APIs,
// scoped to the surfaces this repo currently consumes (Onramp order
// status). Authentication uses CDP API keys signed with Ed25519 (EdDSA),
// regenerated per request — see auth.go.
package coinbase

import (
	"errors"
	"time"
)

type OrderStatus string

const (
	OrderStatusUnknown        OrderStatus = ""
	OrderStatusPendingAuth    OrderStatus = "ONRAMP_ORDER_STATUS_PENDING_AUTH"
	OrderStatusPendingPayment OrderStatus = "ONRAMP_ORDER_STATUS_PENDING_PAYMENT"
	OrderStatusProcessing     OrderStatus = "ONRAMP_ORDER_STATUS_PROCESSING"
	OrderStatusCompleted      OrderStatus = "ONRAMP_ORDER_STATUS_COMPLETED"
	OrderStatusFailed         OrderStatus = "ONRAMP_ORDER_STATUS_FAILED"
)

type PaymentMethod string

const (
	PaymentMethodApplePay  PaymentMethod = "GUEST_CHECKOUT_APPLE_PAY"
	PaymentMethodGooglePay PaymentMethod = "GUEST_CHECKOUT_GOOGLE_PAY"
)

type FeeType string

const (
	FeeTypeNetwork  FeeType = "FEE_TYPE_NETWORK"
	FeeTypeExchange FeeType = "FEE_TYPE_EXCHANGE"
)

const (
	NetworkSolana = "solana"
)

// Order is a Coinbase Onramp order as returned by the v2 API.
type Order struct {
	OrderID            string // UUID assigned by Coinbase
	Status             OrderStatus
	PaymentTotal       Amount // Total charged to the buyer, including fees
	PaymentSubtotal    Amount // Charge before fees
	PaymentMethod      PaymentMethod
	PurchaseAmount     Amount // Amount of the purchased asset delivered
	Fees               []Fee
	ExchangeRate       string
	DestinationAddress string
	DestinationNetwork string
	TxHash             string // Empty until on-chain settlement
	PartnerUserRef     string // Stable per-user reference supplied by us at widget init
	CreatedAt          time.Time
	UpdatedAt          time.Time
}

// Amount mirrors Coinbase's value/currency object.
type Amount struct {
	Value    string // Decimal string — preserve precision
	Currency string
}

type Fee struct {
	Type   FeeType
	Amount Amount
}

// ErrOrderNotFound is returned when the requested order ID isn't present in
// the authenticated CDP project.
var ErrOrderNotFound = errors.New("coinbase order not found")
