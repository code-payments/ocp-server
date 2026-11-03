package transaction

import (
	"math"
	"math/big"

	commonpb "github.com/code-payments/ocp-protobuf-api/generated/go/common/v1"

	"github.com/code-payments/ocp-server/pointer"
	"github.com/code-payments/ocp-server/solana"
)

const (
	// CoinbaseSwapLoadedAccountsDataSizeLimit is the loaded accounts data size
	// limit for v1 transactions against the Coinbase Stable Swapper program.
	// The worst case loads about 1.23 MiB, dominated by program data.
	CoinbaseSwapLoadedAccountsDataSizeLimit = 4 * 1024 * 1024 // 4 MiB

	// ReserveSwapLoadedAccountsDataSizeLimit is the loaded accounts data size
	// limit for v1 reserve swap transactions on existing currencies. Program
	// data loads about 7.4 MiB, dominated by the 7.0 MB currency creator. A VM
	// memory account adds up to 4.9 MiB at the 65,535 timelock accounts the
	// server can index, for a worst case of about 12.2 MiB.
	ReserveSwapLoadedAccountsDataSizeLimit = 16 * 1024 * 1024 // 16 MiB

	microLamportsPerLamport = 1_000_000
)

// NewV1TransactionConfig converts a legacy compute budget into a v1
// transaction config. The priority fee is what ComputeBudget::SetComputeUnitPrice
// charges a legacy or v0 transaction with the same limit and price, so a swap
// costs the same in either version. The heap size is left at the runtime
// default.
func NewV1TransactionConfig(computeUnitLimit uint32, computeUnitPrice uint64, loadedAccountsDataSizeLimit uint32) solana.TransactionConfig {
	return solana.TransactionConfig{
		ComputeUnitLimit:            pointer.Uint32(computeUnitLimit),
		LoadedAccountsDataSizeLimit: pointer.Uint32(loadedAccountsDataSizeLimit),
		PriorityFeeLamports:         pointer.Uint64(PriorityFeeLamports(computeUnitLimit, computeUnitPrice)),
	}
}

// PriorityFeeLamports returns the total priority fee for a compute unit limit
// and price in micro-lamports per compute unit, rounded up and saturating like
// the runtime's legacy prioritization fee.
func PriorityFeeLamports(computeUnitLimit uint32, computeUnitPrice uint64) uint64 {
	fee := new(big.Int).Mul(
		new(big.Int).SetUint64(computeUnitPrice),
		new(big.Int).SetUint64(uint64(computeUnitLimit)),
	)
	fee.Add(fee, big.NewInt(microLamportsPerLamport-1))
	fee.Div(fee, big.NewInt(microLamportsPerLamport))
	if !fee.IsUint64() {
		return math.MaxUint64
	}
	return fee.Uint64()
}

// ToProtoTransactionConfig converts a v1 transaction config into its proto
// form. Unset fields are 0, which clients treat as omitted only for the heap
// size.
func ToProtoTransactionConfig(config solana.TransactionConfig) *commonpb.SolanaTransactionConfig {
	proto := &commonpb.SolanaTransactionConfig{}
	if config.ComputeUnitLimit != nil {
		proto.ComputeUnitLimit = *config.ComputeUnitLimit
	}
	if config.LoadedAccountsDataSizeLimit != nil {
		proto.LoadedAccountsDataSizeLimit = *config.LoadedAccountsDataSizeLimit
	}
	if config.PriorityFeeLamports != nil {
		proto.PriorityFeeLamports = *config.PriorityFeeLamports
	}
	if config.HeapSize != nil {
		proto.HeapSize = *config.HeapSize
	}
	return proto
}
