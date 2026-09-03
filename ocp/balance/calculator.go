package balance

import (
	"context"
	"time"

	"github.com/pkg/errors"

	"github.com/code-payments/ocp-server/metrics"
	"github.com/code-payments/ocp-server/ocp/common"
	ocp_data "github.com/code-payments/ocp-server/ocp/data"
	"github.com/code-payments/ocp-server/ocp/data/balance"
	"github.com/code-payments/ocp-server/solana"
)

type Source uint8

const (
	UnknownSource Source = iota
	CacheSource
	BlockchainSource
)

const (
	metricsPackageName = "balance"
)

var (
	// ErrNotManagedByCode indicates that an account is not owned by Code.
	// It's up to callers to determine how to handle this situation within
	// the context of a balance.
	ErrNotManagedByCode = errors.New("explicitly not handling account not managed by code")
)

// CalculateFromCache is the default and recommended strategy for reliably estimating
// a token account's balance using cached values.
//
// The ledger record is the whole answer: it exists for exactly the accounts
// Code manages, and carries the lock state that says whether it still does.
// An account with no record, or one whose vault has unlocked, is
// ErrNotManagedByCode.
//
// Note: Use this method when calculating balances for accounts that are managed by
// Code (ie. Timelock account) and operate within the L2 system.
func CalculateFromCache(ctx context.Context, data ocp_data.Provider, tokenAccount *common.Account) (uint64, error) {
	tracer := metrics.TraceMethodCall(ctx, metricsPackageName, "CalculateFromCache")
	tracer.AddAttribute("account", tokenAccount.PublicKey().ToBase58())
	defer tracer.End()

	balanceRecord, err := data.GetBalance(ctx, tokenAccount.PublicKey().ToBase58())
	if err == balance.ErrRecordNotFound {
		tracer.OnError(ErrNotManagedByCode)
		return 0, ErrNotManagedByCode
	} else if err != nil {
		tracer.OnError(err)
		return 0, err
	}

	// Once a vault unlocks, funds can move on chain without an intent, so the
	// record stops being maintained and its balance must not be trusted.
	if !balanceRecord.IsLocked {
		tracer.OnError(ErrNotManagedByCode)
		return 0, ErrNotManagedByCode
	}
	return balanceRecord.Quarks, nil
}

// CalculateFromBlockchain is the default and recommended strategy for reliably
// estimating a token account's balance from the blockchain. This strategy is
// resistant to various RPC failure nodes, and may return a cached value. The
// source of the balance calculation is returned.
//
// Note: Use this method when calculating token account balances that are external
// and not managed by Code and outside the L2 system.
//
// todo: add a batching variant
func CalculateFromBlockchain(ctx context.Context, data ocp_data.Provider, tokenAccount *common.Account) (uint64, Source, error) {
	var cachedQuarks uint64
	var cachedSlot uint64
	var cachedUpdateTs time.Time
	checkpointRecord, err := data.GetExternalBalanceCheckpoint(ctx, tokenAccount.PublicKey().ToBase58())
	if err == nil {
		cachedQuarks = checkpointRecord.Quarks
		cachedSlot = checkpointRecord.SlotCheckpoint
		cachedUpdateTs = checkpointRecord.LastUpdatedAt
	} else if err != balance.ErrCheckpointNotFound {
		return 0, UnknownSource, err
	}

	// todo: we may need something that's more resistant to RPC nodes with stale account state
	quarks, slot, err := data.GetBlockchainBalance(ctx, tokenAccount.PublicKey().ToBase58(), solana.CommitmentConfirmed)
	if err == solana.ErrNoBalance {
		// We can't tell whether
		//  1. RPC node is behind, and observed a state before the account existed
		//  2. RPC node is ahead, and the account was closed
		// because we don't have a slot to compare against the checkpoint.
		//
		// If the checkpoint was recently updated, we opt to trust that, optimizing
		// to reduce potential race conditions for 1.
		if time.Since(cachedUpdateTs) < 5*time.Minute {
			return cachedQuarks, CacheSource, nil
		}

		return 0, BlockchainSource, nil
	} else if err != nil {
		// RPC node threw an error. Return the cached balance
		return cachedQuarks, CacheSource, nil
	}

	// RPC node is behind, use cached balance
	if cachedSlot > slot {
		return cachedQuarks, CacheSource, nil
	}

	// Observed a balance that's more recent. Best-effort update the checkpoint.
	if cachedSlot == 0 || (slot > cachedSlot && quarks != cachedQuarks) {
		newCheckpointRecord := &balance.ExternalCheckpointRecord{
			TokenAccount:   tokenAccount.PublicKey().ToBase58(),
			Quarks:         quarks,
			SlotCheckpoint: slot,
		}
		data.SaveExternalBalanceCheckpoint(ctx, newCheckpointRecord)
	}

	return quarks, BlockchainSource, nil
}

// Balance is a token account's quark balance and USD cost basis, in
// balance.UsdQuarksPerUnit, alongside the mint it holds.
type Balance struct {
	MintAccount  string
	Quarks       uint64
	UsdCostBasis int64
}

// BatchCalculateFromCache is the default and recommended batch strategy for
// reliably estimating a set of token accounts' balances using cached values.
// Both values for an account come from the same ledger record read, so they
// are guaranteed consistent with each other.
//
// Accounts the ledger doesn't manage are omitted from the result: those with
// no record, and those whose vault has unlocked. A caller that requires every
// account it asked for to be managed compares the result's length against its
// input.
//
// Note: Use this method when calculating balances for accounts that are managed by
// Code (ie. Timelock account) and operate within the L2 system.
func BatchCalculateFromCache(ctx context.Context, data ocp_data.Provider, tokenAccounts ...*common.Account) (map[string]*Balance, error) {
	tracer := metrics.TraceMethodCall(ctx, metricsPackageName, "BatchCalculateFromCache")
	defer tracer.End()

	tokenAccountStrings := make([]string, len(tokenAccounts))
	for i, tokenAccount := range tokenAccounts {
		tokenAccountStrings[i] = tokenAccount.PublicKey().ToBase58()
	}

	balanceRecords, err := data.GetBalanceBatch(ctx, tokenAccountStrings...)
	if err != nil {
		tracer.OnError(err)
		return nil, err
	}

	res := make(map[string]*Balance, len(balanceRecords))
	for tokenAccount, balanceRecord := range balanceRecords {
		if cached, ok := balanceFromRecord(balanceRecord); ok {
			res[tokenAccount] = cached
		}
	}
	return res, nil
}

// BatchCalculateFromCacheByOwner is the default and recommended strategy for
// reliably estimating the balance of every account an owner holds, keyed by
// token account. Each balance carries the mint its account holds, so a caller
// aggregating across mints doesn't need the account records to say which is
// which.
//
// Accounts the ledger doesn't manage are omitted, as are owners it holds
// nothing for, so an owner outside the L2 system is an empty result rather
// than an error.
//
// Note: Use this method when calculating balances for accounts that are managed by
// Code (ie. Timelock account) and operate within the L2 system.
func BatchCalculateFromCacheByOwner(ctx context.Context, data ocp_data.Provider, owner *common.Account) (map[string]*Balance, error) {
	tracer := metrics.TraceMethodCall(ctx, metricsPackageName, "BatchCalculateFromCacheByOwner")
	tracer.AddAttribute("owner", owner.PublicKey().ToBase58())
	defer tracer.End()

	balanceRecords, err := data.GetAllBalancesByOwner(ctx, owner.PublicKey().ToBase58())
	if err != nil && err != balance.ErrRecordNotFound {
		tracer.OnError(err)
		return nil, err
	}

	res := make(map[string]*Balance, len(balanceRecords))
	for _, balanceRecord := range balanceRecords {
		if cached, ok := balanceFromRecord(balanceRecord); ok {
			res[balanceRecord.TokenAccount] = cached
		}
	}
	return res, nil
}

// balanceFromRecord reports a ledger record as a Balance, and whether the
// ledger still maintains it. Once a vault unlocks, funds can move on chain
// without an intent, so the record's balance must not be trusted or
// aggregated.
func balanceFromRecord(record *balance.Record) (*Balance, bool) {
	if !record.IsLocked {
		return nil, false
	}

	return &Balance{
		MintAccount:  record.MintAccount,
		Quarks:       record.Quarks,
		UsdCostBasis: record.UsdCostBasis,
	}, true
}

func (s Source) String() string {
	switch s {
	case UnknownSource:
		return "unknown"
	case CacheSource:
		return "cache"
	case BlockchainSource:
		return "blockchain"
	}
	return "unknown"
}
