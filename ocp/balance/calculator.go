package balance

import (
	"context"
	"time"

	"github.com/pkg/errors"

	"github.com/code-payments/ocp-server/metrics"
	"github.com/code-payments/ocp-server/ocp/common"
	ocp_data "github.com/code-payments/ocp-server/ocp/data"
	"github.com/code-payments/ocp-server/ocp/data/balance"
	"github.com/code-payments/ocp-server/ocp/data/timelock"
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
	// ErrNegativeBalance indicates that a ledger record holds a negative
	// value, which the store's delta predicates should make impossible.
	ErrNegativeBalance = errors.New("balance calculation resulted in negative value")

	// ErrNotManagedByCode indicates that an account is not owned by Code.
	// It's up to callers to determine how to handle this situation within
	// the context of a balance.
	ErrNotManagedByCode = errors.New("explicitly not handling account not managed by code")
)

// CalculateFromCache is the default and recommended strategy for reliably estimating
// a token account's balance using cached values.
//
// Note: Use this method when calculating balances for accounts that are managed by
// Code (ie. Timelock account) and operate within the L2 system.
func CalculateFromCache(ctx context.Context, data ocp_data.Provider, tokenAccount *common.Account) (uint64, error) {
	tracer := metrics.TraceMethodCall(ctx, metricsPackageName, "CalculateFromCache")
	tracer.AddAttribute("account", tokenAccount.PublicKey().ToBase58())
	defer tracer.End()

	timelockRecord, err := data.GetTimelockByVault(ctx, tokenAccount.PublicKey().ToBase58())
	if err == timelock.ErrTimelockNotFound {
		tracer.OnError(ErrNotManagedByCode)
		return 0, ErrNotManagedByCode
	} else if err != nil {
		tracer.OnError(err)
		return 0, err
	}

	// The balance ledger is only maintained for accounts managed by Code. The
	// account must be managed in order to return accurate values.
	isManagedByCode := common.IsManagedByCode(ctx, timelockRecord)
	if !isManagedByCode {
		tracer.OnError(ErrNotManagedByCode)
		return 0, ErrNotManagedByCode
	}

	balanceRecord, err := data.GetBalance(ctx, tokenAccount.PublicKey().ToBase58())
	if err != nil {
		tracer.OnError(err)
		return 0, err
	}

	quarks, err := quarksFromRecord(balanceRecord)
	if err != nil {
		tracer.OnError(err)
		return 0, err
	}
	return quarks, nil
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

// BatchCalculateFromCacheWithAccountRecords is the default and recommended batch strategy
// or reliably estimating a set of token accounts' balance when common.AccountRecords are
// available.
//
// Note: Use this method when calculating balances for accounts that are managed by
// Code (ie. Timelock account) and operate within the L2 system.
func BatchCalculateFromCacheWithAccountRecords(ctx context.Context, data ocp_data.Provider, accountRecordsBatch ...*common.AccountRecords) (map[string]uint64, error) {
	tracer := metrics.TraceMethodCall(ctx, metricsPackageName, "BatchCalculateFromCacheWithAccountRecords")
	defer tracer.End()

	timelockRecords := make([]*timelock.Record, 0)
	for _, accountRecords := range accountRecordsBatch {
		if !accountRecords.IsTimelock() {
			tracer.OnError(ErrNotManagedByCode)
			return nil, ErrNotManagedByCode
		}

		timelockRecords = append(timelockRecords, accountRecords.Timelock)
	}

	balanceByTokenAccount, err := defaultBatchCalculationFromCache(ctx, data, timelockRecords)
	if err != nil {
		tracer.OnError(err)
		return nil, err
	}
	return balanceByTokenAccount, nil
}

// BatchCalculateFromCacheWithTokenAccounts is the default and recommended batch strategy
// or reliably estimating a set of token accounts' balance when common.Account are
// available.
//
// Note: Use this method when calculating balances for accounts that are managed by
// Code (ie. Timelock account) and operate within the L2 system.
func BatchCalculateFromCacheWithTokenAccounts(ctx context.Context, data ocp_data.Provider, tokenAccounts ...*common.Account) (map[string]uint64, error) {
	tracer := metrics.TraceMethodCall(ctx, metricsPackageName, "BatchCalculateFromCacheWithTokenAccounts")
	defer tracer.End()

	tokenAccountStrings := make([]string, len(tokenAccounts))
	for i, tokenAccount := range tokenAccounts {
		tokenAccountStrings[i] = tokenAccount.PublicKey().ToBase58()
	}

	timelockRecordsByVault, err := data.GetTimelockByVaultBatch(ctx, tokenAccountStrings...)
	if err == timelock.ErrTimelockNotFound {
		tracer.OnError(ErrNotManagedByCode)
		return nil, ErrNotManagedByCode
	} else if err != nil {
		tracer.OnError(err)
		return nil, err
	}

	timelockRecords := make([]*timelock.Record, 0, len(timelockRecordsByVault))
	for _, timelockRecord := range timelockRecordsByVault {
		timelockRecords = append(timelockRecords, timelockRecord)
	}

	balanceByTokenAccount, err := defaultBatchCalculationFromCache(ctx, data, timelockRecords)
	if err != nil {
		tracer.OnError(err)
		return nil, err
	}
	return balanceByTokenAccount, nil
}

func defaultBatchCalculationFromCache(ctx context.Context, data ocp_data.Provider, timelockRecords []*timelock.Record) (map[string]uint64, error) {
	tokenAccounts := make([]string, 0, len(timelockRecords))
	for _, timelockRecord := range timelockRecords {
		// The balance ledger is only maintained for accounts managed by Code.
		// The account must be managed in order to return accurate values.
		isManagedByCode := common.IsManagedByCode(ctx, timelockRecord)
		if !isManagedByCode {
			return nil, ErrNotManagedByCode
		}

		tokenAccounts = append(tokenAccounts, timelockRecord.VaultAddress)
	}

	balanceRecords, err := data.GetBalanceBatch(ctx, tokenAccounts...)
	if err != nil {
		return nil, err
	}

	res := make(map[string]uint64, len(tokenAccounts))
	for _, tokenAccount := range tokenAccounts {
		balanceRecord, ok := balanceRecords[tokenAccount]
		if !ok {
			return nil, balance.ErrRecordNotFound
		}

		quarks, err := quarksFromRecord(balanceRecord)
		if err != nil {
			return nil, err
		}
		res[tokenAccount] = quarks
	}
	return res, nil
}

// BalanceWithUsdCostBasis holds a token account's quark balance and USD cost
// basis, in balance.UsdQuarksPerUnit.
type BalanceWithUsdCostBasis struct {
	Quarks       uint64
	UsdCostBasis int64
}

// BatchCalculateWithUsdCostBasisFromCache calculates balances and USD cost
// bases for a set of account records. Both values for an account come from
// the same balance record read, so they are guaranteed consistent with each
// other.
//
// Note: Use this method when calculating balances for accounts that are managed by
// Code (ie. Timelock account) and operate within the L2 system.
func BatchCalculateWithUsdCostBasisFromCache(ctx context.Context, data ocp_data.Provider, accountRecordsBatch ...*common.AccountRecords) (map[string]*BalanceWithUsdCostBasis, error) {
	tracer := metrics.TraceMethodCall(ctx, metricsPackageName, "BatchCalculateWithUsdCostBasisFromCache")
	defer tracer.End()

	tokenAccounts := make([]string, 0, len(accountRecordsBatch))
	for _, accountRecords := range accountRecordsBatch {
		if !accountRecords.IsTimelock() || !accountRecords.IsManagedByCode(ctx) {
			tracer.OnError(ErrNotManagedByCode)
			return nil, ErrNotManagedByCode
		}
		tokenAccounts = append(tokenAccounts, accountRecords.General.TokenAccount)
	}

	balanceRecords, err := data.GetBalanceBatch(ctx, tokenAccounts...)
	if err != nil {
		tracer.OnError(err)
		return nil, err
	}

	res := make(map[string]*BalanceWithUsdCostBasis, len(tokenAccounts))
	for _, tokenAccount := range tokenAccounts {
		balanceRecord, ok := balanceRecords[tokenAccount]
		if !ok {
			tracer.OnError(balance.ErrRecordNotFound)
			return nil, balance.ErrRecordNotFound
		}

		quarks, err := quarksFromRecord(balanceRecord)
		if err != nil {
			tracer.OnError(err)
			return nil, err
		}
		res[tokenAccount] = &BalanceWithUsdCostBasis{
			Quarks:       quarks,
			UsdCostBasis: balanceRecord.UsdCostBasis,
		}
	}
	return res, nil
}

// CalculateUsdCostBasisFromCache calculates a token account's USD cost basis,
// in balance.UsdQuarksPerUnit, from its ledger record.
//
// Note: Unlike the quark balance calculators, no timelock check is performed,
// since the ledger record carries the lock state itself. Once a vault unlocks
// its record holds the last managed state rather than a live cost basis, so
// reading one returns ErrNotManagedByCode. An account the ledger doesn't
// track returns balance.ErrRecordNotFound.
func CalculateUsdCostBasisFromCache(ctx context.Context, data ocp_data.Provider, tokenAccount *common.Account) (int64, error) {
	tracer := metrics.TraceMethodCall(ctx, metricsPackageName, "CalculateUsdCostBasisFromCache")
	tracer.AddAttribute("account", tokenAccount.PublicKey().ToBase58())
	defer tracer.End()

	balanceRecord, err := data.GetBalance(ctx, tokenAccount.PublicKey().ToBase58())
	if err != nil {
		tracer.OnError(err)
		return 0, err
	}

	if err := checkRecord(balanceRecord); err != nil {
		tracer.OnError(err)
		return 0, err
	}
	return balanceRecord.UsdCostBasis, nil
}

// BatchCalculateUsdCostBasisFromCache is like CalculateUsdCostBasisFromCache,
// but for a set of token accounts.
func BatchCalculateUsdCostBasisFromCache(ctx context.Context, data ocp_data.Provider, tokenAccounts ...*common.Account) (map[string]int64, error) {
	tracer := metrics.TraceMethodCall(ctx, metricsPackageName, "BatchCalculateUsdCostBasisFromCache")
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

	res := make(map[string]int64, len(tokenAccounts))
	for _, tokenAccount := range tokenAccountStrings {
		balanceRecord, ok := balanceRecords[tokenAccount]
		if !ok {
			tracer.OnError(balance.ErrRecordNotFound)
			return nil, balance.ErrRecordNotFound
		}

		if err := checkRecord(balanceRecord); err != nil {
			tracer.OnError(err)
			return nil, err
		}
		res[tokenAccount] = balanceRecord.UsdCostBasis
	}
	return res, nil
}

// checkRecord verifies a ledger record is an authoritative view of an account
// that Code still manages. Quark balance callers reject unlocked vaults on the
// timelock record before reaching here, so for them this only guards against a
// record that disagrees with it.
func checkRecord(record *balance.Record) error {
	if !record.IsLocked {
		return ErrNotManagedByCode
	}
	return nil
}

func quarksFromRecord(record *balance.Record) (uint64, error) {
	if err := checkRecord(record); err != nil {
		return 0, err
	}

	if record.Quarks < 0 {
		return 0, ErrNegativeBalance
	}
	return uint64(record.Quarks), nil
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
