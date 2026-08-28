package balance

import (
	"context"
	"errors"
	"fmt"

	ocp_data "github.com/code-payments/ocp-server/ocp/data"
	"github.com/code-payments/ocp-server/ocp/data/account"
	"github.com/code-payments/ocp-server/ocp/data/balance"
)

// ErrUntrackedAccount is returned when funds would leave an account the
// ledger doesn't track.
var ErrUntrackedAccount = errors.New("account is not tracked by the balance ledger")

// LedgerReadsEnabled reports whether backfilled ledger records are the
// authoritative source for balance reads.
func LedgerReadsEnabled(ctx context.Context) bool {
	return enableLedgerReads.Get(ctx)
}

// LedgerWritesEnabled reports whether the ledger is being written to.
// Callers use it to skip building deltas entirely when writes are disabled,
// since builders reject flows the ledger doesn't support.
func LedgerWritesEnabled(ctx context.Context) bool {
	return enableLedgerWrites.Get(ctx)
}

// ApplyDeltasInTx applies balance deltas to the ledger. It must be called
// within the DB transaction that commits the records the deltas are derived
// from, so the ledger can never disagree with them.
//
// It is a no-op while ledger writes are disabled.
//
// The ledger only tracks timelock accounts. Credits to any other account,
// like an external wallet or the fee collector, are dropped, since delta
// builders don't know which destinations OCP manages. Outgoing deltas from
// an account the ledger doesn't track are ErrUntrackedAccount, since funds
// only ever leave accounts OCP manages.
//
// Any timelock account in the delta set that has no ledger record yet lazily
// gets one that is not backfilled, so accounts that predate the ledger start
// accumulating deltas on first touch regardless of direction.
//
// Store predicate failures (balance.ErrInsufficientBalance,
// balance.ErrBalanceChanged, balance.ErrAccountClosed) are returned as is
// for the caller to map.
func ApplyDeltasInTx(ctx context.Context, data ocp_data.Provider, deltas ...*balance.Delta) error {
	if !enableLedgerWrites.Get(ctx) || len(deltas) == 0 {
		return nil
	}

	for _, delta := range deltas {
		if err := delta.Validate(); err != nil {
			return err
		}
	}

	tracked, err := resolveRecords(ctx, data, deltas)
	if err != nil {
		return err
	}

	var applicable []*balance.Delta
	for _, delta := range deltas {
		if tracked[delta.TokenAccount] {
			applicable = append(applicable, delta)
		} else if delta.Kind != balance.DeltaCredit {
			return fmt.Errorf("%w: %s", ErrUntrackedAccount, delta.TokenAccount)
		}
	}
	if len(applicable) == 0 {
		return nil
	}

	return data.ApplyBalanceDeltas(ctx, applicable...)
}

// CreateRecordInTx creates the ledger record for a newly opened account. It
// must be called within the DB transaction that creates the account info
// record. A new account has no history, so its record is created backfilled
// at zero and predicates are enforced from the start.
//
// It is a no-op while ledger writes are disabled, and for accounts that
// aren't timelock accounts, which the ledger doesn't track.
func CreateRecordInTx(ctx context.Context, data ocp_data.Provider, accountInfoRecord *account.Record) error {
	if !enableLedgerWrites.Get(ctx) || !accountInfoRecord.IsTimelock() {
		return nil
	}

	err := data.CreateBalance(ctx, &balance.Record{
		TokenAccount: accountInfoRecord.TokenAccount,
		OwnerAccount: accountInfoRecord.OwnerAccount,
		MintAccount:  accountInfoRecord.MintAccount,
		IsOpen:       true,
		IsBackfilled: true,
	})
	if errors.Is(err, balance.ErrRecordExists) {
		return nil
	}
	return err
}

// resolveRecords reports which accounts in the delta set the ledger tracks,
// creating a non-backfilled record for every timelock account that doesn't
// have one yet.
func resolveRecords(ctx context.Context, data ocp_data.Provider, deltas []*balance.Delta) (map[string]bool, error) {
	tracked := make(map[string]bool)
	var tokenAccounts []string
	for _, delta := range deltas {
		if _, ok := tracked[delta.TokenAccount]; ok {
			continue
		}
		tracked[delta.TokenAccount] = false
		tokenAccounts = append(tokenAccounts, delta.TokenAccount)
	}

	existing, err := data.GetBalanceBatch(ctx, tokenAccounts...)
	if err != nil {
		return nil, err
	}

	for _, tokenAccount := range tokenAccounts {
		if _, ok := existing[tokenAccount]; ok {
			tracked[tokenAccount] = true
			continue
		}

		accountInfoRecord, err := data.GetAccountInfoByTokenAddress(ctx, tokenAccount)
		if errors.Is(err, account.ErrAccountInfoNotFound) {
			continue
		} else if err != nil {
			return nil, err
		}
		if !accountInfoRecord.IsTimelock() {
			continue
		}

		err = data.CreateBalance(ctx, &balance.Record{
			TokenAccount: accountInfoRecord.TokenAccount,
			OwnerAccount: accountInfoRecord.OwnerAccount,
			MintAccount:  accountInfoRecord.MintAccount,
			IsOpen:       true,
			IsBackfilled: false,
		})
		if err != nil && !errors.Is(err, balance.ErrRecordExists) {
			return nil, err
		}
		tracked[tokenAccount] = true
	}
	return tracked, nil
}
