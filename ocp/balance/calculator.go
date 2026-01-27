package balance

import (
	"context"
	"time"

	"github.com/pkg/errors"

	"github.com/code-payments/ocp-server/metrics"
	"github.com/code-payments/ocp-server/ocp/common"
	ocp_data "github.com/code-payments/ocp-server/ocp/data"
	"github.com/code-payments/ocp-server/ocp/data/balance"
	"github.com/code-payments/ocp-server/ocp/data/currency"
	"github.com/code-payments/ocp-server/ocp/data/swap"
	"github.com/code-payments/ocp-server/ocp/data/timelock"
	"github.com/code-payments/ocp-server/solana"
	"github.com/code-payments/ocp-server/solana/currencycreator"
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
	// ErrNegativeBalance indicates that a balance calculation resulted in a
	// negative value.
	ErrNegativeBalance = errors.New("balance calculation resulted in negative value")

	// ErrNotManagedByCode indicates that an account is not owned by Code.
	// It's up to callers to determine how to handle this situation within
	// the context of a balance.
	ErrNotManagedByCode = errors.New("explicitly not handling account not managed by code")

	// ErrUnhandledAccount indicates that the balance calculator does not
	// have strategies to handle the provided account.
	ErrUnhandledAccount = errors.New("unhandled account")
)

// Calculator is a function that calculates a token account's balance
type Calculator func(ctx context.Context, data ocp_data.Provider, tokenAccount *common.Account) (uint64, error)

type Strategy func(ctx context.Context, tokenAccount *common.Account, state *State) (*State, error)

type State struct {
	// We allow for negative balances in intermediary steps. This is to simplify
	// coordination between strategies. In the end, the sum of all strategies must
	// reflect an accurate picture of the balance, at which point we'll enforce this
	// is positive.
	current int64
}

// Calculate calculates a token account's balance using a starting point and a set
// of strategies. Each may be incomplete individually, but in total must form a
// complete balance calculation.
func Calculate(ctx context.Context, tokenAccount *common.Account, initialBalance uint64, strategies ...Strategy) (balance uint64, err error) {
	balanceState := &State{
		current: int64(initialBalance),
	}

	for _, strategy := range strategies {
		balanceState, err = strategy(ctx, tokenAccount, balanceState)
		if err != nil {
			return 0, err
		}
	}

	if balanceState.current < 0 {
		return 0, ErrNegativeBalance
	}

	return uint64(balanceState.current), nil
}

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

	// The strategy uses cached values from the intents system. The account must
	// be managed by Code in order to return accurate values.
	isManagedByCode := common.IsManagedByCode(ctx, timelockRecord)
	if !isManagedByCode {
		tracer.OnError(ErrNotManagedByCode)
		return 0, ErrNotManagedByCode
	}

	// Pick a set of strategies relevant for the type of account, so we can optimize
	// the number of DB calls.
	//
	// Overall, we're using a simple strategy that iterates over an account's history
	// to unblock a scheduler implementation optimized for privacy.
	//
	// todo: Come up with a heurisitc that enables some form of checkpointing, so
	//       we're not iterating over all records every time.
	strategies := []Strategy{
		FundingFromExternalDeposits(ctx, data),
		NetBalanceFromIntentActions(ctx, data),
	}

	balance, err := Calculate(
		ctx,
		tokenAccount,
		0,
		strategies...,
	)
	if err != nil {
		tracer.OnError(err)
		return 0, errors.Wrap(err, "error calculating token account balance")
	}
	return balance, nil
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
	quarks, slot, err := data.GetBlockchainBalance(ctx, tokenAccount.PublicKey().ToBase58())
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

// NetBalanceFromIntentActions is a balance calculation strategy that incorporates
// the net balance by applying payment intents to the current balance.
func NetBalanceFromIntentActions(ctx context.Context, data ocp_data.Provider) Strategy {
	return func(ctx context.Context, tokenAccount *common.Account, state *State) (*State, error) {
		netBalance, err := data.GetNetBalanceFromActions(ctx, tokenAccount.PublicKey().ToBase58())
		if err != nil {
			return nil, errors.Wrap(err, "error getting net balance from intent actions")
		}

		state.current += netBalance
		return state, nil
	}
}

// FundingFromExternalDeposits is a balance calculation strategy that adds funding
// from deposits from external accounts.
func FundingFromExternalDeposits(ctx context.Context, data ocp_data.Provider) Strategy {
	return func(ctx context.Context, tokenAccount *common.Account, state *State) (*State, error) {
		amount, err := data.GetTotalExternalDepositedAmountInQuarks(ctx, tokenAccount.PublicKey().ToBase58())
		if err != nil {
			return nil, errors.Wrap(err, "error getting external deposit amount")
		}
		state.current += int64(amount)

		return state, nil
	}
}

// BatchCalculator is a functiona that calculates a batch of accounts' balances
type BatchCalculator func(ctx context.Context, data ocp_data.Provider, accountRecordsBatch []*common.AccountRecords) (map[string]uint64, error)

type BatchStrategy func(ctx context.Context, tokenAccounts []string, state *BatchState) (*BatchState, error)

type BatchState struct {
	// We allow for negative balances in intermediary steps. This is to simplify
	// coordination between strategies. In the end, the sum of all strategies must
	// reflect an accurate picture of the balance, at which point we'll enforce this
	// is positive.
	current map[string]int64
}

// CalculateBatch calculates a set of token accounts' balance using a starting point
// and a set of strategies. Each may be incomplete individually, but in total must
// form a complete balance calculation.
func CalculateBatch(ctx context.Context, tokenAccounts []string, strategies ...BatchStrategy) (balanceByTokenAccount map[string]uint64, err error) {
	balanceState := &BatchState{
		current: make(map[string]int64),
	}

	for _, strategy := range strategies {
		balanceState, err = strategy(ctx, tokenAccounts, balanceState)
		if err != nil {
			return nil, err
		}
	}

	res := make(map[string]uint64)
	for tokenAccount, balance := range balanceState.current {
		if balance < 0 {
			return nil, ErrNegativeBalance
		}

		res[tokenAccount] = uint64(balance)
	}

	return res, nil
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
	var tokenAccounts []string
	for _, timelockRecord := range timelockRecords {
		// The strategy uses cached values from the intents system. The account must
		// be managed by Code in order to return accurate values.
		isManagedByCode := common.IsManagedByCode(ctx, timelockRecord)
		if !isManagedByCode {
			return nil, ErrNotManagedByCode
		}

		tokenAccounts = append(tokenAccounts, timelockRecord.VaultAddress)
	}

	return CalculateBatch(
		ctx,
		tokenAccounts,
		FundingFromExternalDepositsBatch(ctx, data),
		NetBalanceFromIntentActionsBatch(ctx, data),
	)
}

// NetBalanceFromIntentActionsBatch is a balance calculation strategy that incorporates
// the net balance by applying payment intents to the current balance.
func NetBalanceFromIntentActionsBatch(ctx context.Context, data ocp_data.Provider) BatchStrategy {
	return func(ctx context.Context, tokenAccounts []string, state *BatchState) (*BatchState, error) {
		netBalanceByAccount, err := data.GetNetBalanceFromActionsBatch(ctx, tokenAccounts...)
		if err != nil {
			return nil, errors.Wrap(err, "error getting net balance from intent actions")
		}

		for tokenAccount, netBalance := range netBalanceByAccount {
			state.current[tokenAccount] += netBalance
		}

		return state, nil
	}
}

// FundingFromExternalDepositsBatch is a balance calculation strategy that adds
// funding from deposits from external accounts.
func FundingFromExternalDepositsBatch(ctx context.Context, data ocp_data.Provider) BatchStrategy {
	return func(ctx context.Context, tokenAccounts []string, state *BatchState) (*BatchState, error) {
		amountByAccount, err := data.GetTotalExternalDepositedAmountInQuarksBatch(ctx, tokenAccounts...)
		if err != nil {
			return nil, errors.Wrap(err, "error getting external deposit amount")
		}

		for tokenAccount, amount := range amountByAccount {
			state.current[tokenAccount] += int64(amount)
		}

		return state, nil
	}
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

// PendingSwapBalance represents the pending balance and the swaps used to compute it.
type PendingSwapBalance struct {
	TargetMint  *common.Account
	DeltaQuarks uint64
	Swaps       []*swap.Record
}

// GetDeltaQuarksFromPendingSwaps returns a mapping of mint to pending swap balance that
// represents the simulated result of executing pending swaps for an owner account.
// Only swaps funded via SubmitIntent in the Funding, Funded, or Submitting states
// are included.
//
// The returned deltas represent the expected output amounts for each ToMint,
// calculated by simulating each swap execution using the current reserve state.
func GetDeltaQuarksFromPendingSwaps(ctx context.Context, data ocp_data.Provider, owner string) (map[string]*PendingSwapBalance, error) {
	tracer := metrics.TraceMethodCall(ctx, metricsPackageName, "GetDeltaQuarksFromPendingSwaps")
	tracer.AddAttribute("owner", owner)
	defer tracer.End()

	pendingSwaps, err := data.GetAllSwapsByOwnerAndStates(
		ctx,
		owner,
		swap.StateFunding,
		swap.StateFunded,
		swap.StateSubmitting,
	)
	if err == swap.ErrNotFound {
		return make(map[string]*PendingSwapBalance), nil
	} else if err != nil {
		tracer.OnError(err)
		return nil, errors.Wrap(err, "error getting pending swaps")
	}

	// Collect all unique launchpad mints that need reserve state
	launchpadMints := make(map[string]struct{})
	for _, swapRecord := range pendingSwaps {
		if swapRecord.FundingSource != swap.FundingSourceSubmitIntent {
			continue
		}

		fromMint, err := common.NewAccountFromPublicKeyString(swapRecord.FromMint)
		if err != nil {
			tracer.OnError(err)
			return nil, err
		}
		toMint, err := common.NewAccountFromPublicKeyString(swapRecord.ToMint)
		if err != nil {
			tracer.OnError(err)
			return nil, err
		}

		if !common.IsCoreMint(fromMint) {
			launchpadMints[swapRecord.FromMint] = struct{}{}
		}
		if !common.IsCoreMint(toMint) {
			launchpadMints[swapRecord.ToMint] = struct{}{}
		}
	}

	// Fetch all reserve states upfront
	reserveByMint := make(map[string]*currency.ReserveRecord)
	now := time.Now()
	for mint := range launchpadMints {
		reserveRecord, err := data.GetCurrencyReserveAtTime(ctx, mint, now)
		if err == currency.ErrNotFound {
			// If the reserve cannot be found, try something a bit further in the past.
			//
			// todo: Fix bug in Postgres implementation
			reserveRecord, err = data.GetCurrencyReserveAtTime(ctx, mint, now.Add(-15*time.Minute))
			if err != nil {
				tracer.OnError(err)
				return nil, errors.Wrapf(err, "error getting reserve for mint %s", mint)
			}
		} else if err != nil {
			tracer.OnError(err)
			return nil, errors.Wrapf(err, "error getting reserve for mint %s", mint)
		}
		reserveByMint[mint] = reserveRecord
	}

	// Simulate each swap and accumulate output by destination mint
	deltaByMint := make(map[string]*PendingSwapBalance)
	for _, swapRecord := range pendingSwaps {
		if swapRecord.FundingSource != swap.FundingSourceSubmitIntent {
			continue
		}

		outputQuarks, err := simulateSwapOutput(swapRecord, reserveByMint)
		if err != nil {
			tracer.OnError(err)
			return nil, errors.Wrap(err, "error simulating swap output")
		}

		if deltaByMint[swapRecord.ToMint] == nil {
			toMint, err := common.NewAccountFromPublicKeyString(swapRecord.ToMint)
			if err != nil {
				tracer.OnError(err)
				return nil, err
			}
			deltaByMint[swapRecord.ToMint] = &PendingSwapBalance{
				TargetMint: toMint,
			}
		}
		deltaByMint[swapRecord.ToMint].DeltaQuarks += outputQuarks
		deltaByMint[swapRecord.ToMint].Swaps = append(deltaByMint[swapRecord.ToMint].Swaps, swapRecord)
	}

	return deltaByMint, nil
}

// simulateSwapOutput calculates the expected output amount for a swap by simulating
// its execution using the provided reserve states.
func simulateSwapOutput(swapRecord *swap.Record, reserveByMint map[string]*currency.ReserveRecord) (uint64, error) {
	fromMint, err := common.NewAccountFromPublicKeyString(swapRecord.FromMint)
	if err != nil {
		return 0, err
	}

	toMint, err := common.NewAccountFromPublicKeyString(swapRecord.ToMint)
	if err != nil {
		return 0, err
	}

	isFromCoreMint := common.IsCoreMint(fromMint)
	isToCoreMint := common.IsCoreMint(toMint)

	switch {
	case isFromCoreMint && !isToCoreMint:
		// Buy: core mint -> launchpad currency
		outputQuarks, err := simulateBuy(reserveByMint[swapRecord.ToMint], swapRecord.Amount)
		if err != nil {
			return 0, errors.Wrapf(err, "failed to simulate buy for mint %s", swapRecord.ToMint)
		}
		return outputQuarks, nil

	case !isFromCoreMint && isToCoreMint:
		// Sell: launchpad currency -> core mint
		outputQuarks, err := simulateSell(reserveByMint[swapRecord.FromMint], swapRecord.Amount)
		if err != nil {
			return 0, errors.Wrapf(err, "failed to simulate sell for mint %s", swapRecord.FromMint)
		}
		return outputQuarks, nil

	case !isFromCoreMint && !isToCoreMint:
		// Buy-Sell: launchpad currency -> launchpad currency
		// First sell FromMint to get core mint, then buy ToMint
		coreMintQuarks, err := simulateSell(reserveByMint[swapRecord.FromMint], swapRecord.Amount)
		if err != nil {
			return 0, errors.Wrapf(err, "failed to simulate sell for mint %s", swapRecord.FromMint)
		}
		outputQuarks, err := simulateBuy(reserveByMint[swapRecord.ToMint], coreMintQuarks)
		if err != nil {
			return 0, errors.Wrapf(err, "failed to simulate buy for mint %s", swapRecord.ToMint)
		}
		return outputQuarks, nil

	default:
		// core mint -> core mint is not a valid swap
		return 0, errors.New("invalid swap: both mints are core mint")
	}
}

// simulateBuy simulates buying a launchpad currency with core mint quarks.
func simulateBuy(reserveRecord *currency.ReserveRecord, coreMintQuarks uint64) (uint64, error) {
	if reserveRecord == nil {
		return 0, errors.New("reserve record is nil")
	}

	return currencycreator.EstimateBuy(&currencycreator.EstimateBuyArgs{
		CurrentSupplyInQuarks: reserveRecord.SupplyFromBonding,
		BuyAmountInQuarks:     coreMintQuarks,
		ValueMintDecimals:     uint8(common.CoreMintDecimals),
	}), nil
}

// simulateSell simulates selling a launchpad currency for core mint quarks.
func simulateSell(reserveRecord *currency.ReserveRecord, sellQuarks uint64) (uint64, error) {
	if reserveRecord == nil {
		return 0, errors.New("reserve record is nil")
	}

	outputQuarks, _ := currencycreator.EstimateSell(&currencycreator.EstimateSellArgs{
		CurrentSupplyInQuarks: reserveRecord.SupplyFromBonding,
		SellAmountInQuarks:    sellQuarks,
		ValueMintDecimals:     uint8(common.CoreMintDecimals),
		SellFeeBps:            currencycreator.DefaultSellFeeBps,
	})
	return outputQuarks, nil
}
