package balance

import (
	"errors"
	"fmt"

	transactionpb "github.com/code-payments/ocp-protobuf-api/generated/go/transaction/v1"

	"github.com/code-payments/ocp-server/ocp/config"
	"github.com/code-payments/ocp-server/ocp/data/action"
	"github.com/code-payments/ocp-server/ocp/data/balance"
	"github.com/code-payments/ocp-server/ocp/data/intent"
)

// ErrUnsupportedBalanceChange is returned when records describe a balance
// change the ledger has no rule for. It's a bug to commit such records with
// ledger writes enabled, so callers should fail the transaction.
var ErrUnsupportedBalanceChange = errors.New("unsupported balance change")

// DeltasForSubmittedIntent returns the ledger deltas for an intent and its
// actions as committed by SubmitIntent. Every account that funds move
// between gets a delta, and USD cost basis moves with the funds.
//
// Actions without a quantity are deferred (eg. a gift card auto-return) and
// contribute nothing until the quantity is set, at which point the flow
// setting it is responsible for the delta.
//
// USD cost basis is attributed per intent: the intent's USD market value is
// the gross amount leaving the source. A fee action carries the configured
// fee's USD value and the principal carries the remainder, so an intent may
// have at most one quantified principal action and at most one fee action.
// withdrawalFeeQuarks is the configured create-on-send withdrawal fee, in
// core mint quarks.
func DeltasForSubmittedIntent(intentRecord *intent.Record, actionRecords []*action.Record, withdrawalFeeQuarks uint64) ([]*balance.Delta, error) {
	if err := requireSupported(intentRecord, actionRecords); err != nil {
		return nil, err
	}

	switch intentRecord.IntentType {
	case intent.OpenAccounts, intent.SendPublicPayment, intent.ReceivePaymentsPublicly:
	default:
		return nil, fmt.Errorf("%w: %d intent is not submitted", ErrUnsupportedBalanceChange, intentRecord.IntentType)
	}

	usdByAction, err := usdCostBasisByAction(intentRecord, actionRecords, withdrawalFeeQuarks)
	if err != nil {
		return nil, err
	}

	var deltas []*balance.Delta
	for _, actionRecord := range actionRecords {
		if actionRecord.Intent != intentRecord.IntentId {
			return nil, errors.New("action does not belong to intent")
		}

		actionDeltas, err := deltasForAction(actionRecord, usdByAction[actionRecord.ActionId])
		if err != nil {
			return nil, err
		}
		deltas = append(deltas, actionDeltas...)
	}

	balance.SortDeltas(deltas)
	return deltas, nil
}

// DeltasForExternalDeposit returns the ledger deltas for an external deposit
// intent, which is created by workers once funds are observed on chain. Only
// confirmed deposits are supported, since that's the only state workers
// commit; the funds are credited to the destination in full.
func DeltasForExternalDeposit(intentRecord *intent.Record) ([]*balance.Delta, error) {
	if intentRecord.IntentType != intent.ExternalDeposit {
		return nil, fmt.Errorf("%w: %d intent is not an external deposit", ErrUnsupportedBalanceChange, intentRecord.IntentType)
	}
	if intentRecord.State != intent.StateConfirmed {
		return nil, fmt.Errorf("%w: external deposit is not confirmed", ErrUnsupportedBalanceChange)
	}

	usdCostBasis, err := UsdCostBasisForIntent(intentRecord)
	if err != nil {
		return nil, err
	}

	metadata := intentRecord.ExternalDepositMetadata
	return []*balance.Delta{{
		TokenAccount: metadata.DestinationTokenAccount,
		Kind:         balance.DeltaCredit,
		Quarks:       metadata.Quantity,
		UsdCostBasis: usdCostBasis,
	}}, nil
}

// DeltasForGiftCardAutoReturn returns the ledger deltas for returning a gift
// card's funds to its issuer. The auto-return action is deferred at issuance
// and contributes nothing until the worker sets its quantity and commits the
// synthetic return intent, which is when this applies. The gift card is
// drained and closed, and the issued value is returned to the issuer.
func DeltasForGiftCardAutoReturn(autoReturnIntent *intent.Record, autoReturnAction *action.Record) ([]*balance.Delta, error) {
	if err := requireSupported(autoReturnIntent, []*action.Record{autoReturnAction}); err != nil {
		return nil, err
	}

	if autoReturnIntent.IntentType != intent.ReceivePaymentsPublicly {
		return nil, fmt.Errorf("%w: %d intent is not a gift card return", ErrUnsupportedBalanceChange, autoReturnIntent.IntentType)
	}
	metadata := autoReturnIntent.ReceivePaymentsPubliclyMetadata
	if !metadata.IsIndirectSend || (!metadata.IsReturned && !metadata.IsIssuerVoidingGiftCard) {
		return nil, fmt.Errorf("%w: intent is not a gift card return", ErrUnsupportedBalanceChange)
	}

	if autoReturnAction.ActionType != action.NoPrivacyWithdraw {
		return nil, fmt.Errorf("%w: auto-return is not a withdraw", ErrUnsupportedBalanceChange)
	}
	if autoReturnAction.Quantity == nil {
		return nil, fmt.Errorf("%w: auto-return quantity is not set", ErrUnsupportedBalanceChange)
	}
	if autoReturnAction.Destination == nil {
		return nil, errors.New("destination is required for a withdraw")
	}
	if autoReturnAction.Source != metadata.Source {
		return nil, errors.New("auto-return action does not match intent")
	}

	usdCostBasis, err := UsdCostBasisForIntent(autoReturnIntent)
	if err != nil {
		return nil, err
	}

	deltas := []*balance.Delta{
		{
			TokenAccount: autoReturnAction.Source,
			Kind:         balance.DeltaDrain,
			Quarks:       *autoReturnAction.Quantity,
			UsdCostBasis: usdCostBasis,
		},
		{
			TokenAccount: *autoReturnAction.Destination,
			Kind:         balance.DeltaCredit,
			Quarks:       *autoReturnAction.Quantity,
			UsdCostBasis: usdCostBasis,
		},
	}
	balance.SortDeltas(deltas)
	return deltas, nil
}

// DeltasForSwapSellReconciliation returns the ledger deltas for reconciling a
// swap sell's funding payment to the value the sell actually realized. The
// funding payment was committed with an estimated USD market value that the
// swap worker later overwrites, so the source's cost basis is adjusted by
// the difference. No quarks move, and the swap destination isn't tracked by
// the ledger, so only the source is adjusted.
//
// previous and updated are the funding intent before and after the worker
// reconciles its value. The actions are those of the funding intent, which
// identify the source.
func DeltasForSwapSellReconciliation(previous, updated *intent.Record, actionRecords []*action.Record) ([]*balance.Delta, error) {
	if previous.IntentId != updated.IntentId {
		return nil, errors.New("intent records do not match")
	}
	if err := requireSupported(updated, actionRecords); err != nil {
		return nil, err
	}
	if updated.IntentType != intent.SendPublicPayment || !updated.SendPublicPaymentMetadata.IsSwapSell {
		return nil, fmt.Errorf("%w: intent is not a swap sell", ErrUnsupportedBalanceChange)
	}
	if previous.IntentType != intent.SendPublicPayment || !previous.SendPublicPaymentMetadata.IsSwapSell {
		return nil, fmt.Errorf("%w: previous intent is not a swap sell", ErrUnsupportedBalanceChange)
	}

	var funding *action.Record
	for _, actionRecord := range actionRecords {
		if actionRecord.Intent != updated.IntentId {
			return nil, errors.New("action does not belong to intent")
		}
		if actionRecord.Quantity == nil {
			continue
		}
		if actionRecord.FeeType != nil {
			return nil, fmt.Errorf("%w: swap sell pays a fee", ErrUnsupportedBalanceChange)
		}
		if funding != nil {
			return nil, fmt.Errorf("%w: intent pays more than one account", ErrUnsupportedBalanceChange)
		}
		funding = actionRecord
	}
	if funding == nil {
		return nil, fmt.Errorf("%w: swap sell has no funding action", ErrUnsupportedBalanceChange)
	}

	adjustment := balance.UsdCostBasisFromFloat(updated.SendPublicPaymentMetadata.UsdMarketValue) - balance.UsdCostBasisFromFloat(previous.SendPublicPaymentMetadata.UsdMarketValue)
	if adjustment == 0 {
		return nil, nil
	}

	// A debit subtracts the signed basis, so a higher realized value removes
	// more basis from the source and a lower one gives some back
	return []*balance.Delta{{
		TokenAccount: funding.Source,
		Kind:         balance.DeltaDebit,
		UsdCostBasis: adjustment,
	}}, nil
}

// UsdCostBasisForIntent is the gross USD cost basis moved by an intent, in
// balance.UsdQuarksPerUnit.
func UsdCostBasisForIntent(intentRecord *intent.Record) (int64, error) {
	switch intentRecord.IntentType {
	case intent.OpenAccounts:
		return 0, nil
	case intent.ExternalDeposit:
		return balance.UsdCostBasisFromFloat(intentRecord.ExternalDepositMetadata.UsdMarketValue), nil
	case intent.SendPublicPayment:
		return balance.UsdCostBasisFromFloat(intentRecord.SendPublicPaymentMetadata.UsdMarketValue), nil
	case intent.ReceivePaymentsPublicly:
		return balance.UsdCostBasisFromFloat(intentRecord.ReceivePaymentsPubliclyMetadata.UsdMarketValue), nil
	default:
		return 0, fmt.Errorf("%w: %d intent", ErrUnsupportedBalanceChange, intentRecord.IntentType)
	}
}

func requireSupported(intentRecord *intent.Record, actionRecords []*action.Record) error {
	if intentRecord.IntentType == intent.PublicDistribution {
		return fmt.Errorf("%w: public distribution", ErrUnsupportedBalanceChange)
	}
	if intentRecord.State == intent.StateRevoked {
		return fmt.Errorf("%w: revoked intent", ErrUnsupportedBalanceChange)
	}
	for _, actionRecord := range actionRecords {
		if actionRecord.State == action.StateRevoked {
			return fmt.Errorf("%w: revoked action", ErrUnsupportedBalanceChange)
		}
	}
	return nil
}

// usdCostBasisByAction splits an intent's USD cost basis across its
// quantified actions, keyed by action ID.
func usdCostBasisByAction(intentRecord *intent.Record, actionRecords []*action.Record, withdrawalFeeQuarks uint64) (map[uint32]int64, error) {
	gross, err := UsdCostBasisForIntent(intentRecord)
	if err != nil {
		return nil, err
	}

	var principal, fee *action.Record
	for _, actionRecord := range actionRecords {
		if actionRecord.Quantity == nil {
			continue
		}

		if actionRecord.FeeType != nil {
			if fee != nil {
				return nil, fmt.Errorf("%w: intent pays more than one fee", ErrUnsupportedBalanceChange)
			}
			fee = actionRecord
			continue
		}

		if principal != nil {
			return nil, fmt.Errorf("%w: intent pays more than one account", ErrUnsupportedBalanceChange)
		}
		principal = actionRecord
	}

	res := make(map[uint32]int64)
	if fee != nil {
		switch *fee.FeeType {
		case transactionpb.FeePaymentAction_CREATE_ON_SEND_WITHDRAWAL:
			// The fee is a fixed core mint amount, so its USD value is fixed
			// regardless of how the intent's mint is valued
			feeUsd := balance.UsdCostBasisFromFloat(float64(withdrawalFeeQuarks) / float64(config.CoreMintQuarksPerUnit))
			res[fee.ActionId] = feeUsd
			gross -= feeUsd
		default:
			return nil, fmt.Errorf("%w: %s fee", ErrUnsupportedBalanceChange, fee.FeeType.String())
		}
	}
	if principal != nil {
		res[principal.ActionId] = gross
	} else if gross != 0 {
		return nil, fmt.Errorf("%w: intent has value but no quantified action", ErrUnsupportedBalanceChange)
	}
	return res, nil
}

func deltasForAction(actionRecord *action.Record, usdCostBasis int64) ([]*balance.Delta, error) {
	switch actionRecord.ActionType {
	case action.OpenAccount:
		return nil, nil

	case action.CloseEmptyAccount:
		return []*balance.Delta{{
			TokenAccount: actionRecord.Source,
			Kind:         balance.DeltaClose,
		}}, nil

	case action.NoPrivacyTransfer, action.NoPrivacyWithdraw:
		if actionRecord.Quantity == nil {
			return nil, nil
		}
		if actionRecord.Destination == nil {
			return nil, errors.New("destination is required for a transfer")
		}

		outgoingKind := balance.DeltaDebit
		if actionRecord.ActionType == action.NoPrivacyWithdraw {
			outgoingKind = balance.DeltaDrain
		}
		return []*balance.Delta{
			{
				TokenAccount: actionRecord.Source,
				Kind:         outgoingKind,
				Quarks:       *actionRecord.Quantity,
				UsdCostBasis: usdCostBasis,
			},
			{
				TokenAccount: *actionRecord.Destination,
				Kind:         balance.DeltaCredit,
				Quarks:       *actionRecord.Quantity,
				UsdCostBasis: usdCostBasis,
			},
		}, nil

	default:
		return nil, fmt.Errorf("%w: %d action", ErrUnsupportedBalanceChange, actionRecord.ActionType)
	}
}
