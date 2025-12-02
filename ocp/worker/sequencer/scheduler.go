package sequencer

import (
	"context"
	"fmt"

	"github.com/pkg/errors"
	"go.uber.org/zap"

	indexerpb "github.com/code-payments/code-vm-indexer/generated/indexer/v1"
	"github.com/code-payments/ocp-server/ocp/common"
	ocp_data "github.com/code-payments/ocp-server/ocp/data"
	"github.com/code-payments/ocp-server/ocp/data/action"
	"github.com/code-payments/ocp-server/ocp/data/fulfillment"
	"github.com/code-payments/ocp-server/pointer"
)

// Scheduler decides when fulfillments can be scheduled for submission to the
// blockchain. It does not manage the internal state of a fulfillment.
type Scheduler interface {
	// CanSubmitToBlockchain determines whether the given fulfillment can be
	// scheduled for submission to the blockchain.
	CanSubmitToBlockchain(ctx context.Context, record *fulfillment.Record) (bool, error)
}

type contextualScheduler struct {
	log            *zap.Logger
	data           ocp_data.Provider
	conf           *conf
	handlersByType map[fulfillment.Type]FulfillmentHandler

	// Workaround config to allow tests to pass
	//
	// todo: Need a path forward to test things that call the blockchain directly.
	includeSubsidizerChecks bool
}

// NewContextualScheduler returns a scheduler that utilizes the global, account,
// intent, action and local context of a fulfillment to determine whether scheduling
// submission to the blockchain should occur.
//
// The implementations has generic handling for:
//  1. Precondition checks
//  2. Circuit breaker safety mechanisms
//  3. Subisider sanity checks
//
// The implementation defers contextualized scheduling logic to handler implementations.
//
// The implementation makes the following assumptions:
//  1. We have full control of user account balances via a timelock account.
//     Otherwise, we'd require a much more complex solution for the knapsack
//     problem (likely a wavefunction collapse implementation).
//  2. Fulfillments that require client signatures are validated to guarantee
//     success before being created.
func NewContextualScheduler(log *zap.Logger, data ocp_data.Provider, indexerClient indexerpb.IndexerClient, configProvider ConfigProvider) Scheduler {
	return &contextualScheduler{
		log:                     log,
		data:                    data,
		conf:                    configProvider(),
		handlersByType:          getFulfillmentHandlers(data, indexerClient),
		includeSubsidizerChecks: true,
	}
}

// CanSubmitToBlockchain implements Scheduler.CanSubmitToBlockchain
func (s *contextualScheduler) CanSubmitToBlockchain(ctx context.Context, fulfillmentRecord *fulfillment.Record) (bool, error) {
	log := s.log.With(
		zap.String("method", "CanSubmitToBlockchain"),
		zap.String("intent_type", fulfillmentRecord.IntentType.String()),
		zap.String("fulfillment_type", fulfillmentRecord.FulfillmentType.String()),
		zap.String("intent", fulfillmentRecord.Intent),
		zap.String("signature", *pointer.StringOrDefault(fulfillmentRecord.Signature, "<nil>")),
	)

	handler, ok := s.handlersByType[fulfillmentRecord.FulfillmentType]
	if !ok {
		log.Warn("no handler for fulfillment type")
		return false, errors.Errorf("no fulfillment handler for %d type", fulfillmentRecord.FulfillmentType)
	}

	involvedAccounts := []string{fulfillmentRecord.Source}
	if fulfillmentRecord.Destination != nil {
		involvedAccounts = append(involvedAccounts, *fulfillmentRecord.Destination)
	}

	//
	// Part 1: Fulfillment state precondition checks
	//

	// Sanity check the fulfillment record. There should be data for a signed
	// transaction when it's not made on demand.
	if !handler.SupportsOnDemandTransactions() && (fulfillmentRecord.Signature == nil || len(*fulfillmentRecord.Signature) == 0) {
		log.Warn("asking to schedule a fulfillment without a signed transaction")
		return false, nil
	}

	// Fulfillment is in a terminal state and can't be submitted to the blockchain
	if fulfillmentRecord.State.IsTerminal() {
		// There's likely a bug somewhere if we hit this case, Either there's a
		// an error in a worker that's not transitioning fulfillment/intent states
		// properly, or we've written code that's caused an intent to fail midway
		// through a set of fulfillments.
		log.Warn("asking to schedule a fulfillment that's in a terminal state")
		return false, nil
	}

	// Fuifillment is already in a scheduled state
	if fulfillmentRecord.State == fulfillment.StatePending {
		log.Warn("asking to schedule a fulfillment that's already scheduled")
		return true, nil
	}

	//
	// Part 2: Action state precondition checks
	//

	actionRecord, err := s.data.GetActionById(ctx, fulfillmentRecord.Intent, fulfillmentRecord.ActionId)
	if err != nil {
		return false, err
	}

	// Action isn't in a state that would indicate a schedulable fulfillment
	switch actionRecord.State {
	case action.StateUnknown:
		log.Debug("not scheduling fulfillment with action in unknown state")
		return false, nil
	case action.StateRevoked, action.StateFailed:
		log.Warn(fmt.Sprintf("cannot schedule fulfillment with action in %s state", actionRecord.State.String()))
		return false, nil
	}

	//
	// Part 3: Circuit breakers
	//

	// Is fulfillment scheduling manually disabled
	if s.conf.disableTransactionScheduling.Get(ctx) {
		log.Debug("not scheduling fulfillment because scheduling is disabled")
		return false, nil
	}

	// Account-level circuit breaker based on whether there's a failed fulfillment
	// for any involved account.
	for _, account := range involvedAccounts {
		log = log.With(zap.String("account", account))

		numFailedFulfillments, err := s.data.GetFulfillmentCountByStateAndAddress(ctx, fulfillment.StateFailed, account)
		if err != nil {
			log.With(zap.Error(err)).Warn("failure getting failed fulfillment count for account")
			return false, err
		}

		// Completely stop scheduling if there are failed fulfillments, which will
		// impact the entire dependency graph of intents starting from this one.
		// We'll need manual intervention to understand what went wrong and how
		// to resolve it.
		if numFailedFulfillments > 0 {
			log.Warn("not scheduling fulfillment because an account has failed fulfillments")
			return false, nil
		}
	}

	// Intent-level circuit breaker based on whether there's a failed fulfillment

	numFailedFulfillments, err := s.data.GetFulfillmentCountByIntentAndState(ctx, fulfillmentRecord.Intent, fulfillment.StateFailed)
	if err != nil {
		log.With(zap.Error(err)).Warn("failure getting failed fulfillment count for intent")
		return false, err
	}

	if numFailedFulfillments > 0 {
		log.Warn("not scheduling fulfillment because intent has failed fulfillments")
		return false, nil
	}

	// Global circuit breaker based on the total failed fulfillment count
	numFailedFulfillments, err = s.data.GetFulfillmentCountByState(ctx, fulfillment.StateFailed)
	if err != nil {
		log.With(zap.Error(err)).Warn("failure getting globlal failed fulfillment count")
		return false, err
	}

	if numFailedFulfillments > s.conf.maxGlobalFailedFulfillments.Get(ctx) {
		log.Warn("not scheduling fulfillment because global circuit breaker was tripped")
		return false, nil
	}

	//
	// Part 4: Contextual scheduling
	//

	isScheduled, err := handler.CanSubmitToBlockchain(ctx, fulfillmentRecord)
	if err != nil {
		log.With(zap.Error(err)).Warn("handler failed scheduling check")
		return false, err
	}

	if !isScheduled {
		log.Debug("handler did not schedule fulfillment")
		return false, nil
	}

	//
	// Part 5: Subsidizer checks
	//

	// todo: Need a path forward to test things that call the blockchain directly.
	if s.includeSubsidizerChecks {
		// Determine if there is sufficient balance in the subsidizer to cover fees
		// for this fulfillment.
		//
		// todo: This is the most naive approach, isn't terribly performant, and won't
		//       be guaranteed to work well beyond a single thread. It's better than
		//       nothing for a quick first pass implementation.
		// todo: We should really consider hardening before launch given sheer amount
		//       of accounts and nonces required for privacy v3.
		err = common.EnforceMinimumSubsidizerBalance(ctx, s.data)
		if err == common.ErrSubsidizerRequiresFunding {
			log.Warn("not scheduling fulfillment because the subsidizer requires additional funding")
			return false, nil
		} else if err != nil {
			log.With(zap.Error(err)).Warn("failure checking minimum subidizer balance")
			return false, err
		}
	}

	log.Debug("scheduling this fulfillment for submission to blockchain")
	return true, nil
}
