package sequencer

import (
	"context"
	"fmt"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/zap"

	indexerpb "github.com/code-payments/code-vm-indexer/generated/indexer/v1"

	ocp_data "github.com/code-payments/ocp-server/ocp/data"
	"github.com/code-payments/ocp-server/ocp/data/action"
	"github.com/code-payments/ocp-server/ocp/data/fulfillment"
	"github.com/code-payments/ocp-server/ocp/data/intent"
	"github.com/code-payments/ocp-server/ocp/data/nonce"
	"github.com/code-payments/ocp-server/ocp/transaction"
	"github.com/code-payments/ocp-server/ocp/worker"
)

var (
	ErrInvalidFulfillmentSignature       = errors.New("invalid fulfillment signature")
	ErrInvalidFulfillmentStateTransition = errors.New("invalid fulfillment state transition")
	ErrCouldNotGetIntentLock             = errors.New("could not get intent lock")
)

type runtime struct {
	log                       *zap.Logger
	conf                      *conf
	data                      ocp_data.Provider
	scheduler                 Scheduler
	vmIndexerClient           indexerpb.IndexerClient
	solanaNoncePool           *transaction.LocalNoncePool
	fulfillmentHandlersByType map[fulfillment.Type]FulfillmentHandler
	actionHandlersByType      map[action.Type]ActionHandler
	intentHandlersByType      map[intent.Type]IntentHandler
}

func New(log *zap.Logger, data ocp_data.Provider, scheduler Scheduler, vmIndexerClient indexerpb.IndexerClient, solanaNoncePool *transaction.LocalNoncePool, configProvider ConfigProvider) (worker.Runtime, error) {
	if err := solanaNoncePool.Validate(nonce.EnvironmentSolana, nonce.EnvironmentInstanceSolanaMainnet, nonce.PurposeOnDemandTransaction); err != nil {
		return nil, err
	}

	return &runtime{
		log:                       log,
		conf:                      configProvider(),
		data:                      data,
		scheduler:                 scheduler,
		vmIndexerClient:           vmIndexerClient,
		solanaNoncePool:           solanaNoncePool,
		fulfillmentHandlersByType: getFulfillmentHandlers(data, vmIndexerClient),
		actionHandlersByType:      getActionHandlers(data),
		intentHandlersByType:      getIntentHandlers(data),
	}, nil
}

func (p *runtime) Start(ctx context.Context, interval time.Duration) error {

	// Setup workers to watch for fulfillment state changes on the Solana side
	for _, item := range []fulfillment.State{
		fulfillment.StateUnknown,
		fulfillment.StatePending,

		// There's no executable logic for these states yet:
		// fulfillment.StateConfirmed,
		// fulfillment.StateFailed,
		// fulfillment.StateRevoked,
	} {
		go func(state fulfillment.State) {

			// todo: Note to our future selves that there are some components of
			//       the scheduler (ie. subsidizer balance checks) that won't
			//       work perfectly in a multi-threaded or multi-node environment.
			err := p.worker(ctx, state, interval)
			if err != nil && err != context.Canceled {
				p.log.With(zap.Error(err)).Warn(fmt.Sprintf("fulfillment processing loop terminated unexpectedly for state %d", state))
			}

		}(item)
	}

	go func() {
		err := p.metricsGaugeWorker(ctx)
		if err != nil && err != context.Canceled {
			p.log.With(zap.Error(err)).Warn("fulfillment metrics gauge loop terminated unexpectedly")
		}
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	}
}
