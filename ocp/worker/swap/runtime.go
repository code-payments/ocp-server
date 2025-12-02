package swap

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"

	indexerpb "github.com/code-payments/code-vm-indexer/generated/indexer/v1"

	ocp_data "github.com/code-payments/ocp-server/ocp/data"
	"github.com/code-payments/ocp-server/ocp/data/swap"
	"github.com/code-payments/ocp-server/ocp/worker"
)

type runtime struct {
	log             *zap.Logger
	conf            *conf
	data            ocp_data.Provider
	vmIndexerClient indexerpb.IndexerClient
	integration     Integration
}

func New(log *zap.Logger, data ocp_data.Provider, vmIndexerClient indexerpb.IndexerClient, integration Integration, configProvider ConfigProvider) worker.Runtime {
	return &runtime{
		log:             log,
		conf:            configProvider(),
		data:            data,
		vmIndexerClient: vmIndexerClient,
		integration:     integration,
	}

}

func (p *runtime) Start(ctx context.Context, interval time.Duration) error {
	for _, state := range []swap.State{
		swap.StateCreated,
		swap.StateFunding,
		swap.StateFunded,
		swap.StateSubmitting,
		swap.StateCancelling,
	} {
		go func(state swap.State) {

			err := p.worker(ctx, state, interval)
			if err != nil && err != context.Canceled {
				p.log.With(zap.Error(err)).Warn(fmt.Sprintf("swap processing loop terminated unexpectedly for state %s", state.String()))
			}

		}(state)
	}

	go func() {
		err := p.metricsGaugeWorker(ctx)
		if err != nil && err != context.Canceled {
			p.log.With(zap.Error(err)).Warn("swap metrics gauge loop terminated unexpectedly")
		}
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	}
}
