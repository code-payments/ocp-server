package async_swap

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"

	indexerpb "github.com/code-payments/code-vm-indexer/generated/indexer/v1"

	"github.com/code-payments/ocp-server/pkg/code/async"
	code_data "github.com/code-payments/ocp-server/pkg/code/data"
	"github.com/code-payments/ocp-server/pkg/code/data/swap"
)

type service struct {
	log             *zap.Logger
	conf            *conf
	data            code_data.Provider
	vmIndexerClient indexerpb.IndexerClient
	integration     Integration
}

func New(log *zap.Logger, data code_data.Provider, vmIndexerClient indexerpb.IndexerClient, integration Integration, configProvider ConfigProvider) async.Service {
	return &service{
		log:             log,
		conf:            configProvider(),
		data:            data,
		vmIndexerClient: vmIndexerClient,
		integration:     integration,
	}

}

func (p *service) Start(ctx context.Context, interval time.Duration) error {
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
