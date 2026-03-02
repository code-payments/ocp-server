package launcher

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"

	ocp_data "github.com/code-payments/ocp-server/ocp/data"
	"github.com/code-payments/ocp-server/ocp/data/currency"
	"github.com/code-payments/ocp-server/ocp/worker"
)

type runtime struct {
	log  *zap.Logger
	conf *conf
	data ocp_data.Provider
}

func New(log *zap.Logger, data ocp_data.Provider, configProvider ConfigProvider) worker.Runtime {
	return &runtime{
		log:  log,
		conf: configProvider(),
		data: data,
	}
}

func (p *runtime) Start(ctx context.Context, interval time.Duration) error {
	for _, state := range []currency.MetadataState{
		currency.MetadataStateUnknown,
		currency.MetadataStateFundingAuthority,
		currency.MetadataStateInitializing,
		currency.MetadataStateFinalValidation,
	} {
		go func(state currency.MetadataState) {
			err := p.worker(ctx, state, interval)
			if err != nil && err != context.Canceled {
				p.log.With(zap.Error(err)).Warn(fmt.Sprintf("currency launcher processing loop terminated unexpectedly for state %s", state.String()))
			}
		}(state)
	}

	go func() {
		err := p.metricsGaugeWorker(ctx)
		if err != nil && err != context.Canceled {
			p.log.With(zap.Error(err)).Warn("currency launcher metrics gauge loop terminated unexpectedly")
		}
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	}
}
