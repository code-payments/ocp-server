package account

import (
	"context"
	"time"

	"go.uber.org/zap"

	ocp_data "github.com/code-payments/ocp-server/ocp/data"
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

	go func() {
		err := p.giftCardAutoReturnWorker(ctx, interval)
		if err != nil && err != context.Canceled {
			p.log.With(zap.Error(err)).Warn("gift card auto-return processing loop terminated unexpectedly")
		}
	}()

	go func() {
		err := p.metricsGaugeWorker(ctx)
		if err != nil && err != context.Canceled {
			p.log.With(zap.Error(err)).Warn("account metrics gauge loop terminated unexpectedly")
		}
	}()

	go func() {
		err := p.balanceBackfillWorker(ctx, interval)
		if err != nil && err != context.Canceled {
			p.log.With(zap.Error(err)).Warn("balance backfill loop terminated unexpectedly")
		}
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	}
}
