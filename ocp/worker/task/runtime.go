package task

import (
	"context"
	"time"

	"go.uber.org/zap"

	ocp_data "github.com/code-payments/ocp-server/ocp/data"
	ocp_task "github.com/code-payments/ocp-server/ocp/task"
	"github.com/code-payments/ocp-server/ocp/worker"
)

type runtime struct {
	log       *zap.Logger
	conf      *conf
	data      ocp_data.Provider
	scheduler *ocp_task.Scheduler
}

func New(
	log *zap.Logger,
	data ocp_data.Provider,
	scheduler *ocp_task.Scheduler,
	configProvider ConfigProvider,
) worker.Runtime {
	return &runtime{
		log:       log,
		conf:      configProvider(),
		data:      data,
		scheduler: scheduler,
	}
}

func (p *runtime) Start(ctx context.Context, interval time.Duration) error {
	go func() {
		err := p.worker(ctx, interval)
		if err != nil && err != context.Canceled {
			p.log.With(zap.Error(err)).Warn("task processing loop terminated unexpectedly")
		}
	}()

	go func() {
		err := p.metricsGaugeWorker(ctx)
		if err != nil && err != context.Canceled {
			p.log.With(zap.Error(err)).Warn("task metrics gauge loop terminated unexpectedly")
		}
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	}
}
