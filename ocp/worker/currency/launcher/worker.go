package launcher

import (
	"context"
	"time"

	"go.uber.org/zap"

	"github.com/code-payments/ocp-server/metrics"
	"github.com/code-payments/ocp-server/retry"
)

func (p *runtime) worker(ctx context.Context, interval time.Duration) error {
	delay := interval

	err := retry.Loop(
		func() (err error) {
			time.Sleep(delay)

			provider := ctx.Value(metrics.ProviderContextKey).(metrics.Provider)
			trace := provider.StartTrace("currency_launcher_runtime__handle")
			defer trace.End()
			tracedCtx := metrics.NewContext(ctx, trace)

			err = p.handle(tracedCtx)
			if err != nil {
				trace.OnError(err)
				p.log.With(zap.Error(err)).Warn("failure processing launcher")
			}

			return nil
		},
		retry.NonRetriableErrors(context.Canceled),
	)

	return err
}

func (p *runtime) handle(ctx context.Context) error {
	// todo: Implement launcher processing logic
	return nil
}
