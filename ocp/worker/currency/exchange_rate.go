package async

import (
	"context"
	"time"

	"github.com/newrelic/go-agent/v3/newrelic"
	"github.com/pkg/errors"
	"go.uber.org/zap"

	"github.com/code-payments/ocp-server/metrics"
	"github.com/code-payments/ocp-server/retry"
	"github.com/code-payments/ocp-server/retry/backoff"

	ocp_data "github.com/code-payments/ocp-server/ocp/data"
	"github.com/code-payments/ocp-server/ocp/worker"
)

type exchangeRateRuntime struct {
	log  *zap.Logger
	data ocp_data.Provider
}

func NewExchangeRateRuntime(log *zap.Logger, data ocp_data.Provider) worker.Runtime {
	return &exchangeRateRuntime{
		log:  log,
		data: data,
	}
}

func (p *exchangeRateRuntime) Start(runtimeCtx context.Context, interval time.Duration) error {
	for {
		_, err := retry.Retry(
			func() error {
				p.log.Debug("updating exchange rates")

				nr := runtimeCtx.Value(metrics.NewRelicContextKey).(*newrelic.Application)
				m := nr.StartTransaction("currency_exchange_rate_runtime")
				defer m.End()
				tracedCtx := newrelic.NewContext(runtimeCtx, m)

				err := p.GetCurrentExchangeRates(tracedCtx)
				if err != nil {
					m.NoticeError(err)
					p.log.With(zap.Error(err)).Warn("failed to process current rate data")
				}

				return err
			},
			retry.NonRetriableErrors(context.Canceled),
			retry.BackoffWithJitter(backoff.BinaryExponential(time.Second), interval, 0.1),
		)
		if err != nil {
			if err != context.Canceled {
				// Should not happen since only non-retriable error is context.Canceled
				p.log.With(zap.Error(err)).Warn("unexpected error when processing current rate data")
			}

			return err
		}

		select {
		case <-runtimeCtx.Done():
			return runtimeCtx.Err()
		case <-time.After(interval):
		}
	}
}

func (p *exchangeRateRuntime) GetCurrentExchangeRates(ctx context.Context) error {
	data, err := p.data.GetCurrentExchangeRatesFromExternalProviders(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to get current rate data")
	}

	if err = p.data.ImportExchangeRates(ctx, data); err != nil {
		return errors.Wrap(err, "failed to store rate data")
	}

	return nil
}
