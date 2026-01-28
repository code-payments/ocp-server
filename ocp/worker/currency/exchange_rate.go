package currency

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/zap"

	"github.com/code-payments/ocp-server/metrics"
	"github.com/code-payments/ocp-server/retry"
	"github.com/code-payments/ocp-server/retry/backoff"

	currency_lib "github.com/code-payments/ocp-server/currency"
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

				provider := runtimeCtx.Value(metrics.ProviderContextKey).(metrics.Provider)
				trace := provider.StartTrace("currency_exchange_rate_runtime")
				defer trace.End()
				tracedCtx := metrics.NewContext(runtimeCtx, trace)

				err := p.GetCurrentExchangeRates(tracedCtx)
				if err != nil {
					trace.OnError(err)
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

	delete(data.Rates, string(currency_lib.BTC))
	delete(data.Rates, string(currency_lib.BYR))
	delete(data.Rates, string(currency_lib.CLF))
	delete(data.Rates, string(currency_lib.CUC))
	delete(data.Rates, string(currency_lib.GGP))
	delete(data.Rates, string(currency_lib.IMP))
	delete(data.Rates, string(currency_lib.JEP))
	delete(data.Rates, string(currency_lib.LSL))
	delete(data.Rates, string(currency_lib.LTL))
	delete(data.Rates, string(currency_lib.LVL))
	delete(data.Rates, string(currency_lib.STD))
	delete(data.Rates, string(currency_lib.SVC))
	delete(data.Rates, string(currency_lib.XAU))
	delete(data.Rates, string(currency_lib.XAG))
	delete(data.Rates, string(currency_lib.XCD))
	delete(data.Rates, string(currency_lib.XDR))
	delete(data.Rates, string(currency_lib.ZMK))
	delete(data.Rates, string(currency_lib.ZWL))

	if err = p.data.ImportExchangeRates(ctx, data); err != nil {
		return errors.Wrap(err, "failed to store rate data")
	}

	return nil
}
