package async_geyser

import (
	"context"
	"time"

	"github.com/mr-tron/base58"
	"github.com/newrelic/go-agent/v3/newrelic"
	"github.com/pkg/errors"
	"go.uber.org/zap"

	"github.com/code-payments/ocp-server/pkg/code/common"
	"github.com/code-payments/ocp-server/pkg/metrics"
)

func (p *service) consumeGeyserProgramUpdateEvents(ctx context.Context) error {
	log := p.log.With(zap.String("method", "consumeGeyserProgramUpdateEvents"))

	for {
		// Is the service stopped?
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		err := p.subscribeToProgramUpdatesFromGeyser(ctx, p.conf.grpcPluginEndpoint.Get(ctx), p.conf.grpcPluginXToken.Get(ctx))
		if err != nil && !errors.Is(err, context.Canceled) {
			log.With(zap.Error(err)).Warn("program update consumer unexpectedly terminated")
		}

		// Avoid spamming new connections when something is wrong
		time.Sleep(time.Second)
	}
}

func (p *service) consumeGeyserSlotUpdateEvents(ctx context.Context) error {
	log := p.log.With(zap.String("method", "consumeGeyserSlotUpdateEvents"))

	for {
		// Is the service stopped?
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		err := p.subscribeToSlotUpdatesFromGeyser(ctx, p.conf.grpcPluginEndpoint.Get(ctx), p.conf.grpcPluginXToken.Get(ctx))
		if err != nil && !errors.Is(err, context.Canceled) {
			log.With(zap.Error(err)).Warn("slot update consumer unexpectedly terminated")
		}

		// Avoid spamming new connections when something is wrong
		time.Sleep(time.Second)
	}
}

func (p *service) programUpdateWorker(serviceCtx context.Context, id int) {
	p.metricStatusLock.Lock()
	_, ok := p.programUpdateWorkerMetrics[id]
	if !ok {
		p.programUpdateWorkerMetrics[id] = &eventWorkerMetrics{}
	}
	p.programUpdateWorkerMetrics[id].active = false
	p.metricStatusLock.Unlock()

	log := p.log.With(
		zap.String("method", "programUpdateWorker"),
		zap.Int("worker_id", id),
	)

	log.Debug("worker started")

	defer func() {
		log.Debug("worker stopped")
	}()

	for update := range p.programUpdatesChan {
		func() {
			nr := serviceCtx.Value(metrics.NewRelicContextKey).(*newrelic.Application)
			m := nr.StartTransaction("async__geyser_consumer_service__program_update_worker")
			defer m.End()
			tracedCtx := newrelic.NewContext(serviceCtx, m)

			p.metricStatusLock.Lock()
			p.programUpdateWorkerMetrics[id].active = true
			p.metricStatusLock.Unlock()
			defer func() {
				p.metricStatusLock.Lock()
				p.programUpdateWorkerMetrics[id].active = false
				p.metricStatusLock.Unlock()
			}()

			publicKey, err := common.NewAccountFromPublicKeyBytes(update.Account.Pubkey)
			if err != nil {
				log.With(zap.Error(err)).Warn("invalid public key")
				return
			}

			program, err := common.NewAccountFromPublicKeyBytes(update.Account.Owner)
			if err != nil {
				log.With(zap.Error(err)).Warn("invalid owner account")
				return
			}

			log := log.With(
				zap.String("program", program.PublicKey().ToBase58()),
				zap.String("account", publicKey.PublicKey().ToBase58()),
				zap.Uint64("slot", update.Slot),
			)
			if update.Account.TxnSignature != nil {
				log = log.With(zap.String("transaction", base58.Encode(update.Account.TxnSignature)))
			}

			handler, ok := p.programUpdateHandlers[program.PublicKey().ToBase58()]
			if !ok {
				log.Debug("not handling update from program")
				return
			}

			err = handler.Handle(tracedCtx, update)
			if err != nil {
				m.NoticeError(err)
				log.With(zap.Error(err)).Warn("failed to process program account update")
			}

			p.metricStatusLock.Lock()
			p.programUpdateWorkerMetrics[id].eventsProcessed += 1
			p.metricStatusLock.Unlock()
		}()
	}
}
