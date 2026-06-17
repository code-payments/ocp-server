package geyser

import (
	"context"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/code-payments/ocp-server/ocp/integration"
	"github.com/code-payments/ocp-server/ocp/worker"
	geyserpb "github.com/code-payments/ocp-server/ocp/worker/geyser/api/gen"

	ocp_data "github.com/code-payments/ocp-server/ocp/data"
	"github.com/code-payments/ocp-server/ocp/data/currency/exchange"
	"github.com/code-payments/ocp-server/ocp/data/currency/reserve"
)

type eventWorkerMetrics struct {
	active          bool
	eventsProcessed int
}

// todo: we can consolidate the various subscription streams into one
type runtime struct {
	log               *zap.Logger
	data              ocp_data.Provider
	exchangeRateStore exchange.Store
	reserveStore      reserve.Store
	conf              *conf

	integration integration.Geyser

	programUpdatesChan    chan *geyserpb.SubscribeUpdateAccount
	programUpdateHandlers map[string]ProgramAccountUpdateHandler

	metricStatusLock sync.RWMutex

	programUpdateSubscriptionStatus bool
	programUpdateWorkerMetrics      map[int]*eventWorkerMetrics

	slotUpdateSubscriptionStatus bool
	highestObservedFinalizedSlot uint64

	backupTimelockStateWorkerDuration *time.Duration
	backupTimelockStateWorkerStatus   bool
	backupTimelockProcessedCache      sync.Map // address -> time.Time

	backupExternalDepositWorkerStatus bool
}

func New(log *zap.Logger, data ocp_data.Provider, exchangeRateStore exchange.Store, reserveStore reserve.Store, integration integration.Geyser, configProvider ConfigProvider) worker.Runtime {
	conf := configProvider()
	return &runtime{
		log:                        log,
		data:                       data,
		exchangeRateStore:          exchangeRateStore,
		reserveStore:               reserveStore,
		conf:                       configProvider(),
		integration:                integration,
		programUpdatesChan:         make(chan *geyserpb.SubscribeUpdateAccount, conf.programUpdateQueueSize.Get(context.Background())),
		programUpdateHandlers:      initializeProgramAccountUpdateHandlers(conf, data, exchangeRateStore, reserveStore, integration),
		programUpdateWorkerMetrics: make(map[int]*eventWorkerMetrics),
	}
}

func (p *runtime) Start(ctx context.Context, _ time.Duration) error {
	// Start backup workers to catch missed events
	go func() {
		err := p.backupTimelockStateWorker(ctx, p.conf.backupTimelockWorkerInterval.Get(ctx))
		if err != nil && err != context.Canceled {
			p.log.With(zap.Error(err)).Warn("timelock backup worker terminated unexpectedly")
		}
	}()

	go func() {
		err := p.backupExternalDepositWorker(ctx, p.conf.backupExternalDepositWorkerInterval.Get(ctx))
		if err != nil && err != context.Canceled {
			p.log.With(zap.Error(err)).Warn("external deposit backup worker terminated unexpectedly")
		}
	}()

	// Setup event worker goroutines
	var wg sync.WaitGroup
	for i := 0; i < int(p.conf.programUpdateWorkerCount.Get(ctx)); i++ {
		wg.Add(1)
		go func(id int) {
			p.programUpdateWorker(ctx, id)
			wg.Done()
		}(i)
	}

	// Main event loops to consume updates from subscriptions to Geyser that
	// will be processed async
	go func() {
		err := p.consumeGeyserProgramUpdateEvents(ctx)
		if err != nil && err != context.Canceled {
			p.log.With(zap.Error(err)).Warn("geyser event consumer terminated unexpectedly")
		}
	}()
	go func() {
		err := p.consumeGeyserSlotUpdateEvents(ctx)
		if err != nil && err != context.Canceled {
			p.log.With(zap.Error(err)).Warn("geyser event consumer terminated unexpectedly")
		}
	}()

	// Start metrics gauge worker
	go func() {
		err := p.metricsGaugeWorker(ctx)
		if err != nil && err != context.Canceled {
			p.log.With(zap.Error(err)).Warn("metrics gauge loop terminated unexpectedly")
		}
	}()

	// Wait for the runtime to stop
	select {
	case <-ctx.Done():
	}

	// Gracefully shutdown
	close(p.programUpdatesChan)
	wg.Wait()

	return nil
}
