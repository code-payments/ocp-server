package currency

import (
	"context"
	"crypto/ed25519"
	"sync"
	"time"

	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonpb "github.com/code-payments/ocp-protobuf-api/generated/go/common/v1"
	currencypb "github.com/code-payments/ocp-protobuf-api/generated/go/currency/v1"
	"github.com/pkg/errors"

	"github.com/code-payments/ocp-server/ocp/auth"
	"github.com/code-payments/ocp-server/ocp/common"
	ocp_data "github.com/code-payments/ocp-server/ocp/data"
	"github.com/code-payments/ocp-server/ocp/data/currency"
)

// liveExchangeRateData represents live exchange rate data with its pre-signed response
type liveExchangeRateData struct {
	Rates          map[string]float64
	SignedResponse *currencypb.StreamLiveMintDataResponse
}

// liveReserveStateData represents live launchpad currency reserve state with its pre-signed response
type liveReserveStateData struct {
	Mint              *common.Account
	SupplyFromBonding uint64
	SignedState       *currencypb.VerifiedLaunchpadCurrencyReserveState
}

type liveMintStateWorker struct {
	log  *zap.Logger
	conf *conf
	data ocp_data.Provider

	stateMu           sync.RWMutex
	exchangeRates     *liveExchangeRateData
	launchpadReserves map[string]*liveReserveStateData

	streamsMu sync.RWMutex
	streams   map[string]*liveMintDataStream
	stopped   bool

	exchangeRatesReady     chan struct{}
	exchangeRatesReadyOnce sync.Once

	reserveReadyMu    sync.Mutex
	reserveReadyChans map[string]chan struct{}

	reservePollTrigger chan struct{}

	ctx    context.Context
	cancel context.CancelFunc
}

func newLiveMintStateWorker(log *zap.Logger, data ocp_data.Provider, conf *conf) *liveMintStateWorker {
	ctx, cancel := context.WithCancel(context.Background())
	return &liveMintStateWorker{
		log:                log,
		conf:               conf,
		data:               data,
		launchpadReserves:  make(map[string]*liveReserveStateData),
		streams:            make(map[string]*liveMintDataStream),
		exchangeRatesReady: make(chan struct{}),
		reserveReadyChans:  make(map[string]chan struct{}),
		reservePollTrigger: make(chan struct{}, 1),
		ctx:                ctx,
		cancel:             cancel,
	}
}

// start begins the polling goroutines for exchange rates and reserve state
func (m *liveMintStateWorker) start(ctx context.Context) error {
	go m.pollExchangeRates(ctx)
	go m.pollReserveState(ctx)
	return nil
}

// stop cancels the polling goroutines and closes all streams.
// After stop is called, no new streams can be registered.
func (m *liveMintStateWorker) stop() {
	m.cancel()

	m.streamsMu.Lock()
	defer m.streamsMu.Unlock()

	m.stopped = true

	for _, stream := range m.streams {
		stream.close()
	}
	m.streams = make(map[string]*liveMintDataStream)
}

// triggerReservePoll sends a non-blocking signal to the reserve poll loop
// to run immediately.
func (m *liveMintStateWorker) triggerReservePoll() {
	select {
	case m.reservePollTrigger <- struct{}{}:
	default:
		// Already triggered, no need to queue another
	}
}

// registerStream creates and registers a new stream for the given mints.
// Returns nil if the worker has been stopped.
func (m *liveMintStateWorker) registerStream(id string, mints []*common.Account) *liveMintDataStream {
	m.streamsMu.Lock()
	defer m.streamsMu.Unlock()

	if m.stopped {
		return nil
	}

	stream := newLiveMintDataStream(id, mints, streamBufferSize)
	m.streams[id] = stream

	return stream
}

// unregisterStream removes a stream and closes it
func (m *liveMintStateWorker) unregisterStream(id string) {
	m.streamsMu.Lock()
	stream, ok := m.streams[id]
	if ok {
		delete(m.streams, id)
	}
	m.streamsMu.Unlock()

	if stream != nil {
		stream.close()
	}
}

// waitForExchangeRates blocks until exchange rate data is available or context is cancelled
func (m *liveMintStateWorker) waitForExchangeRates(ctx context.Context) error {
	select {
	case <-m.exchangeRatesReady:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// waitForReserveState blocks until reserve state data for a specific mint is
// available, the context is cancelled, or the timeout is exceeded. Triggers
// an immediate poll if the mint isn't cached yet.
func (m *liveMintStateWorker) waitForReserveState(ctx context.Context, mint *common.Account, timeout time.Duration) error {
	ch := m.getOrCreateReserveReadyChan(mint)

	select {
	case <-ch:
		return nil
	default:
		m.triggerReservePoll()
	}

	select {
	case <-ch:
		return nil
	case <-time.After(timeout):
		return errors.New("timed out waiting for reserve state")
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *liveMintStateWorker) getOrCreateReserveReadyChan(mint *common.Account) chan struct{} {
	m.reserveReadyMu.Lock()
	defer m.reserveReadyMu.Unlock()

	ch, ok := m.reserveReadyChans[mint.PublicKey().ToBase58()]
	if !ok {
		ch = make(chan struct{})
		m.reserveReadyChans[mint.PublicKey().ToBase58()] = ch
	}
	return ch
}

// getExchangeRates returns the current pre-signed exchange rate data
func (m *liveMintStateWorker) getExchangeRates() *liveExchangeRateData {
	m.stateMu.RLock()
	defer m.stateMu.RUnlock()

	return m.exchangeRates
}

// getReserveState returns a current pre-signed launchpad currency reserve state for a mint
func (m *liveMintStateWorker) getReserveState(mint *common.Account) (*liveReserveStateData, error) {
	m.stateMu.RLock()
	defer m.stateMu.RUnlock()

	data, ok := m.launchpadReserves[mint.PublicKey().ToBase58()]
	if !ok {
		return nil, errors.New("not found")
	}
	return data, nil
}

func (m *liveMintStateWorker) markExchangeRatesReady() {
	m.exchangeRatesReadyOnce.Do(func() {
		close(m.exchangeRatesReady)
	})
}

func (m *liveMintStateWorker) markReserveStateReady(mint *common.Account) {
	m.reserveReadyMu.Lock()
	defer m.reserveReadyMu.Unlock()

	ch, ok := m.reserveReadyChans[mint.PublicKey().ToBase58()]
	if !ok {
		ch = make(chan struct{})
		m.reserveReadyChans[mint.PublicKey().ToBase58()] = ch
	}

	select {
	case <-ch:
		// Already closed
	default:
		close(ch)
	}
}

func (m *liveMintStateWorker) pollExchangeRates(ctx context.Context) {
	log := m.log.With(zap.String("poller", "exchange_rates"))

	// Initial poll immediately
	m.fetchAndUpdateExchangeRates(ctx, log)

	ticker := time.NewTicker(m.conf.exchangeRatePollInterval.Get(ctx))
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-m.ctx.Done():
			return
		case <-ticker.C:
			m.fetchAndUpdateExchangeRates(ctx, log)
		}
	}
}

func (m *liveMintStateWorker) fetchAndUpdateExchangeRates(ctx context.Context, log *zap.Logger) {
	rates, err := m.data.GetAllExchangeRates(ctx, time.Now())
	if err != nil {
		log.With(zap.Error(err)).Warn("failed to fetch exchange rates")
		return
	}

	// Sign the exchange rates once when fetched
	signedResponse, err := m.signExchangeRates(rates)
	if err != nil {
		log.With(zap.Error(err)).Warn("failed to sign exchange rates")
		return
	}

	m.stateMu.Lock()
	m.exchangeRates = &liveExchangeRateData{
		Rates:          rates.Rates,
		SignedResponse: signedResponse,
	}
	m.stateMu.Unlock()

	m.notifyExchangeRates()
	m.markExchangeRatesReady()
}

func (m *liveMintStateWorker) pollReserveState(ctx context.Context) {
	// Initial poll immediately
	m.fetchAndUpdateReserveStates(ctx)

	ticker := time.NewTicker(m.conf.reserveStatePollInterval.Get(ctx))
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-m.ctx.Done():
			return
		case <-ticker.C:
			m.fetchAndUpdateReserveStates(ctx)
		case <-m.reservePollTrigger:
			m.fetchAndUpdateReserveStates(ctx)
		}
	}
}

func (m *liveMintStateWorker) fetchAndUpdateReserveStates(ctx context.Context) {
	liveReserves, err := m.data.GetAllLiveCurrencyReserves(ctx)
	if err == currency.ErrNotFound {
		return
	}
	if err != nil {
		m.log.With(zap.Error(err)).Warn("failed to fetch all live currency reserves")
		return
	}

	var updatedStates []*liveReserveStateData
	for mintAddr, record := range liveReserves {
		mint, err := common.NewAccountFromPublicKeyString(mintAddr)
		if err != nil {
			m.log.With(
				zap.Error(err),
				zap.String("mint", mintAddr),
			).Warn("failed to parse mint address")
			continue
		}

		signedState, err := m.signReserveState(mint, record.SupplyFromBonding, time.Now())
		if err != nil {
			m.log.With(
				zap.Error(err),
				zap.String("mint", mintAddr),
			).Warn("failed to sign reserve state")
			continue
		}

		stateData := &liveReserveStateData{
			Mint:              mint,
			SupplyFromBonding: record.SupplyFromBonding,
			SignedState:       signedState,
		}

		m.stateMu.Lock()
		m.launchpadReserves[mintAddr] = stateData
		m.stateMu.Unlock()

		m.markReserveStateReady(mint)
		updatedStates = append(updatedStates, stateData)
	}

	if len(updatedStates) > 0 {
		m.notifyReserveStates(updatedStates)
	}
}

func (m *liveMintStateWorker) notifyExchangeRates() {
	m.stateMu.RLock()
	data := m.exchangeRates
	m.stateMu.RUnlock()

	if data == nil {
		return
	}

	m.streamsMu.RLock()
	streams := make([]*liveMintDataStream, 0, len(m.streams))
	for _, stream := range m.streams {
		streams = append(streams, stream)
	}
	m.streamsMu.RUnlock()

	for _, stream := range streams {
		if stream.wantsExchangeRates() {
			if err := stream.notifyExchangeRates(data); err != nil {
				m.log.With(
					zap.Error(err),
					zap.String("stream_id", stream.id),
				).Debug("failed to notify stream of exchange rates")
			}
		}
	}
}

func (m *liveMintStateWorker) notifyReserveStates(states []*liveReserveStateData) {
	m.streamsMu.RLock()
	streams := make([]*liveMintDataStream, 0, len(m.streams))
	for _, stream := range m.streams {
		streams = append(streams, stream)
	}
	m.streamsMu.RUnlock()

	for _, stream := range streams {
		if err := stream.notifyReserveStates(states); err != nil {
			m.log.With(
				zap.Error(err),
				zap.String("stream_id", stream.id),
			).Debug("failed to notify stream of reserve states")
		}
	}
}

// signExchangeRates creates a pre-signed response for exchange rates.
func (m *liveMintStateWorker) signExchangeRates(rates *currency.MultiRateRecord) (*currencypb.StreamLiveMintDataResponse, error) {
	subsidizer := common.GetSubsidizer()

	now := time.Now()

	// Build and sign each exchange rate individually
	var verifiedRates []*currencypb.VerifiedCoreMintFiatExchangeRate
	for code, rate := range rates.Rates {
		exchangeRate := &currencypb.CoreMintFiatExchangeRate{
			CurrencyCode: code,
			ExchangeRate: rate,
			Timestamp:    timestamppb.New(now),
		}

		// Sign the individual exchange rate
		messageBytes, err := auth.ForceConsistentMarshal(exchangeRate)
		if err != nil {
			return nil, err
		}
		signature := ed25519.Sign(subsidizer.PrivateKey().ToBytes(), messageBytes)

		verifiedRates = append(verifiedRates, &currencypb.VerifiedCoreMintFiatExchangeRate{
			ExchangeRate: exchangeRate,
			Signature:    &commonpb.Signature{Value: signature},
		})
	}

	return &currencypb.StreamLiveMintDataResponse{
		Type: &currencypb.StreamLiveMintDataResponse_Data{
			Data: &currencypb.StreamLiveMintDataResponse_LiveData{
				Type: &currencypb.StreamLiveMintDataResponse_LiveData_CoreMintFiatExchangeRates{
					CoreMintFiatExchangeRates: &currencypb.VerifiedCoreMintFiatExchangeRateBatch{
						ExchangeRates: verifiedRates,
					},
				},
			},
		},
	}, nil
}

// signReserveState creates a pre-signed verified state for a reserve state.
func (m *liveMintStateWorker) signReserveState(mint *common.Account, supplyFromBonding uint64, ts time.Time) (*currencypb.VerifiedLaunchpadCurrencyReserveState, error) {
	reserveState := &currencypb.LaunchpadCurrencyReserveState{
		Mint:              mint.ToProto(),
		SupplyFromBonding: supplyFromBonding,
		Timestamp:         timestamppb.New(ts),
	}

	subsidizer := common.GetSubsidizer()
	messageBytes, err := auth.ForceConsistentMarshal(reserveState)
	if err != nil {
		return nil, err
	}
	signature := ed25519.Sign(subsidizer.PrivateKey().ToBytes(), messageBytes)

	return &currencypb.VerifiedLaunchpadCurrencyReserveState{
		ReserveState: reserveState,
		Signature:    &commonpb.Signature{Value: signature},
	}, nil
}
