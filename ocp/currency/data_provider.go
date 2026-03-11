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

// LiveExchangeRateData represents live exchange rate data with its pre-signed response
type LiveExchangeRateData struct {
	Rates       map[string]float64
	SignedRates []*currencypb.VerifiedCoreMintFiatExchangeRate
}

// LiveReserveStateData represents live launchpad currency reserve state with its pre-signed response
type LiveReserveStateData struct {
	Mint              *common.Account
	SupplyFromBonding uint64
	SignedState       *currencypb.VerifiedLaunchpadCurrencyReserveState
}

type MintDataProvider struct {
	log  *zap.Logger
	data ocp_data.Provider

	stateMu           sync.RWMutex
	exchangeRates     *LiveExchangeRateData
	launchpadReserves map[string]*LiveReserveStateData

	streamsMu sync.RWMutex
	streams   map[string]*LiveMintDataStream
	stopped   bool

	exchangeRatesReady     chan struct{}
	exchangeRatesReadyOnce sync.Once

	reserveReadyMu    sync.Mutex
	reserveReadyChans map[string]chan struct{}

	reservePollTrigger chan struct{}

	exchangeRatePollInterval time.Duration
	reserveStatePollInterval time.Duration

	ctx    context.Context
	cancel context.CancelFunc
}

func NewMintDataProvider(log *zap.Logger, data ocp_data.Provider, exchangeRatePollInterval, reserveStatePollInterval time.Duration) *MintDataProvider {
	ctx, cancel := context.WithCancel(context.Background())
	return &MintDataProvider{
		log:                      log,
		data:                     data,
		launchpadReserves:        make(map[string]*LiveReserveStateData),
		streams:                  make(map[string]*LiveMintDataStream),
		exchangeRatesReady:       make(chan struct{}),
		reserveReadyChans:        make(map[string]chan struct{}),
		reservePollTrigger:       make(chan struct{}, 1),
		exchangeRatePollInterval: exchangeRatePollInterval,
		reserveStatePollInterval: reserveStatePollInterval,
		ctx:                      ctx,
		cancel:                   cancel,
	}
}

// Start begins the polling goroutines for exchange rates and reserve state
func (m *MintDataProvider) Start(ctx context.Context) error {
	go m.pollExchangeRates(ctx)
	go m.pollReserveState(ctx)
	return nil
}

// stop cancels the polling goroutines and closes all streams.
// After stop is called, no new streams can be registered.
func (m *MintDataProvider) Stop() {
	m.cancel()

	m.streamsMu.Lock()
	defer m.streamsMu.Unlock()

	m.stopped = true

	for _, stream := range m.streams {
		stream.close()
	}
	m.streams = make(map[string]*LiveMintDataStream)
}

// RegisterStream creates and registers a new stream for the given mints.
// Returns nil if the worker has been stopped.
func (m *MintDataProvider) RegisterStream(id string, mints []*common.Account) *LiveMintDataStream {
	m.streamsMu.Lock()
	defer m.streamsMu.Unlock()

	if m.stopped {
		return nil
	}

	stream := newLiveMintDataStream(id, mints, streamBufferSize)
	m.streams[id] = stream

	return stream
}

// UnregisterStream removes a stream and closes it
func (m *MintDataProvider) UnregisterStream(id string) {
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

// WaitForExchangeRates blocks until exchange rate data is available or context is cancelled
func (m *MintDataProvider) WaitForExchangeRates(ctx context.Context) error {
	select {
	case <-m.exchangeRatesReady:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// WaitForReserveState blocks until reserve state data for a specific mint is
// available, the context is cancelled, or the timeout is exceeded. Triggers
// an immediate poll if the mint isn't cached yet.
func (m *MintDataProvider) WaitForReserveState(ctx context.Context, mint *common.Account, timeout time.Duration) error {
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

// triggerReservePoll sends a non-blocking signal to the reserve poll loop
// to run immediately.
func (m *MintDataProvider) triggerReservePoll() {
	select {
	case m.reservePollTrigger <- struct{}{}:
	default:
		// Already triggered, no need to queue another
	}
}

func (m *MintDataProvider) getOrCreateReserveReadyChan(mint *common.Account) chan struct{} {
	m.reserveReadyMu.Lock()
	defer m.reserveReadyMu.Unlock()

	ch, ok := m.reserveReadyChans[mint.PublicKey().ToBase58()]
	if !ok {
		ch = make(chan struct{})
		m.reserveReadyChans[mint.PublicKey().ToBase58()] = ch
	}
	return ch
}

// GetExchangeRates returns the current pre-signed exchange rate data
func (m *MintDataProvider) GetExchangeRates() *LiveExchangeRateData {
	m.stateMu.RLock()
	defer m.stateMu.RUnlock()

	return m.exchangeRates
}

// GetReserveState returns a current pre-signed launchpad currency reserve state for a mint
func (m *MintDataProvider) GetReserveState(mint *common.Account) (*LiveReserveStateData, error) {
	m.stateMu.RLock()
	defer m.stateMu.RUnlock()

	data, ok := m.launchpadReserves[mint.PublicKey().ToBase58()]
	if !ok {
		return nil, errors.New("not found")
	}
	return data, nil
}

func (m *MintDataProvider) markExchangeRatesReady() {
	m.exchangeRatesReadyOnce.Do(func() {
		close(m.exchangeRatesReady)
	})
}

func (m *MintDataProvider) markReserveStateReady(mint *common.Account) {
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

func (m *MintDataProvider) pollExchangeRates(ctx context.Context) {
	log := m.log.With(zap.String("poller", "exchange_rates"))

	// Initial poll immediately
	m.fetchAndUpdateExchangeRates(ctx, log)

	ticker := time.NewTicker(m.exchangeRatePollInterval)
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

func (m *MintDataProvider) fetchAndUpdateExchangeRates(ctx context.Context, log *zap.Logger) {
	rates, err := m.data.GetAllExchangeRates(ctx, time.Now())
	if err != nil {
		log.With(zap.Error(err)).Warn("failed to fetch exchange rates")
		return
	}

	// Sign the exchange rates once when fetched
	signedRates, err := m.signExchangeRates(rates)
	if err != nil {
		log.With(zap.Error(err)).Warn("failed to sign exchange rates")
		return
	}

	m.stateMu.Lock()
	m.exchangeRates = &LiveExchangeRateData{
		Rates:       rates.Rates,
		SignedRates: signedRates,
	}
	m.stateMu.Unlock()

	m.notifyExchangeRates()
	m.markExchangeRatesReady()
}

func (m *MintDataProvider) pollReserveState(ctx context.Context) {
	// Initial poll immediately
	m.fetchAndUpdateReserveStates(ctx)

	ticker := time.NewTicker(m.reserveStatePollInterval)
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

func (m *MintDataProvider) fetchAndUpdateReserveStates(ctx context.Context) {
	liveReserves, err := m.data.GetAllLiveCurrencyReserves(ctx)
	if err == currency.ErrNotFound {
		return
	}
	if err != nil {
		m.log.With(zap.Error(err)).Warn("failed to fetch all live currency reserves")
		return
	}

	var updatedStates []*LiveReserveStateData
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

		stateData := &LiveReserveStateData{
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

func (m *MintDataProvider) notifyExchangeRates() {
	m.stateMu.RLock()
	data := m.exchangeRates
	m.stateMu.RUnlock()

	if data == nil {
		return
	}

	m.streamsMu.RLock()
	streams := make([]*LiveMintDataStream, 0, len(m.streams))
	for _, stream := range m.streams {
		streams = append(streams, stream)
	}
	m.streamsMu.RUnlock()

	for _, stream := range streams {
		if stream.WantsExchangeRates() {
			if err := stream.notifyExchangeRates(data); err != nil {
				m.log.With(
					zap.Error(err),
					zap.String("stream_id", stream.id),
				).Debug("failed to notify stream of exchange rates")
			}
		}
	}
}

func (m *MintDataProvider) notifyReserveStates(states []*LiveReserveStateData) {
	m.streamsMu.RLock()
	streams := make([]*LiveMintDataStream, 0, len(m.streams))
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
func (m *MintDataProvider) signExchangeRates(rates *currency.MultiRateRecord) ([]*currencypb.VerifiedCoreMintFiatExchangeRate, error) {
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

	return verifiedRates, nil
}

// signReserveState creates a pre-signed verified state for a reserve state.
func (m *MintDataProvider) signReserveState(mint *common.Account, supplyFromBonding uint64, ts time.Time) (*currencypb.VerifiedLaunchpadCurrencyReserveState, error) {
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

func (m *MintDataProvider) GetExchangeRatePollInterval() time.Duration {
	return m.exchangeRatePollInterval
}

func (m *MintDataProvider) GetReserveStatePollInterval() time.Duration {
	return m.reserveStatePollInterval
}
