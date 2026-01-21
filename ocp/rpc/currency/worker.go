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

	"github.com/code-payments/ocp-server/ocp/auth"
	"github.com/code-payments/ocp-server/ocp/common"
	"github.com/code-payments/ocp-server/ocp/config"
	currency_util "github.com/code-payments/ocp-server/ocp/currency"
	ocp_data "github.com/code-payments/ocp-server/ocp/data"
	"github.com/code-payments/ocp-server/ocp/data/currency"
)

var (
	jeffyMintAccount, _ = common.NewAccountFromPublicKeyString(config.JeffyMintPublicKey)
)

// trackedLaunchpadMints is the hardcoded set of launchpad mints to track
// (excludes core mint as it only has exchange rate data)
var trackedLaunchpadMints = []*common.Account{
	jeffyMintAccount,
}

// liveExchangeRateData represents live exchange rate data with its pre-signed response
type liveExchangeRateData struct {
	Rates          map[string]float64
	Timestamp      time.Time
	SignedResponse *currencypb.StreamLiveMintDataResponse
}

// liveReserveStateData represents live launchpad currency reserve state with its pre-signed response
type liveReserveStateData struct {
	Mint              *common.Account
	SupplyFromBonding uint64
	Timestamp         time.Time
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

	dataReady     chan struct{} // closed when initial data is loaded
	dataReadyOnce sync.Once

	// Track initial load completion
	initMu                   sync.Mutex
	exchangeRatesLoaded      bool
	reserveStatesLoadedMints map[string]struct{}

	ctx    context.Context
	cancel context.CancelFunc
}

func newLiveMintStateWorker(log *zap.Logger, data ocp_data.Provider, conf *conf) *liveMintStateWorker {
	ctx, cancel := context.WithCancel(context.Background())
	return &liveMintStateWorker{
		log:                      log,
		conf:                     conf,
		data:                     data,
		launchpadReserves:        make(map[string]*liveReserveStateData),
		streams:                  make(map[string]*liveMintDataStream),
		dataReady:                make(chan struct{}),
		reserveStatesLoadedMints: make(map[string]struct{}),
		ctx:                      ctx,
		cancel:                   cancel,
	}
}

// start begins the polling goroutines for exchange rates and reserve state
func (m *liveMintStateWorker) start(ctx context.Context) error {
	go m.pollExchangeRates(ctx)
	go m.pollReserveState(ctx)
	return nil
}

// stop cancels the polling goroutines and closes all streams
func (m *liveMintStateWorker) stop() {
	m.cancel()

	m.streamsMu.Lock()
	defer m.streamsMu.Unlock()

	for _, stream := range m.streams {
		stream.close()
	}
	m.streams = make(map[string]*liveMintDataStream)
}

// registerStream creates and registers a new stream for the given mints
func (m *liveMintStateWorker) registerStream(id string, mints []*common.Account) *liveMintDataStream {
	stream := newLiveMintDataStream(id, mints, streamBufferSize)

	m.streamsMu.Lock()
	m.streams[id] = stream
	m.streamsMu.Unlock()

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

// WaitForData blocks until initial data is loaded or context is cancelled
func (m *liveMintStateWorker) waitForData(ctx context.Context) error {
	select {
	case <-m.dataReady:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// GetExchangeRates returns the current pre-signed exchange rate data
func (m *liveMintStateWorker) getExchangeRates() *liveExchangeRateData {
	m.stateMu.RLock()
	defer m.stateMu.RUnlock()

	return m.exchangeRates
}

// GetReserveStates returns all current pre-signed launchpad currency reserve states
func (m *liveMintStateWorker) getReserveStates() []*liveReserveStateData {
	m.stateMu.RLock()
	defer m.stateMu.RUnlock()

	result := make([]*liveReserveStateData, 0, len(m.launchpadReserves))
	for _, data := range m.launchpadReserves {
		result = append(result, data)
	}
	return result
}

func (m *liveMintStateWorker) tryMarkDataReady() {
	m.initMu.Lock()
	defer m.initMu.Unlock()

	// Check if all data has been loaded
	if !m.exchangeRatesLoaded {
		return
	}
	if len(m.reserveStatesLoadedMints) < len(trackedLaunchpadMints) {
		return
	}

	m.dataReadyOnce.Do(func() {
		close(m.dataReady)
	})
}

func (m *liveMintStateWorker) markExchangeRatesLoaded() {
	m.initMu.Lock()
	m.exchangeRatesLoaded = true
	m.initMu.Unlock()
	m.tryMarkDataReady()
}

func (m *liveMintStateWorker) markReserveStateLoaded(mint string) {
	m.initMu.Lock()
	m.reserveStatesLoadedMints[mint] = struct{}{}
	m.initMu.Unlock()
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
		Timestamp:      rates.Time,
		SignedResponse: signedResponse,
	}
	m.stateMu.Unlock()

	m.notifyExchangeRates()
	m.markExchangeRatesLoaded()
}

func (m *liveMintStateWorker) pollReserveState(ctx context.Context) {
	log := m.log.With(zap.String("poller", "reserve_state"))

	// Initial poll immediately
	m.fetchAndUpdateReserveState(ctx, log)

	ticker := time.NewTicker(m.conf.reserveStatePollInterval.Get(ctx))
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-m.ctx.Done():
			return
		case <-ticker.C:
			m.fetchAndUpdateReserveState(ctx, log)
		}
	}
}

func (m *liveMintStateWorker) fetchAndUpdateReserveState(ctx context.Context, log *zap.Logger) {
	var updatedStates []*liveReserveStateData

	for _, mint := range trackedLaunchpadMints {
		mintAddr := mint.PublicKey().ToBase58()

		supply, ts, err := currency_util.GetLaunchpadCurrencyCirculatingSupply(ctx, m.data, mint)
		if err != nil {
			log.With(
				zap.Error(err),
				zap.String("mint", mintAddr),
			).Warn("failed to fetch launchpad currency circulating supply")
			continue
		}

		// Sign the reserve state once when fetched
		signedState, err := m.signReserveState(mint, supply, ts)
		if err != nil {
			log.With(
				zap.Error(err),
				zap.String("mint", mintAddr),
			).Warn("failed to sign reserve state")
			continue
		}

		// Track that this mint was loaded (before updating launchpadReserves)
		m.markReserveStateLoaded(mintAddr)

		stateData := &liveReserveStateData{
			Mint:              mint,
			SupplyFromBonding: supply,
			Timestamp:         ts,
			SignedState:       signedState,
		}

		m.stateMu.Lock()
		m.launchpadReserves[mintAddr] = stateData
		m.stateMu.Unlock()

		updatedStates = append(updatedStates, stateData)
	}

	if len(updatedStates) > 0 {
		m.notifyReserveStates(updatedStates)
	}

	// Try to mark data ready after all reserve states are processed
	m.tryMarkDataReady()
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
			if err := stream.notifyExchangeRates(data, streamNotifyTimeout); err != nil {
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
		if err := stream.notifyReserveStates(states, streamNotifyTimeout); err != nil {
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

	// Build and sign each exchange rate individually
	var verifiedRates []*currencypb.VerifiedCoreMintFiatExchangeRate
	for code, rate := range rates.Rates {
		exchangeRate := &currencypb.CoreMintFiatExchangeRate{
			CurrencyCode: code,
			ExchangeRate: rate,
			Timestamp:    timestamppb.New(rates.Time),
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
