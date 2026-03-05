package currency

import (
	"bytes"
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
	currency_util "github.com/code-payments/ocp-server/ocp/currency"
	ocp_data "github.com/code-payments/ocp-server/ocp/data"
	"github.com/code-payments/ocp-server/ocp/data/currency"
)

var (
	errMintNotTracked   = errors.New("mint is not being tracked")
	errMintNotSupported = errors.New("mint is not supported")
)

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

	mintsMu      sync.RWMutex
	trackedMints map[string]*common.Account

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

	ctx    context.Context
	cancel context.CancelFunc
}

func newLiveMintStateWorker(log *zap.Logger, data ocp_data.Provider, conf *conf) *liveMintStateWorker {
	ctx, cancel := context.WithCancel(context.Background())
	return &liveMintStateWorker{
		log:                log,
		conf:               conf,
		data:               data,
		trackedMints:       make(map[string]*common.Account),
		launchpadReserves:  make(map[string]*liveReserveStateData),
		streams:            make(map[string]*liveMintDataStream),
		exchangeRatesReady: make(chan struct{}),
		reserveReadyChans:  make(map[string]chan struct{}),
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

// getTrackedMints returns the current set of dynamically tracked mints
func (m *liveMintStateWorker) getTrackedMints() map[string]*common.Account {
	m.mintsMu.RLock()
	defer m.mintsMu.RUnlock()

	result := make(map[string]*common.Account, len(m.trackedMints))
	for k, v := range m.trackedMints {
		result[k] = v
	}
	return result
}

// trackMints validates and adds mints to the tracked set. Only mints that
// pass IsSupportedMint validation are added. Core mint is excluded. Returns
// an error if any non-core mint is unsupported or cannot be validated.
func (m *liveMintStateWorker) trackMints(ctx context.Context, mints []*common.Account) error {
	for _, mint := range mints {
		if common.IsCoreMint(mint) {
			continue
		}

		mintAddr := mint.PublicKey().ToBase58()

		m.mintsMu.RLock()
		_, alreadyTracked := m.trackedMints[mintAddr]
		m.mintsMu.RUnlock()

		if alreadyTracked {
			continue
		}

		isSupported, err := common.IsSupportedMint(ctx, m.data, mint)
		if err != nil {
			return errors.Wrapf(err, "failed to validate mint %s", mintAddr)
		}
		if !isSupported {
			return errMintNotSupported
		}

		m.mintsMu.Lock()
		m.trackedMints[mintAddr] = mint
		m.mintsMu.Unlock()

		m.log.With(zap.String("mint", mintAddr)).Debug("tracking new mint from client request")

		go m.fetchAndUpdateReserveState(ctx, mint)
	}
	return nil
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
// available or context is cancelled. Returns ErrMintNotTracked immediately if
// the mint is not in the tracked set.
func (m *liveMintStateWorker) waitForReserveState(ctx context.Context, mint *common.Account) error {
	if !m.isTrackedMint(mint) {
		return errMintNotTracked
	}

	ch := m.getOrCreateReserveReadyChan(mint)
	select {
	case <-ch:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *liveMintStateWorker) isTrackedMint(mint *common.Account) bool {
	m.mintsMu.RLock()
	defer m.mintsMu.RUnlock()

	_, ok := m.trackedMints[mint.PublicKey().ToBase58()]
	return ok
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

	for _, data := range m.launchpadReserves {
		if bytes.Equal(mint.PublicKey().ToBytes(), data.Mint.PublicKey().ToBytes()) {
			return data, nil
		}
	}
	return nil, errors.New("not found")
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
		Timestamp:      time.Now(),
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
		}
	}
}

// fetchAndUpdateReserveState fetches and updates the reserve state for a single mint.
// Returns the updated state data, or nil if the fetch failed.
func (m *liveMintStateWorker) fetchAndUpdateReserveState(ctx context.Context, mint *common.Account) *liveReserveStateData {
	mintAddr := mint.PublicKey().ToBase58()

	supply, ts, err := currency_util.GetLaunchpadCurrencyCirculatingSupply(ctx, m.data, mint)
	if err != nil {
		m.log.With(
			zap.Error(err),
			zap.String("mint", mintAddr),
		).Warn("failed to fetch launchpad currency circulating supply")
		return nil
	}

	signedState, err := m.signReserveState(mint, supply, ts)
	if err != nil {
		m.log.With(
			zap.Error(err),
			zap.String("mint", mintAddr),
		).Warn("failed to sign reserve state")
		return nil
	}

	m.markReserveStateReady(mint)

	stateData := &liveReserveStateData{
		Mint:              mint,
		SupplyFromBonding: supply,
		Timestamp:         ts,
		SignedState:       signedState,
	}

	m.stateMu.Lock()
	m.launchpadReserves[mintAddr] = stateData
	m.stateMu.Unlock()

	return stateData
}

func (m *liveMintStateWorker) fetchAndUpdateReserveStates(ctx context.Context) {
	trackedMints := m.getTrackedMints()

	var mu sync.Mutex
	var updatedStates []*liveReserveStateData

	var wg sync.WaitGroup
	wg.Add(len(trackedMints))
	for _, mint := range trackedMints {
		go func(mint *common.Account) {
			defer wg.Done()

			if stateData := m.fetchAndUpdateReserveState(ctx, mint); stateData != nil {
				mu.Lock()
				updatedStates = append(updatedStates, stateData)
				mu.Unlock()
			}
		}(mint)
	}
	wg.Wait()

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
