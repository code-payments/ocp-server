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

var errMintNotTracked = errors.New("mint is not being tracked")

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
		launchpadReserves:  make(map[string]*liveReserveStateData),
		streams:            make(map[string]*liveMintDataStream),
		exchangeRatesReady: make(chan struct{}),
		reserveReadyChans:  make(map[string]chan struct{}),
		ctx:                ctx,
		cancel:             cancel,
	}
}

// start begins the polling goroutines for mints, exchange rates, and reserve state
func (m *liveMintStateWorker) start(ctx context.Context) error {
	go m.pollMints(ctx)
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

func (m *liveMintStateWorker) pollMints(ctx context.Context) {
	log := m.log.With(zap.String("poller", "mints"))

	// Initial poll immediately
	m.fetchAndUpdateMints(ctx, log)

	ticker := time.NewTicker(m.conf.mintPollInterval.Get(ctx))
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-m.ctx.Done():
			return
		case <-ticker.C:
			m.fetchAndUpdateMints(ctx, log)
		}
	}
}

func (m *liveMintStateWorker) fetchAndUpdateMints(ctx context.Context, log *zap.Logger) {
	mintStrings, err := m.data.GetAllCurrencyMints(ctx)
	if err != nil {
		log.With(zap.Error(err)).Warn("failed to fetch all mints")
		return
	}

	mints := make(map[string]*common.Account, len(mintStrings))
	for _, mintStr := range mintStrings {
		account, err := common.NewAccountFromPublicKeyString(mintStr)
		if err != nil {
			log.With(zap.Error(err), zap.String("mint", mintStr)).Warn("failed to parse mint public key")
			continue
		}
		if common.IsCoreMint(account) {
			continue
		}
		mints[mintStr] = account
	}

	m.mintsMu.Lock()
	m.trackedMints = mints
	m.mintsMu.Unlock()

	log.With(zap.Int("count", len(mints))).Debug("updated tracked mints")
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
		Timestamp:      rates.Time,
		SignedResponse: signedResponse,
	}
	m.stateMu.Unlock()

	m.notifyExchangeRates()
	m.markExchangeRatesReady()
}

func (m *liveMintStateWorker) pollReserveState(ctx context.Context) {
	log := m.log.With(zap.String("poller", "reserve_state"))

	// Initial poll immediately
	m.fetchAndUpdateReserveStates(ctx, log)

	ticker := time.NewTicker(m.conf.reserveStatePollInterval.Get(ctx))
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-m.ctx.Done():
			return
		case <-ticker.C:
			m.fetchAndUpdateReserveStates(ctx, log)
		}
	}
}

func (m *liveMintStateWorker) fetchAndUpdateReserveStates(ctx context.Context, log *zap.Logger) {
	trackedMints := m.getTrackedMints()

	var mu sync.Mutex
	var updatedStates []*liveReserveStateData

	var wg sync.WaitGroup
	wg.Add(len(trackedMints))
	for mintAddr, mint := range trackedMints {
		go func(mintAddr string, mint *common.Account) {
			defer wg.Done()

			supply, ts, err := currency_util.GetLaunchpadCurrencyCirculatingSupply(ctx, m.data, mint)
			if err != nil {
				log.With(
					zap.Error(err),
					zap.String("mint", mintAddr),
				).Warn("failed to fetch launchpad currency circulating supply")
				return
			}

			signedState, err := m.signReserveState(mint, supply, ts)
			if err != nil {
				log.With(
					zap.Error(err),
					zap.String("mint", mintAddr),
				).Warn("failed to sign reserve state")
				return
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

			mu.Lock()
			updatedStates = append(updatedStates, stateData)
			mu.Unlock()
		}(mintAddr, mint)
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
