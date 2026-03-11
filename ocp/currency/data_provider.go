package currency

import (
	"context"
	"crypto/ed25519"
	"strings"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonpb "github.com/code-payments/ocp-protobuf-api/generated/go/common/v1"
	currencypb "github.com/code-payments/ocp-protobuf-api/generated/go/currency/v1"

	"github.com/code-payments/ocp-server/ocp/auth"
	"github.com/code-payments/ocp-server/ocp/common"
	"github.com/code-payments/ocp-server/ocp/config"
	ocp_data "github.com/code-payments/ocp-server/ocp/data"
	"github.com/code-payments/ocp-server/ocp/data/currency"
	"github.com/code-payments/ocp-server/solana/currencycreator"
	timelock_token "github.com/code-payments/ocp-server/solana/timelock/v1"
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

type cachedProtoMint struct {
	mint          *currencypb.Mint
	lastUpdatedAt time.Time
}

type MintDataProvider struct {
	log  *zap.Logger
	data ocp_data.Provider

	protoMintCacheTTL        time.Duration
	exchangeRatePollInterval time.Duration
	reserveStatePollInterval time.Duration

	protoMintsCacheMu sync.RWMutex
	protoMintsCache   map[string]*cachedProtoMint

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

	ctx    context.Context
	cancel context.CancelFunc
}

func NewMintDataProvider(
	log *zap.Logger,
	data ocp_data.Provider,
	protoMintCacheTTL,
	exchangeRatePollInterval,
	reserveStatePollInterval time.Duration,
) *MintDataProvider {
	ctx, cancel := context.WithCancel(context.Background())
	return &MintDataProvider{
		log:                      log,
		data:                     data,
		protoMintCacheTTL:        protoMintCacheTTL,
		exchangeRatePollInterval: exchangeRatePollInterval,
		reserveStatePollInterval: reserveStatePollInterval,
		protoMintsCache:          make(map[string]*cachedProtoMint),
		launchpadReserves:        make(map[string]*LiveReserveStateData),
		streams:                  make(map[string]*LiveMintDataStream),
		exchangeRatesReady:       make(chan struct{}),
		reserveReadyChans:        make(map[string]chan struct{}),
		reservePollTrigger:       make(chan struct{}, 1),
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

// GetProtoMint gets a proto Mint object. Static and infrequently updated metadata is
// heavily cached.
func (m *MintDataProvider) GetProtoMint(ctx context.Context, mint *common.Account) (*currencypb.Mint, error) {
	if cached, ok := m.getCachedProtoMint(mint); ok {
		// Always overlay fresh circulating supply for launchpad currencies
		if cached.LaunchpadMetadata != nil {
			liveReserveState, err := m.GetLiveReserveState(ctx, mint)
			if err != nil {
				return nil, err
			}

			spotPrice, _ := currencycreator.EstimateCurrentPrice(liveReserveState.SupplyFromBonding).Float64()
			marketCap := CalculateMarketCap(liveReserveState.SupplyFromBonding, 1.0)
			cached.LaunchpadMetadata.SupplyFromBonding = liveReserveState.SupplyFromBonding
			cached.LaunchpadMetadata.Price = spotPrice
			cached.LaunchpadMetadata.MarketCap = marketCap
		}

		return cached, nil
	}

	var protoMetadata *currencypb.Mint
	switch mint.PublicKey().ToBase58() {
	case common.CoreMintAccount.PublicKey().ToBase58():
		vmConfig, err := common.GetVmConfigForMint(ctx, m.data, common.CoreMintAccount)
		if err != nil {
			return nil, err
		}

		protoMetadata = &currencypb.Mint{
			Address:     mint.ToProto(),
			Decimals:    uint32(common.CoreMintDecimals),
			Name:        common.CoreMintName,
			Symbol:      strings.ToUpper(string(common.CoreMintSymbol)),
			Description: config.CoreMintDescription,
			ImageUrl:    config.CoreMintImageUrl,
			VmMetadata: &currencypb.VmMetadata{
				Vm:                 vmConfig.Vm.ToProto(),
				Omnibus:            vmConfig.Omnibus.ToProto(),
				Authority:          vmConfig.Authority.ToProto(),
				LockDurationInDays: uint32(timelock_token.DefaultNumDaysLocked),
			},
			CreatedAt: timestamppb.New(time.Time{}),
		}
	default:
		metadataRecord, err := m.data.GetCurrencyMetadata(ctx, mint.PublicKey().ToBase58())
		if err == currency.ErrNotFound {
			return nil, err
		}
		if metadataRecord.State != currency.MetadataStateAvailable {
			return nil, currency.ErrNotFound
		}

		vmConfig, err := common.GetVmConfigForMint(ctx, m.data, mint)
		if err != nil {
			return nil, err
		}

		currencyAccounts, err := common.GetLaunchpadCurrencyAccounts(metadataRecord)
		if err != nil {
			return nil, err
		}

		seed, err := common.NewAccountFromPublicKeyString(metadataRecord.Seed)
		if err != nil {
			return nil, err
		}

		liveReserveState, err := m.GetLiveReserveState(ctx, mint)
		if err != nil {
			return nil, err
		}

		spotPrice, _ := currencycreator.EstimateCurrentPrice(liveReserveState.SupplyFromBonding).Float64()
		marketCap := CalculateMarketCap(liveReserveState.SupplyFromBonding, 1.0)

		protoMetadata = &currencypb.Mint{
			Address:     mint.ToProto(),
			Decimals:    uint32(metadataRecord.Decimals),
			Name:        metadataRecord.Name,
			Symbol:      metadataRecord.Symbol,
			Description: metadataRecord.Description,
			ImageUrl:    metadataRecord.ImageUrl,
			VmMetadata: &currencypb.VmMetadata{
				Vm:                 vmConfig.Vm.ToProto(),
				Omnibus:            vmConfig.Omnibus.ToProto(),
				Authority:          vmConfig.Authority.ToProto(),
				LockDurationInDays: uint32(timelock_token.DefaultNumDaysLocked),
			},
			LaunchpadMetadata: &currencypb.LaunchpadMetadata{
				CurrencyConfig:    currencyAccounts.CurrencyConfig.ToProto(),
				LiquidityPool:     currencyAccounts.LiquidityPool.ToProto(),
				Seed:              seed.ToProto(),
				Authority:         currencyAccounts.Authority.ToProto(),
				MintVault:         currencyAccounts.VaultMint.ToProto(),
				CoreMintVault:     currencyAccounts.VaultBase.ToProto(),
				SupplyFromBonding: liveReserveState.SupplyFromBonding,
				SellFeeBps:        uint32(metadataRecord.SellFeeBps),
				Price:             spotPrice,
				MarketCap:         marketCap,
			},
			CreatedAt: timestamppb.New(metadataRecord.CreatedAt),
		}

		billColors := metadataRecord.BillColors
		if len(billColors) == 0 {
			billColors = config.DefaultBillColors
		}
		var protoColors []*currencypb.Color
		for _, hex := range billColors {
			protoColors = append(protoColors, &currencypb.Color{Hex: hex})
		}
		protoMetadata.BillCustomization = &currencypb.BillCustomization{
			Colors: protoColors,
		}

		for _, link := range metadataRecord.SocialLinks {
			switch link.Type {
			case currency.SocialLinkTypeWebsite:
				protoMetadata.SocialLinks = append(protoMetadata.SocialLinks, &currencypb.SocialLink{
					Type: &currencypb.SocialLink_Website_{
						Website: &currencypb.SocialLink_Website{Url: link.Value},
					},
				})
			case currency.SocialLinkTypeX:
				protoMetadata.SocialLinks = append(protoMetadata.SocialLinks, &currencypb.SocialLink{
					Type: &currencypb.SocialLink_X_{
						X: &currencypb.SocialLink_X{Username: link.Value},
					},
				})
			case currency.SocialLinkTypeTelegram:
				protoMetadata.SocialLinks = append(protoMetadata.SocialLinks, &currencypb.SocialLink{
					Type: &currencypb.SocialLink_Telegram_{
						Telegram: &currencypb.SocialLink_Telegram{Username: link.Value},
					},
				})
			case currency.SocialLinkTypeDiscord:
				protoMetadata.SocialLinks = append(protoMetadata.SocialLinks, &currencypb.SocialLink{
					Type: &currencypb.SocialLink_Discord_{
						Discord: &currencypb.SocialLink_Discord{InviteCode: link.Value},
					},
				})
			}
		}
	}

	m.setCachedProtoMint(mint, protoMetadata)

	return protoMetadata, nil
}

func (m *MintDataProvider) getCachedProtoMint(mint *common.Account) (*currencypb.Mint, bool) {
	m.protoMintsCacheMu.RLock()
	defer m.protoMintsCacheMu.RUnlock()

	entry, ok := m.protoMintsCache[mint.PublicKey().ToBase58()]
	if !ok || time.Since(entry.lastUpdatedAt) >= m.protoMintCacheTTL {
		return nil, false
	}
	return proto.Clone(entry.mint).(*currencypb.Mint), true
}

func (m *MintDataProvider) setCachedProtoMint(mint *common.Account, protoMint *currencypb.Mint) {
	m.protoMintsCacheMu.Lock()
	defer m.protoMintsCacheMu.Unlock()

	m.protoMintsCache[mint.PublicKey().ToBase58()] = &cachedProtoMint{
		mint:          proto.Clone(protoMint).(*currencypb.Mint),
		lastUpdatedAt: time.Now(),
	}
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

// GetLiveExchangeRates returns the current pre-signed live exchange rate data
func (m *MintDataProvider) GetLiveExchangeRates(ctx context.Context) (*LiveExchangeRateData, error) {
	err := m.waitForExchangeRates(ctx)
	if err != nil {
		return nil, err
	}

	m.stateMu.RLock()
	defer m.stateMu.RUnlock()

	if m.exchangeRates == nil {
		return nil, errors.New("not found")
	}

	return m.exchangeRates, nil
}

// GetLiveReserveState returns a current pre-signed live launchpad currency reserve state for a mint
func (m *MintDataProvider) GetLiveReserveState(ctx context.Context, mint *common.Account) (*LiveReserveStateData, error) {
	isSupported, err := common.IsSupportedMint(ctx, m.data, mint)
	if err != nil {
		return nil, err
	}
	if !isSupported {
		return nil, common.ErrUnsupportedMint
	}

	err = m.waitForReserveState(ctx, mint)
	if err != nil {
		return nil, err
	}

	m.stateMu.RLock()
	defer m.stateMu.RUnlock()

	data, ok := m.launchpadReserves[mint.PublicKey().ToBase58()]
	if !ok {
		return nil, errors.New("not found")
	}
	return data, nil
}

// waitForExchangeRates blocks until exchange rate data is available or context is cancelled
func (m *MintDataProvider) waitForExchangeRates(ctx context.Context) error {
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
func (m *MintDataProvider) waitForReserveState(ctx context.Context, mint *common.Account) error {
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
	case <-time.After(2 * m.reserveStatePollInterval):
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
