package currency

import (
	"sync"

	"github.com/pkg/errors"

	"github.com/code-payments/ocp-server/ocp/common"
)

const (
	streamBufferSize = 100
)

// StreamUpdate represents an update to send to streams
type StreamUpdate struct {
	ExchangeRates *LiveExchangeRateData
	ReserveStates []*LiveReserveStateData
}

type LiveMintDataStream struct {
	sync.Mutex

	id       string
	mints    map[string]struct{} // mints this stream subscribes to
	streamCh chan *StreamUpdate
	closed   bool
}

func newLiveMintDataStream(id string, mints []*common.Account, bufferSize int) *LiveMintDataStream {
	mintSet := make(map[string]struct{}, len(mints))
	for _, mint := range mints {
		mintSet[mint.PublicKey().ToBase58()] = struct{}{}
	}

	return &LiveMintDataStream{
		id:       id,
		mints:    mintSet,
		streamCh: make(chan *StreamUpdate, bufferSize),
	}
}

func (s *LiveMintDataStream) GetUpdateChannel() chan *StreamUpdate {
	return s.streamCh
}

func (s *LiveMintDataStream) notifyExchangeRates(data *LiveExchangeRateData) error {
	if len(data.SignedRates) == 0 {
		return errors.New("signed exchange rates missing")
	}
	update := &StreamUpdate{
		ExchangeRates: data,
	}
	return s.notify(update)
}

func (s *LiveMintDataStream) notifyReserveStates(states []*LiveReserveStateData) error {
	// Filter reserve states based on subscribed mints
	var filtered []*LiveReserveStateData
	for _, state := range states {
		if s.WantsMint(state.Mint.PublicKey().ToBase58()) {
			if state.SignedState == nil {
				return errors.New("reserve state missing pre-signed state")
			}
			filtered = append(filtered, state)
		}
	}

	// Only send if there are relevant updates
	if len(filtered) == 0 {
		return nil
	}

	update := &StreamUpdate{
		ReserveStates: filtered,
	}
	return s.notify(update)
}

func (s *LiveMintDataStream) notify(update *StreamUpdate) error {
	s.Lock()

	if s.closed {
		s.Unlock()
		return errors.New("cannot notify closed stream")
	}

	select {
	case s.streamCh <- update:
	default:
		s.Unlock()
		s.close()
		return errors.New("streamCh is full; closing stream that's falling behind")
	}

	s.Unlock()
	return nil
}

func (s *LiveMintDataStream) close() {
	s.Lock()
	defer s.Unlock()

	if s.closed {
		return
	}

	s.closed = true
	close(s.streamCh)
}

func (s *LiveMintDataStream) WantsMint(mint string) bool {
	_, ok := s.mints[mint]
	return ok
}

func (s *LiveMintDataStream) WantsExchangeRates() bool {
	// Exchange rates are only for core mint
	_, ok := s.mints[common.CoreMintAccount.PublicKey().ToBase58()]
	return ok
}
