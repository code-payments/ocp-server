package currency

import (
	"sync"
	"time"

	"github.com/pkg/errors"

	currencypb "github.com/code-payments/ocp-protobuf-api/generated/go/currency/v1"

	"github.com/code-payments/ocp-server/ocp/common"
)

// streamUpdate represents an update to send to streams (pre-signed)
type streamUpdate struct {
	// Pre-signed response ready to send directly
	response *currencypb.StreamLiveMintDataResponse
}

type liveMintDataStream struct {
	sync.Mutex

	id       string
	mints    map[string]struct{} // mints this stream subscribes to
	streamCh chan *streamUpdate
	closed   bool
}

func newLiveMintDataStream(id string, mints []*common.Account, bufferSize int) *liveMintDataStream {
	mintSet := make(map[string]struct{}, len(mints))
	for _, mint := range mints {
		mintSet[mint.PublicKey().ToBase58()] = struct{}{}
	}

	return &liveMintDataStream{
		id:       id,
		mints:    mintSet,
		streamCh: make(chan *streamUpdate, bufferSize),
	}
}

func (s *liveMintDataStream) notifyExchangeRates(data *liveExchangeRateData, timeout time.Duration) error {
	if data.SignedResponse == nil {
		return errors.New("exchange rates missing pre-signed response")
	}
	update := &streamUpdate{
		response: data.SignedResponse,
	}
	return s.notify(update, timeout)
}

func (s *liveMintDataStream) notifyReserveStates(states []*liveReserveStateData, timeout time.Duration) error {
	// Filter reserve states based on subscribed mints
	var filtered []*currencypb.VerifiedLaunchpadCurrencyReserveState
	for _, state := range states {
		if s.wantsMint(state.Mint.PublicKey().ToBase58()) {
			if state.SignedState == nil {
				return errors.New("reserve state missing pre-signed state")
			}
			filtered = append(filtered, state.SignedState)
		}
	}

	// Only send if there are relevant updates
	if len(filtered) == 0 {
		return nil
	}

	// Build the response with the filtered pre-signed states
	response := &currencypb.StreamLiveMintDataResponse{
		Type: &currencypb.StreamLiveMintDataResponse_Data{
			Data: &currencypb.StreamLiveMintDataResponse_LiveData{
				Type: &currencypb.StreamLiveMintDataResponse_LiveData_LaunchpadCurrencyReserveStates{
					LaunchpadCurrencyReserveStates: &currencypb.VerifiedLaunchapdCurrencyReserveStateBatch{
						ReserveStates: filtered,
					},
				},
			},
		},
	}

	update := &streamUpdate{
		response: response,
	}
	return s.notify(update, timeout)
}

func (s *liveMintDataStream) notify(update *streamUpdate, timeout time.Duration) error {
	s.Lock()

	if s.closed {
		s.Unlock()
		return errors.New("cannot notify closed stream")
	}

	select {
	case s.streamCh <- update:
	case <-time.After(timeout):
		s.Unlock()
		s.close()
		return errors.New("timed out sending data to streamCh")
	}

	s.Unlock()
	return nil
}

func (s *liveMintDataStream) close() {
	s.Lock()
	defer s.Unlock()

	if s.closed {
		return
	}

	s.closed = true
	close(s.streamCh)
}

func (s *liveMintDataStream) wantsMint(mint string) bool {
	_, ok := s.mints[mint]
	return ok
}

func (s *liveMintDataStream) wantsExchangeRates() bool {
	// Exchange rates are only for core mint
	_, ok := s.mints[common.CoreMintAccount.PublicKey().ToBase58()]
	return ok
}
