package currency

import (
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonpb "github.com/code-payments/ocp-protobuf-api/generated/go/common/v1"
	currencypb "github.com/code-payments/ocp-protobuf-api/generated/go/currency/v1"

	"github.com/code-payments/ocp-server/grpc/client"
	"github.com/code-payments/ocp-server/ocp/common"
	"github.com/code-payments/ocp-server/protoutil"
)

const (
	streamPingDelay          = 5 * time.Second
	streamInitialRecvTimeout = 250 * time.Millisecond
)

func (s *currencyServer) StreamLiveMintData(
	streamer currencypb.Currency_StreamLiveMintDataServer,
) error {
	ctx := streamer.Context()

	log := s.log.With(zap.String("method", "StreamLiveMintData"))
	log = client.InjectLoggingMetadata(ctx, log)

	// Wait for the initial request to get the list of mints
	req, err := protoutil.BoundedReceive[currencypb.StreamLiveMintDataRequest](ctx, streamer, streamInitialRecvTimeout)
	if err != nil {
		log.With(zap.Error(err)).Debug("error receiving initial request")
		return err
	}

	// Must be a request type
	request := req.GetRequest()
	if request == nil {
		return status.Error(codes.InvalidArgument, "first message must be a request")
	}

	// Parse requested mints
	var requestedMints []*common.Account
	for _, protoMint := range request.GetMints() {
		mint, err := common.NewAccountFromProto(protoMint)
		if err != nil {
			log.With(zap.Error(err)).Warn("invalid mint")
			return status.Error(codes.Internal, "")
		}
		requestedMints = append(requestedMints, mint)
	}

	// Track requested mints so the worker polls their reserve state
	if err := s.liveMintStateWorker.trackMints(ctx, requestedMints); err != nil {
		log.With(zap.Error(err)).Warn("failed to track requested mints")
		return status.Error(codes.Internal, "")
	}

	// Generate unique stream ID
	streamID := uuid.New().String()
	log = log.With(zap.String("stream_id", streamID))

	// Register stream with state worker
	stream := s.liveMintStateWorker.registerStream(streamID, requestedMints)
	defer s.liveMintStateWorker.unregisterStream(streamID)

	log.Debug("stream registered")

	// Wait for and flush initial exchange rates if the stream wants them
	if stream.wantsExchangeRates() {
		if err := s.liveMintStateWorker.waitForExchangeRates(ctx); err != nil {
			log.With(zap.Error(err)).Debug("context cancelled while waiting for exchange rates")
			return status.Error(codes.Canceled, "")
		}

		exchangeRates := s.liveMintStateWorker.getExchangeRates()
		if exchangeRates != nil && exchangeRates.SignedResponse != nil {
			if err := streamer.Send(exchangeRates.SignedResponse); err != nil {
				log.With(zap.Error(err)).Debug("failed to send initial exchange rates")
				return err
			}
		}
	}

	// Wait for and flush initial reserve states for each requested mint
	var filtered []*currencypb.VerifiedLaunchpadCurrencyReserveState
	for _, mint := range requestedMints {
		if common.IsCoreMint(mint) {
			continue
		}

		err := s.liveMintStateWorker.waitForReserveState(ctx, mint)
		if err != nil {
			log.With(zap.Error(err)).Debug("context cancelled while waiting for reserve state")
			return status.Error(codes.Canceled, "")
		}

		state, err := s.liveMintStateWorker.getReserveState(mint)
		if err != nil {
			continue
		}
		if state.SignedState != nil {
			filtered = append(filtered, state.SignedState)
		}
	}
	if len(filtered) > 0 {
		resp := &currencypb.StreamLiveMintDataResponse{
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
		if err := streamer.Send(resp); err != nil {
			log.With(zap.Error(err)).Debug("failed to send initial reserve states")
			return err
		}
	}

	log.Debug("initial flush complete")

	// Set up ping/pong health monitoring
	sendPingCh := time.After(0) // Send first ping immediately
	streamHealthCh := protoutil.MonitorStreamHealth(ctx, log, streamer, func(req *currencypb.StreamLiveMintDataRequest) bool {
		return req.GetPong() != nil
	})

	// Main loop: listen on stream channel and send updates
	for {
		select {
		case update, ok := <-stream.streamCh:
			if !ok {
				log.Debug("stream channel closed")
				return status.Error(codes.Aborted, "stream closed")
			}

			if update.response == nil {
				continue
			}

			if err := streamer.Send(update.response); err != nil {
				log.With(zap.Error(err)).Debug("failed to send update")
				return err
			}

		case <-sendPingCh:
			log.Debug("sending ping to client")
			sendPingCh = time.After(streamPingDelay)

			err := streamer.Send(&currencypb.StreamLiveMintDataResponse{
				Type: &currencypb.StreamLiveMintDataResponse_Ping{
					Ping: &commonpb.ServerPing{
						Timestamp: timestamppb.Now(),
						PingDelay: durationpb.New(streamPingDelay),
					},
				},
			})
			if err != nil {
				log.Debug("failed to send ping, stream is unhealthy")
				return status.Error(codes.Aborted, "terminating unhealthy stream")
			}

		case <-streamHealthCh:
			log.Debug("stream is unhealthy, terminating")
			return status.Error(codes.Aborted, "terminating unhealthy stream")

		case <-ctx.Done():
			log.Debug("context cancelled")
			return status.Error(codes.Canceled, "")
		}
	}
}
