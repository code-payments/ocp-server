package validation

import (
	"context"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Validator interface {
	Validate() error
}

// UnaryServerInterceptor returns a grpc.UnaryServerInterceptor that validates
// inbound and outbound messages. If an inbound message is invalid, a
// codes.InvalidArgument is returned. If an outbound message is invalid, a
// codes.Internal is returned.
func UnaryServerInterceptor(log *zap.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		if v, ok := req.(Validator); ok {
			if err := v.Validate(); err != nil {
				// We use an info level here because it is outside of 'our' control.
				log.With(zap.Error(err), zap.Any("req", req)).Info("dropping invalid request")
				return nil, status.Error(codes.InvalidArgument, err.Error())
			}
		}

		resp, err := handler(ctx, req)
		if err != nil {
			return nil, err
		}

		if v, ok := resp.(Validator); ok {
			if err := v.Validate(); err != nil {
				// We warn here because this indicates a problem with 'our' service.
				log.With(zap.Error(err), zap.Any("resp", resp)).Warn("dropping invalid response")
				return nil, status.Error(codes.Internal, err.Error())
			}
		}

		return resp, err
	}
}

// StreamServerInterceptor returns a grpc.StreamServerInterceptor that validates
// inbound and outbound messages. If an inbound message is invalid, a
// codes.InvalidArgument is returned. If an outbound message is invalid, a
// codes.Internal is returned.
func StreamServerInterceptor(log *zap.Logger) grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		return handler(srv, &serverStreamWrapper{log, ss})
	}
}

type serverStreamWrapper struct {
	log *zap.Logger

	grpc.ServerStream
}

func (s *serverStreamWrapper) RecvMsg(req interface{}) error {
	if err := s.ServerStream.RecvMsg(req); err != nil {
		return err
	}

	if v, ok := req.(Validator); ok {
		if err := v.Validate(); err != nil {
			s.log.With(zap.Error(err), zap.Any("req", req)).Info("dropping invalid request")
			return status.Error(codes.InvalidArgument, err.Error())
		}
	}

	return nil
}

func (s *serverStreamWrapper) SendMsg(resp interface{}) error {
	if v, ok := resp.(Validator); ok {
		if err := v.Validate(); err != nil {
			s.log.With(zap.Error(err), zap.Any("resp", resp)).Warn("dropping invalid response")
			return status.Error(codes.Internal, err.Error())
		}
	}

	return s.ServerStream.SendMsg(resp)
}
