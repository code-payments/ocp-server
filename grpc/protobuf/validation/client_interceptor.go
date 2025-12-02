package validation

import (
	"context"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// UnaryClientInterceptor returns a grpc.UnaryClientInterceptor that validates
// inbound and outbound messages. If a service request is invalid, a
// codes.InvalidArgument is returned. If a service response is invalid, a
// codes.Internal is returned.
func UnaryClientInterceptor(log *zap.Logger) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, resp interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		// Validate request
		if v, ok := req.(Validator); ok {
			if err := v.Validate(); err != nil {
				// Log warn since the caller is at fault.
				log.With(zap.Error(err), zap.Any("req", req)).Warn("dropping invalid request")
				return status.Error(codes.InvalidArgument, err.Error())
			}
		}

		// Do service call
		if err := invoker(ctx, method, req, resp, cc, opts...); err != nil {
			return err
		}

		// Validate service response
		if v, ok := resp.(Validator); ok {
			if err := v.Validate(); err != nil {
				// Just log info here since the outbound service is mis-behaving.
				log.With(zap.Error(err), zap.Any("resp", resp)).Info("dropping invalid response")
				return status.Error(codes.Internal, err.Error())
			}
		}
		return nil
	}
}

// StreamClientInterceptor returns a grpc.StreamClientInterceptor that validates
// inbound and outbound messages. If any streamed service request is invalid, a
// codes.InvalidArgument is returned. If any streamed service response is invalid, a
// codes.Internal is returned.
func StreamClientInterceptor(log *zap.Logger) grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		clientStream, err := streamer(ctx, desc, cc, method, opts...)
		if err != nil {
			return nil, err
		}
		return &clientStreamWrapper{
			log:          log,
			ClientStream: clientStream,
		}, nil
	}
}

type clientStreamWrapper struct {
	log *zap.Logger

	grpc.ClientStream
}

func (c *clientStreamWrapper) SendMsg(req interface{}) error {
	// Validate request
	if v, ok := req.(Validator); ok {
		if err := v.Validate(); err != nil {
			// Log warn since the caller is at fault.
			c.log.With(zap.Error(err), zap.Any("req", req)).Warn("dropping invalid request")
			return status.Error(codes.InvalidArgument, err.Error())
		}
	}

	return c.ClientStream.SendMsg(req)
}

func (c *clientStreamWrapper) RecvMsg(resp interface{}) error {
	if err := c.ClientStream.RecvMsg(resp); err != nil {
		return err
	}

	// Validate service response
	if v, ok := resp.(Validator); ok {
		if err := v.Validate(); err != nil {
			// Just log info here since the outbound service is mis-behaving.
			c.log.With(zap.Error(err), zap.Any("resp", resp)).Info("dropping invalid response")
			return status.Error(codes.Internal, err.Error())
		}
	}
	return nil
}
