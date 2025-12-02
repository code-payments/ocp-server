package headers

import (
	"context"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// UnaryClientInterceptor sends all the headers in the current context to server
func UnaryClientInterceptor() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		ctx = setAllHeaders(ctx)
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

// StreamClientInterceptor sends all the headers in the current context to a server stream
func StreamClientInterceptor() grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		ctx = setAllHeaders(ctx)
		return streamer(ctx, desc, cc, method, opts...)
	}
}

// setAllHeaders Take all the headers currently in the context, except for the incoming Type,
// and put them into the metadata to be passed on to the next service
func setAllHeaders(ctx context.Context) context.Context {
	allHeader := Headers{}

	if rootHeader, ok := (ctx).Value(rootHeaderKey).(Headers); ok {
		allHeader.merge(rootHeader)
	}
	if propagatingHeader, ok := (ctx).Value(propagatingHeaderKey).(Headers); ok {
		allHeader.merge(propagatingHeader)
	}
	if outboundHeader, ok := (ctx).Value(outboundBinaryHeaderKey).(Headers); ok {
		allHeader.merge(outboundHeader)
	}
	if asciiHeader, ok := (ctx).Value(asciiHeaderKey).(Headers); ok {
		allHeader.merge(asciiHeader)
	}

	for k, v := range allHeader {
		switch strings.ToLower(k) {
		case "content-type", "user-agent", ":authority":
			continue
		}
		if val, ok := v.([]byte); ok {
			ctx = metadata.AppendToOutgoingContext(ctx, k, string(val))
		} else if val, ok := v.(string); ok {
			ctx = metadata.AppendToOutgoingContext(ctx, k, val)
		}
	}

	return ctx
}
