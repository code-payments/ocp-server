package metrics

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	grpc_core "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/code-payments/ocp-server/grpc"
	"github.com/code-payments/ocp-server/grpc/client"
	"github.com/code-payments/ocp-server/metrics"
)

const (
	grpcRequestPackageAttributeKey = "grpc.request.package"
	grpcRequestServiceAttributeKey = "grpc.request.service"
	grpcRequestMethodAttributeKey  = "grpc.request.method"

	grpcResponseStatusCodeAttributeKey      = "grpc.response.statusCode"
	grpcResponseStatusMessageAttributeKey   = "grpc.response.statusMessage"
	grpcResponseStatusCodeLevelAttributeKey = "grpc.response.statusCodeLevel"

	resultCodeAttributeKey      = "code.response.resultCode"
	resultCodeLevelAttributeKey = "code.response.resultCodeLevel"

	clientUserAgentAttributeKey = "grpc.client.userAgent"

	infoLevel    = "info"
	warningLevel = "warning"
	errorLevel   = "error"
)

type traceStatusCodeHandler func(metrics.Trace, *status.Status)
type traceResultCodeHandler func(metrics.Trace, string)

var (
	traceStatusCodeHandlers = map[codes.Code]traceStatusCodeHandler{
		codes.OK:                 infoTraceStatusCodeHandler,
		codes.Aborted:            infoTraceStatusCodeHandler,
		codes.AlreadyExists:      infoTraceStatusCodeHandler,
		codes.Canceled:           infoTraceStatusCodeHandler,
		codes.DataLoss:           infoTraceStatusCodeHandler,
		codes.DeadlineExceeded:   infoTraceStatusCodeHandler,
		codes.FailedPrecondition: infoTraceStatusCodeHandler,
		codes.InvalidArgument:    infoTraceStatusCodeHandler,
		codes.NotFound:           infoTraceStatusCodeHandler,
		codes.OutOfRange:         infoTraceStatusCodeHandler,
		codes.PermissionDenied:   infoTraceStatusCodeHandler,
		codes.ResourceExhausted:  infoTraceStatusCodeHandler,
		codes.Unauthenticated:    infoTraceStatusCodeHandler,
		codes.Unimplemented:      infoTraceStatusCodeHandler,

		codes.Internal:    warningTraceStatusCodeHandler,
		codes.Unavailable: warningTraceStatusCodeHandler,
		codes.Unknown:     warningTraceStatusCodeHandler,
	}
	defaultTraceStatusCodeHandler = infoTraceStatusCodeHandler

	traceResultCodeHandlers = map[string]traceResultCodeHandler{
		"OK":        infoTraceResultCodeHandler,
		"NOT_FOUND": infoTraceResultCodeHandler,

		"DENIED": warningTraceResultCodeHandler,
	}
	defaultTraceResultCodeHandler = infoTraceResultCodeHandler
)

func infoTraceStatusCodeHandler(trace metrics.Trace, s *status.Status) {
	trace.SetResponse(nil).WriteHeader(int(s.Code()))
	trace.AddAttribute(grpcResponseStatusCodeAttributeKey, s.Code().String())
	trace.AddAttribute(grpcResponseStatusMessageAttributeKey, s.Message())
	trace.AddAttribute(grpcResponseStatusCodeLevelAttributeKey, infoLevel)
}

func warningTraceStatusCodeHandler(trace metrics.Trace, s *status.Status) {
	trace.SetResponse(nil).WriteHeader(int(s.Code()))
	trace.AddAttribute(grpcResponseStatusCodeAttributeKey, s.Code().String())
	trace.AddAttribute(grpcResponseStatusMessageAttributeKey, s.Message())
	trace.AddAttribute(grpcResponseStatusCodeLevelAttributeKey, warningLevel)
}

func errorTraceStatusCodeHandler(trace metrics.Trace, s *status.Status) {
	trace.SetResponse(nil).WriteHeader(int(s.Code()))
	trace.AddAttribute(grpcResponseStatusCodeAttributeKey, s.Code().String())
	trace.AddAttribute(grpcResponseStatusMessageAttributeKey, s.Message())
	trace.AddAttribute(grpcResponseStatusCodeLevelAttributeKey, errorLevel)
	trace.OnError(fmt.Errorf("gRPC Status: %s - %s", s.Code().String(), s.Message()))
}

func infoTraceResultCodeHandler(trace metrics.Trace, resultCode string) {
	trace.AddAttribute(resultCodeAttributeKey, resultCode)
	trace.AddAttribute(resultCodeLevelAttributeKey, infoLevel)
}

func warningTraceResultCodeHandler(trace metrics.Trace, resultCode string) {
	trace.AddAttribute(resultCodeAttributeKey, resultCode)
	trace.AddAttribute(resultCodeLevelAttributeKey, warningLevel)
}

func errorTraceResultCodeHandler(trace metrics.Trace, resultCode string) {
	trace.AddAttribute(resultCodeAttributeKey, resultCode)
	trace.AddAttribute(resultCodeLevelAttributeKey, errorLevel)
	trace.OnError(fmt.Errorf("Code RPC Result: %s", resultCode))
}

// UnaryServerInterceptor creates a unary server interceptor that uses the
// generic metrics.Provider interface.
func UnaryServerInterceptor(provider metrics.Provider) grpc_core.UnaryServerInterceptor {
	if provider == nil {
		return func(ctx context.Context, req interface{}, info *grpc_core.UnaryServerInfo, handler grpc_core.UnaryHandler) (interface{}, error) {
			return handler(ctx, req)
		}
	}

	return func(ctx context.Context, req interface{}, info *grpc_core.UnaryServerInfo, handler grpc_core.UnaryHandler) (interface{}, error) {
		// Inject the provider to allow for any custom metrics, events, etc
		// in downstream code.
		ctx = context.WithValue(ctx, metrics.ProviderContextKey, provider)

		trace := startProviderTrace(ctx, provider, info.FullMethod)
		defer trace.End()

		ctx = metrics.NewContext(ctx, trace)

		includeParsedFullMethodName(trace, info.FullMethod)
		includeClientMetadata(ctx, trace)

		resp, err := handler(ctx, req)
		includeGRPCStatusCode(trace, err)
		if err != nil {
			return nil, err
		}

		reflected := resp.(proto.Message).ProtoReflect()
		includeCodeResultCodeForUnaryCall(trace, reflected)

		return resp, nil
	}
}

// StreamServerInterceptor creates a stream server interceptor that uses the
// generic metrics.Provider interface.
func StreamServerInterceptor(provider metrics.Provider) grpc_core.StreamServerInterceptor {
	if provider == nil {
		return func(srv interface{}, ss grpc_core.ServerStream, info *grpc_core.StreamServerInfo, handler grpc_core.StreamHandler) error {
			return handler(srv, ss)
		}
	}

	return func(srv interface{}, ss grpc_core.ServerStream, info *grpc_core.StreamServerInfo, handler grpc_core.StreamHandler) error {
		// Inject the provider to allow for any custom metrics, events, etc
		// in downstream code.
		ctx := context.WithValue(ss.Context(), metrics.ProviderContextKey, provider)

		trace := startProviderTrace(ctx, provider, info.FullMethod)
		defer trace.End()

		ctx = metrics.NewContext(ctx, trace)

		includeParsedFullMethodName(trace, info.FullMethod)
		includeClientMetadata(ctx, trace)

		err := handler(srv, newWrappedStream(ctx, trace, ss))
		includeGRPCStatusCode(trace, err)
		return err
	}
}

type wrappedStream struct {
	ctx   context.Context
	trace metrics.Trace
	grpc_core.ServerStream
}

func (w *wrappedStream) Context() context.Context {
	return w.ctx
}

func (w *wrappedStream) SendMsg(m interface{}) error {
	reflected := m.(proto.Message).ProtoReflect()
	includeOcpResultCodeForServerStreamCall(w.trace, reflected)
	return w.ServerStream.SendMsg(m)
}

func newWrappedStream(ctx context.Context, trace metrics.Trace, wrapped grpc_core.ServerStream) grpc_core.ServerStream {
	return &wrappedStream{ctx, trace, wrapped}
}

func startProviderTrace(ctx context.Context, provider metrics.Provider, fullMethod string) metrics.Trace {
	method := strings.TrimPrefix(fullMethod, "/")

	// todo: we may not want to include all headers, especially if they contain
	//       sensitive information
	var hdrs http.Header
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		hdrs = make(http.Header, len(md))
		for k, vs := range md {
			for _, v := range vs {
				hdrs.Add(k, v)
			}
		}
	}

	target := hdrs.Get(":authority")
	u := getURL(method, target)

	trace := provider.StartTrace(method)
	trace.SetRequest(metrics.Request{
		Header:    hdrs,
		URL:       u,
		Method:    method,
		Transport: "HTTP",
	})

	return trace
}

func includeGRPCStatusCode(trace metrics.Trace, err error) {
	grpcStatus := status.Convert(err)
	handler, ok := traceStatusCodeHandlers[grpcStatus.Code()]
	if !ok {
		handler = defaultTraceStatusCodeHandler
	}
	handler(trace, grpcStatus)
}

func includeCodeResultCodeForUnaryCall(trace metrics.Trace, reflected protoreflect.Message) {
	// Check whether the response message has an enum called Result
	resultEnumDescriptor := reflected.Descriptor().Enums().ByName("Result")
	if resultEnumDescriptor == nil {
		return
	}

	// Check whether the response message has a field called result
	resultFieldDescriptor := reflected.Descriptor().Fields().ByName("result")
	if resultFieldDescriptor == nil {
		return
	}

	// This is the only sketchy part of the implementation. It'll panic if
	// the field isn't an enum. It seems unlikely, because we've already
	// determined an enum named Result exists, so we'd expect a reasonable
	// field name of result.
	resultEnumNumber := reflected.Get(resultFieldDescriptor).Enum()

	resultEnum := resultEnumDescriptor.Values().ByNumber(resultEnumNumber)
	if resultEnum == nil {
		return
	}

	// Augment the trace
	resultCode := strings.ToUpper(string(resultEnum.Name()))
	handler, ok := traceResultCodeHandlers[resultCode]
	if !ok {
		defaultTraceResultCodeHandler(trace, resultCode)
	} else {
		handler(trace, resultCode)
	}
}

func includeOcpResultCodeForServerStreamCall(trace metrics.Trace, reflected protoreflect.Message) {
	// Check whether the response message has a field set called success or error
	var respMessage protoreflect.Message
	resultMessageFieldDescriptor := reflected.Descriptor().Fields().ByName("success")
	if resultMessageFieldDescriptor != nil {
		respMessage = reflected.Get(resultMessageFieldDescriptor).Message()
	}
	if respMessage == nil || !respMessage.IsValid() {
		resultMessageFieldDescriptor := reflected.Descriptor().Fields().ByName("error")
		if resultMessageFieldDescriptor != nil {
			respMessage = reflected.Get(resultMessageFieldDescriptor).Message()
		}
	}
	if respMessage == nil || !respMessage.IsValid() {
		return
	}

	// Check whether the response message has an enum called Code
	resultEnumDescriptor := respMessage.Descriptor().Enums().ByName("Code")
	if resultEnumDescriptor == nil {
		return
	}

	// Check whether the response message has a field called code
	resultEnumFieldDescriptor := respMessage.Descriptor().Fields().ByName("code")
	if resultEnumFieldDescriptor == nil {
		return
	}

	// This is the only sketchy part of the implementation. It'll panic if
	// the field isn't an enum. It seems unlikely, because we've already
	// determined an enum named Result exists, so we'd expect a reasonable
	// field name of result.
	resultEnumNumber := respMessage.Get(resultEnumFieldDescriptor).Enum()

	resultEnum := resultEnumDescriptor.Values().ByNumber(resultEnumNumber)
	if resultEnum == nil {
		return
	}

	// Augment the trace
	resultCode := strings.ToUpper(string(resultEnum.Name()))
	handler, ok := traceResultCodeHandlers[resultCode]
	if !ok {
		defaultTraceResultCodeHandler(trace, resultCode)
	} else {
		handler(trace, resultCode)
	}
}

func includeParsedFullMethodName(trace metrics.Trace, fullMethodName string) {
	packageName, serviceName, methodName, err := grpc.ParseFullMethodName(fullMethodName)
	if err != nil {
		return
	}

	trace.AddAttribute(grpcRequestPackageAttributeKey, packageName)
	trace.AddAttribute(grpcRequestServiceAttributeKey, serviceName)
	trace.AddAttribute(grpcRequestMethodAttributeKey, methodName)
}

func includeClientMetadata(ctx context.Context, trace metrics.Trace) {
	userAgent, err := client.GetUserAgent(ctx)
	if err == nil {
		trace.AddAttribute(clientUserAgentAttributeKey, userAgent.String())
	}
}

func getURL(method, target string) *url.URL {
	var host string
	// target can be anything from
	// https://github.com/grpc/grpc/blob/master/doc/naming.md
	// see https://godoc.org/google.golang.org/grpc#DialContext
	if strings.HasPrefix(target, "unix:") {
		host = "localhost"
	} else {
		host = strings.TrimPrefix(target, "dns:///")
	}
	return &url.URL{
		Scheme: "grpc",
		Host:   host,
		Path:   method,
	}
}
