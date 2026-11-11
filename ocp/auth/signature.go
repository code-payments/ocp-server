package auth

import (
	"context"
	"crypto/ed25519"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	commonpb "github.com/code-payments/ocp-protobuf-api/generated/go/common/v1"

	"github.com/code-payments/ocp-server/metrics"
	"github.com/code-payments/ocp-server/ocp/common"
	ocp_data "github.com/code-payments/ocp-server/ocp/data"
)

const (
	metricsStructName = "auth.rpc_signature_verifier"
)

// RPCSignatureVerifier verifies signed requests messages by owner accounts.
type RPCSignatureVerifier struct {
	log  *zap.Logger
	data ocp_data.Provider
}

func NewRPCSignatureVerifier(log *zap.Logger, data ocp_data.Provider) *RPCSignatureVerifier {
	return &RPCSignatureVerifier{
		log:  log,
		data: data,
	}
}

// Authenticate authenticates that a RPC request message is signed by the owner
// account public key.
func (v *RPCSignatureVerifier) Authenticate(ctx context.Context, owner *common.Account, message proto.Message, signature *commonpb.Signature) error {
	defer metrics.TraceMethodCall(ctx, metricsStructName, "Authenticate").End()

	log := v.log.With(
		zap.String("method", "Authenticate"),
		zap.String("owner_account", owner.PublicKey().ToBase58()),
	)

	isSignatureValid, err := v.isSignatureVerifiedProtoMessage(owner, message, signature)
	if err != nil {
		log.With(zap.Error(err)).Warn("failure verifying signature")
		return status.Error(codes.Internal, "")
	}

	if !isSignatureValid {
		return status.Error(codes.Unauthenticated, "")
	}
	return nil
}

// marshalStrategy is a strategy for marshalling protobuf messages for signature
// verification
type marshalStrategy func(proto.Message) ([]byte, error)

// defaultMarshalStrategies are the default marshal strategies
var defaultMarshalStrategies = []marshalStrategy{
	ForceConsistentMarshal,
	proto.Marshal, // todo: deprecate this option
}

func (v *RPCSignatureVerifier) isSignatureVerifiedProtoMessage(owner *common.Account, message proto.Message, signature *commonpb.Signature) (bool, error) {
	if signature == nil {
		return false, nil
	}

	for _, marshalStrategy := range defaultMarshalStrategies {
		messageBytes, err := marshalStrategy(message)
		if err != nil {
			return false, err
		}

		isSignatureValid := ed25519.Verify(owner.PublicKey().ToBytes(), messageBytes, signature.Value)
		if isSignatureValid {
			return true, nil
		}
	}

	return false, nil
}
