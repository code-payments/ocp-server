package balance

import (
	"context"
	"strings"

	"github.com/pkg/errors"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	commonpb "github.com/code-payments/ocp-protobuf-api/generated/go/common/v1"

	"github.com/code-payments/ocp-server/ocp/auth"
	"github.com/code-payments/ocp-server/ocp/common"
)

// errSubscriberDenied is returned by subscriberAuthenticator when a watch
// request is not one the service will act on: the key is not a registered
// subscriber, or the signature is not its. Handlers answer it with the RPC's
// DENIED result, which deliberately does not say which — an unregistered key
// learns nothing about the registry from the response.
var errSubscriberDenied = errors.New("subscriber denied")

// subscriberAuthenticator resolves the subscriber a watch request is made by
// and checks the request is theirs. A subscriber is an Ed25519 key like any
// owner, and signs requests the way an owner does — over the serialized
// request with the signature field unset — so verification is the owner
// verifier's; what is added is the registry, which is the difference between
// a key and a subscriber. Registration is the WATCH_SUBSCRIBERS config today;
// a store-backed registry with self-service would sit behind the same method.
type subscriberAuthenticator struct {
	log      *zap.Logger
	verifier *auth.RPCSignatureVerifier
	conf     ConfigProvider
}

func newSubscriberAuthenticator(log *zap.Logger, verifier *auth.RPCSignatureVerifier, conf ConfigProvider) *subscriberAuthenticator {
	return &subscriberAuthenticator{
		log:      log,
		verifier: verifier,
		conf:     conf,
	}
}

// authenticate returns the subscriber account a request is made by. The
// caller passes the request with its signature field already cleared, and
// the signature separately, as every signed RPC in this codebase does.
//
// errSubscriberDenied is returned for a key that is not a registered
// subscriber or a signature that is not its; any other error is the
// service's own failure, to be answered with codes.Internal.
func (a *subscriberAuthenticator) authenticate(ctx context.Context, protoSubscriber *commonpb.SolanaAccountId, message proto.Message, signature *commonpb.Signature) (*common.Account, error) {
	subscriber, err := common.NewAccountFromProto(protoSubscriber)
	if err != nil {
		return nil, errors.Wrap(err, "invalid subscriber account")
	}

	// The registry is checked before the signature so that an unregistered
	// key costs no verification.
	if !a.isRegistered(ctx, subscriber) {
		return nil, errSubscriberDenied
	}

	err = a.verifier.Authenticate(ctx, subscriber, message, signature)
	switch status.Code(err) {
	case codes.OK:
		return subscriber, nil
	case codes.Unauthenticated:
		return nil, errSubscriberDenied
	default:
		return nil, errors.Wrap(err, "failure verifying subscriber signature")
	}
}

// isRegistered reports whether the key is a registered subscriber. The
// registry is read from config on every call so a change takes effect
// without a restart; it is a handful of keys.
func (a *subscriberAuthenticator) isRegistered(ctx context.Context, subscriber *common.Account) bool {
	registered := subscriber.PublicKey().ToBase58()
	for _, entry := range strings.Split(a.conf().watchSubscribers.Get(ctx), ",") {
		if strings.TrimSpace(entry) == registered {
			return true
		}
	}
	return false
}
