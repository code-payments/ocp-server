package balance

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
	"google.golang.org/protobuf/proto"

	balancepb "github.com/code-payments/ocp-protobuf-api/generated/go/balance/v1"
	commonpb "github.com/code-payments/ocp-protobuf-api/generated/go/common/v1"

	"github.com/code-payments/ocp-server/ocp/auth"
	"github.com/code-payments/ocp-server/ocp/common"
	ocp_data "github.com/code-payments/ocp-server/ocp/data"
	"github.com/code-payments/ocp-server/testutil"
)

func TestSubscriberAuthenticator(t *testing.T) {
	ctx := context.Background()
	log := zaptest.NewLogger(t)

	registered := testutil.NewRandomAccount(t)
	alsoRegistered := testutil.NewRandomAccount(t)
	unregistered := testutil.NewRandomAccount(t)

	authenticator := newSubscriberAuthenticator(
		log,
		auth.NewRPCSignatureVerifier(log, ocp_data.NewTestDataProvider()),
		withManualTestOverrides(&testOverrides{
			// Whitespace around entries is tolerated.
			watchSubscribers: registered.PublicKey().ToBase58() + " , " + alsoRegistered.PublicKey().ToBase58(),
		}),
	)

	newRequest := func(subscriber *common.Account) *balancepb.DeleteWatchRequest {
		return &balancepb.DeleteWatchRequest{
			Subscriber: subscriber.ToProto(),
			Key: &balancepb.WatchKey{
				Owner: testutil.NewRandomAccount(t).ToProto(),
				Key:   []byte("key"),
			},
			Generation: 1,
		}
	}

	t.Run("registered subscriber with its own signature", func(t *testing.T) {
		for _, subscriber := range []*common.Account{registered, alsoRegistered} {
			req := newRequest(subscriber)
			signature := sign(t, subscriber, req)

			actual, err := authenticator.authenticate(ctx, req.Subscriber, req, signature)
			require.NoError(t, err)
			assert.Equal(t, subscriber.PublicKey().ToBase58(), actual.PublicKey().ToBase58())
		}
	})

	t.Run("unregistered subscriber", func(t *testing.T) {
		req := newRequest(unregistered)
		signature := sign(t, unregistered, req)

		_, err := authenticator.authenticate(ctx, req.Subscriber, req, signature)
		assert.Equal(t, errSubscriberDenied, err)
	})

	t.Run("registered subscriber, someone else's signature", func(t *testing.T) {
		req := newRequest(registered)
		signature := sign(t, alsoRegistered, req)

		_, err := authenticator.authenticate(ctx, req.Subscriber, req, signature)
		assert.Equal(t, errSubscriberDenied, err)
	})

	t.Run("signature over a different message", func(t *testing.T) {
		req := newRequest(registered)
		signature := sign(t, registered, req)
		req.Generation++

		_, err := authenticator.authenticate(ctx, req.Subscriber, req, signature)
		assert.Equal(t, errSubscriberDenied, err)
	})

	t.Run("no signature", func(t *testing.T) {
		req := newRequest(registered)

		_, err := authenticator.authenticate(ctx, req.Subscriber, req, nil)
		assert.Equal(t, errSubscriberDenied, err)
	})

	t.Run("empty registry denies everyone", func(t *testing.T) {
		empty := newSubscriberAuthenticator(
			log,
			auth.NewRPCSignatureVerifier(log, ocp_data.NewTestDataProvider()),
			withManualTestOverrides(&testOverrides{}),
		)
		req := newRequest(registered)
		signature := sign(t, registered, req)

		_, err := empty.authenticate(ctx, req.Subscriber, req, signature)
		assert.Equal(t, errSubscriberDenied, err)
	})

	t.Run("malformed subscriber account", func(t *testing.T) {
		req := newRequest(registered)
		signature := sign(t, registered, req)

		_, err := authenticator.authenticate(ctx, &commonpb.SolanaAccountId{Value: []byte("short")}, req, signature)
		require.Error(t, err)
		assert.NotEqual(t, errSubscriberDenied, err)
	})
}

// sign signs the message as a subscriber does: over the request serialized
// with its signature field unset.
func sign(t *testing.T, signer *common.Account, message proto.Message) *commonpb.Signature {
	messageBytes, err := proto.Marshal(message)
	require.NoError(t, err)

	signature, err := signer.Sign(messageBytes)
	require.NoError(t, err)

	return &commonpb.Signature{Value: signature}
}
