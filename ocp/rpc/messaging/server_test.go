package messaging

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	messagingpb "github.com/code-payments/ocp-protobuf-api/generated/go/messaging/v1"

	"github.com/code-payments/ocp-server/ocp/common"
	"github.com/code-payments/ocp-server/testutil"
)

func TestRendezvousProcess_HappyPath_OpenBeforeSend(t *testing.T) {
	for _, enableMultiServer := range []bool{true, false} {
		for _, enableKeepAlive := range []bool{true, false} {
			func() {
				env, cleanup := setup(t, enableMultiServer)
				defer cleanup()

				rendezvousKey := testutil.NewRandomAccount(t)

				env.client1.openMessageStream(t, rendezvousKey, enableKeepAlive)
				env.server1.assertInitialRendezvousRecordSaved(t, rendezvousKey)
				time.Sleep(500 * time.Millisecond) // allow async flush to finish

				sendMessageCall := env.client2.sendRequestToGrabBillMessage(t, rendezvousKey)
				sendMessageCall.requireSuccess(t)

				records := env.server1.getMessages(t, rendezvousKey)
				require.Len(t, records, 1)

				messages := env.client1.receiveMessagesInRealTime(t, rendezvousKey)
				require.Len(t, messages, 1)

				env.client1.closeMessageStream(t, rendezvousKey)
				env.server1.assertRendezvousRecordDeleted(t, rendezvousKey)

				message := messages[0]
				assert.Equal(t, sendMessageCall.resp.MessageId.Value, message.Id.Value)

				env.client1.ackMessages(t, rendezvousKey, message.Id)
				env.server1.assertNoMessages(t, rendezvousKey)
			}()
		}
	}
}

func TestRendezvousProcess_HappyPath_OpenAfterSend(t *testing.T) {
	for _, enableMultiServer := range []bool{true, false} {
		for _, enableKeepAlive := range []bool{true, false} {
			func() {
				env, cleanup := setup(t, enableMultiServer)
				defer cleanup()

				rendezvousKey := testutil.NewRandomAccount(t)
				sendMessageCall := env.client2.sendRequestToGrabBillMessage(t, rendezvousKey)
				sendMessageCall.requireSuccess(t)

				records := env.server1.getMessages(t, rendezvousKey)
				require.Len(t, records, 1)

				env.client1.openMessageStream(t, rendezvousKey, enableKeepAlive)
				env.server1.assertInitialRendezvousRecordSaved(t, rendezvousKey)

				messages := env.client1.receiveMessagesInRealTime(t, rendezvousKey)
				require.Len(t, messages, 1)

				env.client1.closeMessageStream(t, rendezvousKey)
				env.server1.assertRendezvousRecordDeleted(t, rendezvousKey)

				message := messages[0]
				assert.Equal(t, sendMessageCall.resp.MessageId.Value, message.Id.Value)

				env.client1.ackMessages(t, rendezvousKey, message.Id)
				env.server1.assertNoMessages(t, rendezvousKey)
			}()
		}
	}
}

func TestRendezvousProcess_MultipleOpenStreams(t *testing.T) {
	for i := 0; i < 32; i++ {
		for _, enableMultiServer := range []bool{true, false} {
			func() {
				env, cleanup := setup(t, enableMultiServer)
				defer cleanup()

				rendezvousKey := testutil.NewRandomAccount(t)

				for j := 0; j < 10; j++ {
					env.client1.openMessageStream(t, rendezvousKey, j%2 == 0)
					env.client2.openMessageStream(t, rendezvousKey, j%2 == 1)
				}
				time.Sleep(500 * time.Millisecond) // allow async flush to finish

				senderClient := env.client2
				if i%3 == 0 {
					senderClient = env.client1
				}

				sendMessageCall := senderClient.sendRequestToGrabBillMessage(t, rendezvousKey)
				sendMessageCall.requireSuccess(t)

				records := env.server1.getMessages(t, rendezvousKey)
				require.Len(t, records, 1)

				messages := env.client1.receiveMessagesInRealTime(t, rendezvousKey)
				messages = append(messages, env.client2.receiveMessagesInRealTime(t, rendezvousKey)...)
				require.Len(t, messages, 1)

				env.client1.closeMessageStream(t, rendezvousKey)

				message := messages[0]
				assert.Equal(t, sendMessageCall.resp.MessageId.Value, message.Id.Value)

				env.client1.ackMessages(t, rendezvousKey, message.Id)
				env.server1.assertNoMessages(t, rendezvousKey)
			}()
		}
	}
}

func TestRendezvousProcess_InternallyGeneratedMessage(t *testing.T) {
	t.Skip("no internally forwarded messages defined")
}

func TestSendMessage_RequestToGrabBill_HappyPath(t *testing.T) {
	env, cleanup := setup(t, false)
	defer cleanup()

	rendezvousKey := testutil.NewRandomAccount(t)
	sendMessageCall := env.client2.sendRequestToGrabBillMessage(t, rendezvousKey)
	sendMessageCall.requireSuccess(t)

	records := env.server1.getMessages(t, rendezvousKey)
	require.Len(t, records, 1)
	assert.Equal(t, rendezvousKey.PublicKey().ToBase58(), records[0].Account)
	assert.Equal(t, sendMessageCall.resp.MessageId.Value, records[0].MessageID[:])

	var savedProtoMessage messagingpb.Message
	require.NoError(t, proto.Unmarshal(records[0].Message, &savedProtoMessage))

	assert.Equal(t, sendMessageCall.resp.MessageId.Value, savedProtoMessage.Id.Value)
	require.NotNil(t, savedProtoMessage.GetRequestToGrabBill())
	assert.Equal(t, sendMessageCall.req.Message.GetRequestToGrabBill().RequestorAccount.Value, savedProtoMessage.GetRequestToGrabBill().RequestorAccount.Value)
	assert.Equal(t, sendMessageCall.req.Signature.Value, savedProtoMessage.SendMessageRequestSignature.Value)

	env.client1.openMessageStream(t, rendezvousKey, false)
	messages := env.client1.receiveMessagesInRealTime(t, rendezvousKey)
	env.client1.closeMessageStream(t, rendezvousKey)
	require.Len(t, messages, 1)
	assert.True(t, proto.Equal(&savedProtoMessage, messages[0]))
}

func TestSendMessage_RequestToGrabBill_Validation(t *testing.T) {
	env, cleanup := setup(t, false)
	defer cleanup()

	rendezvousKey := testutil.NewRandomAccount(t)

	env.client1.resetConf()
	env.client1.conf.simulateAccountNotCodeAccount = true
	sendMessageCall := env.client1.sendRequestToGrabBillMessage(t, rendezvousKey)
	sendMessageCall.assertInvalidMessageError(t, "requestor account must be a primary account")
	env.server1.assertNoMessages(t, rendezvousKey)

	env.client1.resetConf()
	env.client1.conf.simulateInvalidAccountType = true
	sendMessageCall = env.client1.sendRequestToGrabBillMessage(t, rendezvousKey)
	sendMessageCall.assertInvalidMessageError(t, "requestor account must be a primary account")
	env.server1.assertNoMessages(t, rendezvousKey)
}

func TestSendMessage_RequestToGiveBill_HappyPath(t *testing.T) {
	env, cleanup := setup(t, false)
	defer cleanup()

	// Core mint without exchange data
	rendezvousKey := testutil.NewRandomAccount(t)
	sendMessageCall := env.client1.sendRequestToGiveBillMessage(t, rendezvousKey)
	sendMessageCall.requireSuccess(t)

	records := env.server1.getMessages(t, rendezvousKey)
	require.Len(t, records, 1)
	assert.Equal(t, rendezvousKey.PublicKey().ToBase58(), records[0].Account)
	assert.Equal(t, sendMessageCall.resp.MessageId.Value, records[0].MessageID[:])

	var savedProtoMessage messagingpb.Message
	require.NoError(t, proto.Unmarshal(records[0].Message, &savedProtoMessage))

	assert.Equal(t, sendMessageCall.resp.MessageId.Value, savedProtoMessage.Id.Value)
	require.NotNil(t, savedProtoMessage.GetRequestToGiveBill())
	assert.Equal(t, sendMessageCall.req.Message.GetRequestToGiveBill().Mint.Value, savedProtoMessage.GetRequestToGiveBill().Mint.Value)
	assert.Equal(t, sendMessageCall.req.Signature.Value, savedProtoMessage.SendMessageRequestSignature.Value)

	polledMessages := env.client2.pollForMessages(t, rendezvousKey)
	require.Len(t, polledMessages, 1)
	assert.Equal(t, savedProtoMessage.Id.Value, polledMessages[0].Id.Value)
	require.NotNil(t, polledMessages[0].GetRequestToGiveBill())
	assert.Equal(t, savedProtoMessage.GetRequestToGiveBill().Mint.Value, polledMessages[0].GetRequestToGiveBill().Mint.Value)

	require.NotNil(t, polledMessages[0].AdditionalContext)
	require.NotNil(t, polledMessages[0].AdditionalContext.GetRequestToGiveBill())
	coreMintMetadata := polledMessages[0].AdditionalContext.GetRequestToGiveBill().MintMetadata
	require.NotNil(t, coreMintMetadata)
	assert.Equal(t, sendMessageCall.req.Message.GetRequestToGiveBill().Mint.Value, coreMintMetadata.Address.Value)
	assert.Equal(t, savedProtoMessage.GetRequestToGiveBill().Mint.Value, coreMintMetadata.Address.Value)
	assert.Equal(t, common.CoreMintName, coreMintMetadata.Name)
	assert.Equal(t, common.CoreMintSymbol, coreMintMetadata.Symbol)
	assert.Equal(t, uint32(common.CoreMintDecimals), coreMintMetadata.Decimals)
	require.NotNil(t, coreMintMetadata.VmMetadata)
	require.Nil(t, coreMintMetadata.LaunchpadMetadata)

	// Core mint with exchange data (no reserve state)
	rendezvousKey = testutil.NewRandomAccount(t)
	env.client1.resetConf()
	env.client1.conf.simulateWithExchangeData = true
	sendMessageCall = env.client1.sendRequestToGiveBillMessage(t, rendezvousKey)
	sendMessageCall.requireSuccess(t)

	records = env.server1.getMessages(t, rendezvousKey)
	require.Len(t, records, 1)

	var savedWithExchangeData messagingpb.Message
	require.NoError(t, proto.Unmarshal(records[0].Message, &savedWithExchangeData))
	require.NotNil(t, savedWithExchangeData.GetRequestToGiveBill())

	sentExchangeData := sendMessageCall.req.Message.GetRequestToGiveBill().ExchangeData
	savedExchangeData := savedWithExchangeData.GetRequestToGiveBill().ExchangeData
	require.NotNil(t, savedExchangeData)
	require.True(t, proto.Equal(sentExchangeData, savedExchangeData))

	polledMessages = env.client2.pollForMessages(t, rendezvousKey)
	require.Len(t, polledMessages, 1)
	require.NotNil(t, polledMessages[0].AdditionalContext)
	require.NotNil(t, polledMessages[0].AdditionalContext.GetRequestToGiveBill())
	coreMintMetadata = polledMessages[0].AdditionalContext.GetRequestToGiveBill().MintMetadata
	require.NotNil(t, coreMintMetadata)
	assert.Equal(t, sendMessageCall.req.Message.GetRequestToGiveBill().Mint.Value, coreMintMetadata.Address.Value)
	assert.Equal(t, savedWithExchangeData.GetRequestToGiveBill().Mint.Value, coreMintMetadata.Address.Value)
	assert.Equal(t, common.CoreMintName, coreMintMetadata.Name)
	require.NotNil(t, coreMintMetadata.VmMetadata)
	require.Nil(t, coreMintMetadata.LaunchpadMetadata)

	// Launchpad currency without exchange data
	rendezvousKey = testutil.NewRandomAccount(t)
	env.client1.resetConf()
	env.client1.conf.simulateLaunchpadMint = true
	sendMessageCall = env.client1.sendRequestToGiveBillMessage(t, rendezvousKey)
	sendMessageCall.requireSuccess(t)

	records = env.server1.getMessages(t, rendezvousKey)
	require.Len(t, records, 1)

	var savedLaunchpadNoExchange messagingpb.Message
	require.NoError(t, proto.Unmarshal(records[0].Message, &savedLaunchpadNoExchange))
	require.NotNil(t, savedLaunchpadNoExchange.GetRequestToGiveBill())
	assert.Equal(t, sendMessageCall.req.Message.GetRequestToGiveBill().Mint.Value, savedLaunchpadNoExchange.GetRequestToGiveBill().Mint.Value)
	assert.Nil(t, savedLaunchpadNoExchange.GetRequestToGiveBill().ExchangeData)

	polledMessages = env.client2.pollForMessages(t, rendezvousKey)
	require.Len(t, polledMessages, 1)
	require.NotNil(t, polledMessages[0].AdditionalContext)
	require.NotNil(t, polledMessages[0].AdditionalContext.GetRequestToGiveBill())
	launchpadMintMetadata := polledMessages[0].AdditionalContext.GetRequestToGiveBill().MintMetadata
	require.NotNil(t, launchpadMintMetadata)
	assert.Equal(t, sendMessageCall.req.Message.GetRequestToGiveBill().Mint.Value, launchpadMintMetadata.Address.Value)
	assert.Equal(t, savedLaunchpadNoExchange.GetRequestToGiveBill().Mint.Value, launchpadMintMetadata.Address.Value)
	require.NotNil(t, launchpadMintMetadata.VmMetadata)
	require.NotNil(t, launchpadMintMetadata.LaunchpadMetadata)

	// Launchpad currency with exchange data and matching reserve state
	rendezvousKey = testutil.NewRandomAccount(t)
	env.client1.resetConf()
	env.client1.conf.simulateLaunchpadMint = true
	env.client1.conf.simulateWithExchangeData = true
	env.client1.conf.simulateWithReserveState = true
	sendMessageCall = env.client1.sendRequestToGiveBillMessage(t, rendezvousKey)
	sendMessageCall.requireSuccess(t)

	records = env.server1.getMessages(t, rendezvousKey)
	require.Len(t, records, 1)

	var savedLaunchpadWithExchange messagingpb.Message
	require.NoError(t, proto.Unmarshal(records[0].Message, &savedLaunchpadWithExchange))
	require.NotNil(t, savedLaunchpadWithExchange.GetRequestToGiveBill())

	sentExchangeData = sendMessageCall.req.Message.GetRequestToGiveBill().ExchangeData
	savedExchangeData = savedLaunchpadWithExchange.GetRequestToGiveBill().ExchangeData
	require.NotNil(t, savedExchangeData)
	require.True(t, proto.Equal(sentExchangeData, savedExchangeData))

	polledMessages = env.client2.pollForMessages(t, rendezvousKey)
	require.Len(t, polledMessages, 1)
	require.NotNil(t, polledMessages[0].AdditionalContext)
	require.NotNil(t, polledMessages[0].AdditionalContext.GetRequestToGiveBill())
	launchpadMintMetadata = polledMessages[0].AdditionalContext.GetRequestToGiveBill().MintMetadata
	require.NotNil(t, launchpadMintMetadata)
	assert.Equal(t, sendMessageCall.req.Message.GetRequestToGiveBill().Mint.Value, launchpadMintMetadata.Address.Value)
	assert.Equal(t, savedLaunchpadWithExchange.GetRequestToGiveBill().Mint.Value, launchpadMintMetadata.Address.Value)
	require.NotNil(t, launchpadMintMetadata.VmMetadata)
	require.NotNil(t, launchpadMintMetadata.LaunchpadMetadata)
}

func TestSendMessage_RequestToGiveBill_Validation(t *testing.T) {
	env, cleanup := setup(t, false)
	defer cleanup()

	rendezvousKey := testutil.NewRandomAccount(t)

	// Unsupported mint
	env.client1.resetConf()
	env.client1.conf.simulateUnsupportedMint = true
	sendMessageCall := env.client1.sendRequestToGiveBillMessage(t, rendezvousKey)
	sendMessageCall.assertInvalidMessageError(t, "mint account must be the core mint or a launchpad currency")
	env.server1.assertNoMessages(t, rendezvousKey)

	// Core mint with reserve state in exchange data
	env.client1.resetConf()
	env.client1.conf.simulateWithExchangeData = true
	env.client1.conf.simulateWithReserveState = true
	sendMessageCall = env.client1.sendRequestToGiveBillMessage(t, rendezvousKey)
	sendMessageCall.assertInvalidMessageError(t, "reserve state cannot be provided for core mint")
	env.server1.assertNoMessages(t, rendezvousKey)

	// Launchpad currency with exchange data but no reserve state
	env.client1.resetConf()
	env.client1.conf.simulateLaunchpadMint = true
	env.client1.conf.simulateWithExchangeData = true
	sendMessageCall = env.client1.sendRequestToGiveBillMessage(t, rendezvousKey)
	sendMessageCall.assertInvalidMessageError(t, "reserve state is required for launchpad currency")
	env.server1.assertNoMessages(t, rendezvousKey)

	// Launchpad currency with mismatched reserve state mint
	env.client1.resetConf()
	env.client1.conf.simulateLaunchpadMint = true
	env.client1.conf.simulateWithExchangeData = true
	env.client1.conf.simulateWithReserveState = true
	env.client1.conf.simulateMismatchedExchangeDataMint = true
	sendMessageCall = env.client1.sendRequestToGiveBillMessage(t, rendezvousKey)
	sendMessageCall.assertInvalidMessageError(t, "reserve state mint doesn't match top-level mint")
	env.server1.assertNoMessages(t, rendezvousKey)
}

func TestSendMessage_InvalidRendezvousKeySignature(t *testing.T) {
	env, cleanup := setup(t, false)
	defer cleanup()

	rendezvousKey := testutil.NewRandomAccount(t)

	env.client1.conf.simulateInvalidRequestSignature = true
	sendMessageCall := env.client1.sendRequestToGrabBillMessage(t, testutil.NewRandomAccount(t))
	sendMessageCall.assertUnauthenticatedError(t, "")
	env.server1.assertNoMessages(t, rendezvousKey)
}

func TestMessagePolling_HappyPath(t *testing.T) {
	env, cleanup := setup(t, false)
	defer cleanup()

	rendezvousKey := testutil.NewRandomAccount(t)
	sendMessageCall := env.client2.sendRequestToGrabBillMessage(t, rendezvousKey)
	sendMessageCall.requireSuccess(t)

	messages := env.client1.pollForMessages(t, rendezvousKey)
	require.Len(t, messages, 1)

	message := messages[0]
	assert.Equal(t, sendMessageCall.resp.MessageId.Value, message.Id.Value)
	assert.Equal(t, sendMessageCall.req.Signature.Value, message.SendMessageRequestSignature.Value)
	require.NotNil(t, message.GetRequestToGrabBill())
	assert.EqualValues(t, sendMessageCall.req.Message.GetRequestToGrabBill().RequestorAccount.Value, message.GetRequestToGrabBill().RequestorAccount.Value)

	env.client1.ackMessages(t, rendezvousKey, sendMessageCall.resp.MessageId)
	messages = env.client1.pollForMessages(t, rendezvousKey)
	require.Empty(t, messages)
}

func TestKeepAlive_HappyPath(t *testing.T) {
	env, cleanup := setup(t, false)
	defer cleanup()

	absoluteTimeout := messageStreamWithoutKeepAliveTimeout

	start := time.Now()
	rendezvousKey := testutil.NewRandomAccount(t)
	env.client1.openMessageStream(t, rendezvousKey, true)
	env.server1.assertInitialRendezvousRecordSaved(t, rendezvousKey)

	pingCount := env.client1.waitUntilStreamTerminationOrTimeout(t, rendezvousKey, true, absoluteTimeout)
	assert.True(t, time.Since(start) >= absoluteTimeout)
	assert.True(t, pingCount >= int(absoluteTimeout/messageStreamPingDelay))
	assert.True(t, pingCount <= int(absoluteTimeout/messageStreamPingDelay)+2)
	env.server1.assertRendezvousRecordRefreshed(t, rendezvousKey)
}

func TestKeepAlive_UnresponsiveClient(t *testing.T) {
	env, cleanup := setup(t, false)
	defer cleanup()

	absoluteTimeout := messageStreamWithoutKeepAliveTimeout

	start := time.Now()
	rendezvousKey := testutil.NewRandomAccount(t)
	env.client1.openMessageStream(t, rendezvousKey, true)

	pingCount := env.client1.waitUntilStreamTerminationOrTimeout(t, rendezvousKey, false, absoluteTimeout)
	assert.True(t, time.Since(start) >= messageStreamKeepAliveRecvTimeout)
	assert.True(t, time.Since(start) <= messageStreamKeepAliveRecvTimeout+50*time.Millisecond)
	assert.True(t, pingCount >= int(messageStreamKeepAliveRecvTimeout/messageStreamPingDelay))
	assert.True(t, pingCount <= int(messageStreamKeepAliveRecvTimeout/messageStreamPingDelay)+1)
}
