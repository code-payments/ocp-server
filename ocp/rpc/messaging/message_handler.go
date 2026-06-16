package messaging

import (
	"bytes"
	"context"
	"time"

	"github.com/pkg/errors"

	commonpb "github.com/code-payments/ocp-protobuf-api/generated/go/common/v1"
	messagingpb "github.com/code-payments/ocp-protobuf-api/generated/go/messaging/v1"

	"github.com/code-payments/ocp-server/ocp/common"
	currency_util "github.com/code-payments/ocp-server/ocp/currency"
	ocp_data "github.com/code-payments/ocp-server/ocp/data"
	"github.com/code-payments/ocp-server/ocp/data/account"
)

// MessageHandler provides message-specific in addition to the generic message
// handling flows. Implementations are responsible for determining whether a
// message can be allowed to be retried.
type MessageHandler interface {
	// Validate validates a message, which determines whether it should be
	// allowed to be sent and persisted
	Validate(ctx context.Context, rendezvous *common.Account, message *messagingpb.Message) error

	// GetAdditionalContext returns additional server-provided context to be
	// injected into the message before delivering to clients. Returns nil
	// if no additional context is needed.
	GetAdditionalContext(ctx context.Context, message *messagingpb.Message) (*messagingpb.AdditionalServerContext, error)

	// GetExpiry returns how long the message remains valid once persisted. A zero
	// duration means the message never expires.
	GetExpiry() time.Duration
}

type RequestToGrabBillMessageHandler struct {
	data ocp_data.Provider
}

func NewRequestToGrabBillMessageHandler(data ocp_data.Provider) MessageHandler {
	return &RequestToGrabBillMessageHandler{
		data: data,
	}
}

func (h *RequestToGrabBillMessageHandler) Validate(ctx context.Context, rendezvous *common.Account, untypedMessage *messagingpb.Message) error {
	typedMessage := untypedMessage.GetRequestToGrabBill()
	if typedMessage == nil {
		return errors.New("invalid message type")
	}

	//
	// Part 1: Requestor account must be a primary account
	//

	requestorAccount, err := common.NewAccountFromProto(typedMessage.RequestorAccount)
	if err != nil {
		return err
	}

	accountInfoRecord, err := h.data.GetAccountInfoByTokenAddress(ctx, requestorAccount.PublicKey().ToBase58())
	if err == account.ErrAccountInfoNotFound || (err == nil && accountInfoRecord.AccountType != commonpb.AccountType_PRIMARY) {
		return newMessageValidationError("requestor account must be a primary account")
	} else if err != nil {
		return err
	}

	return nil
}

func (h *RequestToGrabBillMessageHandler) GetAdditionalContext(ctx context.Context, message *messagingpb.Message) (*messagingpb.AdditionalServerContext, error) {
	return nil, nil
}

func (h *RequestToGrabBillMessageHandler) GetExpiry() time.Duration {
	return 5 * time.Minute
}

// todo: This message type needs tests
type RequestToGiveBillMessageHandler struct {
	data             ocp_data.Provider
	mintDataProvider *currency_util.MintDataProvider
}

func NewRequestToGiveBillMessageHandler(data ocp_data.Provider, mintDataProvider *currency_util.MintDataProvider) MessageHandler {
	return &RequestToGiveBillMessageHandler{
		data:             data,
		mintDataProvider: mintDataProvider,
	}
}

func (h *RequestToGiveBillMessageHandler) Validate(ctx context.Context, rendezvous *common.Account, untypedMessage *messagingpb.Message) error {
	typedMessage := untypedMessage.GetRequestToGiveBill()
	if typedMessage == nil {
		return errors.New("invalid message type")
	}

	//
	// Part 1: Mint account must be the Core Mint or a Launchpad Currency
	//

	mintAccount, err := common.NewAccountFromProto(typedMessage.Mint)
	if err != nil {
		return err
	}

	isSupportedMint, err := common.IsSupportedMint(ctx, h.data, mintAccount)
	if err != nil {
		return err
	} else if !isSupportedMint {
		return newMessageValidationError("mint account must be the core mint or a launchpad currency")
	}

	if typedMessage.ExchangeData != nil {
		if common.IsCoreMint(mintAccount) {
			if typedMessage.ExchangeData.LaunchpadCurrencyReserveState != nil {
				return newMessageValidationError("reserve state cannot be provided for core mint")
			}
		} else {
			if typedMessage.ExchangeData.LaunchpadCurrencyReserveState == nil {
				return newMessageValidationError("reserve state is required for launchpad currency")
			}

			if !bytes.Equal(mintAccount.PublicKey().ToBytes(), typedMessage.ExchangeData.Mint.Value) {
				return newMessageValidationError("reserve state mint doesn't match top-level mint")
			}
		}
	}

	return nil
}

func (h *RequestToGiveBillMessageHandler) GetAdditionalContext(ctx context.Context, message *messagingpb.Message) (*messagingpb.AdditionalServerContext, error) {
	typedMessage := message.GetRequestToGiveBill()
	if typedMessage == nil {
		return nil, errors.New("invalid message type")
	}

	mintAccount, err := common.NewAccountFromProto(typedMessage.Mint)
	if err != nil {
		return nil, err
	}

	mintMetadata, err := h.mintDataProvider.GetProtoMint(ctx, mintAccount)
	if err != nil {
		return nil, err
	}

	return &messagingpb.AdditionalServerContext{
		Type: &messagingpb.AdditionalServerContext_RequestToGiveBill{
			RequestToGiveBill: &messagingpb.RequestToGiveBillServerContext{
				MintMetadata: mintMetadata,
			},
		},
	}, nil
}

func (h *RequestToGiveBillMessageHandler) GetExpiry() time.Duration {
	return 5 * time.Minute
}
