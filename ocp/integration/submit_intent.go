package integration

import (
	"context"

	transactionpb "github.com/code-payments/ocp-protobuf-api/generated/go/transaction/v1"

	"github.com/code-payments/ocp-server/ocp/data/intent"
)

// SubmitIntent is an integration that hooks into SubmitIntent
type SubmitIntent interface {
	// AllowCreation determines whether the new intent creation should be allowed
	// with app-specific validation rules
	AllowCreation(ctx context.Context, intentRecord *intent.Record, metadata *transactionpb.Metadata, actions []*transactionpb.Action) error

	// OnSuccess is a best-effort callback when an intent has been successfully
	// submitted
	OnSuccess(ctx context.Context, intentRecord *intent.Record) error
}

type defaultSubmitIntentIntegration struct {
}

// NewDefaultSubmitIntentIntegration retuns a SubmitIntentIntegration that allows everything
func NewDefaultSubmitIntentIntegration() SubmitIntent {
	return &defaultSubmitIntentIntegration{}
}

func (i *defaultSubmitIntentIntegration) AllowCreation(ctx context.Context, intentRecord *intent.Record, metadata *transactionpb.Metadata, actions []*transactionpb.Action) error {
	return nil
}

func (i *defaultSubmitIntentIntegration) OnSuccess(ctx context.Context, intentRecord *intent.Record) error {
	return nil
}
