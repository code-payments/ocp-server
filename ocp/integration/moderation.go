package integration

import (
	"context"

	"github.com/code-payments/ocp-server/ocp/common"
)

// Moderation is an OCP integration enabling custom moderation rules
type Moderation interface {
	// ValidateAttestation validates a moderation attestation for a piece of moderated content
	ValidateAttestation(ctx context.Context, owner *common.Account, rawAttestation []byte, content any) (bool, error)
}

type allowEverythingModerationIntegration struct {
}

// NewAllowEverythingModerationIntegration returns a default Moderation integration that allows everything
func NewAllowEverythingModerationIntegration() Moderation {
	return &allowEverythingModerationIntegration{}
}

func (i *allowEverythingModerationIntegration) ValidateAttestation(_ context.Context, _ *common.Account, _ []byte, _ any) (bool, error) {
	return true, nil
}
