package integration

import (
	"context"

	"github.com/code-payments/ocp-server/ocp/common"
)

// ModeratedContentKind is what a piece of moderated content is used as. OCP
// names it at each call site from the field being validated, never from the
// client, so an integration can require that the content was moderated as
// exactly that kind: content moderated under a looser kind's checks (e.g. a
// description's) must not pass as one with stricter checks (e.g. a name's).
type ModeratedContentKind uint8

const (
	ModeratedContentKindUnknown ModeratedContentKind = iota
	ModeratedContentKindCurrencyName
	ModeratedContentKindCurrencySymbol
	ModeratedContentKindCurrencyDescription
	ModeratedContentKindCurrencyIcon
)

func (k ModeratedContentKind) String() string {
	switch k {
	case ModeratedContentKindCurrencyName:
		return "currency_name"
	case ModeratedContentKindCurrencySymbol:
		return "currency_symbol"
	case ModeratedContentKindCurrencyDescription:
		return "currency_description"
	case ModeratedContentKindCurrencyIcon:
		return "currency_icon"
	}
	return "unknown"
}

// Moderation is an OCP integration enabling custom moderation rules
type Moderation interface {
	// ValidateAttestation validates a moderation attestation for a piece of
	// moderated content used as the given kind. An integration should refuse an
	// attestation for content moderated as a different kind, and a kind it does
	// not know.
	ValidateAttestation(ctx context.Context, owner *common.Account, rawAttestation []byte, kind ModeratedContentKind, content any) (bool, error)
}

type allowEverythingModerationIntegration struct {
}

// NewAllowEverythingModerationIntegration returns a default Moderation integration that allows everything
func NewAllowEverythingModerationIntegration() Moderation {
	return &allowEverythingModerationIntegration{}
}

func (i *allowEverythingModerationIntegration) ValidateAttestation(_ context.Context, _ *common.Account, _ []byte, _ ModeratedContentKind, _ any) (bool, error) {
	return true, nil
}
