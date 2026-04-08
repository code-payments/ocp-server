package integration

import (
	"context"

	transactionpb "github.com/code-payments/ocp-protobuf-api/generated/go/transaction/v1"

	"github.com/code-payments/ocp-server/ocp/common"
	"github.com/code-payments/ocp-server/ocp/data/swap"
)

// Antispam is an antispam guard integration that apps can implement to check
// whether operations of interest are allowed to be performed.
type Antispam interface {
	AllowOpenAccounts(ctx context.Context, owner *common.Account, accountSet transactionpb.OpenAccountsMetadata_AccountSet) (bool, string, error)

	AllowSendPayment(ctx context.Context, owner, destination *common.Account, isPublic bool) (bool, string, error)

	AllowReceivePayments(ctx context.Context, owner *common.Account, isPublic bool) (bool, string, error)

	AllowDistribution(ctx context.Context, owner *common.Account, isPublic bool) (bool, string, error)

	AllowSwap(ctx context.Context, fundingSource swap.FundingSource, owner, fromMint, toMint *common.Account, amount uint64, initializesMint bool) (bool, string, error)

	AllowCurrencyLaunch(ctx context.Context, owner *common.Account, name, symbol string) (bool, string, error)
}

type allowEverythingAntispamIntegration struct {
}

// NewAllowEverythingAntispamIntegration returns a default antispam integration that allows everything
func NewAllowEverythingAntispamIntegration() Antispam {
	return &allowEverythingAntispamIntegration{}
}

func (i *allowEverythingAntispamIntegration) AllowOpenAccounts(ctx context.Context, owner *common.Account, accountSet transactionpb.OpenAccountsMetadata_AccountSet) (bool, string, error) {
	return true, "", nil
}

func (i *allowEverythingAntispamIntegration) AllowSendPayment(ctx context.Context, owner, destination *common.Account, isPublic bool) (bool, string, error) {
	return true, "", nil
}

func (i *allowEverythingAntispamIntegration) AllowReceivePayments(ctx context.Context, owner *common.Account, isPublic bool) (bool, string, error) {
	return true, "", nil
}

func (i *allowEverythingAntispamIntegration) AllowDistribution(ctx context.Context, owner *common.Account, isPublic bool) (bool, string, error) {
	return true, "", nil
}

func (i *allowEverythingAntispamIntegration) AllowSwap(ctx context.Context, fundingSource swap.FundingSource, owner, fromMint, toMint *common.Account, amount uint64, initializesMint bool) (bool, string, error) {
	return true, "", nil
}

func (i *allowEverythingAntispamIntegration) AllowCurrencyLaunch(ctx context.Context, owner *common.Account, name, symbol string) (bool, string, error) {
	return true, "", nil
}
