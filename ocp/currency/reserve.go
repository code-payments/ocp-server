package currency

import (
	"context"
	"errors"
	"time"

	"github.com/code-payments/ocp-server/ocp/common"
	ocp_data "github.com/code-payments/ocp-server/ocp/data"
	"github.com/code-payments/ocp-server/solana"
	"github.com/code-payments/ocp-server/solana/currencycreator"
	"github.com/code-payments/ocp-server/solana/token"
)

// GetLaunchpadCurrencyCirculatingSupply gets the current circulating supply in
// quarks for a launchpad currency directly from the blockchain
func GetLaunchpadCurrencyCirculatingSupply(ctx context.Context, data ocp_data.Provider, mint *common.Account) (uint64, time.Time, error) {
	metadataRecord, err := data.GetCurrencyMetadata(ctx, mint.PublicKey().ToBase58())
	if err != nil {
		return 0, time.Time{}, err
	}

	accounts, err := common.GetLaunchpadCurrencyAccounts(metadataRecord)
	if err != nil {
		return 0, time.Time{}, err
	}

	ai, err := data.GetBlockchainAccountInfo(ctx, accounts.VaultMint.PublicKey().ToBase58(), solana.CommitmentFinalized)
	if err != nil {
		return 0, time.Time{}, err
	}

	var tokenAccount token.Account
	if !tokenAccount.Unmarshal(ai.Data) {
		return 0, time.Time{}, errors.New("invalid token account state")
	}

	return currencycreator.DefaultMintMaxQuarkSupply - tokenAccount.Amount, time.Now(), nil
}
