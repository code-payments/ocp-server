package geyser

import (
	"bytes"
	"context"

	"github.com/mr-tron/base58"
	"github.com/pkg/errors"

	"github.com/code-payments/ocp-server/ocp/config"
	geyserpb "github.com/code-payments/ocp-server/ocp/worker/geyser/api/gen"

	"github.com/code-payments/ocp-server/ocp/common"
	ocp_data "github.com/code-payments/ocp-server/ocp/data"
	"github.com/code-payments/ocp-server/solana/token"
)

var (
	ErrUnexpectedProgramOwner = errors.New("unexpected program owner")
)

type ProgramAccountUpdateHandler interface {
	// Handle handles account updates from Geyser. Updates are not guaranteed
	// to come in order. Implementations must be idempotent and should not
	// trust the account data passed in. Always refer to finalized blockchain
	// state from another RPC provider.
	Handle(ctx context.Context, update *geyserpb.SubscribeUpdateAccount) error
}

type TokenProgramAccountHandler struct {
	conf        *conf
	data        ocp_data.Provider
	integration Integration
}

func NewTokenProgramAccountHandler(conf *conf, data ocp_data.Provider, integration Integration) ProgramAccountUpdateHandler {
	return &TokenProgramAccountHandler{
		conf:        conf,
		data:        data,
		integration: integration,
	}
}

func (h *TokenProgramAccountHandler) Handle(ctx context.Context, update *geyserpb.SubscribeUpdateAccount) error {
	if !bytes.Equal(update.Account.Owner, token.ProgramKey) {
		return ErrUnexpectedProgramOwner
	}

	// We need to know the amount being deposited, and that's impossible without
	// a transaction signature.
	if len(update.Account.TxnSignature) == 0 {
		return nil
	}
	signature := base58.Encode(update.Account.TxnSignature)

	// We need to know more about the account before accessing our data stores,
	// so skip anything that doesn't have data. I'm assuming this means the account
	// is closed anyways.
	if len(update.Account.Data) == 0 {
		return nil
	}

	var unmarshalled token.Account
	if !unmarshalled.Unmarshal(update.Account.Data) {
		// Probably not a token account (eg. mint)
		return nil
	}

	tokenAccount, err := common.NewAccountFromPublicKeyBytes(update.Account.Pubkey)
	if err != nil {
		return errors.Wrap(err, "invalid token account")
	}

	ownerAccount, err := common.NewAccountFromPublicKeyBytes(unmarshalled.Owner)
	if err != nil {
		return errors.Wrap(err, "invalid owner account")
	}

	mintAccount, err := common.NewAccountFromPublicKeyBytes(unmarshalled.Mint)
	if err != nil {
		return errors.Wrap(err, "invalid mint account")
	}

	switch mintAccount.PublicKey().ToBase58() {

	// todo: Don't hardcode Jeffy and other Flipcash currencies
	case common.CoreMintAccount.PublicKey().ToBase58(), config.JeffyMintPublicKey:
		// Not an ATA, so filter it out. It cannot be a VM deposit ATA
		if bytes.Equal(tokenAccount.PublicKey().ToBytes(), ownerAccount.PublicKey().ToBytes()) {
			return nil
		}

		exists, userAuthorityAccount, err := testForKnownUserAuthorityFromDepositPda(ctx, h.data, ownerAccount)
		if err != nil {
			return errors.Wrap(err, "error testing for user authority from deposit pda")
		} else if !exists {
			return nil
		}

		err = processPotentialExternalDepositIntoVm(ctx, h.data, h.integration, signature, userAuthorityAccount, mintAccount)
		if err != nil {
			return errors.Wrap(err, "error processing signature for external deposit into vm")
		}

		if unmarshalled.Amount > 0 {
			err = initiateExternalDepositIntoVm(ctx, h.data, userAuthorityAccount, mintAccount, unmarshalled.Amount)
			if err != nil {
				return errors.Wrap(err, "error depositing into the vm")
			}
		}

		return nil
	default:
		return nil
	}
}

func initializeProgramAccountUpdateHandlers(conf *conf, data ocp_data.Provider, integration Integration) map[string]ProgramAccountUpdateHandler {
	return map[string]ProgramAccountUpdateHandler{
		base58.Encode(token.ProgramKey): NewTokenProgramAccountHandler(conf, data, integration),
	}
}
