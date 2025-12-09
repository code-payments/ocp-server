package geyser

import (
	"context"
	"time"

	"github.com/code-payments/ocp-server/ocp/common"
	ocp_data "github.com/code-payments/ocp-server/ocp/data"
	"github.com/code-payments/ocp-server/ocp/data/timelock"
	"github.com/code-payments/ocp-server/solana"
	timelock_token "github.com/code-payments/ocp-server/solana/timelock/v1"
	"github.com/code-payments/ocp-server/solana/vm"
)

func updateTimelockAccountRecord(ctx context.Context, data ocp_data.Provider, timelockRecord *timelock.Record) error {
	unlockState, slot, err := getTimelockUnlockState(ctx, data, timelockRecord)
	if err != nil {
		return err
	}

	if unlockState != nil {
		timelockRecord.VaultState = timelock_token.StateWaitingForTimeout
		if unlockState.IsUnlocked() {
			timelockRecord.VaultState = timelock_token.StateUnlocked
		}

		unlockAt := uint64(unlockState.UnlockAt)
		timelockRecord.UnlockAt = &unlockAt
	} else {
		return nil
	}

	timelockRecord.Block = slot
	timelockRecord.LastUpdatedAt = time.Now()
	return data.SaveTimelock(ctx, timelockRecord)
}

func getTimelockUnlockState(ctx context.Context, data ocp_data.Provider, timelockRecord *timelock.Record) (*vm.UnlockStateAccount, uint64, error) {
	accountInfoRecord, err := data.GetAccountInfoByTokenAddress(ctx, timelockRecord.VaultAddress)
	if err != nil {
		return nil, 0, err
	}

	vaultOwnerAccount, err := common.NewAccountFromPublicKeyString(timelockRecord.VaultOwner)
	if err != nil {
		return nil, 0, err
	}

	mintAccount, err := common.NewAccountFromPublicKeyString(accountInfoRecord.MintAccount)
	if err != nil {
		return nil, 0, err
	}

	vmConfig, err := common.GetVmConfigForMint(ctx, data, mintAccount)
	if err != nil {
		return nil, 0, err
	}

	timelockAccounts, err := vaultOwnerAccount.GetTimelockAccounts(vmConfig)
	if err != nil {
		return nil, 0, err
	}

	marshalled, slot, err := data.GetBlockchainAccountDataAfterBlock(ctx, timelockAccounts.Unlock.PublicKey().ToBase58(), timelockRecord.Block)
	switch err {
	case nil:
		var unlockState vm.UnlockStateAccount
		if err = unlockState.Unmarshal(marshalled); err != nil {
			return nil, 0, err
		}
		return &unlockState, slot, nil
	case solana.ErrNoAccountInfo:
		return nil, slot, nil
	default:
		return nil, 0, err
	}
}
