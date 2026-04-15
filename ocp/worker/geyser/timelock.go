package geyser

import (
	"context"
	"time"

	ocp_data "github.com/code-payments/ocp-server/ocp/data"
	"github.com/code-payments/ocp-server/ocp/data/timelock"
	"github.com/code-payments/ocp-server/pointer"
	timelock_token "github.com/code-payments/ocp-server/solana/timelock/v1"
	"github.com/code-payments/ocp-server/solana/vm"
)

func updateTimelockAccountRecord(ctx context.Context, data ocp_data.Provider, timelockRecord *timelock.Record, unlockState *vm.UnlockStateAccount, slot uint64) error {
	if unlockState == nil {
		return nil
	}

	expectedState := timelock_token.StateWaitingForTimeout
	if unlockState.IsUnlocked() {
		expectedState = timelock_token.StateUnlocked
	}

	if timelockRecord.VaultState == expectedState {
		return nil
	}

	timelockRecord.VaultState = expectedState
	timelockRecord.UnlockAt = pointer.Uint64(uint64(unlockState.UnlockAt))
	timelockRecord.Block = slot
	timelockRecord.LastUpdatedAt = time.Now()
	return data.SaveTimelock(ctx, timelockRecord)
}
