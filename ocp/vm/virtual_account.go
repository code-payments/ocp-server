package vm

import (
	"context"
	"time"

	"github.com/pkg/errors"

	indexerpb "github.com/code-payments/code-vm-indexer/generated/indexer/v1"

	"github.com/code-payments/ocp-server/ocp/common"
	ocp_data "github.com/code-payments/ocp-server/ocp/data"
	"github.com/code-payments/ocp-server/ocp/data/fulfillment"
	vm_program "github.com/code-payments/ocp-server/solana/vm"
)

func EnsureVirtualTimelockAccountIsInitialized(ctx context.Context, data ocp_data.Provider, vault *common.Account, waitForInitialization bool) error {
	timelockRecord, err := data.GetTimelockByVault(ctx, vault.PublicKey().ToBase58())
	if err != nil {
		return err
	}

	if !timelockRecord.ExistsOnBlockchain() {
		initializeFulfillmentRecord, err := data.GetFirstSchedulableFulfillmentByAddressAsSource(ctx, timelockRecord.VaultAddress)
		if err != nil {
			return err
		}

		if initializeFulfillmentRecord.FulfillmentType != fulfillment.InitializeLockedTimelockAccount {
			return errors.New("expected an initialize locked timelock account fulfillment")
		}

		err = markFulfillmentAsActivelyScheduled(ctx, data, initializeFulfillmentRecord)
		if err != nil {
			return err
		}
	} else {
		return nil
	}

	if !waitForInitialization {
		return nil
	}

	for range 60 {
		timelockRecord, err := data.GetTimelockByVault(ctx, vault.PublicKey().ToBase58())
		if err != nil {
			continue
		}

		if timelockRecord.ExistsOnBlockchain() {
			return nil
		}

		time.Sleep(time.Second)
	}

	return errors.New("timed out waiting for initialization")
}

func GetVirtualTimelockAccountStateInMemory(ctx context.Context, vmIndexerClient indexerpb.IndexerClient, vm, owner *common.Account) (*vm_program.VirtualTimelockAccount, *common.Account, uint16, error) {
	resp, err := vmIndexerClient.GetVirtualTimelockAccounts(ctx, &indexerpb.GetVirtualTimelockAccountsRequest{
		VmAccount: &indexerpb.Address{Value: vm.PublicKey().ToBytes()},
		Owner:     &indexerpb.Address{Value: owner.PublicKey().ToBytes()},
	})
	if err != nil {
		return nil, nil, 0, err
	} else if resp.Result != indexerpb.GetVirtualTimelockAccountsResponse_OK {
		return nil, nil, 0, errors.Errorf("received rpc result %s", resp.Result.String())
	}

	if len(resp.Items) > 1 {
		return nil, nil, 0, errors.New("multiple results returned")
	} else if resp.Items[0].Storage.GetMemory() == nil {
		return nil, nil, 0, errors.New("account is compressed")
	}

	protoMemory := resp.Items[0].Storage.GetMemory()
	memory, err := common.NewAccountFromPublicKeyBytes(protoMemory.Account.Value)
	if err != nil {
		return nil, nil, 0, err
	}

	protoAccount := resp.Items[0].Account
	state := vm_program.VirtualTimelockAccount{
		Owner: protoAccount.Owner.Value,
		Nonce: vm_program.Hash(protoAccount.Nonce.Value),

		TokenBump:    uint8(protoAccount.TokenBump),
		UnlockBump:   uint8(protoAccount.UnlockBump),
		WithdrawBump: uint8(protoAccount.WithdrawBump),

		Balance: protoAccount.Balance,
		Bump:    uint8(protoAccount.Bump),
	}

	return &state, memory, uint16(protoMemory.Index), nil
}

func GetVirtualTimelockAccountLocationInMemory(ctx context.Context, data ocp_data.Provider, vault *common.Account) (*common.Account, uint16, error) {
	memoryAddress, index, err := data.GetVmMemoryLocationByAddress(ctx, vault.PublicKey().ToBase58())
	if err != nil {
		return nil, 0, err
	}

	memoryAccount, err := common.NewAccountFromPublicKeyString(memoryAddress)
	if err != nil {
		return nil, 0, err
	}

	return memoryAccount, index, err
}

func GetVirtualDurableNonceAccountStateInMemory(ctx context.Context, vmIndexerClient indexerpb.IndexerClient, vm, nonce *common.Account) (*vm_program.VirtualDurableNonce, *common.Account, uint16, error) {
	resp, err := vmIndexerClient.GetVirtualDurableNonce(ctx, &indexerpb.GetVirtualDurableNonceRequest{
		VmAccount: &indexerpb.Address{Value: vm.PublicKey().ToBytes()},
		Address:   &indexerpb.Address{Value: nonce.PublicKey().ToBytes()},
	})
	if err != nil {
		return nil, nil, 0, err
	} else if resp.Result != indexerpb.GetVirtualDurableNonceResponse_OK {
		return nil, nil, 0, errors.Errorf("received rpc result %s", resp.Result.String())
	}

	protoMemory := resp.Item.Storage.GetMemory()
	if protoMemory == nil {
		return nil, nil, 0, errors.New("account is compressed")
	}

	memory, err := common.NewAccountFromPublicKeyBytes(protoMemory.Account.Value)
	if err != nil {
		return nil, nil, 0, err
	}

	protoAccount := resp.Item.Account
	state := vm_program.VirtualDurableNonce{
		Address: protoAccount.Address.Value,
		Value:   vm_program.Hash(protoAccount.Value.Value),
	}

	return &state, memory, uint16(protoMemory.Index), nil
}

func GetVirtualDurableNonceAccountLocationInMemory(ctx context.Context, vmIndexerClient indexerpb.IndexerClient, vm, nonce *common.Account) (*common.Account, uint16, error) {
	_, memory, memoryIndex, err := GetVirtualDurableNonceAccountStateInMemory(ctx, vmIndexerClient, vm, nonce)
	if err != nil {
		return nil, 0, err
	}
	return memory, memoryIndex, nil
}

func markFulfillmentAsActivelyScheduled(ctx context.Context, data ocp_data.Provider, fulfillmentRecord *fulfillment.Record) error {
	if fulfillmentRecord.Id == 0 {
		return nil
	}

	if !fulfillmentRecord.DisableActiveScheduling {
		return nil
	}

	if fulfillmentRecord.State != fulfillment.StateUnknown {
		return nil
	}

	fulfillmentRecord.DisableActiveScheduling = false
	return data.UpdateFulfillment(ctx, fulfillmentRecord)
}
