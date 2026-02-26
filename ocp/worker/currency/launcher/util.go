package launcher

import (
	"context"
	"slices"

	"github.com/mr-tron/base58"
	"github.com/pkg/errors"

	"github.com/code-payments/ocp-server/ocp/common"
	ocp_data "github.com/code-payments/ocp-server/ocp/data"
	"github.com/code-payments/ocp-server/ocp/data/currency"
	"github.com/code-payments/ocp-server/ocp/data/nonce"
	vm_metadata "github.com/code-payments/ocp-server/ocp/data/vm/metadata"
	transaction_util "github.com/code-payments/ocp-server/ocp/transaction"
	"github.com/code-payments/ocp-server/solana"
	address_lookup_table "github.com/code-payments/ocp-server/solana/addresslookuptable"
	"github.com/code-payments/ocp-server/solana/currencycreator"
	"github.com/code-payments/ocp-server/solana/token"
	"github.com/code-payments/ocp-server/solana/vm"
)

// todo: some of these utilities can be promoted into a common package

func (p *runtime) validateCurrencyMetadataState(record *currency.MetadataRecord, states ...currency.MetadataState) error {
	if slices.Contains(states, record.State) {
		return nil
	}
	return errors.New("invalid currency metadata state")
}

func (p *runtime) markCurrencyMetadataFundingAuthority(ctx context.Context, record *currency.MetadataRecord) error {
	err := p.validateCurrencyMetadataState(record, currency.MetadataStateUnknown)
	if err != nil {
		return err
	}

	record.State = currency.MetadataStateFundingAuthority
	return p.data.SaveCurrencyMetadata(ctx, record)
}

func (p *runtime) markCurrencyMetadataInitializing(ctx context.Context, record *currency.MetadataRecord) error {
	err := p.validateCurrencyMetadataState(record, currency.MetadataStateFundingAuthority)
	if err != nil {
		return err
	}

	record.State = currency.MetadataStateInitializing
	return p.data.SaveCurrencyMetadata(ctx, record)
}

func (p *runtime) markCurrencyMetadataFinalValidation(ctx context.Context, record *currency.MetadataRecord) error {
	err := p.validateCurrencyMetadataState(record, currency.MetadataStateInitializing)
	if err != nil {
		return err
	}

	record.State = currency.MetadataStateFinalValidation
	return p.data.SaveCurrencyMetadata(ctx, record)
}

func (p *runtime) markCurrencyMetadataAvailable(ctx context.Context, record *currency.MetadataRecord) error {
	err := p.validateCurrencyMetadataState(record, currency.MetadataStateFinalValidation)
	if err != nil {
		return err
	}

	record.State = currency.MetadataStateAvailable
	return p.data.SaveCurrencyMetadata(ctx, record)
}

func (p *runtime) validateVmMetadataState(record *vm_metadata.Record, states ...vm_metadata.State) error {
	if slices.Contains(states, record.State) {
		return nil
	}
	return errors.New("invalid vm metadata state")
}

func (p *runtime) markVmMetadataInitializing(ctx context.Context, record *vm_metadata.Record) error {
	err := p.validateVmMetadataState(record, vm_metadata.StateUnknown)
	if err != nil {
		return err
	}

	record.State = vm_metadata.StateInitializing
	return p.data.SaveVmMetadata(ctx, record)
}

func (p *runtime) markVmMetadataAvailable(ctx context.Context, record *vm_metadata.Record) error {
	err := p.validateVmMetadataState(record, vm_metadata.StateInitializing)
	if err != nil {
		return err
	}

	record.State = vm_metadata.StateAvailable
	return p.data.SaveVmMetadata(ctx, record)
}

func validateAuthorityPrivateKeyExists(ctx context.Context, data ocp_data.Provider, account *common.Account) (bool, error) {
	vaultRecord, err := data.GetKey(ctx, account.PublicKey().ToBase58())
	if err != nil {
		return false, errors.Wrap(err, "error getting authority vault record")
	}

	fromPrivateKey, err := common.NewAccountFromPrivateKeyString(vaultRecord.PrivateKey)
	if err != nil {
		return false, errors.Wrap(err, "invalid authority private key")
	}

	if fromPrivateKey.PublicKey().ToBase58() != account.PublicKey().ToBase58() {
		return false, nil
	}
	return true, nil
}

func validateMinimumAuthorityFunding(ctx context.Context, data ocp_data.Provider, account *common.Account, amount uint64) (bool, uint64, error) {
	ai, err := data.GetBlockchainAccountInfo(ctx, account.PublicKey().ToBase58(), solana.CommitmentFinalized)
	switch err {
	case nil:
		if ai.Lamports >= initialAuthorityFundingLamports {
			return true, 0, nil
		}
		return false, initialAuthorityFundingLamports - ai.Lamports, nil
	case solana.ErrNoAccountInfo:
		return false, amount, nil
	default:
		return false, 0, errors.Wrap(err, "error getting authority account info")
	}
}

func fundAuthority(ctx context.Context, data ocp_data.Provider, account *common.Account, amount uint64) error {
	bh, err := data.GetBlockchainLatestBlockhash(ctx)
	if err != nil {
		return errors.Wrap(err, "error getting latest blockhash")
	}
	txn, err := transaction_util.MakeSolanaTransferTransaction(common.GetSubsidizer(), account, amount, bh)
	if err != nil {
		return errors.Wrap(err, "error making solana transfer transaction")
	}

	err = txn.Sign(common.GetSubsidizer().PrivateKey().ToBytes())
	if err != nil {
		return errors.Wrap(err, "error signing transaction")
	}

	return transaction_util.SubmitAndWaitForFinalization(ctx, data, &txn)
}

func validateMintExists(ctx context.Context, data ocp_data.Provider, account *common.Account) (bool, error) {
	ai, err := data.GetBlockchainAccountInfo(ctx, account.PublicKey().ToBase58(), solana.CommitmentFinalized)
	if err != nil {
		return false, err
	}

	var state token.Mint
	ok := state.Unmarshal(ai.Data)
	if !ok {
		return false, errors.New("invalid mint account data")
	}

	return true, nil
}

func validateCurrencyConfigExists(ctx context.Context, data ocp_data.Provider, account *common.Account) (bool, error) {
	ai, err := data.GetBlockchainAccountInfo(ctx, account.PublicKey().ToBase58(), solana.CommitmentFinalized)
	if err != nil {
		return false, err
	}

	var state currencycreator.CurrencyConfigAccount
	err = state.Unmarshal(ai.Data)
	if err != nil {
		return false, err
	}

	return true, nil
}

func validateLiquidityPoolExists(ctx context.Context, data ocp_data.Provider, account *common.Account) (bool, error) {
	ai, err := data.GetBlockchainAccountInfo(ctx, account.PublicKey().ToBase58(), solana.CommitmentFinalized)
	if err != nil {
		return false, err
	}

	var state currencycreator.LiquidityPoolAccount
	err = state.Unmarshal(ai.Data)
	if err != nil {
		return false, err
	}

	return true, nil
}

func validateVmExists(ctx context.Context, data ocp_data.Provider, account *common.Account) (bool, error) {
	ai, err := data.GetBlockchainAccountInfo(ctx, account.PublicKey().ToBase58(), solana.CommitmentFinalized)
	if err != nil {
		return false, err
	}

	var state vm.CodeVmAccount
	err = state.Unmarshal(ai.Data)
	if err != nil {
		return false, err
	}

	return true, nil
}

func validateAltExists(ctx context.Context, data ocp_data.Provider, account *common.Account) (bool, error) {
	ai, err := data.GetBlockchainAccountInfo(ctx, account.PublicKey().ToBase58(), solana.CommitmentFinalized)
	if err != nil {
		return false, err
	}

	var state address_lookup_table.AddressLookupTableAccount
	err = state.Unmarshal(ai.Data)
	if err != nil {
		return false, err
	}

	return true, nil
}

func validateAltIsExtended(ctx context.Context, data ocp_data.Provider, account *common.Account) (bool, error) {
	ai, err := data.GetBlockchainAccountInfo(ctx, account.PublicKey().ToBase58(), solana.CommitmentFinalized)
	if err != nil {
		return false, err
	}

	var state address_lookup_table.AddressLookupTableAccount
	err = state.Unmarshal(ai.Data)
	if err != nil {
		return false, err
	}

	// todo: validate addresses
	return len(state.Addresses) > 0, nil
}

func validateMemoryAccountExists(ctx context.Context, data ocp_data.Provider, account *common.Account) (bool, error) {
	ai, err := data.GetBlockchainAccountInfo(ctx, account.PublicKey().ToBase58(), solana.CommitmentFinalized)
	if err != nil {
		return false, err
	}

	var state vm.MemoryAccount
	err = state.Unmarshal(ai.Data)
	if err != nil {
		return false, err
	}

	return true, nil
}

func validateMemoryAccountIsResized(ctx context.Context, data ocp_data.Provider, account *common.Account) (bool, error) {
	ai, err := data.GetBlockchainAccountInfo(ctx, account.PublicKey().ToBase58(), solana.CommitmentFinalized)
	if err != nil {
		return false, err
	}

	var state vm.MemoryAccount
	err = state.Unmarshal(ai.Data)
	if err != nil {
		return false, err
	}

	actualSize := len(ai.Data)
	desiredCapacity := vm.MemoryAccountSize + vm.GetSliceAllocatorSize(int(state.NumAccounts), int(state.AccountSize))
	return actualSize == desiredCapacity, nil
}

func validateNonceMemoryAccountPopulated(ctx context.Context, data ocp_data.Provider, account *common.Account) (bool, error) {
	ai, err := data.GetBlockchainAccountInfo(ctx, account.PublicKey().ToBase58(), solana.CommitmentFinalized)
	if err != nil {
		return false, err
	}

	var state vm.MemoryAccountWithData
	err = state.Unmarshal(ai.Data)
	if err != nil {
		return false, err
	}

	for i := range int(state.NumAccounts) {
		if !state.Data.IsAllocated(i) {
			return false, nil
		}

		data, ok := state.Data.Read(i)
		if !ok {
			return false, nil
		}

		var vdn vm.VirtualDurableNonce
		err = vdn.UnmarshalFromMemory(data)
		if err != nil {
			return false, err
		}
	}

	return true, nil
}

func validateNoncePoolInitialized(ctx context.Context, data ocp_data.Provider, memoryAccount *common.Account) (bool, error) {
	ai, err := data.GetBlockchainAccountInfo(ctx, memoryAccount.PublicKey().ToBase58(), solana.CommitmentFinalized)
	if err != nil {
		return false, err
	}

	var state vm.MemoryAccount
	err = state.Unmarshal(ai.Data)
	if err != nil {
		return false, err
	}

	count, err := data.GetNonceCount(ctx, nonce.EnvironmentVm, base58.Encode(state.Vm))
	if err != nil {
		return false, err
	}
	return count == uint64(state.NumAccounts), nil
}
