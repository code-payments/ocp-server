package transaction

import (
	"bytes"
	"context"

	"github.com/pkg/errors"

	commonpb "github.com/code-payments/ocp-protobuf-api/generated/go/common/v1"
	transactionpb "github.com/code-payments/ocp-protobuf-api/generated/go/transaction/v1"

	"github.com/code-payments/ocp-server/ocp/common"
	ocp_data "github.com/code-payments/ocp-server/ocp/data"
	transaction_util "github.com/code-payments/ocp-server/ocp/transaction"
	vm_util "github.com/code-payments/ocp-server/ocp/vm"
	"github.com/code-payments/ocp-server/solana"
	compute_budget "github.com/code-payments/ocp-server/solana/computebudget"
	"github.com/code-payments/ocp-server/solana/currencycreator"
	"github.com/code-payments/ocp-server/solana/memo"
	"github.com/code-payments/ocp-server/solana/system"
	timelock_token "github.com/code-payments/ocp-server/solana/timelock/v1"
	"github.com/code-payments/ocp-server/solana/token"
	"github.com/code-payments/ocp-server/solana/vm"
)

// todo: Move transaction-related stuff to the transaction utility package

type SwapHandler interface {
	// GetAlts returns the set of ALTs that should be used for v0 transaction
	// construction. It returns nil for v1 transactions.
	GetAlts(ctx context.Context) ([]solana.AddressLookupTable, error)

	// GetTransactionConfig returns the v1 transaction config, or nil when the
	// handler builds a v0 transaction. It's only valid after MakeInstructions.
	GetTransactionConfig() *solana.TransactionConfig

	// GetServerParameter gets the server parameters to return to client for the swap
	GetServerParameters() *transactionpb.StatefulSwapResponse_ServerParameters

	// MakeInstructions makes the Solana transaction instructions to perform the swap
	MakeInstructions(ctx context.Context) ([]solana.Instruction, error)
}

// makeStatefulSwapTransaction builds the unsigned swap transaction in the
// handler's transaction version. The caller sets the blockhash.
func makeStatefulSwapTransaction(ctx context.Context, h SwapHandler) (solana.Transaction, error) {
	ixns, err := h.MakeInstructions(ctx)
	if err != nil {
		return solana.Transaction{}, err
	}

	payer := common.GetSubsidizer().PublicKey().ToBytes()

	if config := h.GetTransactionConfig(); config != nil {
		return solana.NewV1Transaction(payer, *config, ixns...)
	}

	alts, err := h.GetAlts(ctx)
	if err != nil {
		return solana.Transaction{}, err
	}
	return solana.NewV0Transaction(payer, alts, ixns), nil
}

// swapMemoValue returns the memo for a swap flow, suffixed with the
// transaction version
func swapMemoValue(flow string, transactionVersion solana.MessageVersion) string {
	if transactionVersion == solana.MessageVersion1 {
		return flow + "_v1"
	}
	return flow + "_v0"
}

// swapComputeBudgetInstructions returns the ComputeBudget instructions for a
// v0 swap transaction. v1 transactions carry the compute unit limit and
// priority fee in the transaction config instead.
func swapComputeBudgetInstructions(transactionVersion solana.MessageVersion, computeUnitLimit uint32, computeUnitPrice uint64) []solana.Instruction {
	if transactionVersion == solana.MessageVersion1 {
		return nil
	}
	return []solana.Instruction{
		compute_budget.SetComputeUnitLimit(computeUnitLimit),
		compute_budget.SetComputeUnitPrice(computeUnitPrice),
	}
}

// reserveSwapTransactionConfig returns the v1 transaction config for a reserve
// swap on an existing currency, or nil for v0
func reserveSwapTransactionConfig(transactionVersion solana.MessageVersion, computeUnitLimit uint32, computeUnitPrice uint64) *solana.TransactionConfig {
	if transactionVersion != solana.MessageVersion1 {
		return nil
	}

	config := transaction_util.NewV1TransactionConfig(
		computeUnitLimit,
		computeUnitPrice,
		transaction_util.ReserveSwapLoadedAccountsDataSizeLimit,
	)
	return &config
}

// newReserveExistingCurrencyServerParameters builds the server parameters for
// a reserve swap on an existing currency. v1 sets the transaction config, and
// v0 sets the ALTs and compute budget.
func newReserveExistingCurrencyServerParameters(
	selectedNonce *transaction_util.Nonce,
	config *solana.TransactionConfig,
	alts []solana.AddressLookupTable,
	computeUnitLimit uint32,
	computeUnitPrice uint64,
	memoValue string,
	memoryAccount *common.Account,
	memoryIndex uint16,
	feeDestination *common.Account,
) *transactionpb.StatefulSwapResponse_ServerParameters {
	serverParameters := &transactionpb.StatefulSwapResponse_ServerParameters_ReserveExistingCurrencyServerParameters{
		Payer:         common.GetSubsidizer().ToProto(),
		Nonce:         selectedNonce.Account.ToProto(),
		Blockhash:     &commonpb.Blockhash{Value: selectedNonce.Blockhash[:]},
		MemoValue:     memoValue,
		MemoryAccount: memoryAccount.ToProto(),
		MemoryIndex:   uint32(memoryIndex),
	}

	if config != nil {
		serverParameters.TransactionConfig = transaction_util.ToProtoTransactionConfig(*config)
	} else {
		serverParameters.Alts = transaction_util.ToProtoAlts(alts)
		serverParameters.ComputeUnitLimit = computeUnitLimit
		serverParameters.ComputeUnitPrice = computeUnitPrice
	}

	if feeDestination != nil {
		serverParameters.FeeDestination = feeDestination.ToProto()
	}

	return &transactionpb.StatefulSwapResponse_ServerParameters{
		Kind: &transactionpb.StatefulSwapResponse_ServerParameters_ReserveExistingCurrency{
			ReserveExistingCurrency: serverParameters,
		},
	}
}

type ReserveBuySwapHandler struct {
	data ocp_data.Provider

	buyer           *common.Account
	temporaryHolder *common.Account
	mint            *common.Account
	swapAmount      uint64
	feeAmount       uint64

	alts               []solana.AddressLookupTable
	selectedNonce      *transaction_util.Nonce
	transactionVersion solana.MessageVersion
	computeUnitLimit   uint32
	computeUnitPrice   uint64
	memoValue          string

	feeDestination *common.Account
	memoryAccount  *common.Account
	memoryIndex    uint16
}

func NewReserveBuySwapHandler(
	data ocp_data.Provider,
	buyer *common.Account,
	temporaryHolder *common.Account,
	mint *common.Account,
	swapAmount, feeAmount uint64,
	selectedNonce *transaction_util.Nonce,
	transactionVersion solana.MessageVersion,
) SwapHandler {
	h := &ReserveBuySwapHandler{
		data: data,

		buyer:           buyer,
		temporaryHolder: temporaryHolder,
		mint:            mint,
		swapAmount:      swapAmount,
		feeAmount:       feeAmount,

		selectedNonce:      selectedNonce,
		transactionVersion: transactionVersion,
		computeUnitPrice:   10_000,
		memoValue:          swapMemoValue("buy", transactionVersion),
	}

	if feeAmount > 0 {
		h.feeDestination = common.CoreMintFeesAccount
	}

	return h
}

func (h *ReserveBuySwapHandler) GetAlts(ctx context.Context) ([]solana.AddressLookupTable, error) {
	if h.transactionVersion == solana.MessageVersion1 {
		return nil, nil
	}

	alt, err := transaction_util.GetAltForMint(ctx, h.data, h.mint)
	if err != nil {
		return nil, err
	}

	h.alts = []solana.AddressLookupTable{alt}

	// The fee destination is only available in the core mint's ALT
	if h.feeDestination != nil {
		h.alts = append(h.alts, transaction_util.GetAltForCoreMint())
	}

	return h.alts, nil
}

func (h *ReserveBuySwapHandler) GetTransactionConfig() *solana.TransactionConfig {
	return reserveSwapTransactionConfig(h.transactionVersion, h.computeUnitLimit, h.computeUnitPrice)
}

func (h *ReserveBuySwapHandler) GetServerParameters() *transactionpb.StatefulSwapResponse_ServerParameters {
	return newReserveExistingCurrencyServerParameters(
		h.selectedNonce,
		h.GetTransactionConfig(),
		h.alts,
		h.computeUnitLimit,
		h.computeUnitPrice,
		h.memoValue,
		h.memoryAccount,
		h.memoryIndex,
		h.feeDestination,
	)
}

func (h *ReserveBuySwapHandler) MakeInstructions(ctx context.Context) ([]solana.Instruction, error) {
	sourceVmConfig, err := common.GetVmConfigForMint(ctx, h.data, common.CoreMintAccount)
	if err != nil {
		return nil, err
	}

	sourceTimelockAccounts, err := h.buyer.GetTimelockAccounts(sourceVmConfig)
	if err != nil {
		return nil, err
	}

	destinationVmConfig, err := common.GetVmConfigForMint(ctx, h.data, h.mint)
	if err != nil {
		return nil, err
	}

	destinationTimelockAccounts, err := h.buyer.GetTimelockAccounts(destinationVmConfig)
	if err != nil {
		return nil, err
	}

	h.memoryAccount, h.memoryIndex, err = vm_util.GetVirtualTimelockAccountLocationInMemory(ctx, h.data, destinationTimelockAccounts.Vault, true)
	if err != nil {
		return nil, err
	}

	destinationCurrencyMetadataRecord, err := h.data.GetCurrencyMetadata(ctx, h.mint.PublicKey().ToBase58())
	if err != nil {
		return nil, err
	}

	destinationCurrencyAccounts, err := common.GetLaunchpadCurrencyAccounts(destinationCurrencyMetadataRecord)
	if err != nil {
		return nil, err
	}

	createTemporaryCoreMintAtaIxn, temporaryCoreMintAtaBytes, err := token.CreateAssociatedTokenAccountIdempotent(
		common.GetSubsidizer().PublicKey().ToBytes(),
		h.temporaryHolder.PublicKey().ToBytes(),
		common.CoreMintAccount.PublicKey().ToBytes(),
	)
	if err != nil {
		return nil, err
	}
	temporaryCoreMintAta, err := common.NewAccountFromPublicKeyBytes(temporaryCoreMintAtaBytes)
	if err != nil {
		return nil, err
	}

	_, temporaryCoreMintAtaBump, err := token.GetAssociatedAccountAndBump(
		h.temporaryHolder.PublicKey().ToBytes(),
		common.CoreMintAccount.PublicKey().ToBytes(),
	)
	if err != nil {
		return nil, err
	}

	var transferFromSourceVmSwapAtaIxn solana.Instruction
	if h.feeDestination != nil {
		h.computeUnitLimit = transaction_util.ReserveBuyWithFeeSwapComputeUnitLimit(temporaryCoreMintAtaBump)

		transferFromSourceVmSwapAtaIxn = vm.NewTransferForSwapWithFeeInstruction(
			&vm.TransferForSwapWithFeeInstructionAccounts{
				VmAuthority:     sourceVmConfig.Authority.PublicKey().ToBytes(),
				Vm:              sourceVmConfig.Vm.PublicKey().ToBytes(),
				Swapper:         h.buyer.PublicKey().ToBytes(),
				SwapPda:         sourceTimelockAccounts.VmSwapAccounts.Pda.PublicKey().ToBytes(),
				SwapAta:         sourceTimelockAccounts.VmSwapAccounts.Ata.PublicKey().ToBytes(),
				SwapDestination: temporaryCoreMintAta.PublicKey().ToBytes(),
				FeeDestination:  h.feeDestination.PublicKey().ToBytes(),
			},
			&vm.TransferForSwapWithFeeInstructionArgs{
				SwapAmount: h.swapAmount,
				FeeAmount:  h.feeAmount,
				Bump:       sourceTimelockAccounts.VmSwapAccounts.PdaBump,
			},
		)
	} else {
		h.computeUnitLimit = transaction_util.ReserveBuySwapComputeUnitLimit(temporaryCoreMintAtaBump)

		transferFromSourceVmSwapAtaIxn = vm.NewTransferForSwapInstruction(
			&vm.TransferForSwapInstructionAccounts{
				VmAuthority: sourceVmConfig.Authority.PublicKey().ToBytes(),
				Vm:          sourceVmConfig.Vm.PublicKey().ToBytes(),
				Swapper:     h.buyer.PublicKey().ToBytes(),
				SwapPda:     sourceTimelockAccounts.VmSwapAccounts.Pda.PublicKey().ToBytes(),
				SwapAta:     sourceTimelockAccounts.VmSwapAccounts.Ata.PublicKey().ToBytes(),
				Destination: temporaryCoreMintAta.PublicKey().ToBytes(),
			},
			&vm.TransferForSwapInstructionArgs{
				Amount: h.swapAmount,
				Bump:   sourceTimelockAccounts.VmSwapAccounts.PdaBump,
			},
		)
	}

	buyAndDepositIntoDestinationVmIxn := currencycreator.NewBuyAndDepositIntoVmInstruction(
		&currencycreator.BuyAndDepositIntoVmInstructionAccounts{
			Buyer:       h.temporaryHolder.PublicKey().ToBytes(),
			Pool:        destinationCurrencyAccounts.LiquidityPool.PublicKey().ToBytes(),
			TargetMint:  h.mint.PublicKey().ToBytes(),
			BaseMint:    common.CoreMintAccount.PublicKey().ToBytes(),
			VaultTarget: destinationCurrencyAccounts.VaultMint.PublicKey().ToBytes(),
			VaultBase:   destinationCurrencyAccounts.VaultBase.PublicKey().ToBytes(),
			BuyerBase:   temporaryCoreMintAta.PublicKey().ToBytes(),

			VmAuthority: destinationVmConfig.Authority.PublicKey().ToBytes(),
			Vm:          destinationVmConfig.Vm.PublicKey().ToBytes(),
			VmMemory:    h.memoryAccount.PublicKey().ToBytes(),
			VmOmnibus:   destinationVmConfig.Omnibus.PublicKey().ToBytes(),
			VtaOwner:    h.buyer.PublicKey().ToBytes(),
		},
		&currencycreator.BuyAndDepositIntoVmInstructionArgs{
			InAmount:      h.swapAmount,
			MinOutAmount:  0,
			VmMemoryIndex: h.memoryIndex,
		},
	)

	closeTemporaryCoreMintAtaIxn := token.CloseAccount(
		temporaryCoreMintAta.PublicKey().ToBytes(),
		common.GetSubsidizer().PublicKey().ToBytes(),
		h.temporaryHolder.PublicKey().ToBytes(),
	)

	closeSourceVmSwapAccountIfEmptyIxn := vm.NewCloseSwapAccountIfEmptyInstruction(
		&vm.CloseSwapAccountIfEmptyInstructionAccounts{
			VmAuthority: sourceVmConfig.Authority.PublicKey().ToBytes(),
			Vm:          sourceVmConfig.Vm.PublicKey().ToBytes(),
			Swapper:     h.buyer.PublicKey().ToBytes(),
			SwapPda:     sourceTimelockAccounts.VmSwapAccounts.Pda.PublicKey().ToBytes(),
			SwapAta:     sourceTimelockAccounts.VmSwapAccounts.Ata.PublicKey().ToBytes(),
			Destination: common.GetSubsidizer().PublicKey().ToBytes(),
		},
		&vm.CloseSwapAccountIfEmptyInstructionArgs{
			Bump: sourceTimelockAccounts.VmSwapAccounts.PdaBump,
		},
	)

	ixns := []solana.Instruction{
		system.AdvanceNonce(h.selectedNonce.Account.PublicKey().ToBytes(), common.GetSubsidizer().PublicKey().ToBytes()),
	}
	ixns = append(ixns, swapComputeBudgetInstructions(h.transactionVersion, h.computeUnitLimit, h.computeUnitPrice)...)
	return append(
		ixns,
		memo.Instruction(h.memoValue),
		createTemporaryCoreMintAtaIxn,
		transferFromSourceVmSwapAtaIxn,
		buyAndDepositIntoDestinationVmIxn,
		closeTemporaryCoreMintAtaIxn,
		closeSourceVmSwapAccountIfEmptyIxn,
	), nil
}

type ReserveSellSwapHandler struct {
	data ocp_data.Provider

	seller          *common.Account
	temporaryHolder *common.Account
	mint            *common.Account
	amount          uint64

	alts               []solana.AddressLookupTable
	selectedNonce      *transaction_util.Nonce
	transactionVersion solana.MessageVersion
	computeUnitLimit   uint32
	computeUnitPrice   uint64
	memoValue          string

	memoryAccount *common.Account
	memoryIndex   uint16
}

func NewReserveSellSwapHandler(
	data ocp_data.Provider,
	seller *common.Account,
	temporaryHolder *common.Account,
	mint *common.Account,
	amount uint64,
	selectedNonce *transaction_util.Nonce,
	transactionVersion solana.MessageVersion,
) SwapHandler {
	return &ReserveSellSwapHandler{
		data: data,

		seller:          seller,
		temporaryHolder: temporaryHolder,
		mint:            mint,
		amount:          amount,

		selectedNonce:      selectedNonce,
		transactionVersion: transactionVersion,
		computeUnitPrice:   10_000,
		memoValue:          swapMemoValue("sell", transactionVersion),
	}
}

func (h *ReserveSellSwapHandler) GetAlts(ctx context.Context) ([]solana.AddressLookupTable, error) {
	if h.transactionVersion == solana.MessageVersion1 {
		return nil, nil
	}

	alt, err := transaction_util.GetAltForMint(ctx, h.data, h.mint)
	if err != nil {
		return nil, err
	}
	h.alts = []solana.AddressLookupTable{alt}
	return h.alts, nil
}

func (h *ReserveSellSwapHandler) GetTransactionConfig() *solana.TransactionConfig {
	return reserveSwapTransactionConfig(h.transactionVersion, h.computeUnitLimit, h.computeUnitPrice)
}

func (h *ReserveSellSwapHandler) GetServerParameters() *transactionpb.StatefulSwapResponse_ServerParameters {
	return newReserveExistingCurrencyServerParameters(
		h.selectedNonce,
		h.GetTransactionConfig(),
		h.alts,
		h.computeUnitLimit,
		h.computeUnitPrice,
		h.memoValue,
		h.memoryAccount,
		h.memoryIndex,
		nil,
	)
}

func (h *ReserveSellSwapHandler) MakeInstructions(ctx context.Context) ([]solana.Instruction, error) {
	sourceVmConfig, err := common.GetVmConfigForMint(ctx, h.data, h.mint)
	if err != nil {
		return nil, err
	}

	sourceCurrencyMetadataRecord, err := h.data.GetCurrencyMetadata(ctx, h.mint.PublicKey().ToBase58())
	if err != nil {
		return nil, err
	}

	sourceCurrencyAccounts, err := common.GetLaunchpadCurrencyAccounts(sourceCurrencyMetadataRecord)
	if err != nil {
		return nil, err
	}

	sourceTimelockAccounts, err := h.seller.GetTimelockAccounts(sourceVmConfig)
	if err != nil {
		return nil, err
	}

	destinationVmConfig, err := common.GetVmConfigForMint(ctx, h.data, common.CoreMintAccount)
	if err != nil {
		return nil, err
	}

	destinationTimelockAccounts, err := h.seller.GetTimelockAccounts(destinationVmConfig)
	if err != nil {
		return nil, err
	}

	h.memoryAccount, h.memoryIndex, err = vm_util.GetVirtualTimelockAccountLocationInMemory(ctx, h.data, destinationTimelockAccounts.Vault, true)
	if err != nil {
		return nil, err
	}

	createTemporarySourceCurrencyAtaIxn, temporarySourceCurrencyAtaBytes, err := token.CreateAssociatedTokenAccountIdempotent(
		common.GetSubsidizer().PublicKey().ToBytes(),
		h.temporaryHolder.PublicKey().ToBytes(),
		h.mint.PublicKey().ToBytes(),
	)
	if err != nil {
		return nil, err
	}
	temporarySourceCurrencyAta, err := common.NewAccountFromPublicKeyBytes(temporarySourceCurrencyAtaBytes)
	if err != nil {
		return nil, err
	}

	_, temporarySourceCurrencyAtaBump, err := token.GetAssociatedAccountAndBump(
		h.temporaryHolder.PublicKey().ToBytes(),
		h.mint.PublicKey().ToBytes(),
	)
	if err != nil {
		return nil, err
	}
	h.computeUnitLimit = transaction_util.ReserveSellSwapComputeUnitLimit(temporarySourceCurrencyAtaBump)

	transferFromSourceVmSwapAtaIxn := vm.NewTransferForSwapInstruction(
		&vm.TransferForSwapInstructionAccounts{
			VmAuthority: sourceVmConfig.Authority.PublicKey().ToBytes(),
			Vm:          sourceVmConfig.Vm.PublicKey().ToBytes(),
			Swapper:     h.seller.PublicKey().ToBytes(),
			SwapPda:     sourceTimelockAccounts.VmSwapAccounts.Pda.PublicKey().ToBytes(),
			SwapAta:     sourceTimelockAccounts.VmSwapAccounts.Ata.PublicKey().ToBytes(),
			Destination: temporarySourceCurrencyAta.PublicKey().ToBytes(),
		},
		&vm.TransferForSwapInstructionArgs{
			Amount: h.amount,
			Bump:   sourceTimelockAccounts.VmSwapAccounts.PdaBump,
		},
	)

	sellAndDepositIntoDestinationVmIxn := currencycreator.NewSellAndDepositIntoVmInstruction(
		&currencycreator.SellAndDepositIntoVmInstructionAccounts{
			Seller:       h.temporaryHolder.PublicKey().ToBytes(),
			Pool:         sourceCurrencyAccounts.LiquidityPool.PublicKey().ToBytes(),
			TargetMint:   h.mint.PublicKey().ToBytes(),
			BaseMint:     common.CoreMintAccount.PublicKey().ToBytes(),
			VaultTarget:  sourceCurrencyAccounts.VaultMint.PublicKey().ToBytes(),
			VaultBase:    sourceCurrencyAccounts.VaultBase.PublicKey().ToBytes(),
			SellerTarget: temporarySourceCurrencyAta.PublicKey().ToBytes(),

			VmAuthority: destinationVmConfig.Authority.PublicKey().ToBytes(),
			Vm:          destinationVmConfig.Vm.PublicKey().ToBytes(),
			VmMemory:    h.memoryAccount.PublicKey().ToBytes(),
			VmOmnibus:   destinationVmConfig.Omnibus.PublicKey().ToBytes(),
			VtaOwner:    h.seller.PublicKey().ToBytes(),
		},
		&currencycreator.SellAndDepositIntoVmInstructionArgs{
			InAmount:      h.amount,
			MinOutAmount:  0,
			VmMemoryIndex: h.memoryIndex,
		},
	)

	closeTemporarySourceCurrencyAtaIxn := token.CloseAccount(
		temporarySourceCurrencyAta.PublicKey().ToBytes(),
		common.GetSubsidizer().PublicKey().ToBytes(),
		h.temporaryHolder.PublicKey().ToBytes(),
	)

	closeSourceVmSwapAccountIfEmptyIxn := vm.NewCloseSwapAccountIfEmptyInstruction(
		&vm.CloseSwapAccountIfEmptyInstructionAccounts{
			VmAuthority: sourceVmConfig.Authority.PublicKey().ToBytes(),
			Vm:          sourceVmConfig.Vm.PublicKey().ToBytes(),
			Swapper:     h.seller.PublicKey().ToBytes(),
			SwapPda:     sourceTimelockAccounts.VmSwapAccounts.Pda.PublicKey().ToBytes(),
			SwapAta:     sourceTimelockAccounts.VmSwapAccounts.Ata.PublicKey().ToBytes(),
			Destination: common.GetSubsidizer().PublicKey().ToBytes(),
		},
		&vm.CloseSwapAccountIfEmptyInstructionArgs{
			Bump: sourceTimelockAccounts.VmSwapAccounts.PdaBump,
		},
	)

	ixns := []solana.Instruction{
		system.AdvanceNonce(h.selectedNonce.Account.PublicKey().ToBytes(), common.GetSubsidizer().PublicKey().ToBytes()),
	}
	ixns = append(ixns, swapComputeBudgetInstructions(h.transactionVersion, h.computeUnitLimit, h.computeUnitPrice)...)
	return append(
		ixns,
		memo.Instruction(h.memoValue),
		createTemporarySourceCurrencyAtaIxn,
		transferFromSourceVmSwapAtaIxn,
		sellAndDepositIntoDestinationVmIxn,
		closeTemporarySourceCurrencyAtaIxn,
		closeSourceVmSwapAccountIfEmptyIxn,
	), nil
}

type ReserveBuySellSwapHandler struct {
	data ocp_data.Provider

	swapper         *common.Account
	temporaryHolder *common.Account
	fromMint        *common.Account
	toMint          *common.Account
	amount          uint64

	alts               []solana.AddressLookupTable
	selectedNonce      *transaction_util.Nonce
	transactionVersion solana.MessageVersion
	computeUnitLimit   uint32
	computeUnitPrice   uint64
	memoValue          string

	memoryAccount *common.Account
	memoryIndex   uint16
}

func NewReserveBuySellSwapHandler(
	data ocp_data.Provider,
	swapper *common.Account,
	temporaryHolder *common.Account,
	fromMint *common.Account,
	toMint *common.Account,
	amount uint64,
	selectedNonce *transaction_util.Nonce,
	transactionVersion solana.MessageVersion,
) SwapHandler {
	return &ReserveBuySellSwapHandler{
		data: data,

		swapper:         swapper,
		temporaryHolder: temporaryHolder,
		fromMint:        fromMint,
		toMint:          toMint,
		amount:          amount,

		selectedNonce:      selectedNonce,
		transactionVersion: transactionVersion,
		computeUnitPrice:   10_000,
		memoValue:          swapMemoValue("buy_sell", transactionVersion),
	}
}

func (h *ReserveBuySellSwapHandler) GetAlts(ctx context.Context) ([]solana.AddressLookupTable, error) {
	if h.transactionVersion == solana.MessageVersion1 {
		return nil, nil
	}

	alt1, err := transaction_util.GetAltForMint(ctx, h.data, h.fromMint)
	if err != nil {
		return nil, err
	}
	alt2, err := transaction_util.GetAltForMint(ctx, h.data, h.toMint)
	if err != nil {
		return nil, err
	}
	h.alts = []solana.AddressLookupTable{alt1, alt2}
	return h.alts, nil
}

func (h *ReserveBuySellSwapHandler) GetTransactionConfig() *solana.TransactionConfig {
	return reserveSwapTransactionConfig(h.transactionVersion, h.computeUnitLimit, h.computeUnitPrice)
}

func (h *ReserveBuySellSwapHandler) GetServerParameters() *transactionpb.StatefulSwapResponse_ServerParameters {
	return newReserveExistingCurrencyServerParameters(
		h.selectedNonce,
		h.GetTransactionConfig(),
		h.alts,
		h.computeUnitLimit,
		h.computeUnitPrice,
		h.memoValue,
		h.memoryAccount,
		h.memoryIndex,
		nil,
	)
}

func (h *ReserveBuySellSwapHandler) MakeInstructions(ctx context.Context) ([]solana.Instruction, error) {
	sourceVmConfig, err := common.GetVmConfigForMint(ctx, h.data, h.fromMint)
	if err != nil {
		return nil, err
	}

	sourceCurrencyMetadataRecord, err := h.data.GetCurrencyMetadata(ctx, h.fromMint.PublicKey().ToBase58())
	if err != nil {
		return nil, err
	}

	sourceCurrencyAccounts, err := common.GetLaunchpadCurrencyAccounts(sourceCurrencyMetadataRecord)
	if err != nil {
		return nil, err
	}

	sourceTimelockAccounts, err := h.swapper.GetTimelockAccounts(sourceVmConfig)
	if err != nil {
		return nil, err
	}

	destinationVmConfig, err := common.GetVmConfigForMint(ctx, h.data, h.toMint)
	if err != nil {
		return nil, err
	}

	destinationTimelockAccounts, err := h.swapper.GetTimelockAccounts(destinationVmConfig)
	if err != nil {
		return nil, err
	}

	h.memoryAccount, h.memoryIndex, err = vm_util.GetVirtualTimelockAccountLocationInMemory(ctx, h.data, destinationTimelockAccounts.Vault, true)
	if err != nil {
		return nil, err
	}

	destinationCurrencyMetadataRecord, err := h.data.GetCurrencyMetadata(ctx, h.toMint.PublicKey().ToBase58())
	if err != nil {
		return nil, err
	}

	destinationCurrencyAccounts, err := common.GetLaunchpadCurrencyAccounts(destinationCurrencyMetadataRecord)
	if err != nil {
		return nil, err
	}

	createTemporaryCoreMintAtaIxn, temporaryCoreMintAtaBytes, err := token.CreateAssociatedTokenAccountIdempotent(
		common.GetSubsidizer().PublicKey().ToBytes(),
		h.temporaryHolder.PublicKey().ToBytes(),
		common.CoreMintAccount.PublicKey().ToBytes(),
	)
	if err != nil {
		return nil, err
	}
	temporaryCoreMintAta, err := common.NewAccountFromPublicKeyBytes(temporaryCoreMintAtaBytes)
	if err != nil {
		return nil, err
	}

	createTemporarySourceCurrencyAtaIxn, temporarySourceCurrencyAtaBytes, err := token.CreateAssociatedTokenAccountIdempotent(
		common.GetSubsidizer().PublicKey().ToBytes(),
		h.temporaryHolder.PublicKey().ToBytes(),
		h.fromMint.PublicKey().ToBytes(),
	)
	if err != nil {
		return nil, err
	}
	temporarySourceCurrencyAta, err := common.NewAccountFromPublicKeyBytes(temporarySourceCurrencyAtaBytes)
	if err != nil {
		return nil, err
	}

	_, temporaryCoreMintAtaBump, err := token.GetAssociatedAccountAndBump(
		h.temporaryHolder.PublicKey().ToBytes(),
		common.CoreMintAccount.PublicKey().ToBytes(),
	)
	if err != nil {
		return nil, err
	}
	_, temporarySourceCurrencyAtaBump, err := token.GetAssociatedAccountAndBump(
		h.temporaryHolder.PublicKey().ToBytes(),
		h.fromMint.PublicKey().ToBytes(),
	)
	if err != nil {
		return nil, err
	}
	h.computeUnitLimit = transaction_util.ReserveBuySellSwapComputeUnitLimit(temporaryCoreMintAtaBump, temporarySourceCurrencyAtaBump)

	transferFromSourceVmSwapAtaIxn := vm.NewTransferForSwapInstruction(
		&vm.TransferForSwapInstructionAccounts{
			VmAuthority: sourceVmConfig.Authority.PublicKey().ToBytes(),
			Vm:          sourceVmConfig.Vm.PublicKey().ToBytes(),
			Swapper:     h.swapper.PublicKey().ToBytes(),
			SwapPda:     sourceTimelockAccounts.VmSwapAccounts.Pda.PublicKey().ToBytes(),
			SwapAta:     sourceTimelockAccounts.VmSwapAccounts.Ata.PublicKey().ToBytes(),
			Destination: temporarySourceCurrencyAta.PublicKey().ToBytes(),
		},
		&vm.TransferForSwapInstructionArgs{
			Amount: h.amount,
			Bump:   sourceTimelockAccounts.VmSwapAccounts.PdaBump,
		},
	)

	sellIxn := currencycreator.NewSellTokensInstruction(
		&currencycreator.SellTokensInstructionAccounts{
			Seller:       h.temporaryHolder.PublicKey().ToBytes(),
			Pool:         sourceCurrencyAccounts.LiquidityPool.PublicKey().ToBytes(),
			TargetMint:   h.fromMint.PublicKey().ToBytes(),
			BaseMint:     common.CoreMintAccount.PublicKey().ToBytes(),
			VaultTarget:  sourceCurrencyAccounts.VaultMint.PublicKey().ToBytes(),
			VaultBase:    sourceCurrencyAccounts.VaultBase.PublicKey().ToBytes(),
			SellerTarget: temporarySourceCurrencyAta.PublicKey().ToBytes(),
			SellerBase:   temporaryCoreMintAta.PublicKey().ToBytes(),
		},
		&currencycreator.SellTokensInstructionArgs{
			InAmount:     h.amount,
			MinAmountOut: 0,
		},
	)

	buyAndDepositIntoDestinationVmIxn := currencycreator.NewBuyAndDepositIntoVmInstruction(
		&currencycreator.BuyAndDepositIntoVmInstructionAccounts{
			Buyer:       h.temporaryHolder.PublicKey().ToBytes(),
			Pool:        destinationCurrencyAccounts.LiquidityPool.PublicKey().ToBytes(),
			TargetMint:  h.toMint.PublicKey().ToBytes(),
			BaseMint:    common.CoreMintAccount.PublicKey().ToBytes(),
			VaultTarget: destinationCurrencyAccounts.VaultMint.PublicKey().ToBytes(),
			VaultBase:   destinationCurrencyAccounts.VaultBase.PublicKey().ToBytes(),
			BuyerBase:   temporaryCoreMintAta.PublicKey().ToBytes(),

			VmAuthority: destinationVmConfig.Authority.PublicKey().ToBytes(),
			Vm:          destinationVmConfig.Vm.PublicKey().ToBytes(),
			VmMemory:    h.memoryAccount.PublicKey().ToBytes(),
			VmOmnibus:   destinationVmConfig.Omnibus.PublicKey().ToBytes(),
			VtaOwner:    h.swapper.PublicKey().ToBytes(),
		},
		&currencycreator.BuyAndDepositIntoVmInstructionArgs{
			InAmount:      0,
			MinOutAmount:  0,
			VmMemoryIndex: h.memoryIndex,
		},
	)

	closeTemporaryCoreMintAtaIxn := token.CloseAccount(
		temporaryCoreMintAta.PublicKey().ToBytes(),
		common.GetSubsidizer().PublicKey().ToBytes(),
		h.temporaryHolder.PublicKey().ToBytes(),
	)

	closeTemporarySourceCurrencyAtaIxn := token.CloseAccount(
		temporarySourceCurrencyAta.PublicKey().ToBytes(),
		common.GetSubsidizer().PublicKey().ToBytes(),
		h.temporaryHolder.PublicKey().ToBytes(),
	)

	closeSourceVmSwapAccountIfEmptyIxn := vm.NewCloseSwapAccountIfEmptyInstruction(
		&vm.CloseSwapAccountIfEmptyInstructionAccounts{
			VmAuthority: sourceVmConfig.Authority.PublicKey().ToBytes(),
			Vm:          sourceVmConfig.Vm.PublicKey().ToBytes(),
			Swapper:     h.swapper.PublicKey().ToBytes(),
			SwapPda:     sourceTimelockAccounts.VmSwapAccounts.Pda.PublicKey().ToBytes(),
			SwapAta:     sourceTimelockAccounts.VmSwapAccounts.Ata.PublicKey().ToBytes(),
			Destination: common.GetSubsidizer().PublicKey().ToBytes(),
		},
		&vm.CloseSwapAccountIfEmptyInstructionArgs{
			Bump: sourceTimelockAccounts.VmSwapAccounts.PdaBump,
		},
	)

	ixns := []solana.Instruction{
		system.AdvanceNonce(h.selectedNonce.Account.PublicKey().ToBytes(), common.GetSubsidizer().PublicKey().ToBytes()),
	}
	ixns = append(ixns, swapComputeBudgetInstructions(h.transactionVersion, h.computeUnitLimit, h.computeUnitPrice)...)
	return append(
		ixns,
		memo.Instruction(h.memoValue),
		createTemporaryCoreMintAtaIxn,
		createTemporarySourceCurrencyAtaIxn,
		transferFromSourceVmSwapAtaIxn,
		sellIxn,
		buyAndDepositIntoDestinationVmIxn,
		closeTemporaryCoreMintAtaIxn,
		closeTemporarySourceCurrencyAtaIxn,
		closeSourceVmSwapAccountIfEmptyIxn,
	), nil
}

// loadCurrencyLaunch loads the new currency's launch, which the create-and-buy
// handlers build their initialization instructions from
func loadCurrencyLaunch(ctx context.Context, data ocp_data.Provider, mint *common.Account) (*transaction_util.CurrencyLaunch, error) {
	metadataRecord, err := data.GetCurrencyMetadata(ctx, mint.PublicKey().ToBase58())
	if err != nil {
		return nil, err
	}

	// The VM is not supported yet, so we need to work around GetVmConfigForMint
	vmMetadataRecord, err := data.GetVmMetadataByMint(ctx, mint.PublicKey().ToBase58())
	if err != nil {
		return nil, err
	}

	return transaction_util.NewCurrencyLaunch(metadataRecord, vmMetadataRecord)
}

// newReserveNewCurrencyServerParameters builds the server parameters shared by
// every create-and-buy flow
func newReserveNewCurrencyServerParameters(
	launch *transaction_util.CurrencyLaunch,
	selectedNonce *transaction_util.Nonce,
) *transactionpb.StatefulSwapResponse_ServerParameters_ReserveNewCurrencyServerParameter {
	return &transactionpb.StatefulSwapResponse_ServerParameters_ReserveNewCurrencyServerParameter{
		Payer:                common.GetSubsidizer().ToProto(),
		Nonce:                selectedNonce.Account.ToProto(),
		Blockhash:            &commonpb.Blockhash{Value: selectedNonce.Blockhash[:]},
		MemoValue:            "",
		Authority:            launch.Accounts.Authority.ToProto(),
		Name:                 launch.Name,
		Symbol:               launch.Symbol,
		Seed:                 launch.Accounts.Seed.ToProto(),
		SellFeeBps:           currencycreator.DefaultSellFeeBps,
		VmLockDurationInDays: uint32(timelock_token.DefaultNumDaysLocked),
		FeeDestination:       common.CoreMintFeesAccount.ToProto(),
		FeeAccountOwner:      launch.FeeAccountOwner.ToProto(),
	}
}

// newReserveNewCurrencyV1ServerParameters builds the server parameters for a
// v1 create-and-buy flow
func newReserveNewCurrencyV1ServerParameters(
	launch *transaction_util.CurrencyLaunch,
	selectedNonce *transaction_util.Nonce,
	config solana.TransactionConfig,
) *transactionpb.StatefulSwapResponse_ServerParameters_ReserveNewCurrencyServerParameter {
	serverParameters := newReserveNewCurrencyServerParameters(launch, selectedNonce)
	serverParameters.TransactionConfig = transaction_util.ToProtoTransactionConfig(config)
	for _, memoryAccount := range launch.MemoryAccounts {
		serverParameters.MemoryAccounts = append(serverParameters.MemoryAccounts, &transactionpb.StatefulSwapResponse_ServerParameters_ReserveNewCurrencyServerParameter_VmMemoryAccountInitialization{
			Address:           memoryAccount.Address.ToProto(),
			Name:              memoryAccount.Name,
			NumAccounts:       memoryAccount.NumAccounts,
			AccountSize:       uint32(memoryAccount.AccountSize),
			ResizeSizes:       memoryAccount.ResizeSizes(),
			RentTopUpLamports: memoryAccount.RentTopUpLamports,
		})
	}
	return serverParameters
}

// makeCoreMintFundedPurchaseInstructions makes the instructions for a new
// currency's initial purchase funded by the buyer's core mint: the temporary
// core mint and VM deposit ATAs' creation, VM::TransferForSwapWithFee,
// Reserve::BuyTokens, then closing the temporary core mint ATA. The currency
// authority pays for the ATAs. It returns the ATAs' bumps.
//
// The temporary holder owns the temporary core mint ATA and signs the buy and
// the close. Closing fails if anyone pre-funds the ATA, so the holder must be
// a key whose ATA can't be known ahead of the transaction, like a per-swap
// swap authority.
func makeCoreMintFundedPurchaseInstructions(
	launch *transaction_util.CurrencyLaunch,
	buyer *common.Account,
	temporaryHolder *common.Account,
	sourceVmConfig *common.VmConfig,
	swapAmount, feeAmount uint64,
) ([]solana.Instruction, []uint8, error) {
	accounts := launch.Accounts

	buyerVmSwapAccounts, err := buyer.GetVmSwapAccounts(sourceVmConfig)
	if err != nil {
		return nil, nil, err
	}

	buyerVmDepositAccounts, err := buyer.GetVmDepositAccounts(launch.VmConfig)
	if err != nil {
		return nil, nil, err
	}

	createTemporaryCoreMintAtaIxn, temporaryCoreMintAta, temporaryCoreMintAtaBump, err := launch.MakeCreateAtaInstruction(temporaryHolder, common.CoreMintAccount)
	if err != nil {
		return nil, nil, err
	}

	createVmDepositAtaIxn, _, vmDepositAtaBump, err := launch.MakeCreateAtaInstruction(buyerVmDepositAccounts.Pda, accounts.Mint)
	if err != nil {
		return nil, nil, err
	}

	transferForSwapWithFeeIxn := vm.NewTransferForSwapWithFeeInstruction(
		&vm.TransferForSwapWithFeeInstructionAccounts{
			VmAuthority:     common.GetSubsidizer().PublicKey().ToBytes(),
			Vm:              common.CoreMintVmAccount.PublicKey().ToBytes(),
			Swapper:         buyer.PublicKey().ToBytes(),
			SwapPda:         buyerVmSwapAccounts.Pda.PublicKey().ToBytes(),
			SwapAta:         buyerVmSwapAccounts.Ata.PublicKey().ToBytes(),
			SwapDestination: temporaryCoreMintAta,
			FeeDestination:  common.CoreMintFeesAccount.PublicKey().ToBytes(),
		},
		&vm.TransferForSwapWithFeeInstructionArgs{
			SwapAmount: swapAmount,
			FeeAmount:  feeAmount,
			Bump:       buyerVmSwapAccounts.PdaBump,
		},
	)

	buyIxn := currencycreator.NewBuyTokensInstruction(
		&currencycreator.BuyTokensInstructionAccounts{
			Buyer:       temporaryHolder.PublicKey().ToBytes(),
			Pool:        accounts.LiquidityPool.PublicKey().ToBytes(),
			TargetMint:  accounts.Mint.PublicKey().ToBytes(),
			BaseMint:    common.CoreMintAccount.PublicKey().ToBytes(),
			VaultTarget: accounts.VaultMint.PublicKey().ToBytes(),
			VaultBase:   accounts.VaultBase.PublicKey().ToBytes(),
			BuyerTarget: buyerVmDepositAccounts.Ata.PublicKey().ToBytes(),
			BuyerBase:   temporaryCoreMintAta,
		},
		&currencycreator.BuyTokensInstructionArgs{
			InAmount:     swapAmount,
			MinAmountOut: 0,
		},
	)

	closeTemporaryCoreMintAta := token.CloseAccount(
		temporaryCoreMintAta,
		accounts.Authority.PublicKey().ToBytes(),
		temporaryHolder.PublicKey().ToBytes(),
	)

	ixns := []solana.Instruction{
		createTemporaryCoreMintAtaIxn,
		createVmDepositAtaIxn,
		transferForSwapWithFeeIxn,
		buyIxn,
		closeTemporaryCoreMintAta,
	}
	return ixns, []uint8{temporaryCoreMintAtaBump, vmDepositAtaBump}, nil
}

// treasuryFundedPurchase holds what a new currency's initial purchase funded
// by the server-controlled treasury needs
type treasuryFundedPurchase struct {
	launch                 *transaction_util.CurrencyLaunch
	buyer                  *common.Account
	fromMint               *common.Account
	swapAmount             uint64
	feeAmount              uint64
	treasury               *common.Account
	purchaseCoreQuarks     uint64
	sourceVmConfig         *common.VmConfig
	sourceCurrencyAccounts *common.LaunchpadCurrencyAccounts
}

func loadTreasuryFundedPurchase(
	ctx context.Context,
	data ocp_data.Provider,
	buyer *common.Account,
	fromMint *common.Account,
	toMint *common.Account,
	swapAmount, feeAmount uint64,
	treasury *common.Account,
	purchaseCoreQuarks uint64,
) (*treasuryFundedPurchase, error) {
	sourceVmConfig, err := common.GetVmConfigForMint(ctx, data, fromMint)
	if err != nil {
		return nil, err
	}

	sourceCurrencyMetadataRecord, err := data.GetCurrencyMetadata(ctx, fromMint.PublicKey().ToBase58())
	if err != nil {
		return nil, err
	}

	sourceCurrencyAccounts, err := common.GetLaunchpadCurrencyAccounts(sourceCurrencyMetadataRecord)
	if err != nil {
		return nil, err
	}

	launch, err := loadCurrencyLaunch(ctx, data, toMint)
	if err != nil {
		return nil, err
	}

	return &treasuryFundedPurchase{
		launch:                 launch,
		buyer:                  buyer,
		fromMint:               fromMint,
		swapAmount:             swapAmount,
		feeAmount:              feeAmount,
		treasury:               treasury,
		purchaseCoreQuarks:     purchaseCoreQuarks,
		sourceVmConfig:         sourceVmConfig,
		sourceCurrencyAccounts: sourceCurrencyAccounts,
	}, nil
}

func (p *treasuryFundedPurchase) serverParameters(serverParameters *transactionpb.StatefulSwapResponse_ServerParameters_ReserveNewCurrencyServerParameter) *transactionpb.StatefulSwapResponse_ServerParameters {
	serverParameters.Treasury = p.treasury.ToProto()
	serverParameters.TreasuryPurchaseAmount = p.purchaseCoreQuarks

	return &transactionpb.StatefulSwapResponse_ServerParameters{
		Kind: &transactionpb.StatefulSwapResponse_ServerParameters_ReserveNewCurrency{
			ReserveNewCurrency: serverParameters,
		},
	}
}

// makeInstructions makes the purchase's instructions: the treasury's from mint
// ATA's creation, paid for by ataPayer, VM::TransferForSwapWithFee into it,
// Reserve::SellTokens of the from mint, then Reserve::BuyTokens of the new
// currency. It returns the treasury's from mint ATA's bump.
func (p *treasuryFundedPurchase) makeInstructions(ataPayer *common.Account) ([]solana.Instruction, uint8, error) {
	accounts := p.launch.Accounts

	buyerVmSwapAccounts, err := p.buyer.GetVmSwapAccounts(p.sourceVmConfig)
	if err != nil {
		return nil, 0, err
	}

	buyerVmDepositAccounts, err := p.buyer.GetVmDepositAccounts(p.launch.VmConfig)
	if err != nil {
		return nil, 0, err
	}

	treasuryCoreMintAtaAddress, err := token.GetAssociatedAccount(p.treasury.PublicKey().ToBytes(), common.CoreMintAccount.PublicKey().ToBytes())
	if err != nil {
		return nil, 0, err
	}

	createTreasuryFromMintAtaIxn, treasuryFromMintAtaAddress, err := token.CreateAssociatedTokenAccountIdempotent(
		ataPayer.PublicKey().ToBytes(),
		p.treasury.PublicKey().ToBytes(),
		p.fromMint.PublicKey().ToBytes(),
	)
	if err != nil {
		return nil, 0, err
	}

	_, treasuryFromMintAtaBump, err := token.GetAssociatedAccountAndBump(p.treasury.PublicKey().ToBytes(), p.fromMint.PublicKey().ToBytes())
	if err != nil {
		return nil, 0, err
	}

	// The entire swap and fee amount is collected by the treasury, then converted
	// into core mint sent to the fee destination
	transferForSwapWithFeeIxn := vm.NewTransferForSwapWithFeeInstruction(
		&vm.TransferForSwapWithFeeInstructionAccounts{
			VmAuthority:     p.sourceVmConfig.Authority.PublicKey().ToBytes(),
			Vm:              p.sourceVmConfig.Vm.PublicKey().ToBytes(),
			Swapper:         p.buyer.PublicKey().ToBytes(),
			SwapPda:         buyerVmSwapAccounts.Pda.PublicKey().ToBytes(),
			SwapAta:         buyerVmSwapAccounts.Ata.PublicKey().ToBytes(),
			SwapDestination: treasuryFromMintAtaAddress,
			FeeDestination:  treasuryFromMintAtaAddress,
		},
		&vm.TransferForSwapWithFeeInstructionArgs{
			SwapAmount: p.swapAmount,
			FeeAmount:  p.feeAmount,
			Bump:       buyerVmSwapAccounts.PdaBump,
		},
	)

	sellIxn := currencycreator.NewSellTokensInstruction(
		&currencycreator.SellTokensInstructionAccounts{
			Seller:       p.treasury.PublicKey().ToBytes(),
			Pool:         p.sourceCurrencyAccounts.LiquidityPool.PublicKey().ToBytes(),
			TargetMint:   p.fromMint.PublicKey().ToBytes(),
			BaseMint:     common.CoreMintAccount.PublicKey().ToBytes(),
			VaultTarget:  p.sourceCurrencyAccounts.VaultMint.PublicKey().ToBytes(),
			VaultBase:    p.sourceCurrencyAccounts.VaultBase.PublicKey().ToBytes(),
			SellerTarget: treasuryFromMintAtaAddress,
			SellerBase:   common.CoreMintFeesAccount.PublicKey().ToBytes(),
		},
		&currencycreator.SellTokensInstructionArgs{
			InAmount:     p.swapAmount + p.feeAmount,
			MinAmountOut: 0,
		},
	)

	// The purchase is funded by the server-controlled treasury holding core mint
	// tokens, which guarantees the full purchase value regardless of from mint
	// price volatility
	buyIxn := currencycreator.NewBuyTokensInstruction(
		&currencycreator.BuyTokensInstructionAccounts{
			Buyer:       p.treasury.PublicKey().ToBytes(),
			Pool:        accounts.LiquidityPool.PublicKey().ToBytes(),
			TargetMint:  accounts.Mint.PublicKey().ToBytes(),
			BaseMint:    common.CoreMintAccount.PublicKey().ToBytes(),
			VaultTarget: accounts.VaultMint.PublicKey().ToBytes(),
			VaultBase:   accounts.VaultBase.PublicKey().ToBytes(),
			BuyerTarget: buyerVmDepositAccounts.Ata.PublicKey().ToBytes(),
			BuyerBase:   treasuryCoreMintAtaAddress,
		},
		&currencycreator.BuyTokensInstructionArgs{
			InAmount:     p.purchaseCoreQuarks,
			MinAmountOut: 0,
		},
	)

	ixns := []solana.Instruction{
		createTreasuryFromMintAtaIxn,
		transferForSwapWithFeeIxn,
		sellIxn,
		buyIxn,
	}
	return ixns, treasuryFromMintAtaBump, nil
}

// ReserveCreateAndBuySwapHandler launches a new currency within a v1 initial
// purchase funded by the buyer's core mint. The transaction initializes every
// currency account, so the launch is atomic. The swap authority holds the
// temporary core mint ATA, so it must not be the buyer.
type ReserveCreateAndBuySwapHandler struct {
	buyer         *common.Account
	swapAuthority *common.Account
	swapAmount    uint64
	feeAmount     uint64

	selectedNonce    *transaction_util.Nonce
	computeUnitLimit uint32
	computeUnitPrice uint64

	sourceVmConfig *common.VmConfig
	launch         *transaction_util.CurrencyLaunch
}

func NewReserveCreateAndBuySwapHandler(
	ctx context.Context,
	data ocp_data.Provider,
	buyer *common.Account,
	swapAuthority *common.Account,
	mint *common.Account,
	swapAmount, feeAmount uint64,
	selectedNonce *transaction_util.Nonce,
) (SwapHandler, error) {
	var err error

	if bytes.Equal(buyer.PublicKey().ToBytes(), swapAuthority.PublicKey().ToBytes()) {
		return nil, errors.New("buyer cannot be the swap authority")
	}

	h := &ReserveCreateAndBuySwapHandler{
		buyer:         buyer,
		swapAuthority: swapAuthority,
		swapAmount:    swapAmount,
		feeAmount:     feeAmount,

		selectedNonce:    selectedNonce,
		computeUnitPrice: 10_000,
	}

	h.sourceVmConfig, err = common.GetVmConfigForMint(ctx, data, common.CoreMintAccount)
	if err != nil {
		return nil, err
	}

	h.launch, err = loadCurrencyLaunch(ctx, data, mint)
	if err != nil {
		return nil, err
	}

	err = h.launch.SetMemoryAccountRentTopUps(ctx, data.GetBlockchainMinimumBalanceForRentExemption)
	if err != nil {
		return nil, err
	}

	return h, nil
}

func (h *ReserveCreateAndBuySwapHandler) GetTransactionConfig() *solana.TransactionConfig {
	config := h.launch.TransactionConfig(h.computeUnitLimit, h.computeUnitPrice)
	return &config
}

func (h *ReserveCreateAndBuySwapHandler) GetAlts(ctx context.Context) ([]solana.AddressLookupTable, error) {
	return nil, nil
}

func (h *ReserveCreateAndBuySwapHandler) GetServerParameters() *transactionpb.StatefulSwapResponse_ServerParameters {
	return &transactionpb.StatefulSwapResponse_ServerParameters{
		Kind: &transactionpb.StatefulSwapResponse_ServerParameters_ReserveNewCurrency{
			ReserveNewCurrency: newReserveNewCurrencyV1ServerParameters(h.launch, h.selectedNonce, *h.GetTransactionConfig()),
		},
	}
}

func (h *ReserveCreateAndBuySwapHandler) MakeInstructions(ctx context.Context) ([]solana.Instruction, error) {
	err := h.launch.ValidateInstructionTraceLength(false)
	if err != nil {
		return nil, err
	}

	purchaseIxns, purchaseAtaBumps, err := makeCoreMintFundedPurchaseInstructions(h.launch, h.buyer, h.swapAuthority, h.sourceVmConfig, h.swapAmount, h.feeAmount)
	if err != nil {
		return nil, err
	}

	postPurchaseIxns, feeAtaBump, err := h.launch.MakePostPurchaseInstructions()
	if err != nil {
		return nil, err
	}

	ixns := []solana.Instruction{
		system.AdvanceNonce(h.selectedNonce.Account.PublicKey().ToBytes(), common.GetSubsidizer().PublicKey().ToBytes()),
	}
	ixns = append(ixns, h.launch.MakeInitInstructions()...)
	ixns = append(ixns, purchaseIxns...)
	ixns = append(ixns, postPurchaseIxns...)

	h.computeUnitLimit = h.launch.ComputeUnitLimit(append(purchaseAtaBumps, feeAtaBump), false)

	return ixns, nil
}

// ReserveTreasuryFundedCreateAndBuySwapHandler launches a new currency within
// a v1 initial purchase funded by the server-controlled treasury, which the
// buyer pays with another currency. The transaction initializes every
// currency account, so the launch is atomic.
type ReserveTreasuryFundedCreateAndBuySwapHandler struct {
	selectedNonce    *transaction_util.Nonce
	computeUnitLimit uint32
	computeUnitPrice uint64

	purchase *treasuryFundedPurchase
}

func NewReserveTreasuryFundedCreateAndBuySwapHandler(
	ctx context.Context,
	data ocp_data.Provider,
	buyer *common.Account,
	fromMint *common.Account,
	toMint *common.Account,
	swapAmount, feeAmount uint64,
	treasury *common.Account,
	purchaseCoreQuarks uint64,
	selectedNonce *transaction_util.Nonce,
) (SwapHandler, error) {
	purchase, err := loadTreasuryFundedPurchase(ctx, data, buyer, fromMint, toMint, swapAmount, feeAmount, treasury, purchaseCoreQuarks)
	if err != nil {
		return nil, err
	}

	err = purchase.launch.SetMemoryAccountRentTopUps(ctx, data.GetBlockchainMinimumBalanceForRentExemption)
	if err != nil {
		return nil, err
	}

	return &ReserveTreasuryFundedCreateAndBuySwapHandler{
		selectedNonce:    selectedNonce,
		computeUnitPrice: 10_000,

		purchase: purchase,
	}, nil
}

func (h *ReserveTreasuryFundedCreateAndBuySwapHandler) GetTransactionConfig() *solana.TransactionConfig {
	config := h.purchase.launch.TransactionConfig(h.computeUnitLimit, h.computeUnitPrice)
	return &config
}

func (h *ReserveTreasuryFundedCreateAndBuySwapHandler) GetAlts(ctx context.Context) ([]solana.AddressLookupTable, error) {
	return nil, nil
}

func (h *ReserveTreasuryFundedCreateAndBuySwapHandler) GetServerParameters() *transactionpb.StatefulSwapResponse_ServerParameters {
	return h.purchase.serverParameters(newReserveNewCurrencyV1ServerParameters(h.purchase.launch, h.selectedNonce, *h.GetTransactionConfig()))
}

func (h *ReserveTreasuryFundedCreateAndBuySwapHandler) MakeInstructions(ctx context.Context) ([]solana.Instruction, error) {
	launch := h.purchase.launch

	err := launch.ValidateInstructionTraceLength(true)
	if err != nil {
		return nil, err
	}

	buyerVmDepositAccounts, err := h.purchase.buyer.GetVmDepositAccounts(launch.VmConfig)
	if err != nil {
		return nil, err
	}

	createVmDepositAtaIxn, _, vmDepositAtaBump, err := launch.MakeCreateAtaInstruction(buyerVmDepositAccounts.Pda, launch.Accounts.Mint)
	if err != nil {
		return nil, err
	}

	// The currency authority pays for every account the launch opens,
	// including the treasury's from mint ATA
	purchaseIxns, treasuryFromMintAtaBump, err := h.purchase.makeInstructions(launch.Accounts.Authority)
	if err != nil {
		return nil, err
	}

	postPurchaseIxns, feeAtaBump, err := launch.MakePostPurchaseInstructions()
	if err != nil {
		return nil, err
	}

	ixns := []solana.Instruction{
		system.AdvanceNonce(h.selectedNonce.Account.PublicKey().ToBytes(), common.GetSubsidizer().PublicKey().ToBytes()),
	}
	ixns = append(ixns, launch.MakeInitInstructions()...)
	ixns = append(ixns, createVmDepositAtaIxn)
	ixns = append(ixns, purchaseIxns...)
	ixns = append(ixns, postPurchaseIxns...)

	h.computeUnitLimit = launch.ComputeUnitLimit([]uint8{vmDepositAtaBump, treasuryFromMintAtaBump, feeAtaBump}, true)

	return ixns, nil
}

// selectCoinbaseSwapVersion picks the CoinbaseStableSwapper swap instruction
// version from the client's requested swap_version. Clients rebuild and sign
// the transaction locally, so the server must match what the client builds.
// Only an explicit 2 selects v2. Older clients leave it unset and build v1.
func selectCoinbaseSwapVersion(requested uint32) transaction_util.CoinbaseSwapVersion {
	if requested == 2 {
		return transaction_util.CoinbaseSwapV2
	}
	return transaction_util.CoinbaseSwapV1
}

const (
	// Measured on mainnet at 62k CU with an existing destination ATA. Creating
	// the destination ATA adds about 9k, and the swap authority ATA, destination
	// ATA and VM swap PDA derivations each cost 1,500 CU per bump below 255, for
	// a worst case of roughly 85k.
	coinbaseStatefulSwapComputeUnitLimit = 125_000
	coinbaseStatefulSwapComputeUnitPrice = 10_000
)

type CoinbaseStableSwapperSwapHandler struct {
	data ocp_data.Provider

	owner            *common.Account
	swapAuthority    *common.Account
	destinationOwner *common.Account
	fromMint         *common.Account
	toMint           *common.Account
	swapAmount       uint64
	feeAmount        uint64

	alts               []solana.AddressLookupTable
	selectedNonce      *transaction_util.Nonce
	transactionVersion solana.MessageVersion
	computeUnitLimit   uint32
	computeUnitPrice   uint64
	memoValue          string

	feeDestination      *common.Account
	coinbaseSwapVersion transaction_util.CoinbaseSwapVersion
	coinbaseAccounts    *transaction_util.CoinbaseSwapAccounts
}

func NewCoinbaseStableSwapperSwapHandler(
	data ocp_data.Provider,
	owner *common.Account,
	swapAuthority *common.Account,
	destinationOwner *common.Account,
	fromMint *common.Account,
	toMint *common.Account,
	swapAmount uint64,
	feeAmount uint64,
	selectedNonce *transaction_util.Nonce,
	transactionVersion solana.MessageVersion,
	coinbaseSwapVersion transaction_util.CoinbaseSwapVersion,
) *CoinbaseStableSwapperSwapHandler {
	return &CoinbaseStableSwapperSwapHandler{
		data: data,

		owner:            owner,
		swapAuthority:    swapAuthority,
		destinationOwner: destinationOwner,
		fromMint:         fromMint,
		toMint:           toMint,
		swapAmount:       swapAmount,
		feeAmount:        feeAmount,

		selectedNonce:      selectedNonce,
		transactionVersion: transactionVersion,
		computeUnitLimit:   coinbaseStatefulSwapComputeUnitLimit,
		computeUnitPrice:   coinbaseStatefulSwapComputeUnitPrice,
		memoValue:          swapMemoValue("coinbase_stable_swapper", transactionVersion),

		feeDestination:      common.CoreMintFeesAccount,
		coinbaseSwapVersion: coinbaseSwapVersion,
	}
}

func (h *CoinbaseStableSwapperSwapHandler) GetAlts(ctx context.Context) ([]solana.AddressLookupTable, error) {
	if h.transactionVersion == solana.MessageVersion1 {
		return nil, nil
	}

	h.alts = []solana.AddressLookupTable{transaction_util.GetAltForCoreMint()}
	return h.alts, nil
}

func (h *CoinbaseStableSwapperSwapHandler) GetTransactionConfig() *solana.TransactionConfig {
	if h.transactionVersion != solana.MessageVersion1 {
		return nil
	}

	config := transaction_util.NewV1TransactionConfig(
		h.computeUnitLimit,
		h.computeUnitPrice,
		transaction_util.CoinbaseSwapLoadedAccountsDataSizeLimit,
	)
	return &config
}

func (h *CoinbaseStableSwapperSwapHandler) GetServerParameters() *transactionpb.StatefulSwapResponse_ServerParameters {
	feeRecipient, _ := common.NewAccountFromPublicKeyBytes(h.coinbaseAccounts.FeeRecipient)

	serverParameters := &transactionpb.StatefulSwapResponse_ServerParameters_CoinbaseStableSwapperServerParameter{
		Payer:            common.GetSubsidizer().ToProto(),
		Nonce:            h.selectedNonce.Account.ToProto(),
		Blockhash:        &commonpb.Blockhash{Value: h.selectedNonce.Blockhash[:]},
		MemoValue:        h.memoValue,
		FeeDestination:   h.feeDestination.ToProto(),
		PoolFeeRecipient: feeRecipient.ToProto(),
	}

	if config := h.GetTransactionConfig(); config != nil {
		serverParameters.TransactionConfig = transaction_util.ToProtoTransactionConfig(*config)
	} else {
		serverParameters.Alts = transaction_util.ToProtoAlts(h.alts)
		serverParameters.ComputeUnitLimit = h.computeUnitLimit
		serverParameters.ComputeUnitPrice = h.computeUnitPrice
	}

	return &transactionpb.StatefulSwapResponse_ServerParameters{
		Kind: &transactionpb.StatefulSwapResponse_ServerParameters_Stablecoin{
			Stablecoin: serverParameters,
		},
	}
}

func (h *CoinbaseStableSwapperSwapHandler) MakeInstructions(ctx context.Context) ([]solana.Instruction, error) {
	sourceVmConfig, err := common.GetVmConfigForMint(ctx, h.data, h.fromMint)
	if err != nil {
		return nil, err
	}

	sourceTimelockAccounts, err := h.owner.GetTimelockAccounts(sourceVmConfig)
	if err != nil {
		return nil, err
	}

	coinbaseAccounts, err := transaction_util.GetCoinbaseSwapAccounts(
		ctx,
		h.data,
		h.fromMint.PublicKey().ToBytes(),
		h.toMint.PublicKey().ToBytes(),
	)
	if err != nil {
		return nil, err
	}
	h.coinbaseAccounts = coinbaseAccounts

	createSwapAuthorityFromMintAtaIxn, swapAuthorityFromMintAta, err := token.CreateAssociatedTokenAccountIdempotent(
		common.GetSubsidizer().PublicKey().ToBytes(),
		h.swapAuthority.PublicKey().ToBytes(),
		h.fromMint.PublicKey().ToBytes(),
	)
	if err != nil {
		return nil, err
	}

	createDestinationOwnerToMintAtaIxn, destinationOwnerToMintAta, err := token.CreateAssociatedTokenAccountIdempotent(
		common.GetSubsidizer().PublicKey().ToBytes(),
		h.destinationOwner.PublicKey().ToBytes(),
		h.toMint.PublicKey().ToBytes(),
	)
	if err != nil {
		return nil, err
	}

	transferForSwapWithFeeIxn := vm.NewTransferForSwapWithFeeInstruction(
		&vm.TransferForSwapWithFeeInstructionAccounts{
			VmAuthority:     common.GetSubsidizer().PublicKey().ToBytes(),
			Vm:              sourceVmConfig.Vm.PublicKey().ToBytes(),
			Swapper:         h.owner.PublicKey().ToBytes(),
			SwapPda:         sourceTimelockAccounts.VmSwapAccounts.Pda.PublicKey().ToBytes(),
			SwapAta:         sourceTimelockAccounts.VmSwapAccounts.Ata.PublicKey().ToBytes(),
			SwapDestination: swapAuthorityFromMintAta,
			FeeDestination:  h.feeDestination.PublicKey().ToBytes(),
		},
		&vm.TransferForSwapWithFeeInstructionArgs{
			SwapAmount: h.swapAmount,
			FeeAmount:  h.feeAmount,
			Bump:       sourceTimelockAccounts.VmSwapAccounts.PdaBump,
		},
	)

	coinbaseSwapIxn, err := transaction_util.MakeCoinbaseSwapInstruction(
		h.coinbaseSwapVersion,
		coinbaseAccounts,
		h.swapAuthority.PublicKey().ToBytes(),
		h.fromMint.PublicKey().ToBytes(),
		h.toMint.PublicKey().ToBytes(),
		swapAuthorityFromMintAta,
		destinationOwnerToMintAta,
		h.swapAmount,
		h.swapAmount,
	)
	if err != nil {
		return nil, err
	}

	closeSwapAuthorityFromMintAtaIxn := token.CloseAccount(
		swapAuthorityFromMintAta,
		common.GetSubsidizer().PublicKey().ToBytes(),
		h.swapAuthority.PublicKey().ToBytes(),
	)

	ixns := []solana.Instruction{
		system.AdvanceNonce(h.selectedNonce.Account.PublicKey().ToBytes(), common.GetSubsidizer().PublicKey().ToBytes()),
	}
	ixns = append(ixns, swapComputeBudgetInstructions(h.transactionVersion, h.computeUnitLimit, h.computeUnitPrice)...)
	return append(
		ixns,
		memo.Instruction(h.memoValue),
		createSwapAuthorityFromMintAtaIxn,
		createDestinationOwnerToMintAtaIxn,
		transferForSwapWithFeeIxn,
		coinbaseSwapIxn,
		closeSwapAuthorityFromMintAtaIxn,
	), nil
}

// selectSwapTransactionVersion picks the Solana transaction version for a swap
// from the client's requested transaction_version. Older clients leave it
// unset and build v0.
func selectSwapTransactionVersion(requested uint32) solana.MessageVersion {
	if requested == 1 {
		return solana.MessageVersion1
	}
	return solana.MessageVersion0
}
