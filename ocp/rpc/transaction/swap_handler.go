package transaction

import (
	"context"

	commonpb "github.com/code-payments/ocp-protobuf-api/generated/go/common/v1"
	transactionpb "github.com/code-payments/ocp-protobuf-api/generated/go/transaction/v1"

	"github.com/code-payments/ocp-server/ocp/common"
	ocp_data "github.com/code-payments/ocp-server/ocp/data"
	"github.com/code-payments/ocp-server/ocp/data/currency"
	vm_metadata "github.com/code-payments/ocp-server/ocp/data/vm/metadata"
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
	// GetAlts returns the set of ALTs that should be used for versioned transaction construction
	GetAlts(ctx context.Context) ([]solana.AddressLookupTable, error)

	// GetServerParameter gets the server parameters to return to client for the swap
	GetServerParameters() *transactionpb.StatefulSwapResponse_ServerParameters

	// MakeInstructions makes the Solana transaction instructions to perform the swap
	MakeInstructions(ctx context.Context) ([]solana.Instruction, error)
}

type ReserveBuySwapHandler struct {
	data ocp_data.Provider

	buyer           *common.Account
	temporaryHolder *common.Account
	mint            *common.Account
	amount          uint64

	alts             []solana.AddressLookupTable
	selectedNonce    *transaction_util.Nonce
	computeUnitLimit uint32
	computeUnitPrice uint64
	memoValue        string

	memoryAccount *common.Account
	memoryIndex   uint16
}

func NewReserveBuySwapHandler(
	data ocp_data.Provider,
	buyer *common.Account,
	temporaryHolder *common.Account,
	mint *common.Account,
	amount uint64,
	selectedNonce *transaction_util.Nonce,
) SwapHandler {
	return &ReserveBuySwapHandler{
		data: data,

		buyer:           buyer,
		temporaryHolder: temporaryHolder,
		mint:            mint,
		amount:          amount,

		selectedNonce:    selectedNonce,
		computeUnitLimit: 120_000,
		computeUnitPrice: 10_000,
		memoValue:        "buy_v0",
	}
}

func (h *ReserveBuySwapHandler) GetAlts(ctx context.Context) ([]solana.AddressLookupTable, error) {
	alt, err := transaction_util.GetAltForMint(ctx, h.data, h.mint)
	if err != nil {
		return nil, err
	}
	h.alts = []solana.AddressLookupTable{alt}
	return h.alts, nil
}

func (h *ReserveBuySwapHandler) GetServerParameters() *transactionpb.StatefulSwapResponse_ServerParameters {
	return &transactionpb.StatefulSwapResponse_ServerParameters{
		Kind: &transactionpb.StatefulSwapResponse_ServerParameters_ReserveExistingCurrency{
			ReserveExistingCurrency: &transactionpb.StatefulSwapResponse_ServerParameters_ReserveExistingCurrencyServerParameters{
				Payer:            common.GetSubsidizer().ToProto(),
				Nonce:            h.selectedNonce.Account.ToProto(),
				Blockhash:        &commonpb.Blockhash{Value: h.selectedNonce.Blockhash[:]},
				Alts:             transaction_util.ToProtoAlts(h.alts),
				ComputeUnitLimit: h.computeUnitLimit,
				ComputeUnitPrice: h.computeUnitPrice,
				MemoValue:        h.memoValue,
				MemoryAccount:    h.memoryAccount.ToProto(),
				MemoryIndex:      uint32(h.memoryIndex),
			},
		},
	}
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

	transferFromSourceVmSwapAtaIxn := vm.NewTransferForSwapInstruction(
		&vm.TransferForSwapInstructionAccounts{
			VmAuthority: sourceVmConfig.Authority.PublicKey().ToBytes(),
			Vm:          sourceVmConfig.Vm.PublicKey().ToBytes(),
			Swapper:     h.buyer.PublicKey().ToBytes(),
			SwapPda:     sourceTimelockAccounts.VmSwapAccounts.Pda.PublicKey().ToBytes(),
			SwapAta:     sourceTimelockAccounts.VmSwapAccounts.Ata.PublicKey().ToBytes(),
			Destination: temporaryCoreMintAta.PublicKey().ToBytes(),
		},
		&vm.TransferForSwapInstructionArgs{
			Amount: h.amount,
			Bump:   sourceTimelockAccounts.VmSwapAccounts.PdaBump,
		},
	)

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
			InAmount:      h.amount,
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

	return []solana.Instruction{
		system.AdvanceNonce(h.selectedNonce.Account.PublicKey().ToBytes(), common.GetSubsidizer().PublicKey().ToBytes()),
		compute_budget.SetComputeUnitLimit(h.computeUnitLimit),
		compute_budget.SetComputeUnitPrice(h.computeUnitPrice),
		memo.Instruction(h.memoValue),
		createTemporaryCoreMintAtaIxn,
		transferFromSourceVmSwapAtaIxn,
		buyAndDepositIntoDestinationVmIxn,
		closeTemporaryCoreMintAtaIxn,
		closeSourceVmSwapAccountIfEmptyIxn,
	}, nil
}

type ReserveSellSwapHandler struct {
	data ocp_data.Provider

	seller          *common.Account
	temporaryHolder *common.Account
	mint            *common.Account
	amount          uint64

	alts             []solana.AddressLookupTable
	selectedNonce    *transaction_util.Nonce
	computeUnitLimit uint32
	computeUnitPrice uint64
	memoValue        string

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
) SwapHandler {
	return &ReserveSellSwapHandler{
		data: data,

		seller:          seller,
		temporaryHolder: temporaryHolder,
		mint:            mint,
		amount:          amount,

		selectedNonce:    selectedNonce,
		computeUnitLimit: 145_000,
		computeUnitPrice: 10_000,
		memoValue:        "sell_v0",
	}
}

func (h *ReserveSellSwapHandler) GetAlts(ctx context.Context) ([]solana.AddressLookupTable, error) {
	alt, err := transaction_util.GetAltForMint(ctx, h.data, h.mint)
	if err != nil {
		return nil, err
	}
	h.alts = []solana.AddressLookupTable{alt}
	return h.alts, nil
}

func (h *ReserveSellSwapHandler) GetServerParameters() *transactionpb.StatefulSwapResponse_ServerParameters {
	return &transactionpb.StatefulSwapResponse_ServerParameters{
		Kind: &transactionpb.StatefulSwapResponse_ServerParameters_ReserveExistingCurrency{
			ReserveExistingCurrency: &transactionpb.StatefulSwapResponse_ServerParameters_ReserveExistingCurrencyServerParameters{
				Payer:            common.GetSubsidizer().ToProto(),
				Nonce:            h.selectedNonce.Account.ToProto(),
				Blockhash:        &commonpb.Blockhash{Value: h.selectedNonce.Blockhash[:]},
				Alts:             transaction_util.ToProtoAlts(h.alts),
				ComputeUnitLimit: h.computeUnitLimit,
				ComputeUnitPrice: h.computeUnitPrice,
				MemoValue:        h.memoValue,
				MemoryAccount:    h.memoryAccount.ToProto(),
				MemoryIndex:      uint32(h.memoryIndex),
			},
		},
	}
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

	return []solana.Instruction{
		system.AdvanceNonce(h.selectedNonce.Account.PublicKey().ToBytes(), common.GetSubsidizer().PublicKey().ToBytes()),
		compute_budget.SetComputeUnitLimit(h.computeUnitLimit),
		compute_budget.SetComputeUnitPrice(h.computeUnitPrice),
		memo.Instruction(h.memoValue),
		createTemporarySourceCurrencyAtaIxn,
		transferFromSourceVmSwapAtaIxn,
		sellAndDepositIntoDestinationVmIxn,
		closeTemporarySourceCurrencyAtaIxn,
		closeSourceVmSwapAccountIfEmptyIxn,
	}, nil
}

type ReserveBuySellSwapHandler struct {
	data ocp_data.Provider

	swapper         *common.Account
	temporaryHolder *common.Account
	fromMint        *common.Account
	toMint          *common.Account
	amount          uint64

	alts             []solana.AddressLookupTable
	selectedNonce    *transaction_util.Nonce
	computeUnitLimit uint32
	computeUnitPrice uint64
	memoValue        string

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
) SwapHandler {
	return &ReserveBuySellSwapHandler{
		data: data,

		swapper:         swapper,
		temporaryHolder: temporaryHolder,
		fromMint:        fromMint,
		toMint:          toMint,
		amount:          amount,

		selectedNonce:    selectedNonce,
		computeUnitLimit: 400_000,
		computeUnitPrice: 10_000,
		memoValue:        "buy_sell_v0",
	}
}

func (h *ReserveBuySellSwapHandler) GetAlts(ctx context.Context) ([]solana.AddressLookupTable, error) {
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

func (h *ReserveBuySellSwapHandler) GetServerParameters() *transactionpb.StatefulSwapResponse_ServerParameters {
	return &transactionpb.StatefulSwapResponse_ServerParameters{
		Kind: &transactionpb.StatefulSwapResponse_ServerParameters_ReserveExistingCurrency{
			ReserveExistingCurrency: &transactionpb.StatefulSwapResponse_ServerParameters_ReserveExistingCurrencyServerParameters{
				Payer:            common.GetSubsidizer().ToProto(),
				Nonce:            h.selectedNonce.Account.ToProto(),
				Blockhash:        &commonpb.Blockhash{Value: h.selectedNonce.Blockhash[:]},
				Alts:             transaction_util.ToProtoAlts(h.alts),
				ComputeUnitLimit: h.computeUnitLimit,
				ComputeUnitPrice: h.computeUnitPrice,
				MemoValue:        h.memoValue,
				MemoryAccount:    h.memoryAccount.ToProto(),
				MemoryIndex:      uint32(h.memoryIndex),
			},
		},
	}
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

	return []solana.Instruction{
		system.AdvanceNonce(h.selectedNonce.Account.PublicKey().ToBytes(), common.GetSubsidizer().PublicKey().ToBytes()),
		compute_budget.SetComputeUnitLimit(h.computeUnitLimit),
		compute_budget.SetComputeUnitPrice(h.computeUnitPrice),
		memo.Instruction(h.memoValue),
		createTemporaryCoreMintAtaIxn,
		createTemporarySourceCurrencyAtaIxn,
		transferFromSourceVmSwapAtaIxn,
		sellIxn,
		buyAndDepositIntoDestinationVmIxn,
		closeTemporaryCoreMintAtaIxn,
		closeTemporarySourceCurrencyAtaIxn,
		closeSourceVmSwapAccountIfEmptyIxn,
	}, nil
}

type ReserveCreateAndBuySwapHandler struct {
	buyer      *common.Account
	mint       *common.Account
	swapAmount uint64
	feeAmount  uint64

	alts             []solana.AddressLookupTable
	selectedNonce    *transaction_util.Nonce
	computeUnitLimit uint32
	computeUnitPrice uint64

	sourceVmConfig                    *common.VmConfig
	destinationCurrencyMetadataRecord *currency.MetadataRecord
	destinationCurrencyAccounts       *common.LaunchpadCurrencyAccounts
	destinationVmMetadataRecord       *vm_metadata.Record
	destinationVmConfig               *common.VmConfig
}

func NewReserveCreateAndBuySwapHandler(
	ctx context.Context,
	data ocp_data.Provider,
	buyer *common.Account,
	mint *common.Account,
	swapAmount, feeAmount uint64,
	selectedNonce *transaction_util.Nonce,
) (SwapHandler, error) {
	var err error

	h := &ReserveCreateAndBuySwapHandler{
		buyer:      buyer,
		mint:       mint,
		swapAmount: swapAmount,
		feeAmount:  feeAmount,

		selectedNonce:    selectedNonce,
		computeUnitLimit: 300_000, // todo: optimize
		computeUnitPrice: 10_000,
	}

	h.alts = []solana.AddressLookupTable{transaction_util.GetAltForCoreMint()}

	h.sourceVmConfig, err = common.GetVmConfigForMint(ctx, data, common.CoreMintAccount)
	if err != nil {
		return nil, err
	}

	h.destinationCurrencyMetadataRecord, err = data.GetCurrencyMetadata(ctx, h.mint.PublicKey().ToBase58())
	if err != nil {
		return nil, err
	}

	h.destinationCurrencyAccounts, err = common.GetLaunchpadCurrencyAccounts(h.destinationCurrencyMetadataRecord)
	if err != nil {
		return nil, err
	}

	// The VM is not supported yet, so we need to work around GetVmConfigForMint
	h.destinationVmMetadataRecord, err = data.GetVmMetadataByMint(ctx, h.mint.PublicKey().ToBase58())
	if err != nil {
		return nil, err
	}
	vmAccount, err := common.NewAccountFromPublicKeyString(h.destinationVmMetadataRecord.Vm)
	if err != nil {
		return nil, err
	}
	omnibusAccount, err := common.NewAccountFromPublicKeyString(h.destinationVmMetadataRecord.Omnibus)
	if err != nil {
		return nil, err
	}
	h.destinationVmConfig = &common.VmConfig{
		Authority: h.destinationCurrencyAccounts.Authority,
		Vm:        vmAccount,
		Omnibus:   omnibusAccount,
		Mint:      h.mint,
	}

	return h, nil
}

func (h *ReserveCreateAndBuySwapHandler) GetAlts(ctx context.Context) ([]solana.AddressLookupTable, error) {
	return h.alts, nil
}

func (h *ReserveCreateAndBuySwapHandler) GetServerParameters() *transactionpb.StatefulSwapResponse_ServerParameters {
	return &transactionpb.StatefulSwapResponse_ServerParameters{
		Kind: &transactionpb.StatefulSwapResponse_ServerParameters_ReserveNewCurrency{
			ReserveNewCurrency: &transactionpb.StatefulSwapResponse_ServerParameters_ReserveNewCurrencyServerParameter{
				Payer:                common.GetSubsidizer().ToProto(),
				Nonce:                h.selectedNonce.Account.ToProto(),
				Blockhash:            &commonpb.Blockhash{Value: h.selectedNonce.Blockhash[:]},
				Alts:                 transaction_util.ToProtoAlts(h.alts),
				ComputeUnitLimit:     h.computeUnitLimit,
				ComputeUnitPrice:     h.computeUnitPrice,
				MemoValue:            "",
				Authority:            h.destinationCurrencyAccounts.Authority.ToProto(),
				Name:                 h.destinationCurrencyMetadataRecord.Name,
				Symbol:               h.destinationCurrencyMetadataRecord.Symbol,
				Seed:                 h.destinationCurrencyAccounts.Seed.ToProto(),
				SellFeeBps:           currencycreator.DefaultSellFeeBps,
				VmLockDurationInDays: uint32(timelock_token.DefaultNumDaysLocked),
				FeeDestination:       common.CoreMintFeesAccount.ToProto(),
			},
		},
	}
}

func (h *ReserveCreateAndBuySwapHandler) MakeInstructions(ctx context.Context) ([]solana.Instruction, error) {
	buyerVmSwapAccounts, err := h.buyer.GetVmSwapAccounts(h.sourceVmConfig)
	if err != nil {
		return nil, err
	}

	buyerVmDepositAccounts, err := h.buyer.GetVmDepositAccounts(h.destinationVmConfig)
	if err != nil {
		return nil, err
	}

	createTemporaryCoreMintAtaIxn, temporaryCoreMintAta, err := token.CreateAssociatedTokenAccountIdempotent(
		h.destinationCurrencyAccounts.Authority.PublicKey().ToBytes(),
		h.buyer.PublicKey().ToBytes(),
		common.CoreMintAccount.PublicKey().ToBytes(),
	)
	if err != nil {
		return nil, err
	}

	createVmDepositAtaIxn, _, err := token.CreateAssociatedTokenAccountIdempotent(
		h.destinationCurrencyAccounts.Authority.PublicKey().ToBytes(),
		buyerVmDepositAccounts.Pda.PublicKey().ToBytes(),
		h.destinationCurrencyAccounts.Mint.PublicKey().ToBytes(),
	)
	if err != nil {
		return nil, err
	}

	closeTemporaryCoreMintAta := token.CloseAccount(
		temporaryCoreMintAta,
		h.destinationCurrencyAccounts.Authority.PublicKey().ToBytes(),
		h.buyer.PublicKey().ToBytes(),
	)

	initCurrencyIxn := currencycreator.NewInitializeCurrencyInstruction(
		&currencycreator.InitializeCurrencyInstructionAccounts{
			Authority: h.destinationCurrencyAccounts.Authority.PublicKey().ToBytes(),
			Mint:      h.destinationCurrencyAccounts.Mint.PublicKey().ToBytes(),
			Currency:  h.destinationCurrencyAccounts.CurrencyConfig.PublicKey().ToBytes(),
		},
		&currencycreator.InitializeCurrencyInstructionArgs{
			Name:     h.destinationCurrencyMetadataRecord.Name,
			Symbol:   h.destinationCurrencyMetadataRecord.Symbol,
			Seed:     h.destinationCurrencyAccounts.Seed.PublicKey().ToBytes(),
			Bump:     h.destinationCurrencyAccounts.CurrencyConfigBump,
			MintBump: h.destinationCurrencyAccounts.MintBump,
		},
	)

	initPoolIxn := currencycreator.NewInitializePoolInstruction(
		&currencycreator.InitializePoolInstructionAccounts{
			Authority:   h.destinationCurrencyAccounts.Authority.PublicKey().ToBytes(),
			Currency:    h.destinationCurrencyAccounts.CurrencyConfig.PublicKey().ToBytes(),
			TargetMint:  h.destinationCurrencyAccounts.Mint.PublicKey().ToBytes(),
			BaseMint:    common.CoreMintAccount.PublicKey().ToBytes(),
			Pool:        h.destinationCurrencyAccounts.LiquidityPool.PublicKey().ToBytes(),
			VaultTarget: h.destinationCurrencyAccounts.VaultMint.PublicKey().ToBytes(),
			VaultBase:   h.destinationCurrencyAccounts.VaultBase.PublicKey().ToBytes(),
		},
		&currencycreator.InitializePoolInstructionArgs{
			SellFee:         currencycreator.DefaultSellFeeBps,
			Bump:            h.destinationCurrencyAccounts.LiquidityPoolBump,
			VaultTargetBump: h.destinationCurrencyAccounts.VaultMintBump,
			VaultBaseBump:   h.destinationCurrencyAccounts.VaultBaseBump,
		},
	)

	initVmIxn := vm.NewInitVmInstruction(
		&vm.InitVmInstructionAccounts{
			VmAuthority: h.destinationCurrencyAccounts.Authority.PublicKey().ToBytes(),
			Vm:          h.destinationVmConfig.Vm.PublicKey().ToBytes(),
			VmOmnibus:   h.destinationVmConfig.Omnibus.PublicKey().ToBytes(),
			Mint:        h.destinationCurrencyAccounts.Mint.PublicKey().ToBytes(),
		},
		&vm.InitVmInstructionArgs{
			LockDuration:  timelock_token.DefaultNumDaysLocked,
			VmBump:        h.destinationVmMetadataRecord.VmBump,
			VmOmnibusBump: h.destinationVmMetadataRecord.OmnibusBump,
		},
	)

	transferForSwapWithFeeIxn := vm.NewTransferForSwapWithFeeInstruction(
		&vm.TransferForSwapWithFeeInstructionAccounts{
			VmAuthority:     common.GetSubsidizer().PublicKey().ToBytes(),
			Vm:              common.CoreMintVmAccount.PublicKey().ToBytes(),
			Swapper:         h.buyer.PublicKey().ToBytes(),
			SwapPda:         buyerVmSwapAccounts.Pda.PublicKey().ToBytes(),
			SwapAta:         buyerVmSwapAccounts.Ata.PublicKey().ToBytes(),
			SwapDestination: temporaryCoreMintAta,
			FeeDestination:  common.CoreMintFeesAccount.PublicKey().ToBytes(),
		},
		&vm.TransferForSwapWithFeeInstructionArgs{
			SwapAmount: h.swapAmount,
			FeeAmount:  h.feeAmount,
			Bump:       buyerVmSwapAccounts.PdaBump,
		},
	)

	buyIxn := currencycreator.NewBuyTokensInstruction(
		&currencycreator.BuyTokensInstructionAccounts{
			Buyer:       h.buyer.PublicKey().ToBytes(),
			Pool:        h.destinationCurrencyAccounts.LiquidityPool.PublicKey().ToBytes(),
			TargetMint:  h.destinationCurrencyAccounts.Mint.PublicKey().ToBytes(),
			BaseMint:    common.CoreMintAccount.PublicKey().ToBytes(),
			VaultTarget: h.destinationCurrencyAccounts.VaultMint.PublicKey().ToBytes(),
			VaultBase:   h.destinationCurrencyAccounts.VaultBase.PublicKey().ToBytes(),
			BuyerTarget: buyerVmDepositAccounts.Ata.PublicKey().ToBytes(),
			BuyerBase:   temporaryCoreMintAta,
		},
		&currencycreator.BuyTokensInstructionArgs{
			InAmount:     h.swapAmount,
			MinAmountOut: 0,
		},
	)

	return []solana.Instruction{
		system.AdvanceNonce(h.selectedNonce.Account.PublicKey().ToBytes(), common.GetSubsidizer().PublicKey().ToBytes()),
		compute_budget.SetComputeUnitLimit(h.computeUnitLimit),
		compute_budget.SetComputeUnitPrice(h.computeUnitPrice),
		initCurrencyIxn,
		initPoolIxn,
		initVmIxn,
		createTemporaryCoreMintAtaIxn,
		createVmDepositAtaIxn,
		transferForSwapWithFeeIxn,
		buyIxn,
		closeTemporaryCoreMintAta,
	}, nil
}

type CoinbaseStableSwapperSwapHandler struct {
	data ocp_data.Provider

	owner            *common.Account
	swapAuthority    *common.Account
	destinationOwner *common.Account
	fromMint         *common.Account
	toMint           *common.Account
	swapAmount       uint64
	feeAmount        uint64

	alts             []solana.AddressLookupTable
	selectedNonce    *transaction_util.Nonce
	computeUnitLimit uint32
	computeUnitPrice uint64
	memoValue        string

	feeDestination   *common.Account
	coinbaseAccounts *transaction_util.CoinbaseSwapAccounts
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
) SwapHandler {
	return &CoinbaseStableSwapperSwapHandler{
		data: data,

		owner:            owner,
		swapAuthority:    swapAuthority,
		destinationOwner: destinationOwner,
		fromMint:         fromMint,
		toMint:           toMint,
		swapAmount:       swapAmount,
		feeAmount:        feeAmount,

		selectedNonce:    selectedNonce,
		computeUnitLimit: 120_000,
		computeUnitPrice: 10_000,
		memoValue:        "coinbase_stable_swapper_v0",
		feeDestination:   common.CoreMintFeesAccount,
	}
}

func (h *CoinbaseStableSwapperSwapHandler) GetAlts(ctx context.Context) ([]solana.AddressLookupTable, error) {
	h.alts = []solana.AddressLookupTable{transaction_util.GetAltForCoreMint()}
	return h.alts, nil
}

func (h *CoinbaseStableSwapperSwapHandler) GetServerParameters() *transactionpb.StatefulSwapResponse_ServerParameters {
	feeRecipient, _ := common.NewAccountFromPublicKeyBytes(h.coinbaseAccounts.FeeRecipient)
	return &transactionpb.StatefulSwapResponse_ServerParameters{
		Kind: &transactionpb.StatefulSwapResponse_ServerParameters_Stablecoin{
			Stablecoin: &transactionpb.StatefulSwapResponse_ServerParameters_CoinbaseStableSwapperServerParameter{
				Payer:            common.GetSubsidizer().ToProto(),
				Nonce:            h.selectedNonce.Account.ToProto(),
				Blockhash:        &commonpb.Blockhash{Value: h.selectedNonce.Blockhash[:]},
				Alts:             transaction_util.ToProtoAlts(h.alts),
				ComputeUnitLimit: h.computeUnitLimit,
				ComputeUnitPrice: h.computeUnitPrice,
				MemoValue:        h.memoValue,
				FeeDestination:   h.feeDestination.ToProto(),
				PoolFeeRecipient: feeRecipient.ToProto(),
			},
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

	coinbaseSwapIxn := transaction_util.MakeCoinbaseSwapInstruction(
		coinbaseAccounts,
		h.swapAuthority.PublicKey().ToBytes(),
		h.fromMint.PublicKey().ToBytes(),
		h.toMint.PublicKey().ToBytes(),
		swapAuthorityFromMintAta,
		destinationOwnerToMintAta,
		h.swapAmount,
		h.swapAmount,
	)

	closeSwapAuthorityFromMintAtaIxn := token.CloseAccount(
		swapAuthorityFromMintAta,
		common.GetSubsidizer().PublicKey().ToBytes(),
		h.swapAuthority.PublicKey().ToBytes(),
	)

	return []solana.Instruction{
		system.AdvanceNonce(h.selectedNonce.Account.PublicKey().ToBytes(), common.GetSubsidizer().PublicKey().ToBytes()),
		compute_budget.SetComputeUnitLimit(h.computeUnitLimit),
		compute_budget.SetComputeUnitPrice(h.computeUnitPrice),
		memo.Instruction(h.memoValue),
		createSwapAuthorityFromMintAtaIxn,
		createDestinationOwnerToMintAtaIxn,
		transferForSwapWithFeeIxn,
		coinbaseSwapIxn,
		closeSwapAuthorityFromMintAtaIxn,
	}, nil
}
