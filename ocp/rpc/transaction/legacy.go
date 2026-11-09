package transaction

// Legacy v0 currency launch support. v0 initial purchases only initialize the
// currency, pool and VM, and the launcher worker initializes the rest. Delete
// this file once v0 currency launches are deprecated.

import (
	"context"

	transactionpb "github.com/code-payments/ocp-protobuf-api/generated/go/transaction/v1"

	"github.com/code-payments/ocp-server/ocp/common"
	ocp_data "github.com/code-payments/ocp-server/ocp/data"
	transaction_util "github.com/code-payments/ocp-server/ocp/transaction"
	"github.com/code-payments/ocp-server/solana"
	compute_budget "github.com/code-payments/ocp-server/solana/computebudget"
	"github.com/code-payments/ocp-server/solana/system"
)

// newReserveNewCurrencyLegacyServerParameters builds the server parameters for
// a v0 create-and-buy flow
func newReserveNewCurrencyLegacyServerParameters(
	launch *transaction_util.CurrencyLaunch,
	selectedNonce *transaction_util.Nonce,
	alts []solana.AddressLookupTable,
	computeUnitLimit uint32,
	computeUnitPrice uint64,
) *transactionpb.StatefulSwapResponse_ServerParameters_ReserveNewCurrencyServerParameter {
	serverParameters := newReserveNewCurrencyServerParameters(launch, selectedNonce)
	serverParameters.Alts = transaction_util.ToProtoAlts(alts)
	serverParameters.ComputeUnitLimit = computeUnitLimit
	serverParameters.ComputeUnitPrice = computeUnitPrice
	return serverParameters
}

// ReserveCreateAndBuyLegacySwapHandler is the v0 ReserveCreateAndBuySwapHandler
type ReserveCreateAndBuyLegacySwapHandler struct {
	buyer      *common.Account
	swapAmount uint64
	feeAmount  uint64

	alts             []solana.AddressLookupTable
	selectedNonce    *transaction_util.Nonce
	computeUnitLimit uint32
	computeUnitPrice uint64

	sourceVmConfig *common.VmConfig
	launch         *transaction_util.CurrencyLaunch
}

func NewReserveCreateAndBuyLegacySwapHandler(
	ctx context.Context,
	data ocp_data.Provider,
	buyer *common.Account,
	mint *common.Account,
	swapAmount, feeAmount uint64,
	selectedNonce *transaction_util.Nonce,
) (SwapHandler, error) {
	var err error

	h := &ReserveCreateAndBuyLegacySwapHandler{
		buyer:      buyer,
		swapAmount: swapAmount,
		feeAmount:  feeAmount,

		alts:             []solana.AddressLookupTable{transaction_util.GetAltForCoreMint()},
		selectedNonce:    selectedNonce,
		computeUnitLimit: 300_000, // todo: optimize
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

	return h, nil
}

func (h *ReserveCreateAndBuyLegacySwapHandler) GetTransactionConfig() *solana.TransactionConfig {
	return nil
}

func (h *ReserveCreateAndBuyLegacySwapHandler) GetAlts(ctx context.Context) ([]solana.AddressLookupTable, error) {
	return h.alts, nil
}

func (h *ReserveCreateAndBuyLegacySwapHandler) GetServerParameters() *transactionpb.StatefulSwapResponse_ServerParameters {
	return &transactionpb.StatefulSwapResponse_ServerParameters{
		Kind: &transactionpb.StatefulSwapResponse_ServerParameters_ReserveNewCurrency{
			ReserveNewCurrency: newReserveNewCurrencyLegacyServerParameters(
				h.launch,
				h.selectedNonce,
				h.alts,
				h.computeUnitLimit,
				h.computeUnitPrice,
			),
		},
	}
}

func (h *ReserveCreateAndBuyLegacySwapHandler) MakeInstructions(ctx context.Context) ([]solana.Instruction, error) {
	purchaseIxns, _, err := makeCoreMintFundedPurchaseInstructions(h.launch, h.buyer, h.sourceVmConfig, h.swapAmount, h.feeAmount)
	if err != nil {
		return nil, err
	}

	ixns := []solana.Instruction{
		system.AdvanceNonce(h.selectedNonce.Account.PublicKey().ToBytes(), common.GetSubsidizer().PublicKey().ToBytes()),
		compute_budget.SetComputeUnitLimit(h.computeUnitLimit),
		compute_budget.SetComputeUnitPrice(h.computeUnitPrice),
	}
	ixns = append(ixns, h.launch.MakeInitInstructions()...)
	ixns = append(ixns, purchaseIxns...)
	return ixns, nil
}

// ReserveTreasuryFundedCreateAndBuyLegacySwapHandler is the v0
// ReserveTreasuryFundedCreateAndBuySwapHandler. The launcher worker
// initializes the currency, pool, VM and buyer's VM deposit ATA ahead of the
// purchase.
type ReserveTreasuryFundedCreateAndBuyLegacySwapHandler struct {
	data ocp_data.Provider

	alts             []solana.AddressLookupTable
	selectedNonce    *transaction_util.Nonce
	computeUnitLimit uint32
	computeUnitPrice uint64

	purchase *treasuryFundedPurchase
}

func NewReserveTreasuryFundedCreateAndBuyLegacySwapHandler(
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

	return &ReserveTreasuryFundedCreateAndBuyLegacySwapHandler{
		data: data,

		selectedNonce:    selectedNonce,
		computeUnitLimit: 250_000,
		computeUnitPrice: 10_000,

		purchase: purchase,
	}, nil
}

func (h *ReserveTreasuryFundedCreateAndBuyLegacySwapHandler) GetTransactionConfig() *solana.TransactionConfig {
	return nil
}

func (h *ReserveTreasuryFundedCreateAndBuyLegacySwapHandler) GetAlts(ctx context.Context) ([]solana.AddressLookupTable, error) {
	alt, err := transaction_util.GetAltForMint(ctx, h.data, h.purchase.fromMint)
	if err != nil {
		return nil, err
	}
	h.alts = []solana.AddressLookupTable{alt}
	return h.alts, nil
}

func (h *ReserveTreasuryFundedCreateAndBuyLegacySwapHandler) GetServerParameters() *transactionpb.StatefulSwapResponse_ServerParameters {
	return h.purchase.serverParameters(newReserveNewCurrencyLegacyServerParameters(
		h.purchase.launch,
		h.selectedNonce,
		h.alts,
		h.computeUnitLimit,
		h.computeUnitPrice,
	))
}

func (h *ReserveTreasuryFundedCreateAndBuyLegacySwapHandler) MakeInstructions(ctx context.Context) ([]solana.Instruction, error) {
	// The subsidizer pays for the treasury's from mint ATA
	purchaseIxns, _, err := h.purchase.makeInstructions(common.GetSubsidizer())
	if err != nil {
		return nil, err
	}

	ixns := []solana.Instruction{
		system.AdvanceNonce(h.selectedNonce.Account.PublicKey().ToBytes(), common.GetSubsidizer().PublicKey().ToBytes()),
		compute_budget.SetComputeUnitLimit(h.computeUnitLimit),
		compute_budget.SetComputeUnitPrice(h.computeUnitPrice),
	}
	ixns = append(ixns, purchaseIxns...)
	return ixns, nil
}
