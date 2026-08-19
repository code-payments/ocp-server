package history

import (
	"context"

	"github.com/pkg/errors"

	currency_lib "github.com/code-payments/ocp-server/currency"
	"github.com/code-payments/ocp-server/ocp/common"
	currency_util "github.com/code-payments/ocp-server/ocp/currency"
	ocp_data "github.com/code-payments/ocp-server/ocp/data"
	"github.com/code-payments/ocp-server/ocp/data/history"
	"github.com/code-payments/ocp-server/ocp/data/swap"
	"github.com/code-payments/ocp-server/pointer"
)

// BuildRecordForFundedSwap builds the transaction history record for a swap
// whose funding has come through and been validated. The value and the
// client's verified fiat exchange rate are provided by the swap worker, since
// their derivation depends on the funding source.
//
// The record is referenced by the swap's ID, mapping it directly to the swap
// record. The value is the gross amount funding the swap, with fees broken
// out, so the net value is the gross value less fees. The record is pending
// until the swap reaches a terminal state.
//
// launchTerms marks a swap as a currency's initial purchase and carries what
// that launch is charged. It is nil for every other swap. The swap worker
// resolves it, since neither the fact of the launch nor its terms are on the
// swap record.
func BuildRecordForFundedSwap(swapRecord *swap.Record, exchangeCurrency currency_lib.Code, nativeAmount, fiatExchangeRate float64, launchTerms *CurrencyLaunchTerms) (*history.Record, error) {
	switch swapRecord.Kind {
	case swap.KindReserve:
		return buildRecordForFundedReserveSwap(swapRecord, exchangeCurrency, nativeAmount, launchTerms)
	case swap.KindStablecoin:
		return buildRecordForFundedStablecoinSwap(swapRecord, exchangeCurrency, nativeAmount, fiatExchangeRate)
	default:
		return nil, errors.New("unsupported swap kind")
	}
}

// CurrencyLaunchTerms are the amounts a currency launch is charged, in core
// mint quarks as USD values, matching what the launch was validated against.
type CurrencyLaunchTerms struct {
	PurchaseQuarks uint64
	FeeQuarks      uint64
}

// A reserve swap's destination quantity is set once the realized amount is
// known at finalization.
func buildRecordForFundedReserveSwap(swapRecord *swap.Record, exchangeCurrency currency_lib.Code, nativeAmount float64, launchTerms *CurrencyLaunchTerms) (*history.Record, error) {
	fromMint, err := common.NewAccountFromPublicKeyString(swapRecord.FromMint)
	if err != nil {
		return nil, err
	}

	var fees []history.Fee
	if swapRecord.FeeAmount > 0 {
		if launchTerms != nil {
			// A launch is charged a fixed fee alongside a fixed purchase, so the
			// fee's share of the value is those two amounts' ratio. The quarks
			// funding the swap don't give that ratio: a launchpad currency is
			// priced on a bonding curve, so a fee leg worth half the trade is
			// not half of its quarks.
			fundedQuarks := launchTerms.PurchaseQuarks + launchTerms.FeeQuarks
			if fundedQuarks == 0 {
				return nil, errors.New("currency launch terms are empty")
			}

			fees = append(fees, history.Fee{
				Type:         history.CurrencyLaunchFee,
				NativeAmount: nativeAmount * float64(launchTerms.FeeQuarks) / float64(fundedQuarks),
			})
		} else {
			// A buy is charged a percentage of what it bought, which its own
			// quarks do give, since they and the fee are the same mint
			fees = append(fees, history.Fee{
				Type:         history.ReserveBuyFee,
				NativeAmount: nativeAmount - currency_util.DiscountValueForBuyFee(nativeAmount, swapRecord.SwapAmount, swapRecord.FeeAmount),
			})
		}
	}

	// Selling a launchpad currency incurs the liquidity pool's sell fee. A
	// launch is the exception: the treasury sells the whole funding amount for
	// protocol revenue and buys a fixed value on the swapper's behalf, so the
	// pool's fee comes out of the protocol's side rather than theirs.
	if !common.IsCoreMint(fromMint) && launchTerms == nil {
		fees = append(fees, history.Fee{
			Type:         history.ReserveSellFee,
			NativeAmount: nativeAmount - currency_util.ApplySellFee(nativeAmount),
		})
	}

	return &history.Record{
		ReferenceId:            swapRecord.SwapId,
		ReferenceType:          history.SwapReference,
		Type:                   history.Swap,
		OwnerAccount:           swapRecord.Owner,
		ExchangeCurrency:       exchangeCurrency,
		NativeAmount:           nativeAmount,
		Fees:                   fees,
		MintAccount:            swapRecord.FromMint,
		Quantity:               swapRecord.SwapAmount + swapRecord.FeeAmount,
		DestinationMintAccount: pointer.String(swapRecord.ToMint),
		State:                  history.StatePending,
		CreatedAt:              swapRecord.CreatedAt,
	}, nil
}

// A stablecoin swap withdraws the core mint as an external stablecoin, so the
// user sees it as a withdrawal. The swap is 1:1, so the destination quantity
// is known upfront, and its fee is the withdrawal ATA creation fee, quoted in
// core mint quarks as a USD value, whose native value is that USD value at the
// client's verified fiat exchange rate.
func buildRecordForFundedStablecoinSwap(swapRecord *swap.Record, exchangeCurrency currency_lib.Code, nativeAmount, fiatExchangeRate float64) (*history.Record, error) {
	var fees []history.Fee
	if swapRecord.FeeAmount > 0 {
		feeUsdValue := float64(swapRecord.FeeAmount) / float64(common.CoreMintQuarksPerUnit)
		fees = append(fees, history.Fee{
			Type:         history.WithdrawalAccountCreationFee,
			NativeAmount: fiatExchangeRate * feeUsdValue,
		})
	}

	record := &history.Record{
		ReferenceId:            swapRecord.SwapId,
		ReferenceType:          history.SwapReference,
		Type:                   history.Withdrawn,
		OwnerAccount:           swapRecord.Owner,
		ExchangeCurrency:       exchangeCurrency,
		NativeAmount:           nativeAmount,
		Fees:                   fees,
		MintAccount:            swapRecord.FromMint,
		Quantity:               swapRecord.SwapAmount + swapRecord.FeeAmount,
		DestinationMintAccount: pointer.String(swapRecord.ToMint),
		DestinationQuantity:    pointer.Uint64(swapRecord.SwapAmount),
		State:                  history.StatePending,
		CreatedAt:              swapRecord.CreatedAt,
	}
	if len(swapRecord.DestinationOwner) > 0 {
		record.CounterpartyOwnerAccount = pointer.String(swapRecord.DestinationOwner)
	}

	return record, nil
}

// MarkSwapAsCompleted completes a swap's record with the realized destination
// quantity after the swap transaction is finalized.
func MarkSwapAsCompleted(ctx context.Context, data ocp_data.DatabaseData, swapId string, destinationQuantity uint64) error {
	return markSwapTerminal(ctx, data, swapId, history.StateCompleted, pointer.Uint64(destinationQuantity))
}

// MarkSwapAsFailed fails a swap's record after the swap could not be executed
// and the funds are refunded.
func MarkSwapAsFailed(ctx context.Context, data ocp_data.DatabaseData, swapId string) error {
	return markSwapTerminal(ctx, data, swapId, history.StateFailed, nil)
}

func markSwapTerminal(ctx context.Context, data ocp_data.DatabaseData, swapId string, newState history.State, destinationQuantity *uint64) error {
	records, err := data.GetAllTransactionHistoryByReference(ctx, history.SwapReference, swapId)
	if errors.Is(err, history.ErrNotFound) {
		// The swap predates history integration
		return nil
	}
	if err != nil {
		return err
	}

	// A swap is one owner's trade, so it has exactly one record. The reference
	// type is what makes that hold: an ID only names a swap within its own
	// kind, so a swap ID shares no space with an intent's.
	if len(records) != 1 {
		return errors.Errorf("found %d records for swap, expected 1", len(records))
	}
	record := records[0]

	// A swap transitions exactly once, from pending, so anything else is a
	// flow violation
	if record.State != history.StatePending {
		return errors.Errorf("swap record is %s, expected %s", record.State, history.StatePending)
	}

	record.State = newState
	record.DestinationQuantity = destinationQuantity
	return data.SaveTransactionHistory(ctx, record)
}
