package sequencer

import (
	"context"
	"errors"

	ocp_data "github.com/code-payments/ocp-server/ocp/data"
	"github.com/code-payments/ocp-server/ocp/data/action"
	"github.com/code-payments/ocp-server/ocp/data/fulfillment"
)

var (
	ErrInvalidActionStateTransition = errors.New("invalid action state transition")
)

type ActionHandler interface {
	// Note: New fulfillment states are not recorded until the very end of a worker
	//       run, which is why we need to pass it into this function.
	OnFulfillmentStateChange(ctx context.Context, fulfillmentRecord *fulfillment.Record, newState fulfillment.State) error
}

type OpenAccountActionHandler struct {
	data ocp_data.Provider
}

func NewOpenAccountActionHandler(data ocp_data.Provider) ActionHandler {
	return &OpenAccountActionHandler{
		data: data,
	}
}

func (h *OpenAccountActionHandler) OnFulfillmentStateChange(ctx context.Context, fulfillmentRecord *fulfillment.Record, newState fulfillment.State) error {
	if fulfillmentRecord.FulfillmentType != fulfillment.InitializeLockedTimelockAccount {
		return errors.New("unexpected fulfillment type")
	}

	if newState == fulfillment.StateConfirmed {
		return markActionConfirmed(ctx, h.data, fulfillmentRecord.Intent, fulfillmentRecord.ActionId)
	}

	if newState == fulfillment.StateFailed {
		return markActionFailed(ctx, h.data, fulfillmentRecord.Intent, fulfillmentRecord.ActionId)
	}

	return nil
}

type CloseEmptyAccountActionHandler struct {
	data ocp_data.Provider
}

func NewCloseEmptyAccountActionHandler(data ocp_data.Provider) ActionHandler {
	return &CloseEmptyAccountActionHandler{
		data: data,
	}
}

func (h *CloseEmptyAccountActionHandler) OnFulfillmentStateChange(ctx context.Context, fulfillmentRecord *fulfillment.Record, newState fulfillment.State) error {
	if fulfillmentRecord.FulfillmentType != fulfillment.CloseEmptyTimelockAccount {
		return errors.New("unexpected fulfillment type")
	}

	if newState == fulfillment.StateConfirmed {
		return markActionConfirmed(ctx, h.data, fulfillmentRecord.Intent, fulfillmentRecord.ActionId)
	}

	if newState == fulfillment.StateFailed {
		return markActionFailed(ctx, h.data, fulfillmentRecord.Intent, fulfillmentRecord.ActionId)
	}

	return nil
}

type NoPrivacyTransferActionHandler struct {
	data ocp_data.Provider
}

func NewNoPrivacyTransferActionHandler(data ocp_data.Provider) ActionHandler {
	return &NoPrivacyTransferActionHandler{
		data: data,
	}
}

func (h *NoPrivacyTransferActionHandler) OnFulfillmentStateChange(ctx context.Context, fulfillmentRecord *fulfillment.Record, newState fulfillment.State) error {
	if fulfillmentRecord.FulfillmentType != fulfillment.NoPrivacyTransferWithAuthority {
		return errors.New("unexpected fulfillment type")
	}

	if newState == fulfillment.StateConfirmed {
		return markActionConfirmed(ctx, h.data, fulfillmentRecord.Intent, fulfillmentRecord.ActionId)
	}

	if newState == fulfillment.StateFailed {
		return markActionFailed(ctx, h.data, fulfillmentRecord.Intent, fulfillmentRecord.ActionId)
	}

	return nil
}

type NoPrivacyWithdrawActionHandler struct {
	data ocp_data.Provider
}

func NewNoPrivacyWithdrawActionHandler(data ocp_data.Provider) ActionHandler {
	return &NoPrivacyWithdrawActionHandler{
		data: data,
	}
}

func (h *NoPrivacyWithdrawActionHandler) OnFulfillmentStateChange(ctx context.Context, fulfillmentRecord *fulfillment.Record, newState fulfillment.State) error {
	if fulfillmentRecord.FulfillmentType != fulfillment.NoPrivacyWithdraw {
		return errors.New("unexpected fulfillment type")
	}

	if newState == fulfillment.StateConfirmed {
		return markActionConfirmed(ctx, h.data, fulfillmentRecord.Intent, fulfillmentRecord.ActionId)
	}

	if newState == fulfillment.StateFailed {
		return markActionFailed(ctx, h.data, fulfillmentRecord.Intent, fulfillmentRecord.ActionId)
	}

	return nil
}

func validateActionState(record *action.Record, states ...action.State) error {
	for _, validState := range states {
		if record.State == validState {
			return nil
		}
	}
	return ErrInvalidActionStateTransition
}

func markActionConfirmed(ctx context.Context, data ocp_data.Provider, intentId string, actionId uint32) error {
	record, err := data.GetActionById(ctx, intentId, actionId)
	if err != nil {
		return err
	}

	if record.State == action.StateConfirmed {
		return nil
	}

	err = validateActionState(record, action.StatePending)
	if err != nil {
		return err
	}

	record.State = action.StateConfirmed
	return data.UpdateAction(ctx, record)
}

func markActionFailed(ctx context.Context, data ocp_data.Provider, intentId string, actionId uint32) error {
	record, err := data.GetActionById(ctx, intentId, actionId)
	if err != nil {
		return err
	}

	if record.State == action.StateFailed {
		return nil
	}

	err = validateActionState(record, action.StatePending)
	if err != nil {
		return err
	}

	record.State = action.StateFailed
	return data.UpdateAction(ctx, record)
}

func getActionHandlers(data ocp_data.Provider) map[action.Type]ActionHandler {
	handlersByType := make(map[action.Type]ActionHandler)
	handlersByType[action.OpenAccount] = NewOpenAccountActionHandler(data)
	handlersByType[action.CloseEmptyAccount] = NewCloseEmptyAccountActionHandler(data)
	handlersByType[action.NoPrivacyTransfer] = NewNoPrivacyTransferActionHandler(data)
	handlersByType[action.NoPrivacyWithdraw] = NewNoPrivacyWithdrawActionHandler(data)
	return handlersByType
}
