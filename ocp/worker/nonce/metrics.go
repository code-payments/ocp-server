package nonce

import (
	"context"
	"fmt"
	"time"

	"github.com/code-payments/ocp-server/metrics"
	"github.com/code-payments/ocp-server/ocp/common"
	"github.com/code-payments/ocp-server/ocp/data/nonce"
)

const (
	nonceCountCheckEventName = "NonceCountPollingCheck"
)

func (p *runtime) metricsGaugeWorker(ctx context.Context) error {
	delay := time.Second

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
			start := time.Now()

			// todo: optimize number of queries needed per polling check
			for _, state := range []nonce.State{
				nonce.StateUnknown,
				nonce.StateReleased,
				nonce.StateAvailable,
				nonce.StateReserved,
				nonce.StateInvalid,
				nonce.StateClaimed,
			} {
				count, err := p.data.GetNonceCountByStateAndPurpose(ctx, nonce.EnvironmentCvm, common.CodeVmAccount.PublicKey().ToBase58(), state, nonce.PurposeClientIntent)
				if err != nil {
					continue
				}
				recordNonceCountEvent(ctx, nonce.EnvironmentCvm, common.CodeVmAccount.PublicKey().ToBase58(), state, nonce.PurposeClientIntent, count)

				count, err = p.data.GetNonceCountByStateAndPurpose(ctx, nonce.EnvironmentSolana, nonce.EnvironmentInstanceSolanaMainnet, state, nonce.PurposeOnDemandTransaction)
				if err != nil {
					continue
				}
				recordNonceCountEvent(ctx, nonce.EnvironmentSolana, nonce.EnvironmentInstanceSolanaMainnet, state, nonce.PurposeOnDemandTransaction, count)

				count, err = p.data.GetNonceCountByStateAndPurpose(ctx, nonce.EnvironmentSolana, nonce.EnvironmentInstanceSolanaMainnet, state, nonce.PurposeInternalServerProcess)
				if err != nil {
					continue
				}
				recordNonceCountEvent(ctx, nonce.EnvironmentSolana, nonce.EnvironmentInstanceSolanaMainnet, state, nonce.PurposeInternalServerProcess, count)

				count, err = p.data.GetNonceCountByStateAndPurpose(ctx, nonce.EnvironmentSolana, nonce.EnvironmentInstanceSolanaMainnet, state, nonce.PurposeClientSwap)
				if err != nil {
					continue
				}
				recordNonceCountEvent(ctx, nonce.EnvironmentSolana, nonce.EnvironmentInstanceSolanaMainnet, state, nonce.PurposeClientSwap, count)
			}

			delay = time.Second - time.Since(start)
		}
	}
}

func recordNonceCountEvent(ctx context.Context, env nonce.Environment, instance string, state nonce.State, useCase nonce.Purpose, count uint64) {
	metrics.RecordEvent(ctx, nonceCountCheckEventName, map[string]interface{}{
		"pool":     fmt.Sprintf("%s:%s", env.String(), instance),
		"use_case": useCase.String(),
		"state":    state.String(),
		"count":    count,
	})
}
