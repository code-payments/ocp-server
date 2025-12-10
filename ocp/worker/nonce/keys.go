package nonce

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/code-payments/ocp-server/database/query"
	"github.com/code-payments/ocp-server/metrics"
	"github.com/code-payments/ocp-server/ocp/data/vault"
	"github.com/code-payments/ocp-server/retry"
)

var (
	globalKeyLock sync.Mutex
)

func (p *runtime) generateKey(ctx context.Context) (*vault.Record, error) {
	// todo: audit whether we should be creating keys on the same server.
	// Perhaps this should be done outside this box.

	// Grind for a vanity key (slow)
	key, err := vault.GrindKey(p.conf.solanaMainnetNoncePubkeyPrefix.Get(ctx))
	if err != nil {
		return nil, err
	}
	key.State = vault.StateAvailable

	err = p.data.SaveKey(ctx, key)
	if err != nil {
		return nil, err
	}

	return key, nil
}

func (p *runtime) generateKeys(runtimeCtx context.Context) error {
	err := retry.Loop(
		func() (err error) {
			// Give the server some time to breath.
			time.Sleep(time.Second * 15)

			provider := runtimeCtx.Value(metrics.ProviderContextKey).(metrics.Provider)
			trace := provider.StartTrace("nonce_runtime__vault_keys")
			defer trace.End()
			tracedCtx := metrics.NewContext(runtimeCtx, trace)

			res, err := p.data.GetKeyCountByState(tracedCtx, vault.StateAvailable)
			if err != nil {
				return err
			}

			reserveSize := ((p.conf.onDemandTransactionNoncePoolSize.Get(tracedCtx) + p.conf.clientSwapNoncePoolSize.Get(tracedCtx)) * 2)

			// If we have sufficient keys, don't generate any more.
			if res >= reserveSize {
				return nil
			}

			missing := reserveSize - res

			// Clamp the maximum number of keys to create in one run.
			if missing > 5 {
				missing = 5
			}

			p.log.Info(fmt.Sprintf("Not enough reserve keys available, generating %d more.", missing))

			// We don't have enough in the reserve, so we need to generate some.
			for i := 0; i < int(missing); i++ {
				key, err := p.generateKey(tracedCtx)
				if err != nil {
					p.log.With(zap.Error(err)).Warn("Failure generating key")
					continue
				}
				p.log.Debug(fmt.Sprintf("key: %s", key.PublicKey))
			}

			return nil
		},
		retry.NonRetriableErrors(context.Canceled, ErrInvalidNonceLimitExceeded),
	)

	return err
}

func (p *runtime) reserveExistingKey(ctx context.Context) (*vault.Record, error) {
	globalKeyLock.Lock()
	defer globalKeyLock.Unlock()

	keys, err := p.data.GetAllKeysByState(ctx, vault.StateAvailable,
		query.WithLimit(1),
	)
	if err != nil {
		return nil, err
	}

	res := keys[0]
	res.State = vault.StateReserved

	err = p.data.SaveKey(ctx, res)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func (p *runtime) getVaultKey(ctx context.Context) (*vault.Record, error) {
	key, err := p.reserveExistingKey(ctx)
	if err == nil {
		return key, nil
	}

	return nil, ErrNoAvailableKeys
}
