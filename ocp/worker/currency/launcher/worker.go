package launcher

import (
	"context"
	"database/sql"
	"sync"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/zap"

	"github.com/code-payments/ocp-server/database/query"
	"github.com/code-payments/ocp-server/metrics"
	"github.com/code-payments/ocp-server/ocp/common"
	"github.com/code-payments/ocp-server/ocp/data/currency"
	"github.com/code-payments/ocp-server/retry"
)

const (
	initialAuthorityFundingLamports = 2_000_000_000 // 2 SOL

	initialNoncePoolSize          = 1_000
	initialNonceMemoryAccountName = "nonce-0"

	initialTimelockAccounts          = 1_000
	initialTimelockMemoryAccountName = "timelock-0"
)

func (p *runtime) worker(runtimeCtx context.Context, state currency.MetadataState, interval time.Duration) error {
	var cursor query.Cursor
	delay := interval

	err := retry.Loop(
		func() (err error) {
			time.Sleep(delay)

			provider := runtimeCtx.Value(metrics.ProviderContextKey).(metrics.Provider)
			trace := provider.StartTrace("currency_launcher_runtime__handle_" + state.String())
			defer trace.End()
			tracedCtx := metrics.NewContext(runtimeCtx, trace)

			items, err := p.data.GetAllCurrencyMetadataByState(
				tracedCtx,
				state,
				query.WithLimit(p.conf.batchSize.Get(runtimeCtx)),
				query.WithCursor(cursor),
			)
			if err == currency.ErrNotFound {
				cursor = query.EmptyCursor
				return nil
			} else if err != nil {
				cursor = query.EmptyCursor
				return err
			}

			var wg sync.WaitGroup
			for _, item := range items {
				wg.Add(1)

				go func(record *currency.MetadataRecord) {
					defer wg.Done()

					err := p.handle(tracedCtx, record)
					if err != nil {
						trace.OnError(err)
					}
				}(item)
			}
			wg.Wait()

			if len(items) > 0 {
				cursor = query.ToCursor(items[len(items)-1].Id)
			} else {
				cursor = query.EmptyCursor
			}

			return nil
		},
		retry.NonRetriableErrors(context.Canceled),
	)

	return err
}

func (p *runtime) handle(ctx context.Context, record *currency.MetadataRecord) error {
	log := p.log.With(
		zap.String("method", "handle"),
		zap.String("state", record.State.String()),
		zap.String("mint", record.Mint),
	)

	var err error
	switch record.State {
	case currency.MetadataStateUnknown:
		err = p.handleStateUnknown(ctx, record)
	case currency.MetadataStateFundingAuthority:
		err = p.handleStateFundingAuthority(ctx, record)
	case currency.MetadataStateInitializing:
		err = p.handleStateInitializing(ctx, record)
	case currency.MetadataStateFinalValidation:
		err = p.handleStateFinalValidation(ctx, record)
	}
	if err != nil {
		log.With(zap.Error(err)).Warn("failure processing currency for launch")
		return err
	}
	return nil
}

func (p *runtime) handleStateUnknown(ctx context.Context, record *currency.MetadataRecord) error {
	err := p.validateCurrencyMetadataState(record, currency.MetadataStateUnknown)
	if err != nil {
		return err
	}

	// Nothing to do here currently

	return p.markCurrencyMetadataFundingAuthority(ctx, record)
}

// Note: Assumes unique authority per currency
func (p *runtime) handleStateFundingAuthority(ctx context.Context, currencyMetadataRecord *currency.MetadataRecord) error {
	err := p.validateCurrencyMetadataState(currencyMetadataRecord, currency.MetadataStateFundingAuthority)
	if err != nil {
		return err
	}

	authorityAccount, err := common.NewAccountFromPublicKeyString(currencyMetadataRecord.Authority)
	if err != nil {
		return errors.Wrap(err, "invalid authority")
	}

	privateKeyExists, err := validateAuthorityPrivateKeyExists(ctx, p.data, authorityAccount)
	if err != nil {
		return errors.Wrap(err, "error checking authority private key")
	} else if !privateKeyExists {
		return errors.New("authority private key doesn't exist")
	}

	isAuthorityFunded, remainingLamports, err := validateMinimumAuthorityFunding(ctx, p.data, authorityAccount, initialAuthorityFundingLamports)
	if err != nil {
		return errors.Wrap(err, "error validating minimum authority funding")
	} else if !isAuthorityFunded {
		err = fundAuthority(ctx, p.data, authorityAccount, remainingLamports)
		if err != nil {
			return errors.Wrap(err, "error funding authority")
		}
	}

	vmMetadataRecord, err := p.data.GetVmMetadataByMint(ctx, currencyMetadataRecord.Mint)
	if err != nil {
		return errors.Wrap(err, "error getting vm metadata record")
	}

	return p.data.ExecuteInTx(ctx, sql.LevelDefault, func(ctx context.Context) error {
		err := p.markVmMetadataInitializing(ctx, vmMetadataRecord)
		if err != nil {
			return err
		}
		return p.markCurrencyMetadataInitializing(ctx, currencyMetadataRecord)
	})
}

func (p *runtime) handleStateInitializing(ctx context.Context, record *currency.MetadataRecord) error {
	err := p.validateCurrencyMetadataState(record, currency.MetadataStateInitializing)
	if err != nil {
		return err
	}

	return nil
}

func (p *runtime) handleStateFinalValidation(ctx context.Context, record *currency.MetadataRecord) error {
	err := p.validateCurrencyMetadataState(record, currency.MetadataStateFinalValidation)
	if err != nil {
		return err
	}

	return nil
}
