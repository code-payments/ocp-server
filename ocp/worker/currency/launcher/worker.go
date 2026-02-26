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
	"github.com/code-payments/ocp-server/solana/vm"
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

func (p *runtime) handleStateFinalValidation(ctx context.Context, currencyMetadataRecord *currency.MetadataRecord) error {
	err := p.validateCurrencyMetadataState(currencyMetadataRecord, currency.MetadataStateFinalValidation)
	if err != nil {
		return err
	}

	vmMetadataRecord, err := p.data.GetVmMetadataByMint(ctx, currencyMetadataRecord.Mint)
	if err != nil {
		return errors.Wrap(err, "error getting vm metadata record")
	}

	currencyAccounts, err := common.GetLaunchpadCurrencyAccounts(currencyMetadataRecord)
	if err != nil {
		return errors.Wrap(err, "error getting launchpad currency accounts")
	}
	vmAccount, err := common.NewAccountFromPublicKeyString(vmMetadataRecord.Vm)
	if err != nil {
		return errors.Wrap(err, "invalid vm")
	}
	timelockMemoryAddress, _, err := vm.GetMemoryAccountAddress(&vm.GetMemoryAccountAddressArgs{
		Name: initialTimelockMemoryAccountName,
		Vm:   vmAccount.PublicKey().ToBytes(),
	})
	if err != nil {
		return errors.Wrap(err, "error deriving timelock memory account address")
	}
	timelockMemoryAccount, err := common.NewAccountFromPublicKeyBytes(timelockMemoryAddress)
	if err != nil {
		return errors.Wrap(err, "invalid timelock memory account")
	}
	nonceMemoryAddress, _, err := vm.GetMemoryAccountAddress(&vm.GetMemoryAccountAddressArgs{
		Name: initialNonceMemoryAccountName,
		Vm:   vmAccount.PublicKey().ToBytes(),
	})
	if err != nil {
		return errors.Wrap(err, "error deriving nonce memory account address")
	}
	nonceMemoryAccount, err := common.NewAccountFromPublicKeyBytes(nonceMemoryAddress)
	if err != nil {
		return errors.Wrap(err, "invalid timelock memory account")
	}

	ok, err := validateMintExists(ctx, p.data, currencyAccounts.Mint)
	if err != nil {
		return errors.Wrap(err, "error validating mint exists")
	} else if !ok {
		return errors.New("mint doesn't exist")
	}

	ok, err = validateCurrencyConfigExists(ctx, p.data, currencyAccounts.CurrencyConfig)
	if err != nil {
		return errors.Wrap(err, "error validating currency config exists")
	} else if !ok {
		return errors.New("currency config doesn't exist")
	}

	ok, err = validateLiquidityPoolExists(ctx, p.data, currencyAccounts.LiquidityPool)
	if err != nil {
		return errors.Wrap(err, "error validating liquidity pool exists")
	} else if !ok {
		return errors.New("")
	}

	ok, err = validateVmExists(ctx, p.data, vmAccount)
	if err != nil {
		return errors.Wrap(err, "error validating vm exists")
	} else if !ok {
		return errors.New("vm doesn't exist")
	}

	ok, err = validateAltIsExtended(ctx, p.data, currencyAccounts.Alt)
	if err != nil {
		return errors.Wrap(err, "error validating alt exists and is extended")
	} else if !ok {
		return errors.New("alt failed validation")
	}

	ok, err = validateMemoryAccountIsResized(ctx, p.data, timelockMemoryAccount)
	if err != nil {
		return errors.Wrap(err, "error validating timelock memory account exists and is resized")
	} else if !ok {
		return errors.New("timelock memory account failed validation")
	}

	ok, err = validateNonceMemoryAccountPopulated(ctx, p.data, nonceMemoryAccount)
	if err != nil {
		return errors.Wrap(err, "error validating nonce memory account exists and is populated")
	} else if !ok {
		return errors.New("nonce memory account failed validation")
	}

	ok, err = validateNoncePoolInitialized(ctx, p.data, nonceMemoryAccount)
	if err != nil {
		return errors.Wrap(err, "error validating nonce pool is initialized")
	} else if !ok {
		return errors.New("nonce pool is not fully initialized")
	}

	return p.data.ExecuteInTx(ctx, sql.LevelDefault, func(ctx context.Context) error {
		err := p.markVmMetadataAvailable(ctx, vmMetadataRecord)
		if err != nil {
			return err
		}
		return p.markCurrencyMetadataAvailable(ctx, currencyMetadataRecord)
	})
}
