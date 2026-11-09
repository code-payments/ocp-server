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
	"github.com/code-payments/ocp-server/ocp/data/swap"
	"github.com/code-payments/ocp-server/retry"
)

const (
	initialAuthorityFundingLamports = 1_300_000_000 // 1.3 SOL

	initNoncesPerTxn = 50
)

// todo: Improve paralellization, given blocking initialization takes a long time
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
		zap.String("name", record.Name),
	)

	var err error
	switch record.State {
	case currency.MetadataStateWaitingForInitialPurchase:
		err = p.handleStateWaitingForInitialPurchase(ctx, record)
	case currency.MetadataStatePrePurchaseSetup:
		err = p.handleStatePrePurchaseSetup(ctx, record)
	case currency.MetadataStateCompletingInitialization:
		err = p.handleStateCompletingInitialization(ctx, record)
	case currency.MetadataStateFinalValidation:
		err = p.handleStateFinalValidation(ctx, record)
	case currency.MetadataStateAbandoning:
		err = p.handleStateAbandoning(ctx, record)
	}
	if err != nil {
		log.With(zap.Error(err)).Warn("failure processing currency to launch")
		return err
	}
	return nil
}

func (p *runtime) handleStateWaitingForInitialPurchase(ctx context.Context, currencyMetadataRecord *currency.MetadataRecord) error {
	err := p.validateCurrencyMetadataState(currencyMetadataRecord, currency.MetadataStateWaitingForInitialPurchase)
	if err != nil {
		return err
	}

	timeout := p.conf.initialPurchaseTimeout.Get(ctx)
	if time.Since(currencyMetadataRecord.CreatedAt) < timeout {
		return nil
	}

	return p.markCurrencyMetadataAbandoning(ctx, currencyMetadataRecord)
}

// Note: Assumes unique authority per currency
func (p *runtime) handleStatePrePurchaseSetup(ctx context.Context, currencyMetadataRecord *currency.MetadataRecord) error {
	err := p.validateCurrencyMetadataState(currencyMetadataRecord, currency.MetadataStatePrePurchaseSetup)
	if err != nil {
		return err
	}

	// Validate the associated swap is in the submitting state before funding
	// the authority. This prevents wasting SOL if the swap has been cancelled
	// or hasn't progressed far enough.
	swapRecords, err := p.data.GetAllSwapsByOwnerMintAndState(ctx, currencyMetadataRecord.CreatedBy, currencyMetadataRecord.Mint, swap.StateSubmitting)
	if err == swap.ErrNotFound {
		return nil
	} else if err != nil {
		return errors.Wrap(err, "error checking associated swap state")
	}
	if len(swapRecords) == 0 {
		return nil
	}

	authorityAccount, err := common.NewAccountFromPublicKeyString(currencyMetadataRecord.Authority)
	if err != nil {
		return errors.Wrap(err, "invalid authority")
	}

	ok, err := validateAuthorityPrivateKeyExists(ctx, p.data, authorityAccount)
	if err != nil {
		return errors.Wrap(err, "error checking authority private key")
	} else if !ok {
		return errors.New("authority private key doesn't exist")
	}

	ok, remainingLamports, err := validateMinimumAuthorityFunding(ctx, p.data, authorityAccount, initialAuthorityFundingLamports)
	if err != nil {
		return errors.Wrap(err, "error validating minimum authority funding")
	} else if !ok {
		err = fundAuthority(ctx, p.data, p.subsidizer, authorityAccount, remainingLamports)
		if err != nil {
			return errors.Wrap(err, "error funding authority")
		}
	}

	vmMetadataRecord, err := p.data.GetVmMetadataByMint(ctx, currencyMetadataRecord.Mint)
	if err != nil {
		return errors.Wrap(err, "error getting vm metadata record")
	}

	err = p.initCurrencyAheadOfLegacyPurchase(ctx, currencyMetadataRecord, vmMetadataRecord, swapRecords[0])
	if err != nil {
		return err
	}

	return p.data.ExecuteInTx(ctx, sql.LevelDefault, func(ctx context.Context) error {
		err := p.markVmMetadataInitializing(ctx, vmMetadataRecord)
		if err != nil {
			return err
		}
		return p.markCurrencyMetadataExecutingInitialPurchase(ctx, currencyMetadataRecord)
	})
}

func (p *runtime) handleStateCompletingInitialization(ctx context.Context, currencyMetadataRecord *currency.MetadataRecord) error {
	err := p.validateCurrencyMetadataState(currencyMetadataRecord, currency.MetadataStateCompletingInitialization)
	if err != nil {
		return err
	}

	vmMetadataRecord, err := p.data.GetVmMetadataByMint(ctx, currencyMetadataRecord.Mint)
	if err != nil {
		return errors.Wrap(err, "error getting vm metadata record")
	}

	accounts, err := p.getNewCurrencyAccounts(ctx, currencyMetadataRecord, vmMetadataRecord)
	if err != nil {
		return errors.Wrap(err, "error getting currency accounts")
	}

	err = p.completeBlockchainInitialization(ctx, currencyMetadataRecord, accounts)
	if err != nil {
		return err
	}

	// todo: Need mechanism for nonce memory accounts, which are pre-populated with random accounts
	//       not known until after finalization. No direct impact here, given that we don't need to
	//       reserve state later.

	err = p.addTimelockMemoryAccountToDb(ctx, accounts)
	if err != nil {
		return errors.Wrap(err, "error adding timelock memory account to db")
	}

	//
	// Initialization phase 5 (nonce pool)
	//

	ok, err := validateNonceMemoryAccountPopulated(ctx, p.data, accounts.NonceMemoryAccount)
	if err != nil {
		return errors.Wrap(err, "error checking if initialization phase 5 is complete")
	} else if !ok {
		err := p.populateNonceMemory(ctx, accounts)
		if err != nil {
			return errors.Wrap(err, "error populating nonce memory")
		}
	}

	ok, err = validateNoncePoolInitialized(ctx, p.data, accounts.NonceMemoryAccount)
	if err != nil {
		return errors.Wrap(err, "error checking if initialization phase 5 is complete")
	} else if !ok {
		err := p.initializeNoncePool(ctx, accounts)
		if err != nil {
			return errors.Wrap(err, "error initializing nonce pool")
		}
	}

	//
	// Initialization complete
	//

	return p.markCurrencyMetadataFinalValidation(ctx, currencyMetadataRecord)
}

// completeBlockchainInitialization initializes the blockchain accounts the
// initial purchase didn't, through the ALT. Each phase is gated on blockchain
// state, so v0 and v1 initial purchases converge, and a phase interrupted by a
// failure is retried on the next attempt.
func (p *runtime) completeBlockchainInitialization(ctx context.Context, currencyMetadataRecord *currency.MetadataRecord, accounts *newCurrencyAccounts) error {
	//
	// Initialization phase 1 (sanity check that initial purchase and init succeeded)
	//

	ok, err := validateMintExists(ctx, p.data, accounts.Mint)
	if err != nil {
		return errors.Wrap(err, "error checking if initialization phase 1 is complete")
	} else if !ok {
		// todo: How do we recover from this?
		return errors.New("initialization phase 1 did not happen")
	}

	//
	// Initialization phase 2 (metadata, memory accounts and fee account). v1
	// initial purchases do this within the purchase.
	//

	err = p.completeLegacyInitialization(ctx, accounts)
	if err != nil {
		return err
	}

	//
	// Initialization phase 3 (memory account resizes). Every flow resizes after
	// the initial purchase, because the resizes don't fit within the v1
	// purchase transaction's instruction trace limit.
	//

	for _, memoryAccount := range []*common.Account{accounts.NonceMemoryAccount, accounts.TimelockMemoryAccount} {
		ok, err = validateMemoryAccountIsResized(ctx, p.data, memoryAccount)
		if err != nil {
			return errors.Wrap(err, "error checking if memory accounts are resized")
		} else if !ok {
			err = p.resizeMemoryAccounts(ctx, accounts)
			if err != nil {
				return errors.Wrap(err, "error resizing memory accounts")
			}
			break
		}
	}

	//
	// Initialization phase 4 (ALT). Every flow creates the ALT after the
	// initial purchase, and legacy clients require it.
	//

	return p.initializeAlt(ctx, currencyMetadataRecord, accounts)
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

	accounts, err := p.getNewCurrencyAccounts(ctx, currencyMetadataRecord, vmMetadataRecord)
	if err != nil {
		return errors.Wrap(err, "error getting currency accounts")
	}

	ok, err := validateMintExists(ctx, p.data, accounts.Mint)
	if err != nil {
		return errors.Wrap(err, "error validating mint exists")
	} else if !ok {
		return errors.New("mint doesn't exist")
	}

	ok, err = validateCurrencyConfigExists(ctx, p.data, accounts.CurrencyConfig)
	if err != nil {
		return errors.Wrap(err, "error validating currency config exists")
	} else if !ok {
		return errors.New("currency config doesn't exist")
	}

	ok, err = validateLiquidityPoolExists(ctx, p.data, accounts.LiquidityPool)
	if err != nil {
		return errors.Wrap(err, "error validating liquidity pool exists")
	} else if !ok {
		return errors.New("liquidity pool doesn't exist")
	}

	ok, err = validateVmExists(ctx, p.data, accounts.Vm)
	if err != nil {
		return errors.Wrap(err, "error validating vm exists")
	} else if !ok {
		return errors.New("vm doesn't exist")
	}

	ok, err = validateAltIsExtended(ctx, p.data, accounts.Alt)
	if err != nil {
		return errors.Wrap(err, "error validating alt exists and is extended")
	} else if !ok {
		return errors.New("alt failed validation")
	}

	ok, err = validateFeeAccountExists(ctx, p.data, accounts.Fees)
	if err != nil {
		return errors.Wrap(err, "error validating fee acount exists")
	} else if !ok {
		return errors.New("fee account doesn't exist")
	}

	ok, err = validateMemoryAccountIsResized(ctx, p.data, accounts.TimelockMemoryAccount)
	if err != nil {
		return errors.Wrap(err, "error validating timelock memory account exists and is resized")
	} else if !ok {
		return errors.New("timelock memory account failed validation")
	}

	ok, err = validateNonceMemoryAccountPopulated(ctx, p.data, accounts.NonceMemoryAccount)
	if err != nil {
		return errors.Wrap(err, "error validating nonce memory account exists and is populated")
	} else if !ok {
		return errors.New("nonce memory account failed validation")
	}

	ok, err = validateNoncePoolInitialized(ctx, p.data, accounts.NonceMemoryAccount)
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

		err = p.markCurrencyMetadataAvailable(ctx, currencyMetadataRecord)
		if err != nil {
			return err
		}

		err = p.putInitialReserveState(ctx, currencyMetadataRecord)
		if err != nil {
			return err
		}

		err = p.putInitialHolderCount(ctx, currencyMetadataRecord)
		if err != nil {
			return err
		}

		return p.initializeCreatorAcccount(ctx, currencyMetadataRecord, accounts)
	})
}

func (p *runtime) handleStateAbandoning(ctx context.Context, currencyMetadataRecord *currency.MetadataRecord) error {
	err := p.validateCurrencyMetadataState(currencyMetadataRecord, currency.MetadataStateAbandoning)
	if err != nil {
		return err
	}

	mintAccount, err := common.NewAccountFromPublicKeyString(currencyMetadataRecord.Mint)
	if err != nil {
		return errors.Wrap(err, "invalid mint")
	}

	ok, err := validateMintExists(ctx, p.data, mintAccount)
	if err != nil {
		return errors.Wrap(err, "error checking if currency is initialized")
	} else if ok {
		// todo: requires manual intervention
		return errors.New("currency exists")
	}

	authorityVaultRecord, err := p.data.GetKey(ctx, currencyMetadataRecord.Authority)
	if err != nil {
		return errors.Wrap(err, "error getting authority vault record")
	}

	authority, err := common.NewAccountFromPrivateKeyString(authorityVaultRecord.PrivateKey)
	if err != nil {
		return errors.Wrap(err, "invalid authority private key")
	}

	err = returnAuthorityFundsToSubsidizer(ctx, p.data, p.subsidizer, authority)
	if err != nil {
		return errors.Wrap(err, "error returning authority funds to subsidizer")
	}

	return p.markCurrencyMetadataAbandoned(ctx, currencyMetadataRecord)
}
