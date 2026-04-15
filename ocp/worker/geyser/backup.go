package geyser

import (
	"context"
	"sync"
	"time"

	"github.com/mr-tron/base58"
	"go.uber.org/zap"

	"github.com/code-payments/ocp-server/metrics"
	"github.com/code-payments/ocp-server/ocp/common"
	"github.com/code-payments/ocp-server/ocp/data/account"
	"github.com/code-payments/ocp-server/ocp/data/timelock"
	"github.com/code-payments/ocp-server/solana/vm"
)

// Backup system workers can be found here. This is necessary because we can't rely
// on receiving all updates from Geyser. As a result, we should design backup systems
// to assume Geyser doesn't function/exist at all. Why do we need Geyser if this is
// the case? Real time updates. Backup workers likely won't be able to guarantee
// real time (or near real time) updates at scale.

func (p *runtime) backupTimelockStateWorker(runtimeCtx context.Context, interval time.Duration) error {
	log := p.log.With(zap.String("method", "backupTimelockStateWorker"))
	log.Debug("worker started")

	p.metricStatusLock.Lock()
	p.backupTimelockStateWorkerStatus = true
	p.metricStatusLock.Unlock()
	defer func() {
		p.metricStatusLock.Lock()
		p.backupTimelockStateWorkerStatus = false
		p.metricStatusLock.Unlock()

		log.Debug("worker stopped")
	}()

	delay := 0 * time.Second // Initially no delay, so we can run right after a deploy
	for {
		select {
		case <-time.After(delay):
			start := time.Now()

			func() {
				provider := runtimeCtx.Value(metrics.ProviderContextKey).(metrics.Provider)
				trace := provider.StartTrace("geyser_consumer_runtime__backup_timelock_state_worker")
				defer trace.End()
				tracedCtx := metrics.NewContext(runtimeCtx, trace)

				// todo: Also filter on unlock at if the result set gets too large
				programAccounts, slot, err := p.data.GetBlockchainFilteredProgramAccounts(
					tracedCtx,
					base58.Encode(vm.PROGRAM_ID),
					0,
					vm.UnlockStateAccountDiscriminator,
				)
				if err != nil {
					log.With(zap.Error(err)).Warn("failed to get unlock state program accounts")
					return
				}

				reprocessDelay := p.conf.backupTimelockWorkerReprocessDelay.Get(runtimeCtx)

				for _, programAccount := range programAccounts {
					var unlockState vm.UnlockStateAccount
					if err := unlockState.Unmarshal(programAccount.Data); err != nil {
						log.With(zap.Error(err)).Warn("failed to unmarshal unlock state account")
						continue
					}

					stateAddress := base58.Encode(unlockState.Address)
					log := log.With(zap.String("timelock", stateAddress))

					if lastProcessed, ok := p.backupTimelockProcessedCache.Load(stateAddress); ok {
						if time.Since(lastProcessed.(time.Time)) < reprocessDelay {
							continue
						}
					}

					timelockRecord, err := p.data.GetTimelockByAddress(tracedCtx, stateAddress)
					if err == timelock.ErrTimelockNotFound {
						p.backupTimelockProcessedCache.Store(stateAddress, time.Now())
						continue
					} else if err != nil {
						log.With(zap.Error(err)).Warn("failed to get timelock record")
						continue
					}

					if err := updateTimelockAccountRecord(tracedCtx, p.data, timelockRecord, &unlockState, slot); err != nil && err != timelock.ErrStaleTimelockState {
						log.With(zap.Error(err)).Warn("failed to update timelock account")
						continue
					}

					p.backupTimelockProcessedCache.Store(stateAddress, time.Now())
				}

				p.metricStatusLock.Lock()
				duration := time.Since(start)
				if p.backupTimelockStateWorkerDuration == nil || *p.backupTimelockStateWorkerDuration < duration {
					p.backupTimelockStateWorkerDuration = &duration
				}
				p.metricStatusLock.Unlock()
			}()

			delay = interval - time.Since(start)
		case <-runtimeCtx.Done():
			return runtimeCtx.Err()
		}
	}
}

func (p *runtime) backupExternalDepositWorker(runtimeCtx context.Context, interval time.Duration) error {
	log := p.log.With(zap.String("method", "backupExternalDepositWorker"))
	log.Debug("worker started")

	p.metricStatusLock.Lock()
	p.backupExternalDepositWorkerStatus = true
	p.metricStatusLock.Unlock()
	defer func() {
		p.metricStatusLock.Lock()
		p.backupExternalDepositWorkerStatus = false
		p.metricStatusLock.Unlock()

		log.Debug("worker stopped")
	}()

	for {
		select {
		case <-time.After(interval):
			func() {
				provider := runtimeCtx.Value(metrics.ProviderContextKey).(metrics.Provider)
				trace := provider.StartTrace("geyser_consumer_runtime__backup_external_deposit_worker")
				defer trace.End()
				tracedCtx := metrics.NewContext(runtimeCtx, trace)

				accountInfoRecords, err := p.data.GetPrioritizedAccountInfosRequiringDepositSync(tracedCtx, p.conf.backupExternalDepositWorkerBatchSize.Get(runtimeCtx))
				if err == account.ErrAccountInfoNotFound {
					return
				} else if err != nil {
					log.With(zap.Error(err)).Warn("failed to get account info records")
					return
				}

				var wg sync.WaitGroup
				for _, accountInfoRecord := range accountInfoRecords {
					wg.Add(1)

					go func(accountInfoRecord *account.Record) {
						defer wg.Done()

						authorityAccount, err := common.NewAccountFromPublicKeyString(accountInfoRecord.AuthorityAccount)
						if err != nil {
							log.With(zap.Error(err)).Warn("invalid authority account")
							return
						}

						mintAccount, err := common.NewAccountFromPublicKeyString(accountInfoRecord.MintAccount)
						if err != nil {
							log.With(zap.Error(err)).Warn("invalid mint account")
							return
						}

						log := log.With(
							zap.String("authority", authorityAccount.PublicKey().ToBase58()),
							zap.String("mint", mintAccount.PublicKey().ToBase58()),
						)

						err = fixMissingExternalDeposits(tracedCtx, p.data, p.integration, authorityAccount, mintAccount)
						if err != nil {
							log.With(zap.Error(err)).Warn("failed to fix missing external deposits")
						}
					}(accountInfoRecord)
				}

				wg.Wait()
			}()
		case <-runtimeCtx.Done():
			return runtimeCtx.Err()
		}
	}
}
