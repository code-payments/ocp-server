package geyser

import (
	"context"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/code-payments/ocp-server/database/query"
	"github.com/code-payments/ocp-server/metrics"
	"github.com/code-payments/ocp-server/ocp/common"
	"github.com/code-payments/ocp-server/ocp/data/account"
	"github.com/code-payments/ocp-server/ocp/data/timelock"
	timelock_token "github.com/code-payments/ocp-server/solana/timelock/v1"
)

// Backup system workers can be found here. This is necessary because we can't rely
// on receiving all updates from Geyser. As a result, we should design backup systems
// to assume Geyser doesn't function/exist at all. Why do we need Geyser if this is
// the case? Real time updates. Backup workers likely won't be able to guarantee
// real time (or near real time) updates at scale.

func (p *runtime) backupTimelockStateWorker(runtimeCtx context.Context, state timelock_token.TimelockState, interval time.Duration) error {
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

	start := time.Now()
	cursor := query.EmptyCursor
	delay := 0 * time.Second // Initially no delay, so we can run right after a deploy
	for {
		select {
		case <-time.After(delay):
			batchStart := time.Now()

			func() {
				provider := runtimeCtx.Value(metrics.ProviderContextKey).(metrics.Provider)
				trace := provider.StartTrace("geyser_consumer_runtime__backup_timelock_state_worker")
				defer trace.End()
				tracedCtx := metrics.NewContext(runtimeCtx, trace)

				timelockRecords, err := p.data.GetAllTimelocksByState(
					tracedCtx,
					state,
					query.WithDirection(query.Ascending),
					query.WithCursor(cursor),
					query.WithLimit(256),
				)
				if err == timelock.ErrTimelockNotFound {
					p.metricStatusLock.Lock()
					duration := time.Since(start)
					if p.backupTimelockStateWorkerDuration == nil || *p.backupTimelockStateWorkerDuration < duration {
						p.backupTimelockStateWorkerDuration = &duration
					}
					p.metricStatusLock.Unlock()

					start = time.Now()
					cursor = query.EmptyCursor
					return
				} else if err != nil {
					log.With(zap.Error(err)).Warn("failed to get timelock records")
					return
				}

				var wg sync.WaitGroup
				for _, timelockRecord := range timelockRecords {
					wg.Add(1)

					go func(timelockRecord *timelock.Record) {
						defer wg.Done()

						log := log.With(zap.String("timelock", timelockRecord.Address))

						err := updateTimelockAccountRecord(tracedCtx, p.data, timelockRecord)
						if err != nil {
							log.With(zap.Error(err)).Warn("failed to update timelock account")
						}
					}(timelockRecord)
				}

				wg.Wait()

				cursor = query.ToCursor(timelockRecords[len(timelockRecords)-1].Id)
			}()

			delay = interval - time.Since(batchStart)
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

				accountInfoRecords, err := p.data.GetPrioritizedAccountInfosRequiringDepositSync(tracedCtx, 256)
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

						err = fixMissingExternalDeposits(tracedCtx, p.data, p.vmIndexerClient, p.integration, authorityAccount, mintAccount)
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
