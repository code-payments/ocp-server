package nonce

import (
	"context"
	"fmt"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/zap"

	indexerpb "github.com/code-payments/code-vm-indexer/generated/indexer/v1"

	"github.com/code-payments/ocp-server/ocp/common"
	ocp_data "github.com/code-payments/ocp-server/ocp/data"
	"github.com/code-payments/ocp-server/ocp/data/nonce"
	"github.com/code-payments/ocp-server/ocp/worker"
)

var (
	ErrInvalidNonceAccountSize   = errors.New("invalid nonce account size")
	ErrInvalidNonceLimitExceeded = errors.New("nonce account limit exceeded")
	ErrNoAvailableKeys           = errors.New("no available keys in the vault")
)

type runtime struct {
	log             *zap.Logger
	conf            *conf
	data            ocp_data.Provider
	vmIndexerClient indexerpb.IndexerClient

	rent uint64
}

func New(log *zap.Logger, data ocp_data.Provider, vmIndexerClient indexerpb.IndexerClient, configProvider ConfigProvider) worker.Runtime {
	return &runtime{
		log:             log,
		conf:            configProvider(),
		data:            data,
		vmIndexerClient: vmIndexerClient,
	}
}

func (p *runtime) Start(ctx context.Context, interval time.Duration) error {
	// Generate vault keys until we have a minimum in reserve to use for the pool
	// on Solana mainnet
	go p.generateKeys(ctx)

	// Watch the size of the Solana mainnet nonce pool and create accounts if necessary
	go p.generateNonceAccountsOnSolanaMainnet(ctx, nonce.PurposeOnDemandTransaction, p.conf.onDemandTransactionNoncePoolSize.Get(ctx))
	go p.generateNonceAccountsOnSolanaMainnet(ctx, nonce.PurposeClientSwap, p.conf.clientSwapNoncePoolSize.Get(ctx))

	// Setup workers to watch for nonce state changes on the Solana side
	for _, state := range []nonce.State{
		nonce.StateUnknown,
		nonce.StateReleased,
	} {
		go func(state nonce.State) {

			err := p.worker(ctx, nonce.EnvironmentSolana, nonce.EnvironmentInstanceSolanaMainnet, state, interval)
			if err != nil && err != context.Canceled {
				p.log.With(zap.Error(err)).Warn(fmt.Sprintf("nonce processing loop terminated unexpectedly for env %s, instance %s, state %d", nonce.EnvironmentSolana, nonce.EnvironmentInstanceSolanaMainnet, state))
			}

		}(state)
	}

	// Setup workers to watch for nonce state changes on the CVM side
	//
	// todo: Dynamically detect VMs
	for _, vm := range []string{
		common.CodeVmAccount.PublicKey().ToBase58(),
	} {
		for _, state := range []nonce.State{
			nonce.StateReleased,
		} {
			go func(vm string, state nonce.State) {

				err := p.worker(ctx, nonce.EnvironmentCvm, vm, state, interval)
				if err != nil && err != context.Canceled {
					p.log.With(zap.Error(err)).Warn(fmt.Sprintf("nonce processing loop terminated unexpectedly for env %s, instance %s, state %d", nonce.EnvironmentCvm, vm, state))
				}

			}(vm, state)
		}
	}

	go func() {
		err := p.metricsGaugeWorker(ctx)
		if err != nil && err != context.Canceled {
			p.log.With(zap.Error(err)).Warn("nonce metrics gauge loop terminated unexpectedly")
		}
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	}
}
