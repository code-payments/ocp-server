package launcher

import (
	"context"
	"fmt"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/zap"

	"github.com/code-payments/ocp-server/ocp/common"
	ocp_data "github.com/code-payments/ocp-server/ocp/data"
	"github.com/code-payments/ocp-server/ocp/data/currency"
	"github.com/code-payments/ocp-server/ocp/worker"
)

type runtime struct {
	log        *zap.Logger
	conf       *conf
	data       ocp_data.Provider
	subsidizer *common.Account
}

func New(log *zap.Logger, data ocp_data.Provider, configProvider ConfigProvider) (worker.Runtime, error) {
	p := &runtime{
		log:  log,
		conf: configProvider(),
		data: data,
	}

	err := p.loadSubsidizer()
	if err != nil {
		return nil, err
	}
	return p, nil
}

func (p *runtime) Start(ctx context.Context, interval time.Duration) error {
	for _, state := range []currency.MetadataState{
		currency.MetadataStateFundingAuthority,
		currency.MetadataStateCompletingInitialization,
		currency.MetadataStateFinalValidation,
	} {
		go func(state currency.MetadataState) {
			err := p.worker(ctx, state, interval)
			if err != nil && err != context.Canceled {
				p.log.With(zap.Error(err)).Warn(fmt.Sprintf("currency launcher processing loop terminated unexpectedly for state %s", state.String()))
			}
		}(state)
	}

	go func() {
		err := p.metricsGaugeWorker(ctx)
		if err != nil && err != context.Canceled {
			p.log.With(zap.Error(err)).Warn("currency launcher metrics gauge loop terminated unexpectedly")
		}
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *runtime) loadSubsidizer() error {
	ctx := context.TODO()

	vaultRecord, err := p.data.GetKey(ctx, p.conf.subsidizer.Get(ctx))
	if err != nil {
		return errors.Wrap(err, "error getting subsidizer vault record")
	}

	p.subsidizer, err = common.NewAccountFromPrivateKeyString(vaultRecord.PrivateKey)
	if err != nil {
		return errors.Wrap(err, "invalid subsidizer private key")
	}
	return nil
}
