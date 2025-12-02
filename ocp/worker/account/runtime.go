package account

import (
	"context"
	"time"

	"go.uber.org/zap"

	"github.com/code-payments/ocp-server/ocp/common"
	ocp_data "github.com/code-payments/ocp-server/ocp/data"
	"github.com/code-payments/ocp-server/ocp/worker"
)

type runtime struct {
	log  *zap.Logger
	conf *conf
	data ocp_data.Provider

	airdropper *common.TimelockAccounts
}

func New(log *zap.Logger, data ocp_data.Provider, configProvider ConfigProvider) worker.Runtime {
	ctx := context.Background()

	p := &runtime{
		log:  log,
		conf: configProvider(),
		data: data,
	}

	airdropper := p.conf.airdropperOwnerPublicKey.Get(ctx)
	if len(airdropper) > 0 && airdropper != defaultAirdropperOwnerPublicKey {
		p.mustLoadAirdropper(ctx)
	}

	return p
}

func (p *runtime) Start(ctx context.Context, interval time.Duration) error {

	go func() {
		err := p.giftCardAutoReturnWorker(ctx, interval)
		if err != nil && err != context.Canceled {
			p.log.With(zap.Error(err)).Warn("gift card auto-return processing loop terminated unexpectedly")
		}
	}()

	go func() {
		err := p.metricsGaugeWorker(ctx)
		if err != nil && err != context.Canceled {
			p.log.With(zap.Error(err)).Warn("account metrics gauge loop terminated unexpectedly")
		}
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *runtime) mustLoadAirdropper(ctx context.Context) {
	log := p.log.With(
		zap.String("method", "mustLoadAirdropper"),
		zap.String("key", p.conf.airdropperOwnerPublicKey.Get(ctx)),
	)

	err := func() error {
		vmConfig, err := common.GetVmConfigForMint(ctx, p.data, common.CoreMintAccount)
		if err != nil {
			return err
		}

		vaultRecord, err := p.data.GetKey(ctx, p.conf.airdropperOwnerPublicKey.Get(ctx))
		if err != nil {
			return err
		}

		ownerAccount, err := common.NewAccountFromPrivateKeyString(vaultRecord.PrivateKey)
		if err != nil {
			return err
		}

		timelockAccounts, err := ownerAccount.GetTimelockAccounts(vmConfig)
		if err != nil {
			return err
		}

		p.airdropper = timelockAccounts
		return nil
	}()
	if err != nil {
		log.With(zap.Error(err)).Fatal("failure loading account")
	}
}
