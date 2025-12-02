package async_account

import (
	"context"
	"time"

	"go.uber.org/zap"

	"github.com/code-payments/ocp-server/pkg/code/async"
	"github.com/code-payments/ocp-server/pkg/code/common"
	code_data "github.com/code-payments/ocp-server/pkg/code/data"
)

type service struct {
	log  *zap.Logger
	conf *conf
	data code_data.Provider

	airdropper *common.TimelockAccounts
}

func New(log *zap.Logger, data code_data.Provider, configProvider ConfigProvider) async.Service {
	ctx := context.Background()

	p := &service{
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

func (p *service) Start(ctx context.Context, interval time.Duration) error {

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

func (p *service) mustLoadAirdropper(ctx context.Context) {
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
