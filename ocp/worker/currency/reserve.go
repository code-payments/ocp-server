package currency

import (
	"context"
	"time"

	"go.uber.org/zap"

	"github.com/code-payments/ocp-server/metrics"
	"github.com/code-payments/ocp-server/ocp/common"
	"github.com/code-payments/ocp-server/ocp/config"
	currency_util "github.com/code-payments/ocp-server/ocp/currency"
	ocp_data "github.com/code-payments/ocp-server/ocp/data"
	"github.com/code-payments/ocp-server/ocp/data/currency"
	"github.com/code-payments/ocp-server/ocp/worker"
	"github.com/code-payments/ocp-server/retry"
	"github.com/code-payments/ocp-server/retry/backoff"
)

type reserveRuntime struct {
	log  *zap.Logger
	data ocp_data.Provider
}

func NewReserveRuntime(log *zap.Logger, data ocp_data.Provider) worker.Runtime {
	return &reserveRuntime{
		log:  log,
		data: data,
	}
}

func (p *reserveRuntime) Start(runtimeCtx context.Context, interval time.Duration) error {
	for {
		_, err := retry.Retry(
			func() error {
				p.log.Debug("updating reserves")

				provider := runtimeCtx.Value(metrics.ProviderContextKey).(metrics.Provider)
				trace := provider.StartTrace("currency_reserve_runtime")
				defer trace.End()
				tracedCtx := metrics.NewContext(runtimeCtx, trace)

				err := p.UpdateAllLaunchpadCurrencyReserves(tracedCtx)
				if err != nil {
					trace.OnError(err)
					p.log.With(zap.Error(err)).Warn("failed to process current reserve data")
				}

				return err
			},
			retry.NonRetriableErrors(context.Canceled),
			retry.BackoffWithJitter(backoff.BinaryExponential(time.Second), interval, 0.1),
		)
		if err != nil {
			if err != context.Canceled {
				// Should not happen since only non-retriable error is context.Canceled
				p.log.With(zap.Error(err)).Warn("unexpected error when processing current reserve data")
			}

			return err
		}

		select {
		case <-runtimeCtx.Done():
			return runtimeCtx.Err()
		case <-time.After(interval):
		}
	}
}

// todo: Don't hardcode Jeffy and other Flipcash currencies
func (p *reserveRuntime) UpdateAllLaunchpadCurrencyReserves(ctx context.Context) error {
	err1 := func() error {
		bitsMintAccount, _ := common.NewAccountFromPublicKeyString(config.BitsMintPublicKey)

		ciculatingSupply, ts, err := currency_util.GetLaunchpadCurrencyCirculatingSupply(ctx, p.data, bitsMintAccount)
		if err != nil {
			return err
		}

		return p.data.PutCurrencyReserve(ctx, &currency.ReserveRecord{
			Mint:              bitsMintAccount.PublicKey().ToBase58(),
			SupplyFromBonding: ciculatingSupply,
			Time:              ts,
		})
	}()

	err2 := func() error {
		bogeyMintAccount, _ := common.NewAccountFromPublicKeyString(config.BogeyMintPublicKey)

		ciculatingSupply, ts, err := currency_util.GetLaunchpadCurrencyCirculatingSupply(ctx, p.data, bogeyMintAccount)
		if err != nil {
			return err
		}

		return p.data.PutCurrencyReserve(ctx, &currency.ReserveRecord{
			Mint:              bogeyMintAccount.PublicKey().ToBase58(),
			SupplyFromBonding: ciculatingSupply,
			Time:              ts,
		})
	}()

	err3 := func() error {
		floatMintAccount, _ := common.NewAccountFromPublicKeyString(config.FloatMintPublicKey)

		ciculatingSupply, ts, err := currency_util.GetLaunchpadCurrencyCirculatingSupply(ctx, p.data, floatMintAccount)
		if err != nil {
			return err
		}

		return p.data.PutCurrencyReserve(ctx, &currency.ReserveRecord{
			Mint:              floatMintAccount.PublicKey().ToBase58(),
			SupplyFromBonding: ciculatingSupply,
			Time:              ts,
		})
	}()

	err4 := func() error {
		jeffyMintAccount, _ := common.NewAccountFromPublicKeyString(config.JeffyMintPublicKey)

		ciculatingSupply, ts, err := currency_util.GetLaunchpadCurrencyCirculatingSupply(ctx, p.data, jeffyMintAccount)
		if err != nil {
			return err
		}

		return p.data.PutCurrencyReserve(ctx, &currency.ReserveRecord{
			Mint:              jeffyMintAccount.PublicKey().ToBase58(),
			SupplyFromBonding: ciculatingSupply,
			Time:              ts,
		})
	}()

	err5 := func() error {
		marketCoinMintAccount, _ := common.NewAccountFromPublicKeyString(config.MarketCoinMintPublicKey)

		ciculatingSupply, ts, err := currency_util.GetLaunchpadCurrencyCirculatingSupply(ctx, p.data, marketCoinMintAccount)
		if err != nil {
			return err
		}

		return p.data.PutCurrencyReserve(ctx, &currency.ReserveRecord{
			Mint:              marketCoinMintAccount.PublicKey().ToBase58(),
			SupplyFromBonding: ciculatingSupply,
			Time:              ts,
		})
	}()

	err6 := func() error {
		xpMintAccount, _ := common.NewAccountFromPublicKeyString(config.XpMintPublicKey)

		ciculatingSupply, ts, err := currency_util.GetLaunchpadCurrencyCirculatingSupply(ctx, p.data, xpMintAccount)
		if err != nil {
			return err
		}

		return p.data.PutCurrencyReserve(ctx, &currency.ReserveRecord{
			Mint:              xpMintAccount.PublicKey().ToBase58(),
			SupplyFromBonding: ciculatingSupply,
			Time:              ts,
		})
	}()

	err7 := func() error {
		badBoysMintAccount, _ := common.NewAccountFromPublicKeyString(config.BadBoysMintPublicKey)

		ciculatingSupply, ts, err := currency_util.GetLaunchpadCurrencyCirculatingSupply(ctx, p.data, badBoysMintAccount)
		if err != nil {
			return err
		}

		return p.data.PutCurrencyReserve(ctx, &currency.ReserveRecord{
			Mint:              badBoysMintAccount.PublicKey().ToBase58(),
			SupplyFromBonding: ciculatingSupply,
			Time:              ts,
		})
	}()

	err8 := func() error {
		testMintAccount, _ := common.NewAccountFromPublicKeyString(config.TestMintPublicKey)

		ciculatingSupply, ts, err := currency_util.GetLaunchpadCurrencyCirculatingSupply(ctx, p.data, testMintAccount)
		if err != nil {
			return err
		}

		return p.data.PutCurrencyReserve(ctx, &currency.ReserveRecord{
			Mint:              testMintAccount.PublicKey().ToBase58(),
			SupplyFromBonding: ciculatingSupply,
			Time:              ts,
		})
	}()

	err9 := func() error {
		moonyMintAccount, _ := common.NewAccountFromPublicKeyString(config.MoonyMintPublicKey)

		ciculatingSupply, ts, err := currency_util.GetLaunchpadCurrencyCirculatingSupply(ctx, p.data, moonyMintAccount)
		if err != nil {
			return err
		}

		return p.data.PutCurrencyReserve(ctx, &currency.ReserveRecord{
			Mint:              moonyMintAccount.PublicKey().ToBase58(),
			SupplyFromBonding: ciculatingSupply,
			Time:              ts,
		})
	}()

	err10 := func() error {
		bluebucksMintAccount, _ := common.NewAccountFromPublicKeyString(config.BluebucksMintPublicKey)

		ciculatingSupply, ts, err := currency_util.GetLaunchpadCurrencyCirculatingSupply(ctx, p.data, bluebucksMintAccount)
		if err != nil {
			return err
		}

		return p.data.PutCurrencyReserve(ctx, &currency.ReserveRecord{
			Mint:              bluebucksMintAccount.PublicKey().ToBase58(),
			SupplyFromBonding: ciculatingSupply,
			Time:              ts,
		})
	}()

	err11 := func() error {
		linksMintAccount, _ := common.NewAccountFromPublicKeyString(config.LinksMintPublicKey)

		ciculatingSupply, ts, err := currency_util.GetLaunchpadCurrencyCirculatingSupply(ctx, p.data, linksMintAccount)
		if err != nil {
			return err
		}

		return p.data.PutCurrencyReserve(ctx, &currency.ReserveRecord{
			Mint:              linksMintAccount.PublicKey().ToBase58(),
			SupplyFromBonding: ciculatingSupply,
			Time:              ts,
		})
	}()

	err12 := func() error {
		lightspeedMintAccount, _ := common.NewAccountFromPublicKeyString(config.LightspeedMintPublicKey)

		ciculatingSupply, ts, err := currency_util.GetLaunchpadCurrencyCirculatingSupply(ctx, p.data, lightspeedMintAccount)
		if err != nil {
			return err
		}

		return p.data.PutCurrencyReserve(ctx, &currency.ReserveRecord{
			Mint:              lightspeedMintAccount.PublicKey().ToBase58(),
			SupplyFromBonding: ciculatingSupply,
			Time:              ts,
		})
	}()

	err13 := func() error {
		toshiMintAccount, _ := common.NewAccountFromPublicKeyString(config.ToshiMintPublicKey)

		ciculatingSupply, ts, err := currency_util.GetLaunchpadCurrencyCirculatingSupply(ctx, p.data, toshiMintAccount)
		if err != nil {
			return err
		}

		return p.data.PutCurrencyReserve(ctx, &currency.ReserveRecord{
			Mint:              toshiMintAccount.PublicKey().ToBase58(),
			SupplyFromBonding: ciculatingSupply,
			Time:              ts,
		})
	}()

	if err1 != nil {
		return err1
	}
	if err2 != nil {
		return err2
	}
	if err3 != nil {
		return err3
	}
	if err4 != nil {
		return err4
	}
	if err5 != nil {
		return err5
	}
	if err6 != nil {
		return err6
	}
	if err7 != nil {
		return err7
	}
	if err8 != nil {
		return err8
	}
	if err9 != nil {
		return err9
	}
	if err10 != nil {
		return err10
	}
	if err11 != nil {
		return err11
	}
	if err12 != nil {
		return err12
	}
	if err13 != nil {
		return err13
	}

	return nil
}
