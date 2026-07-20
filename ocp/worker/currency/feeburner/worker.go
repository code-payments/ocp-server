package feeburner

import (
	"context"

	"github.com/pkg/errors"
	"go.uber.org/zap"

	"github.com/code-payments/ocp-server/database/query"
	"github.com/code-payments/ocp-server/metrics"
	"github.com/code-payments/ocp-server/ocp/common"
	"github.com/code-payments/ocp-server/ocp/data/currency"
	transaction_util "github.com/code-payments/ocp-server/ocp/transaction"
	"github.com/code-payments/ocp-server/solana"
	compute_budget "github.com/code-payments/ocp-server/solana/computebudget"
	"github.com/code-payments/ocp-server/solana/currencycreator"
)

const (
	burnFeesComputeUnitLimit = 200_000
	computeUnitPrice         = 10_000
)

type burnTarget struct {
	mint string
	ixn  solana.Instruction
}

func (p *runtime) sweep(runtimeCtx context.Context) error {
	provider := runtimeCtx.Value(metrics.ProviderContextKey).(metrics.Provider)
	trace := provider.StartTrace("currency_fee_burner_runtime__sweep")
	defer trace.End()
	tracedCtx := metrics.NewContext(runtimeCtx, trace)

	var cursor query.Cursor
	for {
		items, err := p.data.GetAllCurrencyMetadataByState(
			tracedCtx,
			currency.MetadataStateAvailable,
			query.WithLimit(p.conf.batchSize.Get(tracedCtx)),
			query.WithCursor(cursor),
		)
		if err == currency.ErrNotFound {
			return nil
		} else if err != nil {
			trace.OnError(err)
			return err
		}

		targets := make([]*burnTarget, 0, len(items))
		for _, item := range items {
			target, err := p.makeBurnTarget(item)
			if err != nil {
				trace.OnError(err)
				p.log.With(
					zap.Error(err),
					zap.String("mint", item.Mint),
				).Warn("skipping currency with invalid metadata")
				continue
			}

			hasFees, err := p.hasFeesToBurn(tracedCtx, item)
			if err != nil {
				trace.OnError(err)
				p.log.With(
					zap.Error(err),
					zap.String("mint", item.Mint),
				).Warn("failure checking for fees to burn")
				continue
			}
			if !hasFees {
				continue
			}

			targets = append(targets, target)
		}

		for _, batch := range p.packBurnBatches(targets) {
			err := p.burnFeesForBatch(tracedCtx, batch)
			if err != nil {
				trace.OnError(err)
				p.log.With(
					zap.Error(err),
					zap.Int("batch_size", len(batch)),
				).Warn("failure burning fees for batch")
			}
		}

		cursor = query.ToCursor(items[len(items)-1].Id)
	}
}

func (p *runtime) makeBurnTarget(record *currency.MetadataRecord) (*burnTarget, error) {
	poolAccount, err := common.NewAccountFromPublicKeyString(record.LiquidityPool)
	if err != nil {
		return nil, errors.Wrap(err, "invalid liquidity pool")
	}

	vaultCoreAccount, err := common.NewAccountFromPublicKeyString(record.VaultCore)
	if err != nil {
		return nil, errors.Wrap(err, "invalid core vault")
	}

	return &burnTarget{
		mint: record.Mint,
		ixn: currencycreator.NewBurnFeesInstruction(
			&currencycreator.BurnFeesInstructionAccounts{
				Payer:     p.subsidizer.PublicKey().ToBytes(),
				Pool:      poolAccount.PublicKey().ToBytes(),
				BaseMint:  common.CoreMintAccount.PublicKey().ToBytes(),
				VaultBase: vaultCoreAccount.PublicKey().ToBytes(),
			},
			&currencycreator.BurnFeesInstructionArgs{},
		),
	}, nil
}

func (p *runtime) hasFeesToBurn(ctx context.Context, record *currency.MetadataRecord) (bool, error) {
	ai, _, err := p.data.GetBlockchainAccountInfo(ctx, record.LiquidityPool, solana.CommitmentFinalized)
	if err == solana.ErrNoAccountInfo {
		return false, nil
	} else if err != nil {
		return false, errors.Wrap(err, "error getting liquidity pool account info")
	}

	var pool currencycreator.LiquidityPoolAccount
	err = pool.Unmarshal(ai.Data)
	if err != nil {
		return false, errors.Wrap(err, "invalid liquidity pool account data")
	}

	return pool.FeesAccumulated > 0, nil
}

// packBurnBatches greedily packs burn targets into the fewest transactions
// that fit within the transaction size limit.
func (p *runtime) packBurnBatches(targets []*burnTarget) [][]*burnTarget {
	var batches [][]*burnTarget
	var current []*burnTarget
	for _, target := range targets {
		candidate := append(current, target)
		txn := p.makeBurnTransaction(candidate)
		if len(txn.Marshal()) > solana.MaxTransactionSize {
			if len(current) == 0 {
				p.log.With(zap.String("mint", target.mint)).Warn("skipping currency with oversized burn transaction")
				continue
			}
			batches = append(batches, current)
			current = []*burnTarget{target}
			continue
		}
		current = candidate
	}
	if len(current) > 0 {
		batches = append(batches, current)
	}
	return batches
}

func (p *runtime) makeBurnTransaction(batch []*burnTarget) solana.Transaction {
	ixns := make([]solana.Instruction, 0, len(batch)+2)
	ixns = append(
		ixns,
		compute_budget.SetComputeUnitLimit(uint32(len(batch))*burnFeesComputeUnitLimit),
		compute_budget.SetComputeUnitPrice(computeUnitPrice),
	)
	for _, target := range batch {
		ixns = append(ixns, target.ixn)
	}
	return solana.NewLegacyTransaction(p.subsidizer.PublicKey().ToBytes(), ixns...)
}

func (p *runtime) burnFeesForBatch(ctx context.Context, batch []*burnTarget) error {
	txn := p.makeBurnTransaction(batch)

	bh, err := p.data.GetBlockchainLatestBlockhash(ctx)
	if err != nil {
		return errors.Wrap(err, "error getting latest blockhash")
	}
	txn.SetBlockhash(bh)

	err = txn.Sign(p.subsidizer.PrivateKey().ToBytes())
	if err != nil {
		return errors.Wrap(err, "error signing transaction")
	}

	err = transaction_util.SubmitAndWaitForFinalization(ctx, p.data, &txn)
	if err != nil {
		return errors.Wrap(err, "error submitting transaction")
	}

	return nil
}
