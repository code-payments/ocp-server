package feeburner

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/code-payments/ocp-server/ocp/data/currency"
	"github.com/code-payments/ocp-server/solana"
	"github.com/code-payments/ocp-server/testutil"
)

func TestPackBurnBatches(t *testing.T) {
	p := &runtime{
		log:        zap.NewNop(),
		subsidizer: testutil.NewRandomAccount(t),
	}

	var targets []*burnTarget
	for range 40 {
		record := &currency.MetadataRecord{
			Mint:          testutil.NewRandomAccount(t).PublicKey().ToBase58(),
			LiquidityPool: testutil.NewRandomAccount(t).PublicKey().ToBase58(),
			VaultCore:     testutil.NewRandomAccount(t).PublicKey().ToBase58(),
		}

		target, err := p.makeBurnTarget(record)
		require.NoError(t, err)
		targets = append(targets, target)
	}

	batches := p.packBurnBatches(targets, defaultMaxBurnsPerBatch)

	var flattened []*burnTarget
	for _, batch := range batches {
		flattened = append(flattened, batch...)
	}
	require.Len(t, flattened, len(targets))
	for i, target := range targets {
		assert.Equal(t, target.mint, flattened[i].mint)
	}

	for i, batch := range batches {
		txn := p.makeBurnTransaction(batch)
		assert.LessOrEqual(t, len(txn.Marshal()), solana.MaxTransactionSize, fmt.Sprintf("batch %d exceeds size limit", i))
		assert.LessOrEqual(t, len(batch), defaultMaxBurnsPerBatch, fmt.Sprintf("batch %d exceeds max burns", i))
	}

	// Every batch except the last must be full: it hit the max burn count, or
	// adding the next target would exceed the transaction size limit
	for i := 0; i < len(batches)-1; i++ {
		if len(batches[i]) == defaultMaxBurnsPerBatch {
			continue
		}
		overfilled := append(append([]*burnTarget{}, batches[i]...), batches[i+1][0])
		txn := p.makeBurnTransaction(overfilled)
		assert.Greater(t, len(txn.Marshal()), solana.MaxTransactionSize, fmt.Sprintf("batch %d is not fully packed", i))
	}

	assert.Greater(t, len(batches[0]), 1)
}

func TestPackBurnBatches_Empty(t *testing.T) {
	p := &runtime{
		log:        zap.NewNop(),
		subsidizer: testutil.NewRandomAccount(t),
	}

	assert.Empty(t, p.packBurnBatches(nil, defaultMaxBurnsPerBatch))
}
