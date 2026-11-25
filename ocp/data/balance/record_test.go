package balance

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMergeDeltas(t *testing.T) {
	input := []*Delta{
		{TokenAccount: "b", Kind: DeltaDebit, Quarks: 5, UsdCostBasis: 1},
		{TokenAccount: "a", Kind: DeltaCredit, Quarks: 1, UsdCostBasis: 10},
		{TokenAccount: "b", Kind: DeltaCredit, Quarks: 2, UsdCostBasis: 20},
		{TokenAccount: "b", Kind: DeltaDebit, Quarks: 7, UsdCostBasis: -3},
		{TokenAccount: "a", Kind: DeltaCredit, Quarks: 3, UsdCostBasis: 30},
		{TokenAccount: "c", Kind: DeltaDrain, Quarks: 9, UsdCostBasis: 9},
		{TokenAccount: "c", Kind: DeltaDrain, Quarks: 9, UsdCostBasis: 9},
		{TokenAccount: "d", Kind: DeltaClose},
		{TokenAccount: "d", Kind: DeltaClose},
		{TokenAccount: "a", Kind: DeltaAdjustUsdCostBasis, UsdCostBasis: 5},
		{TokenAccount: "a", Kind: DeltaAdjustUsdCostBasis, UsdCostBasis: -8},
	}
	original := make([]Delta, len(input))
	for i, delta := range input {
		original[i] = *delta
	}

	merged, err := MergeDeltas(input)
	require.NoError(t, err)

	assert.Equal(t, []*Delta{
		{TokenAccount: "a", Kind: DeltaCredit, Quarks: 4, UsdCostBasis: 40},
		{TokenAccount: "a", Kind: DeltaAdjustUsdCostBasis, UsdCostBasis: -3},
		{TokenAccount: "b", Kind: DeltaCredit, Quarks: 2, UsdCostBasis: 20},
		{TokenAccount: "b", Kind: DeltaDebit, Quarks: 12, UsdCostBasis: -2},
		{TokenAccount: "c", Kind: DeltaDrain, Quarks: 9, UsdCostBasis: 9},
		{TokenAccount: "c", Kind: DeltaDrain, Quarks: 9, UsdCostBasis: 9},
		{TokenAccount: "d", Kind: DeltaClose},
		{TokenAccount: "d", Kind: DeltaClose},
	}, merged)

	// The input is left untouched
	for i, delta := range input {
		assert.Equal(t, original[i], *delta)
	}

	merged, err = MergeDeltas(nil)
	require.NoError(t, err)
	assert.Empty(t, merged)

	// Combined amounts can't exceed what a delta can carry
	_, err = MergeDeltas([]*Delta{
		{TokenAccount: "a", Kind: DeltaCredit, Quarks: MaxDeltaQuarks},
		{TokenAccount: "a", Kind: DeltaCredit, Quarks: 1},
	})
	assert.Equal(t, ErrDeltaQuarksOverflow, err)
}
