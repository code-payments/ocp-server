package transaction

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/code-payments/ocp-server/pointer"
	"github.com/code-payments/ocp-server/solana"
)

func TestPriorityFeeLamports(t *testing.T) {
	for _, tc := range []struct {
		limit    uint32
		price    uint64
		expected uint64
	}{
		{200_000, 10_000, 2_000},
		{116_725, 10_000, 1_168}, // 1,167.25 rounds up
		{1, 1, 1},
		{0, 10_000, 0},
		{200_000, 0, 0},
		{math.MaxUint32, math.MaxUint64, math.MaxUint64}, // saturates
	} {
		assert.Equal(t, tc.expected, PriorityFeeLamports(tc.limit, tc.price), "limit=%d price=%d", tc.limit, tc.price)
	}
}

func TestNewV1TransactionConfig(t *testing.T) {
	config := NewV1TransactionConfig(200_000, 10_000, CoinbaseSwapLoadedAccountsDataSizeLimit)

	require.NotNil(t, config.ComputeUnitLimit)
	assert.EqualValues(t, 200_000, *config.ComputeUnitLimit)
	require.NotNil(t, config.LoadedAccountsDataSizeLimit)
	assert.EqualValues(t, 4*1024*1024, *config.LoadedAccountsDataSizeLimit)
	require.NotNil(t, config.PriorityFeeLamports)
	assert.EqualValues(t, 2_000, *config.PriorityFeeLamports)
	assert.Nil(t, config.HeapSize)

	proto := ToProtoTransactionConfig(config)
	assert.EqualValues(t, 200_000, proto.ComputeUnitLimit)
	assert.EqualValues(t, 4*1024*1024, proto.LoadedAccountsDataSizeLimit)
	assert.EqualValues(t, 2_000, proto.PriorityFeeLamports)
	assert.EqualValues(t, 0, proto.HeapSize)

	proto = ToProtoTransactionConfig(solana.TransactionConfig{HeapSize: pointer.Uint32(64 * 1024)})
	assert.EqualValues(t, 64*1024, proto.HeapSize)
}
