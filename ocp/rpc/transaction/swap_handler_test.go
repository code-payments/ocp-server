package transaction

import (
	"testing"

	"github.com/stretchr/testify/assert"

	transaction_util "github.com/code-payments/ocp-server/ocp/transaction"
)

func TestSelectCoinbaseSwapVersion(t *testing.T) {
	// Unset (older clients) and 1 both use the legacy swap instruction
	assert.Equal(t, transaction_util.CoinbaseSwapV1, selectCoinbaseSwapVersion(0))
	assert.Equal(t, transaction_util.CoinbaseSwapV1, selectCoinbaseSwapVersion(1))

	assert.Equal(t, transaction_util.CoinbaseSwapV2, selectCoinbaseSwapVersion(2))
}
