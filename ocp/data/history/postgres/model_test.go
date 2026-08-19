package postgres

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/code-payments/ocp-server/ocp/data/history"
)

// The encoded form is the storage schema, so it is asserted literally rather
// than round tripped. A round trip passes when a field is renamed or a fee type
// is reordered, because both sides of the trip move together, and every fee
// already stored is left decoding to a zero value.
func TestMarshalFees_StorageSchema(t *testing.T) {
	fees := []history.Fee{
		{Type: history.ReserveSellFee, NativeAmount: 0.25},
		{Type: history.CurrencyLaunchFee, NativeAmount: 10},
	}

	actual, err := marshalFees(fees)
	require.NoError(t, err)
	assert.Equal(t, `[{"t":2,"na":0.25},{"t":4,"na":10}]`, actual)

	decoded, err := unmarshalFees(actual)
	require.NoError(t, err)
	assert.Equal(t, fees, decoded)
}

func TestMarshalFees_Empty(t *testing.T) {
	for _, fees := range [][]history.Fee{nil, {}} {
		actual, err := marshalFees(fees)
		require.NoError(t, err)
		assert.Equal(t, "[]", actual)

		decoded, err := unmarshalFees(actual)
		require.NoError(t, err)
		assert.Nil(t, decoded)
	}
}

// A blob that cannot be decoded must be reported. Returning no fees would be
// written back as no fees by the next state transition, which reads a record,
// changes the state it has reached, and saves the whole thing.
func TestUnmarshalFees_Undecodable(t *testing.T) {
	for _, invalid := range []string{"", "not json", `{"t":1}`, `[{"t":`} {
		_, err := unmarshalFees(invalid)
		assert.Error(t, err, "expected an error for %q", invalid)
	}
}
