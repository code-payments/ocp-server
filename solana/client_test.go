package solana

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSignatureStatus(t *testing.T) {
	zero, one := 0, 1

	testCases := []struct {
		s         SignatureStatus
		confirmed bool
		finalized bool
	}{
		{
			s: SignatureStatus{
				Slot:               10,
				ErrorResult:        nil,
				Confirmations:      &zero,
				ConfirmationStatus: "",
			},
		},
		{
			s: SignatureStatus{
				Slot:               10,
				ErrorResult:        nil,
				Confirmations:      &zero,
				ConfirmationStatus: "random",
			},
		},
		{
			s: SignatureStatus{
				Slot:               10,
				ErrorResult:        nil,
				Confirmations:      &zero,
				ConfirmationStatus: confirmationStatusProcessed,
			},
		},
		{
			s: SignatureStatus{
				Slot:               10,
				ErrorResult:        nil,
				Confirmations:      &one,
				ConfirmationStatus: "",
			},
			confirmed: true,
		},
		{
			s: SignatureStatus{
				Slot:               10,
				ErrorResult:        nil,
				Confirmations:      &zero,
				ConfirmationStatus: confirmationStatusConfirmed,
			},
			confirmed: true,
		},
		{
			s: SignatureStatus{
				Slot:               10,
				ErrorResult:        nil,
				Confirmations:      &zero,
				ConfirmationStatus: confirmationStatusFinalized,
			},
			confirmed: true,
			finalized: true,
		},
	}

	for _, tc := range testCases {
		assert.Equal(t, tc.confirmed, tc.s.Confirmed())
		assert.Equal(t, tc.finalized, tc.s.Finalized())
	}
}

func TestGetAccountDataAfterBlock(t *testing.T) {
	account := make(ed25519.PublicKey, ed25519.PublicKeySize)
	data := []byte{1, 2, 3}

	for _, tc := range []struct {
		name     string
		response string
		expected []byte
		slot     uint64
		err      error
	}{
		{
			name:     "success",
			response: `{"jsonrpc":"2.0","id":0,"result":{"context":{"slot":101},"value":{"data":["` + base64.StdEncoding.EncodeToString(data) + `","base64"]}}}`,
			expected: data,
			slot:     101,
		},
		{
			name:     "min context slot not reached",
			response: `{"jsonrpc":"2.0","id":0,"error":{"code":-32016,"message":"Minimum context slot has not been reached"}}`,
			err:      ErrStaleData,
		},
		{
			name:     "min context slot not honoured",
			response: `{"jsonrpc":"2.0","id":0,"result":{"context":{"slot":100},"value":{"data":["` + base64.StdEncoding.EncodeToString(data) + `","base64"]}}}`,
			err:      ErrStaleData,
		},
		{
			name:     "no account",
			response: `{"jsonrpc":"2.0","id":0,"result":{"context":{"slot":101},"value":null}}`,
			slot:     101,
			err:      ErrNoAccountInfo,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var req struct {
					Method string            `json:"method"`
					Params []json.RawMessage `json:"params"`
				}
				if assert.NoError(t, json.NewDecoder(r.Body).Decode(&req)) {
					assert.Equal(t, "getAccountInfo", req.Method)
					if assert.Len(t, req.Params, 2) {
						assert.JSONEq(t, `"11111111111111111111111111111111"`, string(req.Params[0]))
						assert.JSONEq(t, `{"commitment":"finalized","encoding":"base64","minContextSlot":101}`, string(req.Params[1]))
					}
				}

				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(tc.response))
			}))
			defer server.Close()

			actual, slot, err := New(server.URL).GetAccountDataAfterBlock(account, 100)
			if tc.err != nil {
				assert.ErrorIs(t, err, tc.err)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, tc.expected, actual)
			assert.Equal(t, tc.slot, slot)
		})
	}
}
