package solana

import (
	"crypto/ed25519"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockClient implements Client for testing.
type mockClient struct {
	getSlotFunc            func(Commitment) (uint64, error)
	getAccountInfoFunc     func(ed25519.PublicKey, Commitment) (AccountInfo, uint64, error)
	submitTransactionFunc  func(Transaction, Commitment) (Signature, error)
	getBalanceFunc         func(ed25519.PublicKey) (uint64, error)
	getBlockFunc           func(uint64) (*Block, error)
	getLatestBlockhashFunc func() (Blockhash, error)

	callCount int
}

func (m *mockClient) GetSlot(c Commitment) (uint64, error) {
	m.callCount++
	if m.getSlotFunc != nil {
		return m.getSlotFunc(c)
	}
	return 0, nil
}

func (m *mockClient) GetAccountInfo(pk ed25519.PublicKey, c Commitment) (AccountInfo, uint64, error) {
	m.callCount++
	if m.getAccountInfoFunc != nil {
		return m.getAccountInfoFunc(pk, c)
	}
	return AccountInfo{}, 0, nil
}

func (m *mockClient) SubmitTransaction(tx Transaction, c Commitment) (Signature, error) {
	m.callCount++
	if m.submitTransactionFunc != nil {
		return m.submitTransactionFunc(tx, c)
	}
	return Signature{}, nil
}

func (m *mockClient) GetBalance(pk ed25519.PublicKey) (uint64, error) {
	m.callCount++
	if m.getBalanceFunc != nil {
		return m.getBalanceFunc(pk)
	}
	return 0, nil
}

func (m *mockClient) GetBlock(slot uint64) (*Block, error) {
	m.callCount++
	if m.getBlockFunc != nil {
		return m.getBlockFunc(slot)
	}
	return nil, nil
}

func (m *mockClient) GetLatestBlockhash() (Blockhash, error) {
	m.callCount++
	if m.getLatestBlockhashFunc != nil {
		return m.getLatestBlockhashFunc()
	}
	return Blockhash{}, nil
}

// Stub implementations for remaining interface methods.
func (m *mockClient) GetAccountDataAfterBlock(ed25519.PublicKey, uint64) ([]byte, uint64, error) {
	m.callCount++
	return nil, 0, nil
}
func (m *mockClient) GetBlockSignatures(uint64) ([]string, error) {
	m.callCount++
	return nil, nil
}
func (m *mockClient) GetBlockTime(uint64) (time.Time, error) {
	m.callCount++
	return time.Time{}, nil
}
func (m *mockClient) GetConfirmationStatus(Signature, Commitment) (bool, error) {
	m.callCount++
	return false, nil
}
func (m *mockClient) GetConfirmedBlock(uint64) (*Block, error) {
	m.callCount++
	return nil, nil
}
func (m *mockClient) GetConfirmedBlocksWithLimit(uint64, uint64) ([]uint64, error) {
	m.callCount++
	return nil, nil
}
func (m *mockClient) GetFilteredProgramAccounts(ed25519.PublicKey, uint, []byte) ([]ProgramAccount, uint64, error) {
	m.callCount++
	return nil, 0, nil
}
func (m *mockClient) GetMinimumBalanceForRentExemption(uint64) (uint64, error) {
	m.callCount++
	return 0, nil
}
func (m *mockClient) GetSignatureStatus(Signature, Commitment) (*SignatureStatus, error) {
	m.callCount++
	return nil, nil
}
func (m *mockClient) GetSignatureStatuses([]Signature) ([]*SignatureStatus, error) {
	m.callCount++
	return nil, nil
}
func (m *mockClient) GetSignaturesForAddress(ed25519.PublicKey, Commitment, uint64, string, string) ([]*TransactionSignature, error) {
	m.callCount++
	return nil, nil
}
func (m *mockClient) GetTokenAccountBalance(ed25519.PublicKey, Commitment) (uint64, uint64, error) {
	m.callCount++
	return 0, 0, nil
}
func (m *mockClient) GetTokenAccountsByOwner(ed25519.PublicKey, ed25519.PublicKey) ([]ed25519.PublicKey, error) {
	m.callCount++
	return nil, nil
}
func (m *mockClient) GetTransaction(Signature, Commitment) (ConfirmedTransaction, error) {
	m.callCount++
	return ConfirmedTransaction{}, nil
}
func (m *mockClient) GetTransactionTokenBalances(Signature) (TransactionTokenBalances, error) {
	m.callCount++
	return TransactionTokenBalances{}, nil
}

func TestClientWithFallback_PrimarySuccess(t *testing.T) {
	primary := &mockClient{
		getSlotFunc: func(c Commitment) (uint64, error) {
			return 42, nil
		},
	}
	fallback := &mockClient{}

	client := NewWithFallbackFromClients(primary, fallback)
	slot, err := client.GetSlot(CommitmentFinalized)

	require.NoError(t, err)
	assert.Equal(t, uint64(42), slot)
	assert.Equal(t, 1, primary.callCount)
	assert.Equal(t, 0, fallback.callCount)
}

func TestClientWithFallback_UnexpectedError_UsesFallback(t *testing.T) {
	primary := &mockClient{
		getSlotFunc: func(c Commitment) (uint64, error) {
			return 0, errors.New("connection refused")
		},
	}
	fallback := &mockClient{
		getSlotFunc: func(c Commitment) (uint64, error) {
			return 99, nil
		},
	}

	client := NewWithFallbackFromClients(primary, fallback)
	slot, err := client.GetSlot(CommitmentFinalized)

	require.NoError(t, err)
	assert.Equal(t, uint64(99), slot)
	assert.Equal(t, 1, primary.callCount)
	assert.Equal(t, 1, fallback.callCount)
}

func TestClientWithFallback_ExpectedError_NoFallback(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{"ErrNoAccountInfo", ErrNoAccountInfo},
		{"ErrSignatureNotFound", ErrSignatureNotFound},
		{"ErrNoBalance", ErrNoBalance},
		{"ErrBlockNotAvailable", ErrBlockNotAvailable},
		{"ErrStaleData", ErrStaleData},
		{"TransactionError", NewTransactionError(TransactionErrorAccountNotFound)},
		{"InstructionError", &InstructionError{Index: 0, Err: errors.New("test")}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			primary := &mockClient{
				getBalanceFunc: func(pk ed25519.PublicKey) (uint64, error) {
					return 0, tt.err
				},
			}
			fallback := &mockClient{
				getBalanceFunc: func(pk ed25519.PublicKey) (uint64, error) {
					return 100, nil
				},
			}

			client := NewWithFallbackFromClients(primary, fallback)
			_, err := client.GetBalance(ed25519.PublicKey{})

			assert.Error(t, err)
			assert.Equal(t, 1, primary.callCount)
			assert.Equal(t, 0, fallback.callCount)
		})
	}
}

func TestClientWithFallback_UnexpectedErrors_TriggerFallback(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{"network error", errors.New("connection refused")},
		{"timeout", errors.New("context deadline exceeded")},
		{"unknown error", errors.New("something went wrong")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			primary := &mockClient{
				getBalanceFunc: func(pk ed25519.PublicKey) (uint64, error) {
					return 0, tt.err
				},
			}
			fallback := &mockClient{
				getBalanceFunc: func(pk ed25519.PublicKey) (uint64, error) {
					return 100, nil
				},
			}

			client := NewWithFallbackFromClients(primary, fallback)
			balance, err := client.GetBalance(ed25519.PublicKey{})

			require.NoError(t, err)
			assert.Equal(t, uint64(100), balance)
			assert.Equal(t, 1, primary.callCount)
			assert.Equal(t, 1, fallback.callCount)
		})
	}
}

func TestClientWithFallback_BothFail_ReturnsFallbackError(t *testing.T) {
	primaryErr := errors.New("primary down")
	fallbackErr := errors.New("fallback down")

	primary := &mockClient{
		getSlotFunc: func(c Commitment) (uint64, error) {
			return 0, primaryErr
		},
	}
	fallback := &mockClient{
		getSlotFunc: func(c Commitment) (uint64, error) {
			return 0, fallbackErr
		},
	}

	client := NewWithFallbackFromClients(primary, fallback)
	_, err := client.GetSlot(CommitmentFinalized)

	assert.ErrorIs(t, err, fallbackErr)
	assert.Equal(t, 1, primary.callCount)
	assert.Equal(t, 1, fallback.callCount)
}

func TestClientWithFallback_MultiReturn_UsesFallback(t *testing.T) {
	primary := &mockClient{
		getAccountInfoFunc: func(pk ed25519.PublicKey, c Commitment) (AccountInfo, uint64, error) {
			return AccountInfo{}, 0, errors.New("rpc error")
		},
	}
	fallback := &mockClient{
		getAccountInfoFunc: func(pk ed25519.PublicKey, c Commitment) (AccountInfo, uint64, error) {
			return AccountInfo{Lamports: 1000}, 50, nil
		},
	}

	client := NewWithFallbackFromClients(primary, fallback)
	info, slot, err := client.GetAccountInfo(ed25519.PublicKey{}, CommitmentFinalized)

	require.NoError(t, err)
	assert.Equal(t, uint64(1000), info.Lamports)
	assert.Equal(t, uint64(50), slot)
	assert.Equal(t, 1, primary.callCount)
	assert.Equal(t, 1, fallback.callCount)
}

func TestClientWithFallback_SubmitTransaction_UsesFallback(t *testing.T) {
	primary := &mockClient{
		submitTransactionFunc: func(tx Transaction, c Commitment) (Signature, error) {
			return Signature{}, errors.New("connection timeout")
		},
	}

	expectedSig := Signature{1, 2, 3}
	fallback := &mockClient{
		submitTransactionFunc: func(tx Transaction, c Commitment) (Signature, error) {
			return expectedSig, nil
		},
	}

	client := NewWithFallbackFromClients(primary, fallback)
	sig, err := client.SubmitTransaction(Transaction{}, CommitmentProcessed)

	require.NoError(t, err)
	assert.Equal(t, expectedSig, sig)
	assert.Equal(t, 1, primary.callCount)
	assert.Equal(t, 1, fallback.callCount)
}

func TestIsExpectedError(t *testing.T) {
	assert.True(t, isExpectedError(nil))
	assert.True(t, isExpectedError(ErrNoAccountInfo))
	assert.True(t, isExpectedError(ErrSignatureNotFound))
	assert.True(t, isExpectedError(ErrNoBalance))
	assert.True(t, isExpectedError(ErrBlockNotAvailable))
	assert.True(t, isExpectedError(ErrStaleData))
	assert.True(t, isExpectedError(NewTransactionError(TransactionErrorAccountNotFound)))
	assert.True(t, isExpectedError(&InstructionError{Index: 0, Err: errors.New("test")}))

	assert.False(t, isExpectedError(errors.New("network error")))
}
