package solana

import (
	"crypto/ed25519"
	"time"

	"github.com/pkg/errors"
)

type clientWithFallback struct {
	primary  Client
	fallback Client
}

// NewWithFallback returns a Client that uses the fallback endpoint as a retry
// mechanism when the primary returns an unexpected error (e.g. network failures,
// unknown RPC errors). Domain errors like ErrNoAccountInfo or transaction errors
// are not retried since they would produce the same result on any node.
func NewWithFallback(primaryEndpoint, fallbackEndpoint string) Client {
	return &clientWithFallback{
		primary:  New(primaryEndpoint),
		fallback: New(fallbackEndpoint),
	}
}

// NewWithFallbackFromClients is like NewWithFallback but accepts pre-constructed
// Client instances. This is useful for testing or when clients need custom
// RPC options.
func NewWithFallbackFromClients(primary, fallback Client) Client {
	return &clientWithFallback{
		primary:  primary,
		fallback: fallback,
	}
}

// isExpectedError returns true for domain errors that would produce the same
// result on any RPC node, making a fallback retry pointless.
func isExpectedError(err error) bool {
	if err == nil {
		return true
	}

	if errors.Is(err, ErrNoAccountInfo) ||
		errors.Is(err, ErrSignatureNotFound) ||
		errors.Is(err, ErrNoBalance) ||
		errors.Is(err, ErrBlockNotAvailable) ||
		errors.Is(err, ErrStaleData) {
		return true
	}

	var txErr *TransactionError
	if errors.As(err, &txErr) {
		return true
	}

	var instrErr *InstructionError
	if errors.As(err, &instrErr) {
		return true
	}

	return false
}

func withFallback[T any](primary, fallback func() (T, error)) (T, error) {
	result, err := primary()
	if isExpectedError(err) {
		return result, err
	}
	return fallback()
}

func withFallback2[T1, T2 any](primary, fallback func() (T1, T2, error)) (T1, T2, error) {
	r1, r2, err := primary()
	if isExpectedError(err) {
		return r1, r2, err
	}
	return fallback()
}

func (c *clientWithFallback) GetAccountInfo(account ed25519.PublicKey, commitment Commitment) (AccountInfo, uint64, error) {
	return withFallback2(
		func() (AccountInfo, uint64, error) { return c.primary.GetAccountInfo(account, commitment) },
		func() (AccountInfo, uint64, error) { return c.fallback.GetAccountInfo(account, commitment) },
	)
}

func (c *clientWithFallback) GetAccountDataAfterBlock(account ed25519.PublicKey, slot uint64) ([]byte, uint64, error) {
	return withFallback2(
		func() ([]byte, uint64, error) { return c.primary.GetAccountDataAfterBlock(account, slot) },
		func() ([]byte, uint64, error) { return c.fallback.GetAccountDataAfterBlock(account, slot) },
	)
}

func (c *clientWithFallback) GetBalance(account ed25519.PublicKey) (uint64, error) {
	return withFallback(
		func() (uint64, error) { return c.primary.GetBalance(account) },
		func() (uint64, error) { return c.fallback.GetBalance(account) },
	)
}

func (c *clientWithFallback) GetBlock(slot uint64) (*Block, error) {
	return withFallback(
		func() (*Block, error) { return c.primary.GetBlock(slot) },
		func() (*Block, error) { return c.fallback.GetBlock(slot) },
	)
}

func (c *clientWithFallback) GetBlockSignatures(slot uint64) ([]string, error) {
	return withFallback(
		func() ([]string, error) { return c.primary.GetBlockSignatures(slot) },
		func() ([]string, error) { return c.fallback.GetBlockSignatures(slot) },
	)
}

func (c *clientWithFallback) GetBlockTime(block uint64) (time.Time, error) {
	return withFallback(
		func() (time.Time, error) { return c.primary.GetBlockTime(block) },
		func() (time.Time, error) { return c.fallback.GetBlockTime(block) },
	)
}

func (c *clientWithFallback) GetConfirmationStatus(sig Signature, commitment Commitment) (bool, error) {
	return withFallback(
		func() (bool, error) { return c.primary.GetConfirmationStatus(sig, commitment) },
		func() (bool, error) { return c.fallback.GetConfirmationStatus(sig, commitment) },
	)
}

func (c *clientWithFallback) GetConfirmedBlock(slot uint64) (*Block, error) {
	return withFallback(
		func() (*Block, error) { return c.primary.GetConfirmedBlock(slot) },
		func() (*Block, error) { return c.fallback.GetConfirmedBlock(slot) },
	)
}

func (c *clientWithFallback) GetConfirmedBlocksWithLimit(start, limit uint64) ([]uint64, error) {
	return withFallback(
		func() ([]uint64, error) { return c.primary.GetConfirmedBlocksWithLimit(start, limit) },
		func() ([]uint64, error) { return c.fallback.GetConfirmedBlocksWithLimit(start, limit) },
	)
}

func (c *clientWithFallback) GetFilteredProgramAccounts(program ed25519.PublicKey, offset uint, filterValue []byte) ([]ProgramAccount, uint64, error) {
	return withFallback2(
		func() ([]ProgramAccount, uint64, error) {
			return c.primary.GetFilteredProgramAccounts(program, offset, filterValue)
		},
		func() ([]ProgramAccount, uint64, error) {
			return c.fallback.GetFilteredProgramAccounts(program, offset, filterValue)
		},
	)
}

func (c *clientWithFallback) GetLatestBlockhash() (Blockhash, error) {
	return withFallback(
		func() (Blockhash, error) { return c.primary.GetLatestBlockhash() },
		func() (Blockhash, error) { return c.fallback.GetLatestBlockhash() },
	)
}

func (c *clientWithFallback) GetMinimumBalanceForRentExemption(size uint64) (uint64, error) {
	return withFallback(
		func() (uint64, error) { return c.primary.GetMinimumBalanceForRentExemption(size) },
		func() (uint64, error) { return c.fallback.GetMinimumBalanceForRentExemption(size) },
	)
}

func (c *clientWithFallback) GetSignatureStatus(sig Signature, commitment Commitment) (*SignatureStatus, error) {
	return withFallback(
		func() (*SignatureStatus, error) { return c.primary.GetSignatureStatus(sig, commitment) },
		func() (*SignatureStatus, error) { return c.fallback.GetSignatureStatus(sig, commitment) },
	)
}

func (c *clientWithFallback) GetSignatureStatuses(sigs []Signature) ([]*SignatureStatus, error) {
	return withFallback(
		func() ([]*SignatureStatus, error) { return c.primary.GetSignatureStatuses(sigs) },
		func() ([]*SignatureStatus, error) { return c.fallback.GetSignatureStatuses(sigs) },
	)
}

func (c *clientWithFallback) GetSignaturesForAddress(owner ed25519.PublicKey, commitment Commitment, limit uint64, before, until string) ([]*TransactionSignature, error) {
	return withFallback(
		func() ([]*TransactionSignature, error) {
			return c.primary.GetSignaturesForAddress(owner, commitment, limit, before, until)
		},
		func() ([]*TransactionSignature, error) {
			return c.fallback.GetSignaturesForAddress(owner, commitment, limit, before, until)
		},
	)
}

func (c *clientWithFallback) GetSlot(commitment Commitment) (uint64, error) {
	return withFallback(
		func() (uint64, error) { return c.primary.GetSlot(commitment) },
		func() (uint64, error) { return c.fallback.GetSlot(commitment) },
	)
}

func (c *clientWithFallback) GetTokenAccountBalance(account ed25519.PublicKey, commitment Commitment) (uint64, uint64, error) {
	return withFallback2(
		func() (uint64, uint64, error) { return c.primary.GetTokenAccountBalance(account, commitment) },
		func() (uint64, uint64, error) { return c.fallback.GetTokenAccountBalance(account, commitment) },
	)
}

func (c *clientWithFallback) GetTokenAccountsByOwner(owner, mint ed25519.PublicKey) ([]ed25519.PublicKey, error) {
	return withFallback(
		func() ([]ed25519.PublicKey, error) { return c.primary.GetTokenAccountsByOwner(owner, mint) },
		func() ([]ed25519.PublicKey, error) { return c.fallback.GetTokenAccountsByOwner(owner, mint) },
	)
}

func (c *clientWithFallback) GetTransaction(sig Signature, commitment Commitment) (ConfirmedTransaction, error) {
	return withFallback(
		func() (ConfirmedTransaction, error) { return c.primary.GetTransaction(sig, commitment) },
		func() (ConfirmedTransaction, error) { return c.fallback.GetTransaction(sig, commitment) },
	)
}

func (c *clientWithFallback) GetTransactionTokenBalances(sig Signature) (TransactionTokenBalances, error) {
	return withFallback(
		func() (TransactionTokenBalances, error) { return c.primary.GetTransactionTokenBalances(sig) },
		func() (TransactionTokenBalances, error) { return c.fallback.GetTransactionTokenBalances(sig) },
	)
}

func (c *clientWithFallback) SubmitTransaction(txn Transaction, commitment Commitment) (Signature, error) {
	return withFallback(
		func() (Signature, error) { return c.primary.SubmitTransaction(txn, commitment) },
		func() (Signature, error) { return c.fallback.SubmitTransaction(txn, commitment) },
	)
}
