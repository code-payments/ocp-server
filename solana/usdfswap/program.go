package usdf_swap

import (
	"crypto/ed25519"
	"errors"
)

var (
	ErrInvalidProgram         = errors.New("invalid program id")
	ErrInvalidAccountData     = errors.New("unexpected account data")
	ErrInvalidInstructionData = errors.New("unexpected instruction data")
)

var (
	PROGRAM_ADDRESS = mustBase58Decode("usdfcP2V1bh1Lz7Y87pxR4zJd3wnVtssJ6GeSHFeZeu")
	PROGRAM_ID      = ed25519.PublicKey(PROGRAM_ADDRESS)
)

var (
	SPL_TOKEN_PROGRAM_ID = ed25519.PublicKey(mustBase58Decode("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"))
	SYSTEM_PROGRAM_ID    = ed25519.PublicKey(mustBase58Decode("11111111111111111111111111111111"))

	SYSVAR_RENT_PUBKEY = ed25519.PublicKey(mustBase58Decode("SysvarRent111111111111111111111111111111111"))
)

const (
	MaxPoolNameLength = 32
	MaxSwapDollars    = 1000
)
