package coinbase_stable_swapper

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
	PROGRAM_ADDRESS = mustBase58Decode("pqgqKahpG1y2wsgxFhzaAnkV1cL9vk8MSg9qm4q646F")
	PROGRAM_ID      = ed25519.PublicKey(PROGRAM_ADDRESS)
)

var (
	SPL_TOKEN_PROGRAM_ID        = ed25519.PublicKey(mustBase58Decode("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"))
	ASSOCIATED_TOKEN_PROGRAM_ID = ed25519.PublicKey(mustBase58Decode("ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL"))
	SYSTEM_PROGRAM_ID           = ed25519.PublicKey(mustBase58Decode("11111111111111111111111111111111"))
)
