package coinbase_stable_swapper

import (
	"crypto/ed25519"

	"github.com/code-payments/ocp-server/solana"
)

const (
	SwapInstructionArgsSize = (8 + // discriminator
		8 + // amount_in
		8) // min_amount_out
)

type SwapInstructionArgs struct {
	AmountIn     uint64
	MinAmountOut uint64
}

type SwapInstructionAccounts struct {
	Pool                     ed25519.PublicKey
	InVault                  ed25519.PublicKey
	OutVault                 ed25519.PublicKey
	InVaultTokenAccount      ed25519.PublicKey
	OutVaultTokenAccount     ed25519.PublicKey
	UserFromTokenAccount     ed25519.PublicKey
	ToTokenAccount           ed25519.PublicKey
	FeeRecipientTokenAccount ed25519.PublicKey
	FeeRecipient             ed25519.PublicKey
	FromMint                 ed25519.PublicKey
	ToMint                   ed25519.PublicKey
	User                     ed25519.PublicKey
	Whitelist                ed25519.PublicKey
}

func NewSwapInstruction(
	accounts *SwapInstructionAccounts,
	args *SwapInstructionArgs,
) solana.Instruction {
	var offset int

	// Serialize instruction arguments
	data := make([]byte, SwapInstructionArgsSize)

	putDiscriminator(data, SwapInstructionDiscriminator, &offset)
	putUint64(data, args.AmountIn, &offset)
	putUint64(data, args.MinAmountOut, &offset)

	return solana.Instruction{
		Program: PROGRAM_ADDRESS,

		// Instruction args
		Data: data,

		// Instruction accounts
		Accounts: []solana.AccountMeta{
			{
				PublicKey:  accounts.Pool,
				IsWritable: false,
				IsSigner:   false,
			},
			{
				PublicKey:  accounts.InVault,
				IsWritable: false,
				IsSigner:   false,
			},
			{
				PublicKey:  accounts.OutVault,
				IsWritable: false,
				IsSigner:   false,
			},
			{
				PublicKey:  accounts.InVaultTokenAccount,
				IsWritable: true,
				IsSigner:   false,
			},
			{
				PublicKey:  accounts.OutVaultTokenAccount,
				IsWritable: true,
				IsSigner:   false,
			},
			{
				PublicKey:  accounts.UserFromTokenAccount,
				IsWritable: true,
				IsSigner:   false,
			},
			{
				PublicKey:  accounts.ToTokenAccount,
				IsWritable: true,
				IsSigner:   false,
			},
			{
				PublicKey:  accounts.FeeRecipientTokenAccount,
				IsWritable: true,
				IsSigner:   false,
			},
			{
				PublicKey:  accounts.FeeRecipient,
				IsWritable: false,
				IsSigner:   false,
			},
			{
				PublicKey:  accounts.FromMint,
				IsWritable: false,
				IsSigner:   false,
			},
			{
				PublicKey:  accounts.ToMint,
				IsWritable: false,
				IsSigner:   false,
			},
			{
				PublicKey:  accounts.User,
				IsWritable: true,
				IsSigner:   true,
			},
			{
				PublicKey:  accounts.Whitelist,
				IsWritable: false,
				IsSigner:   false,
			},
			{
				PublicKey:  SPL_TOKEN_PROGRAM_ID,
				IsWritable: false,
				IsSigner:   false,
			},
			{
				PublicKey:  ASSOCIATED_TOKEN_PROGRAM_ID,
				IsWritable: false,
				IsSigner:   false,
			},
			{
				PublicKey:  SYSTEM_PROGRAM_ID,
				IsWritable: false,
				IsSigner:   false,
			},
		},
	}
}
