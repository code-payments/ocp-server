package usdf_swap

import (
	"crypto/ed25519"

	"github.com/code-payments/ocp-server/solana"
)

const (
	SwapInstructionArgsSize = (8 + // amount
		1) // usdf_to_other
)

type SwapInstructionArgs struct {
	Amount      uint64
	UsdfToOther bool
}

type SwapInstructionAccounts struct {
	User           ed25519.PublicKey
	Pool           ed25519.PublicKey
	UsdfVault      ed25519.PublicKey
	OtherVault     ed25519.PublicKey
	UserUsdfToken  ed25519.PublicKey
	UserOtherToken ed25519.PublicKey
}

func NewSwapInstruction(
	accounts *SwapInstructionAccounts,
	args *SwapInstructionArgs,
) solana.Instruction {
	var offset int

	// Serialize instruction arguments
	data := make([]byte, 1+SwapInstructionArgsSize)

	putInstructionType(data, InstructionTypeSwap, &offset)
	putUint64(data, args.Amount, &offset)
	putBool(data, args.UsdfToOther, &offset)

	return solana.Instruction{
		Program: PROGRAM_ADDRESS,

		// Instruction args
		Data: data,

		// Instruction accounts
		Accounts: []solana.AccountMeta{
			{
				PublicKey:  accounts.User,
				IsWritable: true,
				IsSigner:   true,
			},
			{
				PublicKey:  accounts.Pool,
				IsWritable: false,
				IsSigner:   false,
			},
			{
				PublicKey:  accounts.UsdfVault,
				IsWritable: true,
				IsSigner:   false,
			},
			{
				PublicKey:  accounts.OtherVault,
				IsWritable: true,
				IsSigner:   false,
			},
			{
				PublicKey:  accounts.UserUsdfToken,
				IsWritable: true,
				IsSigner:   false,
			},
			{
				PublicKey:  accounts.UserOtherToken,
				IsWritable: true,
				IsSigner:   false,
			},
			{
				PublicKey:  SPL_TOKEN_PROGRAM_ID,
				IsWritable: false,
				IsSigner:   false,
			},
		},
	}
}
