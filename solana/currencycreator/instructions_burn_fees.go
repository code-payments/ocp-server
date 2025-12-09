package currencycreator

import (
	"crypto/ed25519"

	"github.com/code-payments/ocp-server/solana"
)

const (
	BurnFeesInstructionArgsSize = 0
)

type BurnFeesInstructionArgs struct {
}

type BurnFeesInstructionAccounts struct {
	Payer     ed25519.PublicKey
	Pool      ed25519.PublicKey
	BaseMint  ed25519.PublicKey
	VaultBase ed25519.PublicKey
}

func NewBurnFeesInstruction(
	accounts *BurnFeesInstructionAccounts,
	args *BurnFeesInstructionArgs,
) solana.Instruction {
	var offset int

	// Serialize instruction arguments
	data := make([]byte, 1+BurnFeesInstructionArgsSize)

	putInstructionType(data, InstructionTypeBurnFees, &offset)

	return solana.Instruction{
		Program: PROGRAM_ADDRESS,

		// Instruction args
		Data: data,

		// Instruction accounts
		Accounts: []solana.AccountMeta{
			{
				PublicKey:  accounts.Payer,
				IsWritable: true,
				IsSigner:   true,
			},
			{
				PublicKey:  accounts.Pool,
				IsWritable: true,
				IsSigner:   false,
			},
			{
				PublicKey:  accounts.BaseMint,
				IsWritable: true,
				IsSigner:   false,
			},
			{
				PublicKey:  accounts.VaultBase,
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
