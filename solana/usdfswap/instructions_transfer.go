package usdf_swap

import (
	"crypto/ed25519"

	"github.com/code-payments/ocp-server/solana"
)

const (
	TransferInstructionArgsSize = (8 + // amount
		1) // is_usdf
)

type TransferInstructionArgs struct {
	Amount uint64
	IsUsdf bool
}

type TransferInstructionAccounts struct {
	Authority   ed25519.PublicKey
	Pool        ed25519.PublicKey
	Vault       ed25519.PublicKey
	Destination ed25519.PublicKey
}

func NewTransferInstruction(
	accounts *TransferInstructionAccounts,
	args *TransferInstructionArgs,
) solana.Instruction {
	var offset int

	// Serialize instruction arguments
	data := make([]byte, 1+TransferInstructionArgsSize)

	putInstructionType(data, InstructionTypeTransfer, &offset)
	putUint64(data, args.Amount, &offset)
	putBool(data, args.IsUsdf, &offset)

	return solana.Instruction{
		Program: PROGRAM_ADDRESS,

		// Instruction args
		Data: data,

		// Instruction accounts
		Accounts: []solana.AccountMeta{
			{
				PublicKey:  accounts.Authority,
				IsWritable: true,
				IsSigner:   true,
			},
			{
				PublicKey:  accounts.Pool,
				IsWritable: false,
				IsSigner:   false,
			},
			{
				PublicKey:  accounts.Vault,
				IsWritable: true,
				IsSigner:   false,
			},
			{
				PublicKey:  accounts.Destination,
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
