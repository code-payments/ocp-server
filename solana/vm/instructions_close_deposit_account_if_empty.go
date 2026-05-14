package vm

import (
	"crypto/ed25519"

	"github.com/code-payments/ocp-server/solana"
)

const (
	CloseDepositAccountIfEmptyInstructionArgsSize = 1 // bump
)

type CloseDepositAccountIfEmptyInstructionArgs struct {
	Bump uint8
}

type CloseDepositAccountIfEmptyInstructionAccounts struct {
	VmAuthority ed25519.PublicKey
	Vm          ed25519.PublicKey
	Depositor   ed25519.PublicKey
	DepositPda  ed25519.PublicKey
	DepositAta  ed25519.PublicKey
	Destination ed25519.PublicKey
}

func NewCloseDepositAccountIfEmptyInstruction(
	accounts *CloseDepositAccountIfEmptyInstructionAccounts,
	args *CloseDepositAccountIfEmptyInstructionArgs,
) solana.Instruction {
	var offset int

	// Serialize instruction arguments
	data := make([]byte, 1+CloseDepositAccountIfEmptyInstructionArgsSize)

	putCodeInstruction(data, CodeInstructionCloseDepositAccountIfEmpty, &offset)
	putUint8(data, args.Bump, &offset)

	return solana.Instruction{
		Program: PROGRAM_ADDRESS,

		// Instruction args
		Data: data,

		// Instruction accounts
		Accounts: []solana.AccountMeta{
			{
				PublicKey:  accounts.VmAuthority,
				IsWritable: true,
				IsSigner:   true,
			},
			{
				PublicKey:  accounts.Vm,
				IsWritable: true,
				IsSigner:   false,
			},
			{
				PublicKey:  accounts.Depositor,
				IsWritable: false,
				IsSigner:   false,
			},
			{
				PublicKey:  accounts.DepositPda,
				IsWritable: false,
				IsSigner:   false,
			},
			{
				PublicKey:  accounts.DepositAta,
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
