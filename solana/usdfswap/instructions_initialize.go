package usdf_swap

import (
	"crypto/ed25519"

	"github.com/code-payments/ocp-server/solana"
)

const (
	InitializeInstructionArgsSize = (MaxPoolNameLength + // name
		1 + // bump
		1 + // usdf_vault_bump
		1) // other_vault_bump
)

type InitializeInstructionArgs struct {
	Name           string
	Bump           uint8
	UsdfVaultBump  uint8
	OtherVaultBump uint8
}

type InitializeInstructionAccounts struct {
	Authority  ed25519.PublicKey
	UsdfMint   ed25519.PublicKey
	OtherMint  ed25519.PublicKey
	Pool       ed25519.PublicKey
	UsdfVault  ed25519.PublicKey
	OtherVault ed25519.PublicKey
}

func NewInitializeInstruction(
	accounts *InitializeInstructionAccounts,
	args *InitializeInstructionArgs,
) solana.Instruction {
	var offset int

	// Serialize instruction arguments
	data := make([]byte, 1+InitializeInstructionArgsSize)

	putInstructionType(data, InstructionTypeInitialize, &offset)
	putFixedString(data, args.Name, MaxPoolNameLength, &offset)
	putUint8(data, args.Bump, &offset)
	putUint8(data, args.UsdfVaultBump, &offset)
	putUint8(data, args.OtherVaultBump, &offset)

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
				PublicKey:  accounts.UsdfMint,
				IsWritable: false,
				IsSigner:   false,
			},
			{
				PublicKey:  accounts.OtherMint,
				IsWritable: false,
				IsSigner:   false,
			},
			{
				PublicKey:  accounts.Pool,
				IsWritable: true,
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
				PublicKey:  SPL_TOKEN_PROGRAM_ID,
				IsWritable: false,
				IsSigner:   false,
			},
			{
				PublicKey:  SYSTEM_PROGRAM_ID,
				IsWritable: false,
				IsSigner:   false,
			},
			{
				PublicKey:  SYSVAR_RENT_PUBKEY,
				IsWritable: false,
				IsSigner:   false,
			},
		},
	}
}
