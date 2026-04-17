package vm

import (
	"crypto/ed25519"

	"github.com/code-payments/ocp-server/solana"
)

const (
	TransferForSwapWithFeeInstructionArgsSize = (8 + // swap_amount
		8 + // fee_amount
		1) // bump
)

type TransferForSwapWithFeeInstructionArgs struct {
	SwapAmount uint64
	FeeAmount  uint64
	Bump       uint8
}

type TransferForSwapWithFeeInstructionAccounts struct {
	VmAuthority     ed25519.PublicKey
	Vm              ed25519.PublicKey
	Swapper         ed25519.PublicKey
	SwapPda         ed25519.PublicKey
	SwapAta         ed25519.PublicKey
	SwapDestination ed25519.PublicKey
	FeeDestination  ed25519.PublicKey
}

func NewTransferForSwapWithFeeInstruction(
	accounts *TransferForSwapWithFeeInstructionAccounts,
	args *TransferForSwapWithFeeInstructionArgs,
) solana.Instruction {
	var offset int

	// Serialize instruction arguments
	data := make([]byte, 1+TransferForSwapWithFeeInstructionArgsSize)

	putCodeInstruction(data, CodeInstructionTransferForSwapWithFee, &offset)
	putUint64(data, args.SwapAmount, &offset)
	putUint64(data, args.FeeAmount, &offset)
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
				PublicKey:  accounts.Swapper,
				IsWritable: true,
				IsSigner:   true,
			},
			{
				PublicKey:  accounts.SwapPda,
				IsWritable: false,
				IsSigner:   false,
			},
			{
				PublicKey:  accounts.SwapAta,
				IsWritable: true,
				IsSigner:   false,
			},
			{
				PublicKey:  accounts.SwapDestination,
				IsWritable: true,
				IsSigner:   false,
			},
			{
				PublicKey:  accounts.FeeDestination,
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
