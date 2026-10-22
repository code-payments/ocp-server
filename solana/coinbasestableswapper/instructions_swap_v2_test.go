package coinbase_stable_swapper

import (
	"crypto/ed25519"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/code-payments/ocp-server/solana"
)

func TestSwapV2Instruction_MatchesSwapWithoutWhitelist(t *testing.T) {
	keys := make([]ed25519.PublicKey, 13)
	for i := range keys {
		keys[i] = make([]byte, ed25519.PublicKeySize)
		keys[i][0] = byte(i + 1)
	}

	v1 := NewSwapInstruction(
		&SwapInstructionAccounts{
			Pool:                     keys[0],
			InVault:                  keys[1],
			OutVault:                 keys[2],
			InVaultTokenAccount:      keys[3],
			OutVaultTokenAccount:     keys[4],
			UserFromTokenAccount:     keys[5],
			ToTokenAccount:           keys[6],
			FeeRecipientTokenAccount: keys[7],
			FeeRecipient:             keys[8],
			FromMint:                 keys[9],
			ToMint:                   keys[10],
			User:                     keys[11],
			Whitelist:                keys[12],
		},
		&SwapInstructionArgs{
			AmountIn:     1_500_000,
			MinAmountOut: 1_490_000,
		},
	)

	v2 := NewSwapV2Instruction(
		&SwapV2InstructionAccounts{
			Pool:                     keys[0],
			InVault:                  keys[1],
			OutVault:                 keys[2],
			InVaultTokenAccount:      keys[3],
			OutVaultTokenAccount:     keys[4],
			UserFromTokenAccount:     keys[5],
			ToTokenAccount:           keys[6],
			FeeRecipientTokenAccount: keys[7],
			FeeRecipient:             keys[8],
			FromMint:                 keys[9],
			ToMint:                   keys[10],
			User:                     keys[11],
		},
		&SwapV2InstructionArgs{
			AmountIn:     1_500_000,
			MinAmountOut: 1_490_000,
		},
	)

	require.EqualValues(t, PROGRAM_ADDRESS, v2.Program)

	require.Equal(t, []byte{
		43, 4, 237, 11, 26, 201, 30, 98, // discriminator
		0x60, 0xe3, 0x16, 0, 0, 0, 0, 0, // amount_in = 1_500_000
		0x50, 0xbc, 0x16, 0, 0, 0, 0, 0, // min_amount_out = 1_490_000
	}, v2.Data)
	require.Equal(t, v1.Data[8:], v2.Data[8:])

	// The whitelist is the 13th account in swap; swap_v2 drops it and keeps
	// everything else in order.
	require.Len(t, v1.Accounts, 16)
	require.Len(t, v2.Accounts, 15)
	require.EqualValues(t, keys[12], v1.Accounts[12].PublicKey)
	expected := append(append([]solana.AccountMeta{}, v1.Accounts[:12]...), v1.Accounts[13:]...)
	require.Equal(t, expected, v2.Accounts)
}
