package coinbase_stable_swapper

import (
	"crypto/ed25519"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/code-payments/ocp-server/solana"
)

func TestSwapV2Instruction(t *testing.T) {
	keys := make([]ed25519.PublicKey, 12)
	for i := range keys {
		keys[i] = make([]byte, ed25519.PublicKeySize)
		keys[i][0] = byte(i + 1)
	}

	ixn := NewSwapV2Instruction(
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

	require.EqualValues(t, PROGRAM_ADDRESS, ixn.Program)

	require.Equal(t, []byte{
		43, 4, 237, 11, 26, 201, 30, 98, // discriminator
		0x60, 0xe3, 0x16, 0, 0, 0, 0, 0, // amount_in = 1_500_000
		0x50, 0xbc, 0x16, 0, 0, 0, 0, 0, // min_amount_out = 1_490_000
	}, ixn.Data)

	require.Equal(t, []solana.AccountMeta{
		{PublicKey: keys[0]},
		{PublicKey: keys[1]},
		{PublicKey: keys[2]},
		{PublicKey: keys[3], IsWritable: true},
		{PublicKey: keys[4], IsWritable: true},
		{PublicKey: keys[5], IsWritable: true},
		{PublicKey: keys[6], IsWritable: true},
		{PublicKey: keys[7], IsWritable: true},
		{PublicKey: keys[8]},
		{PublicKey: keys[9]},
		{PublicKey: keys[10]},
		{PublicKey: keys[11], IsWritable: true, IsSigner: true},
		{PublicKey: SPL_TOKEN_PROGRAM_ID},
		{PublicKey: ASSOCIATED_TOKEN_PROGRAM_ID},
		{PublicKey: SYSTEM_PROGRAM_ID},
	}, ixn.Accounts)
}
