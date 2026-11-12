package transaction

import (
	"crypto/ed25519"
	"testing"

	"github.com/stretchr/testify/assert"

	coinbase_stable_swapper "github.com/code-payments/ocp-server/solana/coinbasestableswapper"
)

func TestMakeCoinbaseSwapInstruction(t *testing.T) {
	keys := make([]ed25519.PublicKey, 12)
	for i := range keys {
		keys[i] = make([]byte, ed25519.PublicKeySize)
		keys[i][0] = byte(i + 1)
	}

	accounts := &CoinbaseSwapAccounts{
		Pool:                     keys[0],
		InVault:                  keys[1],
		OutVault:                 keys[2],
		InVaultTokenAccount:      keys[3],
		OutVaultTokenAccount:     keys[4],
		FeeRecipient:             keys[5],
		FeeRecipientTokenAccount: keys[6],
	}
	user, fromMint, toMint, userFrom, userTo := keys[7], keys[8], keys[9], keys[10], keys[11]

	ixn := MakeCoinbaseSwapInstruction(accounts, user, fromMint, toMint, userFrom, userTo, 1_000_000, 990_000)

	expected := coinbase_stable_swapper.NewSwapV2Instruction(
		&coinbase_stable_swapper.SwapV2InstructionAccounts{
			Pool:                     accounts.Pool,
			InVault:                  accounts.InVault,
			OutVault:                 accounts.OutVault,
			InVaultTokenAccount:      accounts.InVaultTokenAccount,
			OutVaultTokenAccount:     accounts.OutVaultTokenAccount,
			UserFromTokenAccount:     userFrom,
			ToTokenAccount:           userTo,
			FeeRecipientTokenAccount: accounts.FeeRecipientTokenAccount,
			FeeRecipient:             accounts.FeeRecipient,
			FromMint:                 fromMint,
			ToMint:                   toMint,
			User:                     user,
		},
		&coinbase_stable_swapper.SwapV2InstructionArgs{
			AmountIn:     1_000_000,
			MinAmountOut: 990_000,
		},
	)
	assert.Equal(t, expected, ixn)
	assert.Equal(t, coinbase_stable_swapper.SwapV2InstructionDiscriminator, ixn.Data[:8])
}
