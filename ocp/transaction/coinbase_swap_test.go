package transaction

import (
	"crypto/ed25519"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	coinbase_stable_swapper "github.com/code-payments/ocp-server/solana/coinbasestableswapper"
)

func TestMakeCoinbaseSwapInstruction(t *testing.T) {
	keys := make([]ed25519.PublicKey, 14)
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
		Whitelist:                keys[5],
		FeeRecipient:             keys[6],
		FeeRecipientTokenAccount: keys[7],
	}
	user, fromMint, toMint, userFrom, userTo := keys[8], keys[9], keys[10], keys[11], keys[12]

	build := func(version CoinbaseSwapVersion) ([]ed25519.PublicKey, []byte, error) {
		ixn, err := MakeCoinbaseSwapInstruction(version, accounts, user, fromMint, toMint, userFrom, userTo, 1_000_000, 990_000)
		if err != nil {
			return nil, nil, err
		}
		assert.EqualValues(t, coinbase_stable_swapper.PROGRAM_ADDRESS, ixn.Program)
		var ixnKeys []ed25519.PublicKey
		for _, meta := range ixn.Accounts {
			ixnKeys = append(ixnKeys, meta.PublicKey)
		}
		return ixnKeys, ixn.Data, nil
	}

	v1Keys, v1Data, err := build(CoinbaseSwapV1)
	require.NoError(t, err)
	assert.Equal(t, coinbase_stable_swapper.SwapInstructionDiscriminator, v1Data[:8])
	assert.Len(t, v1Keys, 16)
	assert.Contains(t, v1Keys, accounts.Whitelist)

	v2Keys, v2Data, err := build(CoinbaseSwapV2)
	require.NoError(t, err)
	assert.Equal(t, coinbase_stable_swapper.SwapV2InstructionDiscriminator, v2Data[:8])
	assert.Len(t, v2Keys, 15)
	assert.NotContains(t, v2Keys, accounts.Whitelist)

	// Same args and same accounts, other than the whitelist
	assert.Equal(t, v1Data[8:], v2Data[8:])
	assert.Equal(t, append(append([]ed25519.PublicKey{}, v1Keys[:12]...), v1Keys[13:]...), v2Keys)

	for _, version := range []CoinbaseSwapVersion{0, CoinbaseSwapV2 + 1} {
		_, _, err = build(version)
		assert.Error(t, err)
	}
}
