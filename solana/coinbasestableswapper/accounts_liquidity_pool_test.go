package coinbase_stable_swapper

import (
	"encoding/base64"
	"encoding/binary"
	"testing"

	"github.com/mr-tron/base58"
	"github.com/stretchr/testify/require"
)

// First 315 bytes of mainnet pool CrDL9SoCyW1tBgn8k7rgGSpWhnszneWDbvKvqPAU4PL9
// at slot ~445672210, after the 2026-09-08 MigrateAuthorities instruction. The
// live account is 2107 bytes with the remainder zeroed.
const mainnetLiquidityPoolPrefix = "QiYRQLxQRIEFHqE9vluQFKO1wbEwnd22aRe9qGrV03SIsz1AV7GqT/yEzcR/f+ALaKG4KMxbBfZ5dTNFPqxxZHMfbTqNfj6St0p+" +
	"+yObz2IILMcKsoko07O+oRSDek7YwzrH9TroL1+QbHdT/t9qpcq4Kyx8OPWZm79AIUM9UlN+X6ujF0hPgzT4z8SXtrVTfBhZj7Lz" +
	"SPTgCpHoi6cjfPqXvflOJvRBAgAAAN0H70q0C5DeChX575Umuo4KwnYx+lqZmPTGnq1wMK2QSoyv1lJlvQkMgeq0VkN3NML2MHaz" +
	"cTzSusODf5c6RhACAAAAxvp6877brTo9ZfNqq8l0MbG75MLS9uDkfKYCA0UvXWE908SAij1Ps5+uycukm1pjSlLt4RVI9SSqIPLN" +
	"Ru+AcQAAAAAAAAAAAAD/"

const mainnetLiquidityPoolSize = 2107

func mainnetLiquidityPoolData(t *testing.T) []byte {
	prefix, err := base64.StdEncoding.DecodeString(mainnetLiquidityPoolPrefix)
	require.NoError(t, err)
	require.Len(t, prefix, 315)

	data := make([]byte, mainnetLiquidityPoolSize)
	copy(data, prefix)
	return data
}

func TestLiquidityPoolAccount_Unmarshal_Mainnet(t *testing.T) {
	var pool LiquidityPoolAccount
	require.NoError(t, pool.Unmarshal(mainnetLiquidityPoolData(t)))

	assert58 := func(expected string, actual []byte) {
		require.Equal(t, expected, base58.Encode(actual))
	}
	assert58("Lz8QXHjETKQnt1fzsKbN4AyQEhhVAFB2YKwAMqksr7G", pool.PauseAuthority)
	assert58("HzjC9U1WifkqLhYMx592UG1fJqX3BtRjCqPPPTo3hA8R", pool.UnpauseAuthority)
	assert58("DLVVcd3xfwWeCwGz1EUQbqaNC88NooN6s9ifWo87QZst", pool.TreasuryAuthority)
	assert58("Aimdv5hcHfm2PKuGwDW9H81iibZoYKLv3TPMEaZhmvqG", pool.ConfigureAuthority)
	assert58("4ZnFXk7KyB5khDqjWSHqHBQH1nQCnmvkr1pRFivWcP7e", pool.FeeRecipient)

	require.Len(t, pool.WithdrawRecipients, 2)
	assert58("Fsp7tPTMrkPFK4uYhVtc1Gi86eB7todJPEEKUawVZoyh", pool.WithdrawRecipients[0])
	assert58("621bSaUU77AgaUXa7tJwNLHj6sVKUTS67ejBLaTuk4fH", pool.WithdrawRecipients[1])

	require.Len(t, pool.SupportedTokens, 2)
	assert58("EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v", pool.SupportedTokens[0])
	assert58("5AMAA9JV9H97YYVxx8F6FsCMmTwXSuTTQneiup4RYAUQ", pool.SupportedTokens[1])

	require.EqualValues(t, 0, pool.FeeRate)
	require.False(t, pool.SwapsPaused)
	require.False(t, pool.LiquidityPaused)
	require.EqualValues(t, 255, pool.Bump)
}

func TestLiquidityPoolAccount_Unmarshal_RejectsOversizedVector(t *testing.T) {
	for _, vecOffset := range []int{168, 236} {
		data := mainnetLiquidityPoolData(t)
		binary.LittleEndian.PutUint32(data[vecOffset:], 0xffffffff)

		var pool LiquidityPoolAccount
		require.ErrorIs(t, pool.Unmarshal(data), ErrInvalidAccountData)
	}
}

func TestLiquidityPoolAccount_Unmarshal_RejectsTruncatedData(t *testing.T) {
	data := mainnetLiquidityPoolData(t)

	for _, size := range []int{LiquidityPoolAccountMinSize - 1, 200, 314} {
		var pool LiquidityPoolAccount
		require.ErrorIs(t, pool.Unmarshal(data[:size]), ErrInvalidAccountData, "size %d", size)
	}

	var pool LiquidityPoolAccount
	require.NoError(t, pool.Unmarshal(data[:315]))
}

func TestLiquidityPoolAccount_Unmarshal_RejectsWrongDiscriminator(t *testing.T) {
	data := mainnetLiquidityPoolData(t)
	copy(data, TokenVaultAccountDiscriminator)

	var pool LiquidityPoolAccount
	require.ErrorIs(t, pool.Unmarshal(data), ErrInvalidAccountData)
}
