package coinbase_stable_swapper

import (
	"crypto/ed25519"

	"github.com/code-payments/ocp-server/solana"
)

var (
	LiquidityPoolPrefix     = []byte("liquidity_pool")
	TokenVaultPrefix        = []byte("token_vault")
	VaultTokenAccountPrefix = []byte("vault_token_account")
	AddressWhitelistPrefix  = []byte("address_whitelist")
)

// GetPoolAddress returns the PDA for the liquidity pool
func GetPoolAddress() (ed25519.PublicKey, uint8, error) {
	return solana.FindProgramAddressAndBump(
		PROGRAM_ID,
		LiquidityPoolPrefix,
	)
}

type GetTokenVaultAddressArgs struct {
	Pool ed25519.PublicKey
	Mint ed25519.PublicKey
}

// GetTokenVaultAddress returns the PDA for a token vault
func GetTokenVaultAddress(args *GetTokenVaultAddressArgs) (ed25519.PublicKey, uint8, error) {
	return solana.FindProgramAddressAndBump(
		PROGRAM_ID,
		TokenVaultPrefix,
		args.Pool,
		args.Mint,
	)
}

type GetVaultTokenAccountAddressArgs struct {
	Vault ed25519.PublicKey
}

// GetVaultTokenAccountAddress returns the PDA for a vault's token account
func GetVaultTokenAccountAddress(args *GetVaultTokenAccountAddressArgs) (ed25519.PublicKey, uint8, error) {
	return solana.FindProgramAddressAndBump(
		PROGRAM_ID,
		VaultTokenAccountPrefix,
		args.Vault,
	)
}

// GetWhitelistAddress returns the PDA for the address whitelist
func GetWhitelistAddress() (ed25519.PublicKey, uint8, error) {
	return solana.FindProgramAddressAndBump(
		PROGRAM_ID,
		AddressWhitelistPrefix,
	)
}
