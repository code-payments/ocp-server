package usdf_swap

import (
	"crypto/ed25519"

	"github.com/code-payments/ocp-server/solana"
)

var (
	PoolPrefix  = []byte("pool")
	VaultPrefix = []byte("vault")
)

type GetPoolAddressArgs struct {
	Authority ed25519.PublicKey
	Name      string
	UsdfMint  ed25519.PublicKey
	OtherMint ed25519.PublicKey
}

func GetPoolAddress(args *GetPoolAddressArgs) (ed25519.PublicKey, uint8, error) {
	return solana.FindProgramAddressAndBump(
		PROGRAM_ID,
		PoolPrefix,
		args.Authority,
		[]byte(toFixedString(args.Name, MaxPoolNameLength)),
		args.UsdfMint,
		args.OtherMint,
	)
}

type GetVaultAddressArgs struct {
	Pool ed25519.PublicKey
	Mint ed25519.PublicKey
}

func GetVaultAddress(args *GetVaultAddressArgs) (ed25519.PublicKey, uint8, error) {
	return solana.FindProgramAddressAndBump(
		PROGRAM_ID,
		VaultPrefix,
		args.Pool,
		args.Mint,
	)
}
