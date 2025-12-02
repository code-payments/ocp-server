package common

import (
	"bytes"
	"context"
	"errors"

	commonpb "github.com/code-payments/ocp-protobuf-api/generated/go/common/v1"
	"github.com/code-payments/ocp-server/ocp/config"
	ocp_data "github.com/code-payments/ocp-server/ocp/data"
	"github.com/code-payments/ocp-server/solana/currencycreator"
	"github.com/code-payments/ocp-server/usdf"
)

var (
	CoreMintAccount, _    = NewAccountFromPublicKeyBytes(config.CoreMintPublicKeyBytes)
	CoreMintQuarksPerUnit = uint64(config.CoreMintQuarksPerUnit)
	CoreMintDecimals      = config.CoreMintDecimals
	CoreMintName          = config.CoreMintName
	CoreMintSymbol        = config.CoreMintSymbol

	ErrUnsupportedMint = errors.New("unsupported mint")

	jeffyMintAccount, _ = NewAccountFromPublicKeyString(config.JeffyMintPublicKey)
)

func GetBackwardsCompatMint(protoMint *commonpb.SolanaAccountId) (*Account, error) {
	if protoMint == nil {
		return CoreMintAccount, nil
	}
	return NewAccountFromProto(protoMint)

}

func FromCoreMintQuarks(quarks uint64) uint64 {
	return quarks / CoreMintQuarksPerUnit
}

func ToCoreMintQuarks(units uint64) uint64 {
	return units * CoreMintQuarksPerUnit
}

func IsCoreMint(mint *Account) bool {
	return bytes.Equal(mint.PublicKey().ToBytes(), CoreMintAccount.PublicKey().ToBytes())
}

func IsCoreMintUsdStableCoin() bool {
	switch CoreMintAccount.PublicKey().ToBase58() {
	case usdf.Mint:
		return true
	default:
		return false
	}
}

func GetMintQuarksPerUnit(mint *Account) uint64 {
	if mint.PublicKey().ToBase58() == CoreMintAccount.PublicKey().ToBase58() {
		return CoreMintQuarksPerUnit
	}
	return currencycreator.DefaultMintQuarksPerUnit
}

func IsSupportedMint(ctx context.Context, data ocp_data.Provider, mintAccount *Account) (bool, error) {
	_, err := GetVmConfigForMint(ctx, data, mintAccount)
	if err == ErrUnsupportedMint {
		return false, nil
	} else if err != nil {
		return false, err
	}
	return true, nil
}
