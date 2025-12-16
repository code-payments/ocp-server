package usdf_swap

import (
	"bytes"
	"crypto/ed25519"
	"fmt"

	"github.com/mr-tron/base58"
)

const (
	PoolAccountSize = (8 + // discriminator
		32 + // authority
		MaxPoolNameLength + // name
		32 + // usdf_mint
		32 + // other_mint
		32 + // usdf_vault
		32 + // other_vault
		1 + // bump
		1 + // usdf_vault_bump
		1 + // other_vault_bump
		1 + // usdf_decimals
		1 + // other_decimals
		3) // padding
)

var PoolAccountDiscriminator = []byte{byte(AccountTypePool), 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}

type PoolAccount struct {
	Authority      ed25519.PublicKey
	Name           string
	UsdfMint       ed25519.PublicKey
	OtherMint      ed25519.PublicKey
	UsdfVault      ed25519.PublicKey
	OtherVault     ed25519.PublicKey
	Bump           uint8
	UsdfVaultBump  uint8
	OtherVaultBump uint8
	UsdfDecimals   uint8
	OtherDecimals  uint8
}

func (obj *PoolAccount) Unmarshal(data []byte) error {
	if len(data) < PoolAccountSize {
		return ErrInvalidAccountData
	}

	var offset int

	var discriminator []byte
	getDiscriminator(data, &discriminator, &offset)
	if !bytes.Equal(discriminator, PoolAccountDiscriminator) {
		return ErrInvalidAccountData
	}

	getKey(data, &obj.Authority, &offset)
	getFixedString(data, &obj.Name, MaxPoolNameLength, &offset)
	getKey(data, &obj.UsdfMint, &offset)
	getKey(data, &obj.OtherMint, &offset)
	getKey(data, &obj.UsdfVault, &offset)
	getKey(data, &obj.OtherVault, &offset)
	getUint8(data, &obj.Bump, &offset)
	getUint8(data, &obj.UsdfVaultBump, &offset)
	getUint8(data, &obj.OtherVaultBump, &offset)
	getUint8(data, &obj.UsdfDecimals, &offset)
	getUint8(data, &obj.OtherDecimals, &offset)
	offset += 3 // padding

	return nil
}

func (obj *PoolAccount) String() string {
	return fmt.Sprintf(
		"Pool{authority=%s,name=%s,usdf_mint=%s,other_mint=%s,usdf_vault=%s,other_vault=%s,bump=%d,usdf_vault_bump=%d,other_vault_bump=%d,usdf_decimals=%d,other_decimals=%d}",
		base58.Encode(obj.Authority),
		obj.Name,
		base58.Encode(obj.UsdfMint),
		base58.Encode(obj.OtherMint),
		base58.Encode(obj.UsdfVault),
		base58.Encode(obj.OtherVault),
		obj.Bump,
		obj.UsdfVaultBump,
		obj.OtherVaultBump,
		obj.UsdfDecimals,
		obj.OtherDecimals,
	)
}
