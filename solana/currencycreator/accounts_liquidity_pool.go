package currencycreator

import (
	"bytes"
	"crypto/ed25519"
	"fmt"

	"github.com/mr-tron/base58"
)

const (
	DefaultSellFeeBps = 100 // 1% fee
)

const (
	LiquidityPoolAccountSize = (8 + //discriminator
		32 + // authority
		32 + // currency
		32 + // target_mint
		32 + // base_mint
		32 + // vault_target
		32 + // vault_base
		2 + // sell_fee
		1 + // bump
		1 + // vault_target_bump
		1 + // vault_base_bump
		1) // padding
)

var LiquidityPoolAccountDiscriminator = []byte{byte(AccountTypeLiquidityPool), 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}

type LiquidityPoolAccount struct {
	Authority       ed25519.PublicKey
	Currency        ed25519.PublicKey
	TargetMint      ed25519.PublicKey
	BaseMint        ed25519.PublicKey
	VaultTarget     ed25519.PublicKey
	VaultBase       ed25519.PublicKey
	SellFee         uint16
	Bump            uint8
	VaultTargetBump uint8
	VaultBaseBump   uint8
}

func (obj *LiquidityPoolAccount) Unmarshal(data []byte) error {
	if len(data) < LiquidityPoolAccountSize {
		return ErrInvalidAccountData
	}

	var offset int

	var discriminator []byte
	getDiscriminator(data, &discriminator, &offset)
	if !bytes.Equal(discriminator, LiquidityPoolAccountDiscriminator) {
		return ErrInvalidAccountData
	}

	getKey(data, &obj.Authority, &offset)
	getKey(data, &obj.Currency, &offset)
	getKey(data, &obj.TargetMint, &offset)
	getKey(data, &obj.BaseMint, &offset)
	getKey(data, &obj.VaultTarget, &offset)
	getKey(data, &obj.VaultBase, &offset)
	getUint16(data, &obj.SellFee, &offset)
	getUint8(data, &obj.Bump, &offset)
	getUint8(data, &obj.VaultTargetBump, &offset)
	getUint8(data, &obj.VaultBaseBump, &offset)
	offset += 1 // padding

	return nil
}

func (obj *LiquidityPoolAccount) String() string {
	return fmt.Sprintf(
		"LiquidityPool{authority=%s,currency=%s,target_mint=%s,base_mint=%s,vault_target=%s,vault_base=%s,sell_fee=%d,bump=%d,vault_target_bump=%d,vault_base_bump=%d}",
		base58.Encode(obj.Authority),
		base58.Encode(obj.Currency),
		base58.Encode(obj.TargetMint),
		base58.Encode(obj.BaseMint),
		base58.Encode(obj.VaultTarget),
		base58.Encode(obj.VaultBase),
		obj.SellFee,
		obj.Bump,
		obj.VaultTargetBump,
		obj.VaultBaseBump,
	)
}
