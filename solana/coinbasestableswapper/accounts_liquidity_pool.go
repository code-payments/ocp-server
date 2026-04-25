package coinbase_stable_swapper

import (
	"bytes"
	"crypto/ed25519"
	"encoding/binary"
	"fmt"

	"github.com/mr-tron/base58"
)

// Minimum size without the dynamic supported_tokens vector
const (
	LiquidityPoolAccountMinSize = (8 + // discriminator
		32 + // operations_authority
		32 + // pause_authority
		32 + // fee_recipient
		4 + // supported_tokens vector length (empty)
		8 + // fee_rate
		1 + // swaps_paused
		1 + // liquidity_paused
		1) // bump
)

type LiquidityPoolAccount struct {
	OperationsAuthority ed25519.PublicKey
	PauseAuthority      ed25519.PublicKey
	FeeRecipient        ed25519.PublicKey
	SupportedTokens     []ed25519.PublicKey
	FeeRate             uint64
	SwapsPaused         bool
	LiquidityPaused     bool
	Bump                uint8
}

func (obj *LiquidityPoolAccount) Unmarshal(data []byte) error {
	if len(data) < LiquidityPoolAccountMinSize {
		return ErrInvalidAccountData
	}

	var offset int

	var discriminator []byte
	getDiscriminator(data, &discriminator, &offset)
	if !bytes.Equal(discriminator, LiquidityPoolAccountDiscriminator) {
		return ErrInvalidAccountData
	}

	getKey(data, &obj.OperationsAuthority, &offset)
	getKey(data, &obj.PauseAuthority, &offset)
	getKey(data, &obj.FeeRecipient, &offset)

	// Read supported_tokens vector (4-byte length prefix + pubkeys)
	vecLen := binary.LittleEndian.Uint32(data[offset:])
	offset += 4

	obj.SupportedTokens = make([]ed25519.PublicKey, vecLen)
	for i := uint32(0); i < vecLen; i++ {
		getKey(data, &obj.SupportedTokens[i], &offset)
	}

	getUint64(data, &obj.FeeRate, &offset)
	getBool(data, &obj.SwapsPaused, &offset)
	getBool(data, &obj.LiquidityPaused, &offset)
	getUint8(data, &obj.Bump, &offset)

	return nil
}

func (obj *LiquidityPoolAccount) String() string {
	tokensList := make([]string, len(obj.SupportedTokens))
	for i, t := range obj.SupportedTokens {
		tokensList[i] = base58.Encode(t)
	}

	return fmt.Sprintf(
		"LiquidityPool{operations_authority=%s,pause_authority=%s,fee_recipient=%s,supported_tokens=%v,fee_rate=%d,swaps_paused=%t,liquidity_paused=%t,bump=%d}",
		base58.Encode(obj.OperationsAuthority),
		base58.Encode(obj.PauseAuthority),
		base58.Encode(obj.FeeRecipient),
		tokensList,
		obj.FeeRate,
		obj.SwapsPaused,
		obj.LiquidityPaused,
		obj.Bump,
	)
}
