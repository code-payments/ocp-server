package coinbase_stable_swapper

import (
	"bytes"
	"crypto/ed25519"
	"fmt"

	"github.com/mr-tron/base58"
)

// Minimum size with empty withdraw_recipients and supported_tokens vectors
const (
	LiquidityPoolAccountMinSize = (8 + // discriminator
		32 + // pause_authority
		32 + // unpause_authority
		32 + // treasury_authority
		32 + // configure_authority
		32 + // fee_recipient
		4 + // withdraw_recipients vector length (empty)
		4 + // supported_tokens vector length (empty)
		8 + // fee_rate
		1 + // swaps_paused
		1 + // liquidity_paused
		1) // bump
)

// LiquidityPoolAccount mirrors LiquidityPool in coinbase/stable-swapper
// (solana/programs/stable-swapper/src/state.rs) after the role-based
// authority migration (coinbase/stable-swapper#20).
type LiquidityPoolAccount struct {
	PauseAuthority     ed25519.PublicKey
	UnpauseAuthority   ed25519.PublicKey
	TreasuryAuthority  ed25519.PublicKey // Signs withdraw_liquidity
	ConfigureAuthority ed25519.PublicKey // Signs add/remove_withdraw_recipient
	FeeRecipient       ed25519.PublicKey
	WithdrawRecipients []ed25519.PublicKey
	SupportedTokens    []ed25519.PublicKey
	FeeRate            uint64
	SwapsPaused        bool
	LiquidityPaused    bool
	Bump               uint8
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

	getKey(data, &obj.PauseAuthority, &offset)
	getKey(data, &obj.UnpauseAuthority, &offset)
	getKey(data, &obj.TreasuryAuthority, &offset)
	getKey(data, &obj.ConfigureAuthority, &offset)
	getKey(data, &obj.FeeRecipient, &offset)

	if err := getKeyVec(data, &obj.WithdrawRecipients, &offset); err != nil {
		return err
	}
	if err := getKeyVec(data, &obj.SupportedTokens, &offset); err != nil {
		return err
	}

	if len(data)-offset < 8+1+1+1 {
		return ErrInvalidAccountData
	}

	getUint64(data, &obj.FeeRate, &offset)
	getBool(data, &obj.SwapsPaused, &offset)
	getBool(data, &obj.LiquidityPaused, &offset)
	getUint8(data, &obj.Bump, &offset)

	return nil
}

func (obj *LiquidityPoolAccount) String() string {
	return fmt.Sprintf(
		"LiquidityPool{pause_authority=%s,unpause_authority=%s,treasury_authority=%s,configure_authority=%s,fee_recipient=%s,withdraw_recipients=%v,supported_tokens=%v,fee_rate=%d,swaps_paused=%t,liquidity_paused=%t,bump=%d}",
		base58.Encode(obj.PauseAuthority),
		base58.Encode(obj.UnpauseAuthority),
		base58.Encode(obj.TreasuryAuthority),
		base58.Encode(obj.ConfigureAuthority),
		base58.Encode(obj.FeeRecipient),
		encodeKeys(obj.WithdrawRecipients),
		encodeKeys(obj.SupportedTokens),
		obj.FeeRate,
		obj.SwapsPaused,
		obj.LiquidityPaused,
		obj.Bump,
	)
}

func encodeKeys(keys []ed25519.PublicKey) []string {
	encoded := make([]string, len(keys))
	for i, k := range keys {
		encoded[i] = base58.Encode(k)
	}
	return encoded
}
