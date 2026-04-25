package coinbase_stable_swapper

import (
	"crypto/ed25519"
	"encoding/binary"

	"github.com/mr-tron/base58"
)

func putKey(dst []byte, v ed25519.PublicKey, offset *int) {
	copy(dst[*offset:], v)
	*offset += ed25519.PublicKeySize
}

func getKey(src []byte, dst *ed25519.PublicKey, offset *int) {
	*dst = make([]byte, ed25519.PublicKeySize)
	copy(*dst, src[*offset:])
	*offset += ed25519.PublicKeySize
}

func putUint64(dst []byte, v uint64, offset *int) {
	binary.LittleEndian.PutUint64(dst[*offset:], v)
	*offset += 8
}

func getUint64(src []byte, dst *uint64, offset *int) {
	*dst = binary.LittleEndian.Uint64(src[*offset:])
	*offset += 8
}

func putUint16(dst []byte, v uint16, offset *int) {
	binary.LittleEndian.PutUint16(dst[*offset:], v)
	*offset += 2
}

func getUint16(src []byte, dst *uint16, offset *int) {
	*dst = binary.LittleEndian.Uint16(src[*offset:])
	*offset += 2
}

func putUint8(dst []byte, v uint8, offset *int) {
	dst[*offset] = v
	*offset += 1
}

func getUint8(src []byte, dst *uint8, offset *int) {
	*dst = src[*offset]
	*offset += 1
}

func getDiscriminator(src []byte, dst *[]byte, offset *int) {
	*dst = make([]byte, 8)
	copy(*dst, src[*offset:])
	*offset += 8
}

func putBool(dst []byte, v bool, offset *int) {
	if v {
		dst[*offset] = 1
	} else {
		dst[*offset] = 0
	}
	*offset += 1
}

func getBool(src []byte, dst *bool, offset *int) {
	*dst = src[*offset] != 0
	*offset += 1
}

func mustBase58Decode(value string) []byte {
	decoded, err := base58.Decode(value)
	if err != nil {
		panic(err)
	}
	return decoded
}
