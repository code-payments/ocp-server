package coinbase_stable_swapper

// Instruction discriminators from the on-chain IDL
var (
	// swap discriminator: [248, 198, 158, 145, 225, 117, 135, 200]
	SwapInstructionDiscriminator = []byte{248, 198, 158, 145, 225, 117, 135, 200}
)

func putDiscriminator(dst []byte, discriminator []byte, offset *int) {
	copy(dst[*offset:], discriminator)
	*offset += 8
}
