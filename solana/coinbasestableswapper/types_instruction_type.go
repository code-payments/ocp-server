package coinbase_stable_swapper

// Instruction discriminators from the on-chain IDL
var (
	// swap_v2 discriminator: [43, 4, 237, 11, 26, 201, 30, 98]
	SwapV2InstructionDiscriminator = []byte{43, 4, 237, 11, 26, 201, 30, 98}
)

func putDiscriminator(dst []byte, discriminator []byte, offset *int) {
	copy(dst[*offset:], discriminator)
	*offset += 8
}
