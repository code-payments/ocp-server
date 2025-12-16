package usdf_swap

type InstructionType uint8

const (
	InstructionTypeUnknown InstructionType = iota

	InstructionTypeInitialize
	InstructionTypeSwap
	InstructionTypeTransfer
)

func putInstructionType(dst []byte, v InstructionType, offset *int) {
	dst[*offset] = uint8(v)
	*offset += 1
}
