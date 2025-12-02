package vm

type VirtualInstruction struct {
	Opcode Opcode
	Data   []byte
}
