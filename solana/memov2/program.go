package memov2

import (
	"bytes"
	"crypto/ed25519"

	"github.com/pkg/errors"

	"github.com/code-payments/ocp-server/solana"
)

// ProgramKey is the address of the v2 memo program.
//
// Current key: MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr
var ProgramKey = ed25519.PublicKey{5, 74, 83, 90, 153, 41, 33, 6, 77, 36, 232, 113, 96, 218, 56, 124, 124, 53, 181, 221, 188, 146, 187, 129, 228, 31, 168, 64, 65, 5, 68, 141}

// Reference: https://github.com/solana-program/memo
func Instruction(data string) solana.Instruction {
	return solana.NewInstruction(
		ProgramKey,
		[]byte(data),
	)
}

type DecompiledMemo struct {
	Data []byte
}

func DecompileMemo(m solana.Message, index int) (*DecompiledMemo, error) {
	if index >= len(m.Instructions) {
		return nil, errors.Errorf("instruction doesn't exist at %d", index)
	}

	i := m.Instructions[index]

	if !bytes.Equal(m.Accounts[i.ProgramIndex], ProgramKey) {
		return nil, solana.ErrIncorrectProgram
	}

	return &DecompiledMemo{Data: i.Data}, nil
}
