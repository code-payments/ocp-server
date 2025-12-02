package transaction

import (
	"github.com/code-payments/ocp-server/ocp/common"
	"github.com/code-payments/ocp-server/solana"
	"github.com/code-payments/ocp-server/solana/system"
)

func makeAdvanceNonceInstruction(nonce, authority *common.Account) (solana.Instruction, error) {
	return system.AdvanceNonce(
		nonce.PublicKey().ToBytes(),
		authority.PublicKey().ToBytes(),
	), nil
}
