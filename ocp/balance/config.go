package balance

import (
	"github.com/code-payments/ocp-server/config"
	"github.com/code-payments/ocp-server/config/env"
)

const (
	// EnableLedgerReadsConfigEnvName gates whether balance calculators read
	// from the new ocp__core_balance ledger. When disabled, calculators use
	// the legacy strategies exclusively.
	EnableLedgerReadsConfigEnvName = "BALANCE_ENABLE_LEDGER_READS"

	// EnableLedgerWritesConfigEnvName gates whether ApplyDeltasInTx writes
	// to the ledger at all. When disabled, it is a no-op.
	EnableLedgerWritesConfigEnvName = "BALANCE_ENABLE_LEDGER_WRITES"

	defaultEnableLedgerReads  = true
	defaultEnableLedgerWrites = true
)

var (
	enableLedgerReads  config.Bool = env.NewBoolConfig(EnableLedgerReadsConfigEnvName, defaultEnableLedgerReads)
	enableLedgerWrites config.Bool = env.NewBoolConfig(EnableLedgerWritesConfigEnvName, defaultEnableLedgerWrites)
)
