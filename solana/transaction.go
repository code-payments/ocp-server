package solana

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"fmt"
	"maps"
	"sort"
	"strings"

	"github.com/mr-tron/base58/base58"
	"github.com/pkg/errors"

	"github.com/code-payments/ocp-server/pointer"
)

const (
	// MaxLegacyTransactionSize applies to both legacy and v0 transactions.
	// Taken from: https://github.com/solana-labs/solana/blob/39b3ac6a8d29e14faa1de73d8b46d390ad41797b/sdk/src/packet.rs#L9-L13
	MaxLegacyTransactionSize = 1232

	// MaxV1TransactionSize taken from: https://github.com/solana-foundation/solana-improvement-documents/blob/main/proposals/0296-larger-transactions.md
	MaxV1TransactionSize = 4096

	// Sanitization constraints for v1 transactions from SIMD-0385
	maxV1Signatures    = 12
	maxV1Accounts      = 64
	maxV1Instructions  = 64
	minV1HeapSize      = 32 * 1024
	maxV1HeapSize      = 256 * 1024
	v1HeapSizeMultiple = 1024

	// Runtime maximums for v1 config requests. Larger values aren't a
	// sanitization failure, and are silently clamped by the runtime.
	// Taken from: https://github.com/anza-xyz/agave/blob/443ba0e197adafd0206e651f86934d22a44d2729/program-runtime/src/execution_budget.rs#L26-L41
	maxV1ComputeUnitLimit            = 1_400_000
	maxV1LoadedAccountsDataSizeLimit = 64 * 1024 * 1024
)

type Signature [ed25519.SignatureSize]byte
type Blockhash [sha256.Size]byte

type MessageVersion uint8

const (
	MessageVersionLegacy MessageVersion = iota
	MessageVersion0
	MessageVersion1
)

type Header struct {
	NumSignatures     byte
	NumReadonlySigned byte
	NumReadOnly       byte
}

type Message struct {
	Version             MessageVersion
	Header              Header
	Accounts            []ed25519.PublicKey
	RecentBlockhash     Blockhash
	Instructions        []CompiledInstruction
	AddressTableLookups []MessageAddressTableLookup
	Config              TransactionConfig
}

// TransactionConfig is the set of fee and resource requests carried in a v1
// message, replacing ComputeBudgetProgram instructions (which are ignored for
// configuration in v1 transactions).
//
// Unset fields use the minimum allowed value. Notably, an unset compute unit
// limit or loaded accounts data size limit is zero, not the runtime default.
// Fields are optional here so decoded transactions round trip exactly, but
// NewV1Transaction requires both limits.
type TransactionConfig struct {
	PriorityFeeLamports         *uint64
	ComputeUnitLimit            *uint32
	LoadedAccountsDataSizeLimit *uint32
	HeapSize                    *uint32

	// Values for mask bits this package doesn't understand, keyed by bit, so
	// transactions using config fields from future SIMDs still decode and
	// round trip exactly.
	unknown map[uint8][4]byte
}

func (c TransactionConfig) clone() TransactionConfig {
	cloned := TransactionConfig{
		PriorityFeeLamports:         pointer.Uint64Copy(c.PriorityFeeLamports),
		ComputeUnitLimit:            pointer.Uint32Copy(c.ComputeUnitLimit),
		LoadedAccountsDataSizeLimit: pointer.Uint32Copy(c.LoadedAccountsDataSizeLimit),
		HeapSize:                    pointer.Uint32Copy(c.HeapSize),
	}
	if c.unknown != nil {
		cloned.unknown = make(map[uint8][4]byte, len(c.unknown))
		maps.Copy(cloned.unknown, c.unknown)
	}
	return cloned
}

type MessageAddressTableLookup struct {
	PublicKey       ed25519.PublicKey
	WritableIndexes []byte
	ReadonlyIndexes []byte
}

type Transaction struct {
	Signatures []Signature
	Message    Message
}

type AddressLookupTable struct {
	PublicKey ed25519.PublicKey
	Addresses []ed25519.PublicKey
}

type SortableAddressLookupTables []AddressLookupTable

func (s SortableAddressLookupTables) Len() int {
	return len(s)
}

func (s SortableAddressLookupTables) Less(i int, j int) bool {
	return bytes.Compare(s[i].PublicKey, s[j].PublicKey) < 0
}

func (s SortableAddressLookupTables) Swap(i int, j int) {
	s[i], s[j] = s[j], s[i]
}

func NewLegacyTransaction(payer ed25519.PublicKey, instructions ...Instruction) Transaction {
	accounts := []AccountMeta{
		{
			PublicKey:  payer,
			IsSigner:   true,
			IsWritable: true,
			isPayer:    true,
		},
	}

	// Extract all of the unique accounts from the instructions.
	for _, i := range instructions {
		accounts = append(accounts, AccountMeta{
			PublicKey: i.Program,
			isProgram: true,
		})
		accounts = append(accounts, i.Accounts...)
	}

	// Sort the account meta's based on:
	//   1. Payer is always the first account / signer.
	//   1. All signers are before non-signers.
	//   2. Writable accounts before read-only accounts.
	//   3. Programs last
	accounts = filterUnique(accounts)
	sort.Sort(SortableAccountMeta(accounts))

	var m Message

	m.Version = MessageVersionLegacy

	for _, account := range accounts {
		m.Accounts = append(m.Accounts, account.PublicKey)

		if account.IsSigner {
			m.Header.NumSignatures++

			if !account.IsWritable {
				m.Header.NumReadonlySigned++
			}
		} else if !account.IsWritable {
			m.Header.NumReadOnly++
		}
	}

	// Generate the compiled instruction, which uses indices instead
	// of raw account keys.
	for _, i := range instructions {
		c := CompiledInstruction{
			ProgramIndex: byte(indexOf(m.Accounts, i.Program)),
			Data:         i.Data,
		}

		for _, a := range i.Accounts {
			c.Accounts = append(c.Accounts, byte(indexOf(m.Accounts, a.PublicKey)))
		}

		m.Instructions = append(m.Instructions, c)
	}

	for i := range m.Accounts {
		if len(m.Accounts[i]) == 0 {
			m.Accounts[i] = make([]byte, ed25519.PublicKeySize)
		}
	}

	return Transaction{
		Signatures: make([]Signature, m.Header.NumSignatures),
		Message:    m,
	}
}

// NewV1Transaction builds a v1 transaction as specified in SIMD-0385. Account
// ordering is unchanged from legacy transactions, but address lookup tables
// are not supported and all accounts are included inline.
//
// The config must set nonzero compute unit and loaded accounts data size
// limits, because v1 treats them as zero when unset, which fails every
// transaction. Limits above the runtime maximum are rejected rather than left
// for the runtime to clamp, since they indicate a misconfigured budget.
// Transactions violating the SIMD-0385 sanitization constraints are rejected,
// since they would never be included in a block.
func NewV1Transaction(payer ed25519.PublicKey, config TransactionConfig, instructions ...Instruction) (Transaction, error) {
	// Copy so later changes to the caller's config can't alter the signed message
	config = config.clone()

	if config.ComputeUnitLimit == nil {
		return Transaction{}, errors.New("compute unit limit is required")
	}
	if *config.ComputeUnitLimit == 0 || *config.ComputeUnitLimit > maxV1ComputeUnitLimit {
		return Transaction{}, errors.Errorf("compute unit limit must be between 1 and %d", maxV1ComputeUnitLimit)
	}
	if config.LoadedAccountsDataSizeLimit == nil {
		return Transaction{}, errors.New("loaded accounts data size limit is required")
	}
	if *config.LoadedAccountsDataSizeLimit == 0 || *config.LoadedAccountsDataSizeLimit > maxV1LoadedAccountsDataSizeLimit {
		return Transaction{}, errors.Errorf("loaded accounts data size limit must be between 1 and %d", maxV1LoadedAccountsDataSizeLimit)
	}
	if config.HeapSize != nil {
		if *config.HeapSize < minV1HeapSize || *config.HeapSize > maxV1HeapSize || *config.HeapSize%v1HeapSizeMultiple != 0 {
			return Transaction{}, errors.Errorf("heap size must be a multiple of %d between %d and %d", v1HeapSizeMultiple, minV1HeapSize, maxV1HeapSize)
		}
	}

	txn := NewLegacyTransaction(payer, instructions...)
	txn.Message.Version = MessageVersion1
	txn.Message.Config = config

	if txn.Message.Header.NumSignatures > maxV1Signatures {
		return Transaction{}, errors.Errorf("transaction exceeds %d signatures", maxV1Signatures)
	}
	if len(txn.Message.Accounts) > maxV1Accounts {
		return Transaction{}, errors.Errorf("transaction exceeds %d accounts", maxV1Accounts)
	}
	if len(txn.Message.Instructions) > maxV1Instructions {
		return Transaction{}, errors.Errorf("transaction exceeds %d instructions", maxV1Instructions)
	}
	marshalled, err := txn.Marshal()
	if err != nil {
		return Transaction{}, err
	}
	if len(marshalled) > MaxV1TransactionSize {
		return Transaction{}, errors.Errorf("transaction size %d exceeds %d bytes", len(marshalled), MaxV1TransactionSize)
	}

	return txn, nil
}

func NewV0Transaction(payer ed25519.PublicKey, addressLookupTables []AddressLookupTable, instructions []Instruction) Transaction {
	accounts := []AccountMeta{
		{
			PublicKey:  payer,
			IsSigner:   true,
			IsWritable: true,
			isPayer:    true,
		},
	}

	// Extract all of the unique accounts from the instructions.
	for _, i := range instructions {
		accounts = append(accounts, AccountMeta{
			PublicKey: i.Program,
			isProgram: true,
		})
		accounts = append(accounts, i.Accounts...)
	}

	// Sort the account meta's based on:
	//   1. Payer is always the first account / signer.
	//   1. All signers are before non-signers.
	//   2. Writable accounts before read-only accounts.
	//   3. Programs last
	accounts = filterUnique(accounts)
	sort.Sort(SortableAccountMeta(accounts))

	// Sort address tables to guarantee consistent marshalling
	sortedAddressLookupTables := make([]AddressLookupTable, len(addressLookupTables))
	copy(sortedAddressLookupTables, addressLookupTables)
	sort.Sort(SortableAddressLookupTables(sortedAddressLookupTables))

	var m Message

	m.Version = MessageVersion0

	writableAddressTableLookupIndexes := make([][]byte, len(sortedAddressLookupTables))
	readonlyAddressTableLookupIndexes := make([][]byte, len(sortedAddressLookupTables))
	for _, account := range accounts {
		// If the account is eligible for dynamic loading, then pull its index
		// from the first address table where it's defined.
		var isDynamicallyLoaded bool
		if !account.isPayer && !account.IsSigner && !account.isProgram {
			for i, addressLookupTable := range sortedAddressLookupTables {
				for j, address := range addressLookupTable.Addresses {
					if bytes.Equal(address, account.PublicKey) {
						isDynamicallyLoaded = true

						if account.IsWritable {
							writableAddressTableLookupIndexes[i] = append(writableAddressTableLookupIndexes[i], byte(j))
						} else {
							readonlyAddressTableLookupIndexes[i] = append(readonlyAddressTableLookupIndexes[i], byte(j))
						}

						break
					}
				}

				if isDynamicallyLoaded {
					break
				}
			}
		}
		if isDynamicallyLoaded {
			continue
		}

		m.Accounts = append(m.Accounts, account.PublicKey)

		if account.IsSigner {
			m.Header.NumSignatures++

			if !account.IsWritable {
				m.Header.NumReadonlySigned++
			}
		} else if !account.IsWritable {
			m.Header.NumReadOnly++
		}
	}

	// Consolidate static and dynamically loaded accounts into an ordered list,
	// which is used for index references encoded in the message
	dynamicWritableAccounts := make([]ed25519.PublicKey, 0)
	dynamicReadonlyAccounts := make([]ed25519.PublicKey, 0)
	for i, writableAddressTableIndexes := range writableAddressTableLookupIndexes {
		for _, index := range writableAddressTableIndexes {
			writableAccount := sortedAddressLookupTables[i].Addresses[index]
			dynamicWritableAccounts = append(dynamicWritableAccounts, writableAccount)
		}
	}
	for i, readonlyAddressTableIndexes := range readonlyAddressTableLookupIndexes {
		for _, index := range readonlyAddressTableIndexes {
			readonlyAccount := sortedAddressLookupTables[i].Addresses[index]
			dynamicReadonlyAccounts = append(dynamicReadonlyAccounts, readonlyAccount)
		}
	}

	var allAccounts []ed25519.PublicKey
	allAccounts = append(allAccounts, m.Accounts...)
	allAccounts = append(allAccounts, dynamicWritableAccounts...)
	allAccounts = append(allAccounts, dynamicReadonlyAccounts...)

	// Generate the compiled instruction, which uses indices instead
	// of raw account keys.
	for _, i := range instructions {
		c := CompiledInstruction{
			ProgramIndex: byte(indexOf(allAccounts, i.Program)),
			Data:         i.Data,
		}

		for _, a := range i.Accounts {
			c.Accounts = append(c.Accounts, byte(indexOf(allAccounts, a.PublicKey)))
		}

		m.Instructions = append(m.Instructions, c)
	}

	// Generate the compiled message address table lookups
	for i, addressLookupTable := range sortedAddressLookupTables {
		if len(writableAddressTableLookupIndexes[i]) == 0 && len(readonlyAddressTableLookupIndexes[i]) == 0 {
			continue
		}

		m.AddressTableLookups = append(m.AddressTableLookups, MessageAddressTableLookup{
			PublicKey:       addressLookupTable.PublicKey,
			WritableIndexes: writableAddressTableLookupIndexes[i],
			ReadonlyIndexes: readonlyAddressTableLookupIndexes[i],
		})
	}

	for i := range m.Accounts {
		if len(m.Accounts[i]) == 0 {
			m.Accounts[i] = make([]byte, ed25519.PublicKeySize)
		}
	}

	return Transaction{
		Signatures: make([]Signature, m.Header.NumSignatures),
		Message:    m,
	}
}

func (t *Transaction) Signature() []byte {
	return t.Signatures[0][:]
}

func (t *Transaction) String() string {
	var sb strings.Builder
	sb.WriteString("Signatures:\n")
	for i, s := range t.Signatures {
		sb.WriteString(fmt.Sprintf("  %d: %s\n", i, base58.Encode(s[:])))
	}
	sb.WriteString("Message:\n")
	sb.WriteString(fmt.Sprintf(" Version: %s\n", t.Message.Version.String()))
	sb.WriteString("  Header:\n")
	sb.WriteString(fmt.Sprintf("    NumSignatures: %d\n", t.Message.Header.NumSignatures))
	sb.WriteString(fmt.Sprintf("    NumReadOnly: %d\n", t.Message.Header.NumReadOnly))
	sb.WriteString(fmt.Sprintf("    NumReadOnlySigned: %d\n", t.Message.Header.NumReadonlySigned))
	sb.WriteString(fmt.Sprintf("  Blockhash: %s\n", base58.Encode(t.Message.RecentBlockhash[:])))
	sb.WriteString("  Accounts:\n")
	for i, a := range t.Message.Accounts {
		sb.WriteString(fmt.Sprintf("    %d: %s\n", i, base58.Encode(a)))
	}
	sb.WriteString("  Instructions:\n")
	for i := range t.Message.Instructions {
		sb.WriteString(fmt.Sprintf("    %d:\n", i))
		sb.WriteString(fmt.Sprintf("      ProgramIndex: %d\n", t.Message.Instructions[i].ProgramIndex))
		sb.WriteString(fmt.Sprintf("      Accounts: %v\n", t.Message.Instructions[i].Accounts))
		sb.WriteString(fmt.Sprintf("      Data: %v\n", t.Message.Instructions[i].Data))
	}
	if t.Message.Version == MessageVersion1 {
		sb.WriteString("  Config:\n")
		if t.Message.Config.PriorityFeeLamports != nil {
			sb.WriteString(fmt.Sprintf("    PriorityFeeLamports: %d\n", *t.Message.Config.PriorityFeeLamports))
		}
		if t.Message.Config.ComputeUnitLimit != nil {
			sb.WriteString(fmt.Sprintf("    ComputeUnitLimit: %d\n", *t.Message.Config.ComputeUnitLimit))
		}
		if t.Message.Config.LoadedAccountsDataSizeLimit != nil {
			sb.WriteString(fmt.Sprintf("    LoadedAccountsDataSizeLimit: %d\n", *t.Message.Config.LoadedAccountsDataSizeLimit))
		}
		if t.Message.Config.HeapSize != nil {
			sb.WriteString(fmt.Sprintf("    HeapSize: %d\n", *t.Message.Config.HeapSize))
		}
	}
	if t.Message.Version == MessageVersion0 {
		sb.WriteString("  Address Table Lookups:\n")
		for i := range t.Message.AddressTableLookups {
			sb.WriteString(fmt.Sprintf("    %s:\n", base58.Encode(t.Message.AddressTableLookups[i].PublicKey)))
			sb.WriteString(fmt.Sprintf("      Writable Indexes: %v\n", t.Message.AddressTableLookups[i].WritableIndexes))
			sb.WriteString(fmt.Sprintf("      Readonly Indexes: %v\n", t.Message.AddressTableLookups[i].ReadonlyIndexes))
		}
	}

	return sb.String()
}

func (t *Transaction) SetBlockhash(bh Blockhash) {
	t.Message.RecentBlockhash = bh
}

func (t *Transaction) Sign(signers ...ed25519.PrivateKey) error {
	messageBytes, err := t.Message.Marshal()
	if err != nil {
		return err
	}

	for _, s := range signers {
		pub := s.Public().(ed25519.PublicKey)
		index := indexOf(t.Message.Accounts, pub)
		if index < 0 {
			return errors.Errorf("signing account %s is not in the account list", base58.Encode(pub))
		}
		if index >= len(t.Signatures) {
			return errors.Errorf("signing account %s is not in the list of signers", base58.Encode(pub))
		}

		copy(t.Signatures[index][:], ed25519.Sign(s, messageBytes))
	}

	return nil
}

func filterUnique(accounts []AccountMeta) []AccountMeta {
	filtered := make([]AccountMeta, 0, len(accounts))

	for i := range accounts {
		for j := range filtered {
			// If we've already seen the account before, then we should check to
			// see if we should promote any of the permissions or statuses.
			if bytes.Equal(accounts[i].PublicKey, filtered[j].PublicKey) {
				if accounts[i].IsSigner {
					filtered[j].IsSigner = true
				}
				if accounts[i].IsWritable {
					filtered[j].IsWritable = true
				}
				if accounts[i].isPayer {
					filtered[j].isPayer = true
				}
				if accounts[i].isProgram {
					filtered[j].isProgram = true
				}

				goto next
			}
		}

		filtered = append(filtered, accounts[i])
	next:
	}

	return filtered
}

func indexOf(slice []ed25519.PublicKey, item ed25519.PublicKey) int {
	for i, val := range slice {
		if bytes.Equal(val, item) {
			return i
		}
	}

	return -1
}

func (v MessageVersion) String() string {
	switch v {
	case MessageVersionLegacy:
		return "legacy"
	case MessageVersion0:
		return "v0"
	case MessageVersion1:
		return "v1"
	}
	return "unknown"
}
