package solana

import (
	"bytes"
	"crypto/ed25519"
	"encoding/binary"
	"io"
	"math"
	"math/bits"

	"github.com/mr-tron/base58"
	"github.com/pkg/errors"

	"github.com/code-payments/ocp-server/solana/shortvec"
)

const (
	messageVersionSerializationOffset = 127

	// Each config mask bit corresponds to 4 bytes of config values, ordered by
	// bit position. The priority fee is the only field spanning two bits.
	transactionConfigBitPriorityFee                 uint8 = 0
	transactionConfigBitComputeUnitLimit            uint8 = 2
	transactionConfigBitLoadedAccountsDataSizeLimit uint8 = 3
	transactionConfigBitHeapSize                    uint8 = 4

	transactionConfigMaskPriorityFee uint32 = 0b11 << transactionConfigBitPriorityFee
)

func (s TransactionSignature) ToBase58() string {
	return base58.Encode(s.Signature[:])
}

func (t Transaction) Marshal() ([]byte, error) {
	message, err := t.Message.Marshal()
	if err != nil {
		return nil, err
	}

	b := bytes.NewBuffer(nil)

	// v1 transactions place the message first, followed by signatures with no
	// length prefix, since the count is implied by the message header.
	if t.Message.Version == MessageVersion1 {
		if len(t.Signatures) != int(t.Message.Header.NumSignatures) {
			return nil, errors.Errorf("transaction has %d signatures, but header requires %d", len(t.Signatures), t.Message.Header.NumSignatures)
		}

		_, _ = b.Write(message)
		for _, s := range t.Signatures {
			_, _ = b.Write(s[:])
		}
		return b.Bytes(), nil
	}

	// Signatures
	_, _ = shortvec.EncodeLen(b, len(t.Signatures))
	for _, s := range t.Signatures {
		_, _ = b.Write(s[:])
	}

	// Message
	_, _ = b.Write(message)

	return b.Bytes(), nil
}

func (t *Transaction) Unmarshal(b []byte) error {
	// A legacy or v0 transaction starts with its signature count, which can
	// never collide with the v1 version byte.
	if len(b) > 0 && b[0] == byte(MessageVersion1+messageVersionSerializationOffset) {
		return t.unmarshalV1(b)
	}

	buf := bytes.NewBuffer(b)

	sigLen, err := shortvec.DecodeLen(buf)
	if err != nil {
		return errors.Wrap(err, "failed to read signature length")
	}

	t.Signatures = make([]Signature, sigLen)
	for i := 0; i < sigLen; i++ {
		if _, err = io.ReadFull(buf, t.Signatures[i][:]); err != nil {
			return errors.Wrapf(err, "failed to read signature at %d", i)
		}
	}

	return (&t.Message).Unmarshal(buf.Bytes())
}

func (t *Transaction) unmarshalV1(b []byte) error {
	buf := bytes.NewBuffer(b)

	if err := t.Message.unmarshalV1(buf); err != nil {
		return err
	}

	t.Signatures = make([]Signature, t.Message.Header.NumSignatures)
	for i := range t.Signatures {
		if _, err := io.ReadFull(buf, t.Signatures[i][:]); err != nil {
			return errors.Wrapf(err, "failed to read signature at %d", i)
		}
	}

	if buf.Len() > 0 {
		return errors.New("unexpected trailing data after signatures")
	}

	return nil
}

func (m Message) Marshal() ([]byte, error) {
	buf := bytes.NewBuffer(nil)
	switch m.Version {
	case MessageVersionLegacy:
		m.marshalLegacy(buf)
	case MessageVersion0:
		m.marshalV0(buf)
	case MessageVersion1:
		if err := m.marshalV1(buf); err != nil {
			return nil, err
		}
	default:
		return nil, errors.New("unsupported message version")
	}
	return buf.Bytes(), nil
}

func (m *Message) marshalLegacy(b *bytes.Buffer) {
	// Header
	_ = b.WriteByte(m.Header.NumSignatures)
	_ = b.WriteByte(m.Header.NumReadonlySigned)
	_ = b.WriteByte(m.Header.NumReadOnly)

	// Accounts
	_, _ = shortvec.EncodeLen(b, len(m.Accounts))
	for _, a := range m.Accounts {
		_, _ = b.Write(a)
	}

	// Recent Blockhash
	_, _ = b.Write(m.RecentBlockhash[:])

	// Instructions
	_, _ = shortvec.EncodeLen(b, len(m.Instructions))
	for _, i := range m.Instructions {
		_ = b.WriteByte(i.ProgramIndex)

		// Accounts
		_, _ = shortvec.EncodeLen(b, len(i.Accounts))
		_, _ = b.Write(i.Accounts)

		// Data
		_, _ = shortvec.EncodeLen(b, len(i.Data))
		_, _ = b.Write(i.Data)
	}
}

func (m *Message) marshalV0(b *bytes.Buffer) {
	// Version Number
	_ = b.WriteByte(byte(m.Version + messageVersionSerializationOffset))

	// Message Content
	//
	// Note: The "middle" section remains the same as legacy format
	m.marshalLegacy(b)

	// Address Table Lookups
	_, _ = shortvec.EncodeLen(b, len(m.AddressTableLookups))
	for _, addressTableLookup := range m.AddressTableLookups {
		_, _ = b.Write(addressTableLookup.PublicKey)

		_, _ = shortvec.EncodeLen(b, len(addressTableLookup.WritableIndexes))
		_, _ = b.Write(addressTableLookup.WritableIndexes)

		_, _ = shortvec.EncodeLen(b, len(addressTableLookup.ReadonlyIndexes))
		_, _ = b.Write(addressTableLookup.ReadonlyIndexes)
	}
}

func (m *Message) marshalV1(b *bytes.Buffer) error {
	// Counts are fixed width in v1, so values that don't fit would otherwise
	// silently wrap and produce a different transaction than was signed.
	if len(m.Instructions) > math.MaxUint8 {
		return errors.New("too many instructions for v1 message")
	}
	if len(m.Accounts) > math.MaxUint8 {
		return errors.New("too many accounts for v1 message")
	}
	for _, i := range m.Instructions {
		if len(i.Accounts) > math.MaxUint8 {
			return errors.New("too many instruction accounts for v1 message")
		}
		if len(i.Data) > math.MaxUint16 {
			return errors.New("instruction data too large for v1 message")
		}
	}

	// Version Number
	_ = b.WriteByte(byte(m.Version + messageVersionSerializationOffset))

	// Header
	_ = b.WriteByte(m.Header.NumSignatures)
	_ = b.WriteByte(m.Header.NumReadonlySigned)
	_ = b.WriteByte(m.Header.NumReadOnly)

	// Transaction Config Mask
	mask, configValues := m.Config.marshal()
	_ = binary.Write(b, binary.LittleEndian, mask)

	// Lifetime Specifier
	_, _ = b.Write(m.RecentBlockhash[:])

	// Counts
	_ = b.WriteByte(byte(len(m.Instructions)))
	_ = b.WriteByte(byte(len(m.Accounts)))

	// Addresses
	for _, a := range m.Accounts {
		_, _ = b.Write(a)
	}

	// Config Values
	_, _ = b.Write(configValues)

	// Instruction Headers
	for _, i := range m.Instructions {
		_ = b.WriteByte(i.ProgramIndex)
		_ = b.WriteByte(byte(len(i.Accounts)))
		_ = binary.Write(b, binary.LittleEndian, uint16(len(i.Data)))
	}

	// Instruction Payloads
	for _, i := range m.Instructions {
		_, _ = b.Write(i.Accounts)
		_, _ = b.Write(i.Data)
	}

	return nil
}

// marshal returns the config mask and the concatenated config values, which
// are ordered by their bit position in the mask.
func (c TransactionConfig) marshal() (uint32, []byte) {
	var mask uint32
	var values []byte

	for bit := uint8(0); bit < 32; bit++ {
		switch bit {
		case transactionConfigBitPriorityFee:
			if c.PriorityFeeLamports != nil {
				mask |= transactionConfigMaskPriorityFee
				values = binary.LittleEndian.AppendUint64(values, *c.PriorityFeeLamports)
			}
		case transactionConfigBitPriorityFee + 1:
			// Second half of the priority fee, written above
		case transactionConfigBitComputeUnitLimit:
			if c.ComputeUnitLimit != nil {
				mask |= 1 << bit
				values = binary.LittleEndian.AppendUint32(values, *c.ComputeUnitLimit)
			}
		case transactionConfigBitLoadedAccountsDataSizeLimit:
			if c.LoadedAccountsDataSizeLimit != nil {
				mask |= 1 << bit
				values = binary.LittleEndian.AppendUint32(values, *c.LoadedAccountsDataSizeLimit)
			}
		case transactionConfigBitHeapSize:
			if c.HeapSize != nil {
				mask |= 1 << bit
				values = binary.LittleEndian.AppendUint32(values, *c.HeapSize)
			}
		default:
			if v, ok := c.unknown[bit]; ok {
				mask |= 1 << bit
				values = append(values, v[:]...)
			}
		}
	}

	return mask, values
}

func (m *Message) Unmarshal(b []byte) (err error) {
	if len(b) == 0 {
		return errors.New("invalid byte buffer")
	}

	if b[0] < messageVersionSerializationOffset {
		m.Version = MessageVersionLegacy
	} else if b[0] == byte(MessageVersion0+messageVersionSerializationOffset) {
		m.Version = MessageVersion0
	} else if b[0] == byte(MessageVersion1+messageVersionSerializationOffset) {
		m.Version = MessageVersion1
	} else {
		return errors.New("unsupported message version")
	}

	buf := bytes.NewBuffer(b)

	switch m.Version {
	case MessageVersionLegacy:
		return m.unmarshalLegacy(buf)
	case MessageVersion0:
		return m.unmarshalV0(buf)
	case MessageVersion1:
		if err := m.unmarshalV1(buf); err != nil {
			return err
		}
		// Matches Transaction.Unmarshal, so a full v1 transaction isn't
		// accepted as a message with its signatures silently ignored.
		if buf.Len() > 0 {
			return errors.New("unexpected trailing data after message")
		}
		return nil
	default:
		return errors.New("unsupported message version")
	}
}

func (m *Message) unmarshalLegacy(buf *bytes.Buffer) (err error) {
	// Header
	if m.Header.NumSignatures, err = buf.ReadByte(); err != nil {
		return errors.Wrap(err, "failed to read num signatures")
	}
	if m.Header.NumReadonlySigned, err = buf.ReadByte(); err != nil {
		return errors.Wrap(err, "failed to read num readonly signatures")
	}
	if m.Header.NumReadOnly, err = buf.ReadByte(); err != nil {
		return errors.Wrap(err, "failed to read num readonly")
	}

	// Accounts
	accountLen, err := shortvec.DecodeLen(buf)
	if err != nil {
		return errors.Wrap(err, "failed to read account len")
	}
	m.Accounts = make([]ed25519.PublicKey, accountLen)
	for i := 0; i < accountLen; i++ {
		m.Accounts[i] = make([]byte, ed25519.PublicKeySize)
		if _, err = io.ReadFull(buf, m.Accounts[i]); err != nil {
			return errors.Wrapf(err, "failed to read account at index %d", i)
		}
	}

	// Recent blockhash
	if _, err = io.ReadFull(buf, m.RecentBlockhash[:]); err != nil {
		return errors.Wrap(err, "failed to read recent block hash")
	}

	// Instructions
	instructionLen, err := shortvec.DecodeLen(buf)
	if err != nil {
		return errors.Wrap(err, "failed to read instruction len")
	}
	m.Instructions = make([]CompiledInstruction, instructionLen)
	for i := 0; i < instructionLen; i++ {
		var c CompiledInstruction

		// Program Index
		if c.ProgramIndex, err = buf.ReadByte(); err != nil {
			return errors.Wrapf(err, "failed to read instruction[%d] program index", i)
		}
		if int(c.ProgramIndex) >= len(m.Accounts) {
			return errors.Errorf("program index out of range: %d:%d", i, c.ProgramIndex)
		}

		// Account Indexes
		accountLen, err = shortvec.DecodeLen(buf)
		if err != nil {
			return errors.Wrapf(err, "failed to read instruction[%d] account len", i)
		}
		c.Accounts = make([]byte, accountLen)
		if _, err = io.ReadFull(buf, c.Accounts); err != nil {
			return errors.Wrapf(err, "failed to read instruction[%d] accounts", i)
		}

		for _, index := range c.Accounts {
			if int(index) >= len(m.Accounts) && m.Version == MessageVersionLegacy {
				return errors.Errorf("account index out of range: %d:%d", i, index)
			}
		}

		// Data
		dataLen, err := shortvec.DecodeLen(buf)
		if err != nil {
			return errors.Wrapf(err, "failed to read instruction[%d] data len", i)
		}
		c.Data = make([]byte, dataLen)
		if _, err = io.ReadFull(buf, c.Data); err != nil {
			return errors.Wrapf(err, "failed to read instruction[%d] data", i)
		}

		m.Instructions[i] = c
	}

	return nil
}

func (m *Message) unmarshalV0(buf *bytes.Buffer) (err error) {
	// Message Version
	version, err := buf.ReadByte()
	if err != nil {
		return errors.Wrap(err, "failed to read version byte")
	}
	if version != byte(MessageVersion0+messageVersionSerializationOffset) {
		return errors.New("message version is not v0")
	}

	// Message Content
	//
	// Note: The "middle" section remains the same as legacy format
	err = m.unmarshalLegacy(buf)
	if err != nil {
		return err
	}

	// Address Table Lookups
	addressTableLookupLen, err := shortvec.DecodeLen(buf)
	if err != nil {
		return errors.Wrap(err, "failed to read address table lookup len")
	}

	m.AddressTableLookups = make([]MessageAddressTableLookup, addressTableLookupLen)
	for i := range addressTableLookupLen {
		// Public Key
		m.AddressTableLookups[i].PublicKey = make([]byte, ed25519.PublicKeySize)
		if _, err = io.ReadFull(buf, m.AddressTableLookups[i].PublicKey); err != nil {
			return errors.Wrapf(err, "failed to read address table lookup[%d] public key", i)
		}

		// Writeable indexes
		writableIndexesLen, err := shortvec.DecodeLen(buf)
		if err != nil {
			return errors.Wrapf(err, "failed to read address table lookup[%d] writable indexes len", i)
		}
		m.AddressTableLookups[i].WritableIndexes = make([]byte, writableIndexesLen)
		if _, err = io.ReadFull(buf, m.AddressTableLookups[i].WritableIndexes); err != nil {
			return errors.Wrapf(err, "failed to read address table lookup[%d] writeable indexes", i)
		}

		// Readonly indexes
		readonlyIndexesLen, err := shortvec.DecodeLen(buf)
		if err != nil {
			return errors.Wrapf(err, "failed to read address table lookup[%d] readonly indexes len", i)
		}
		m.AddressTableLookups[i].ReadonlyIndexes = make([]byte, readonlyIndexesLen)
		if _, err = io.ReadFull(buf, m.AddressTableLookups[i].ReadonlyIndexes); err != nil {
			return errors.Wrapf(err, "failed to read address table lookup[%d] readonly indexes", i)
		}
	}

	return nil
}

func (m *Message) unmarshalV1(buf *bytes.Buffer) (err error) {
	// Message Version
	version, err := buf.ReadByte()
	if err != nil {
		return errors.Wrap(err, "failed to read version byte")
	}
	if version != byte(MessageVersion1+messageVersionSerializationOffset) {
		return errors.New("message version is not v1")
	}
	m.Version = MessageVersion1

	// Header
	if m.Header.NumSignatures, err = buf.ReadByte(); err != nil {
		return errors.Wrap(err, "failed to read num signatures")
	}
	if m.Header.NumReadonlySigned, err = buf.ReadByte(); err != nil {
		return errors.Wrap(err, "failed to read num readonly signatures")
	}
	if m.Header.NumReadOnly, err = buf.ReadByte(); err != nil {
		return errors.Wrap(err, "failed to read num readonly")
	}

	// Transaction Config Mask
	var mask uint32
	if err = binary.Read(buf, binary.LittleEndian, &mask); err != nil {
		return errors.Wrap(err, "failed to read transaction config mask")
	}
	if bits.OnesCount32(mask&transactionConfigMaskPriorityFee) == 1 {
		return errors.New("transaction config mask sets only one priority fee bit")
	}

	// Lifetime Specifier
	if _, err = io.ReadFull(buf, m.RecentBlockhash[:]); err != nil {
		return errors.Wrap(err, "failed to read lifetime specifier")
	}

	// Counts
	numInstructions, err := buf.ReadByte()
	if err != nil {
		return errors.Wrap(err, "failed to read num instructions")
	}
	numAddresses, err := buf.ReadByte()
	if err != nil {
		return errors.Wrap(err, "failed to read num addresses")
	}

	// Addresses
	m.Accounts = make([]ed25519.PublicKey, numAddresses)
	for i := range m.Accounts {
		m.Accounts[i] = make([]byte, ed25519.PublicKeySize)
		if _, err = io.ReadFull(buf, m.Accounts[i]); err != nil {
			return errors.Wrapf(err, "failed to read address at index %d", i)
		}
	}

	// Config Values
	m.Config = TransactionConfig{}
	for bit := uint8(0); bit < 32; bit++ {
		if mask&(1<<bit) == 0 {
			continue
		}

		switch bit {
		case transactionConfigBitPriorityFee:
			var v uint64
			if err = binary.Read(buf, binary.LittleEndian, &v); err != nil {
				return errors.Wrap(err, "failed to read priority fee")
			}
			m.Config.PriorityFeeLamports = &v
		case transactionConfigBitPriorityFee + 1:
			// Second half of the priority fee, read above
		case transactionConfigBitComputeUnitLimit:
			var v uint32
			if err = binary.Read(buf, binary.LittleEndian, &v); err != nil {
				return errors.Wrap(err, "failed to read compute unit limit")
			}
			m.Config.ComputeUnitLimit = &v
		case transactionConfigBitLoadedAccountsDataSizeLimit:
			var v uint32
			if err = binary.Read(buf, binary.LittleEndian, &v); err != nil {
				return errors.Wrap(err, "failed to read loaded accounts data size limit")
			}
			m.Config.LoadedAccountsDataSizeLimit = &v
		case transactionConfigBitHeapSize:
			var v uint32
			if err = binary.Read(buf, binary.LittleEndian, &v); err != nil {
				return errors.Wrap(err, "failed to read heap size")
			}
			m.Config.HeapSize = &v
		default:
			var v [4]byte
			if _, err = io.ReadFull(buf, v[:]); err != nil {
				return errors.Wrapf(err, "failed to read config value for bit %d", bit)
			}
			if m.Config.unknown == nil {
				m.Config.unknown = make(map[uint8][4]byte)
			}
			m.Config.unknown[bit] = v
		}
	}

	// Instruction Headers
	type instructionHeader struct {
		ProgramIndex byte
		NumAccounts  byte
		NumDataBytes uint16
	}
	headers := make([]instructionHeader, numInstructions)
	for i := range headers {
		if err = binary.Read(buf, binary.LittleEndian, &headers[i]); err != nil {
			return errors.Wrapf(err, "failed to read instruction[%d] header", i)
		}
		if int(headers[i].ProgramIndex) >= len(m.Accounts) {
			return errors.Errorf("program index out of range: %d:%d", i, headers[i].ProgramIndex)
		}
	}

	// Instruction Payloads
	m.Instructions = make([]CompiledInstruction, numInstructions)
	for i, header := range headers {
		c := CompiledInstruction{
			ProgramIndex: header.ProgramIndex,
			Accounts:     make([]byte, header.NumAccounts),
			Data:         make([]byte, header.NumDataBytes),
		}

		if _, err = io.ReadFull(buf, c.Accounts); err != nil {
			return errors.Wrapf(err, "failed to read instruction[%d] accounts", i)
		}
		for _, index := range c.Accounts {
			if int(index) >= len(m.Accounts) {
				return errors.Errorf("account index out of range: %d:%d", i, index)
			}
		}

		if _, err = io.ReadFull(buf, c.Data); err != nil {
			return errors.Wrapf(err, "failed to read instruction[%d] data", i)
		}

		m.Instructions[i] = c
	}

	return nil
}
