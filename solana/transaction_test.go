package solana

import (
	"bytes"
	"crypto/ed25519"
	"encoding/base64"
	"math"
	"math/rand"
	"slices"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/code-payments/ocp-server/pointer"
)

// Taken from: https://github.com/solana-labs/solana/blob/14339dec0a960e8161d1165b6a8e5cfb73e78f23/sdk/src/transaction.rs#L523
const rustGenerated = "AUc7Cbu+gZalFSGeSFdukHhP7oSGaSdmdNEd5ZokaSysdoMWfIOzjrAbdaBZZuDMAfyNAogAJdrhgVya+jthsgoBAAEDnON0wdcmjhYIDuXvd10F2qEjAyEAJGSe/CGhYbk+WWMBAQEEBQYHCAkJCQkJCQkJCQkJCQkJCQkIBwYFBAEBAQICAgQFBgcICQEBAQEBAQEBAQEBAQEBCQgHBgUEAgICAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABAgIAAQMBAgM="

// The above example does not have the correct public key encoded in the keypair.
// This is the above example with the correctly generated keypair.
const rustGeneratedAdjusted = "ATMfBMZ8phHEheLph8K9TJhRKhnE4qNZvWiXdUdJRmlTCRsQjWmW2CkQJeRHBCcsqFm2gynjL40M9mTe0Dxp4QIBAAEDfEya6wnC7f3Cv53qnOEywwIJ928rIdqAlfXYI1adXroBAQEEBQYHCAkJCQkJCQkJCQkJCQkJCQkIBwYFBAEBAQICAgQFBgcICQEBAQEBAQEBAQEBAQEBCQgHBgUEAgICAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABAgIAAQMBAgM="

func TestLegacyTransaction_CrossImpl(t *testing.T) {
	keypair := ed25519.PrivateKey{48, 83, 2, 1, 1, 48, 5, 6, 3, 43, 101, 112, 4, 34, 4, 32, 255, 101, 36, 24, 124, 23,
		167, 21, 132, 204, 155, 5, 185, 58, 121, 75, 156, 227, 116, 193, 215, 38, 142, 22, 8,
		14, 229, 239, 119, 93, 5, 218, 161, 35, 3, 33, 0, 36, 100, 158, 252, 33, 161, 97, 185,
		62, 89, 99}
	programID := ed25519.PublicKey{2, 2, 2, 4, 5, 6, 7, 8, 9, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 9, 8, 7, 6, 5, 4,
		2, 2, 2}
	to := ed25519.PublicKey{1, 1, 1, 4, 5, 6, 7, 8, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 8, 7, 6, 5, 4, 1, 1, 1}

	tx := NewLegacyTransaction(
		keypair.Public().(ed25519.PublicKey),
		NewInstruction(
			programID,
			[]byte{1, 2, 3},
			NewAccountMeta(keypair.Public().(ed25519.PublicKey), true),
			NewAccountMeta(to, false),
		),
	)
	require.NoError(t, tx.Sign(keypair))

	generated, err := base64.StdEncoding.DecodeString(rustGenerated)
	require.NoError(t, err)
	assert.Equal(t, generated, mustMarshal(t, tx))
}

func TestLegacyTransaction_GenerateValidCrossImpl(t *testing.T) {
	keypair := ed25519.NewKeyFromSeed([]byte{48, 83, 2, 1, 1, 48, 5, 6, 3, 43, 101, 112, 4, 34, 4, 32, 255, 101, 36, 24, 124, 23,
		167, 21, 132, 204, 155, 5, 185, 58, 121, 75})
	programID := ed25519.PublicKey{2, 2, 2, 4, 5, 6, 7, 8, 9, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 9, 8, 7, 6, 5, 4,
		2, 2, 2}
	to := ed25519.PublicKey{1, 1, 1, 4, 5, 6, 7, 8, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 8, 7, 6, 5, 4, 1, 1, 1}

	tx := NewLegacyTransaction(
		keypair.Public().(ed25519.PublicKey),
		NewInstruction(
			programID,
			[]byte{1, 2, 3},
			NewAccountMeta(keypair.Public().(ed25519.PublicKey), true),
			NewAccountMeta(to, false),
		),
	)
	require.NoError(t, tx.Sign(keypair))
	assert.Equal(t, rustGeneratedAdjusted, base64.StdEncoding.EncodeToString(mustMarshal(t, tx)))
}

func TestLegacyTransaction_EmptyAccount(t *testing.T) {
	program, _, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	pub, priv, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)

	tx := NewLegacyTransaction(
		pub,
		NewInstruction(
			program,
			[]byte{1, 2, 3},
			NewAccountMeta(nil, false),
		),
	)
	assert.NoError(t, tx.Sign(priv))

	var rtt Transaction
	assert.NoError(t, rtt.Unmarshal(mustMarshal(t, tx)))
}

func TestLegacyTransaction_MarshalRoundTrip(t *testing.T) {
	expected := "AaZAGNONKTsNypCfvwHGipcWmAX/J03VfLQEHgMDSuHz0ktydqlLb7I4tZnX0Yw8KMTbma28M+yiZPaRolOJGgwBAAgQCR2hNbdxjAiYwC9CSEo2Vso3yq8OXlgoCbepyseaRXoIFE8MTz2ZtOsdNl55fj/zi0S+ArjIP4zJ3Y+MC4tKyQu7s1JPy6Hur6YbU0nF+1XBJYwii/dKtLsNFU/pTo19J7jOgutpJBZbNIhC5ppqC/OYlbzW1KqamkV3p+cslAoyBJxvWrSMXX+X0Ih0+sEzarslIYSV0T/NuLFcjpX8S7ajCdht+3+POhvGcGFzDyc4kIgjN/SAdypJM1Grs+eEtzXhQGM4VMy0p0J2CiOH+k2kwfya5F7fSaYXWOi3CJUGp9UXGSxWjuCKhF9z0peIzwNcMUWyGrNE2AYuqUAAAAan1RcZLFxRIYzJTD1K8X9Y2u4Im6H9ROPb2YoAAAAABt324ddloZPZy+FGzut5rBy0he1fWzeROoz1hX7/AKlDDB9w5G7eh4xhLJIgxblM0E4dxW+ZTABRcCVBt2LcH8b6evO+2606PWXzaqvJdDGxu+TC0vbg5HymAgNFL11hDcYoaKd+VYB6HNWIyaKadms+4q7NwH3gjP6RB91LMWUAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAMGRm/lIRcy/+ytunLDm+e8jOW7xfcSayxDmzpAAAAAjJclj04kifG7PRApFI4NgwtaE5na/xCEBI572Nvp+FmMVCZzhQC2pwD9u6aAm8haUDNRSZG/a7c1U/ltYtc+KAUNAwIHAAQEAAAADgAJA+gDAAAAAAAADgAFAkjoAQAPBwADCgsNCQgBAQwLAAUBBAwMBgwMAwlcCAoCAAAAmhMJCgIAAAAAAUgAAABlmEW1THFmZqyjBehuSli5bMSJBNiQMkZcr19LINSM4KF/whE1IayV174tmVwC9MMlQSmG3j6aJVhIDGMUITUNXRMTAAAAAAA="
	decoded, err := base64.StdEncoding.DecodeString(expected)
	require.NoError(t, err)
	var txn Transaction
	require.NoError(t, txn.Unmarshal(decoded))
	assert.Equal(t, decoded, mustMarshal(t, txn))
}

func TestLegacyTransaction_MissingBlockhash(t *testing.T) {
	program, _, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	pub, priv, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)

	tx := NewLegacyTransaction(
		pub,
		NewInstruction(
			program,
			[]byte{1, 2, 3},
			NewAccountMeta(pub, false),
		),
	)
	assert.NoError(t, tx.Sign(priv))

	var rtt Transaction
	assert.NoError(t, rtt.Unmarshal(mustMarshal(t, tx)))
}

func TestLegacyTransaction_InvalidAccounts(t *testing.T) {
	keys := generateKeys(t, 2)
	tx := NewLegacyTransaction(
		public(keys[0]),
		NewInstruction(
			public(keys[1]),
			nil,
			NewAccountMeta(public(keys[0]), true),
		),
	)
	tx.Message.Instructions[0].ProgramIndex = 2
	assert.Error(t, tx.Unmarshal(mustMarshal(t, tx)))

	tx = NewLegacyTransaction(
		public(keys[0]),
		NewInstruction(
			public(keys[1]),
			nil,
			NewAccountMeta(public(keys[0]), true),
		),
	)
	tx.Message.Instructions[0].Accounts = []byte{2}
}

func TestLegacyTransaction_SingleInstruction(t *testing.T) {
	keys := generateKeys(t, 2)
	payer := keys[0]
	program := keys[1]

	keys = generateKeys(t, 4)
	data := []byte{1, 2, 3}

	tx := NewLegacyTransaction(
		public(payer),
		NewInstruction(
			public(program),
			data,
			NewReadonlyAccountMeta(public(keys[0]), true),
			NewReadonlyAccountMeta(public(keys[1]), false),
			NewAccountMeta(public(keys[2]), false),
			NewAccountMeta(public(keys[3]), true),
		),
	)

	// Intentionally sign out of order to ensure ordering is fixed.
	assert.NoError(t, tx.Sign(keys[0], keys[3], payer))

	require.Len(t, tx.Signatures, 3)
	require.Len(t, tx.Message.Accounts, 6)
	assert.EqualValues(t, 3, tx.Message.Header.NumSignatures)
	assert.EqualValues(t, 1, tx.Message.Header.NumReadonlySigned)
	assert.EqualValues(t, 2, tx.Message.Header.NumReadOnly)

	message := mustMarshalMessage(t, tx.Message)

	assert.True(t, ed25519.Verify(public(payer), message, tx.Signatures[0][:]))
	assert.True(t, ed25519.Verify(public(keys[3]), message, tx.Signatures[1][:]))
	assert.True(t, ed25519.Verify(public(keys[0]), message, tx.Signatures[2][:]))

	assert.Equal(t, MessageVersionLegacy, tx.Message.Version)

	assert.Equal(t, public(payer), tx.Message.Accounts[0])
	assert.Equal(t, public(keys[3]), tx.Message.Accounts[1])
	assert.Equal(t, public(keys[0]), tx.Message.Accounts[2])
	assert.Equal(t, public(keys[2]), tx.Message.Accounts[3])
	assert.Equal(t, public(keys[1]), tx.Message.Accounts[4])
	assert.Equal(t, public(program), tx.Message.Accounts[5])

	assert.Equal(t, byte(5), tx.Message.Instructions[0].ProgramIndex)
	assert.Equal(t, data, tx.Message.Instructions[0].Data)
	assert.Equal(t, []byte{2, 4, 3, 1}, tx.Message.Instructions[0].Accounts)
}

func TestLegacyTransaction_DuplicateKeys(t *testing.T) {
	keys := generateKeys(t, 2)
	payer := keys[0]
	program := keys[1]

	keys = generateKeys(t, 4)
	sort.Slice(keys, func(i, j int) bool {
		return bytes.Compare(public(keys[i]), public(keys[j])) < 0
	})

	data := []byte{1, 2, 3}

	// Key[0]: ReadOnlySigner -> WritableSigner
	// Key[1]: ReadOnly       -> ReadOnlySigner
	// Key[2]: Writable       -> Writable       (ReadOnly,noop)
	// Key[3]: WritableSigner -> WritableSigner (ReadOnly,noop)

	tx := NewLegacyTransaction(
		public(payer),
		NewInstruction(
			public(program),
			data,
			NewReadonlyAccountMeta(public(keys[0]), true),
			NewReadonlyAccountMeta(public(keys[1]), false),
			NewAccountMeta(public(keys[2]), false),
			NewAccountMeta(public(keys[3]), true),
			// Upgrade keys [0] and [1]
			NewAccountMeta(public(keys[0]), false),
			NewReadonlyAccountMeta(public(keys[1]), true),
			// 'Downgrade' keys [2] and [3] (noop)
			NewReadonlyAccountMeta(public(keys[2]), false),
			NewReadonlyAccountMeta(public(keys[3]), false),
		),
	)

	// Intentionally sign out of order to ensure ordering is fixed.
	assert.NoError(t, tx.Sign(
		keys[0],
		keys[1],
		keys[3],
		payer,
	))

	require.Len(t, tx.Signatures, 4)
	require.Len(t, tx.Message.Accounts, 6)
	assert.EqualValues(t, 4, tx.Message.Header.NumSignatures)
	assert.EqualValues(t, 1, tx.Message.Header.NumReadonlySigned)
	assert.EqualValues(t, 1, tx.Message.Header.NumReadOnly)

	message := mustMarshalMessage(t, tx.Message)

	assert.True(t, ed25519.Verify(public(payer), message, tx.Signatures[0][:]))
	assert.True(t, ed25519.Verify(public(keys[0]), message, tx.Signatures[1][:]))
	assert.True(t, ed25519.Verify(public(keys[3]), message, tx.Signatures[2][:]))
	assert.True(t, ed25519.Verify(public(keys[1]), message, tx.Signatures[3][:]))

	assert.Equal(t, MessageVersionLegacy, tx.Message.Version)

	assert.Equal(t, payer.Public(), tx.Message.Accounts[0])
	assert.Equal(t, keys[0].Public(), tx.Message.Accounts[1])
	assert.Equal(t, keys[3].Public(), tx.Message.Accounts[2])
	assert.Equal(t, keys[1].Public(), tx.Message.Accounts[3])
	assert.Equal(t, keys[2].Public(), tx.Message.Accounts[4])
	assert.Equal(t, program.Public(), tx.Message.Accounts[5])

	assert.Equal(t, byte(5), tx.Message.Instructions[0].ProgramIndex)
	assert.Equal(t, data, tx.Message.Instructions[0].Data)
	assert.Equal(t, []byte{1, 3, 4, 2, 1, 3, 4, 2}, tx.Message.Instructions[0].Accounts)
}

func TestLegacyTransaction_MultiInstruction(t *testing.T) {
	keys := generateKeys(t, 3)
	sort.Slice(keys, func(i, j int) bool {
		return bytes.Compare(public(keys[i]), public(keys[j])) < 0
	})

	payer := keys[0]
	program := keys[1]
	program2 := keys[2]

	keys = generateKeys(t, 6)
	sort.Slice(keys, func(i, j int) bool {
		return bytes.Compare(public(keys[i]), public(keys[j])) < 0
	})

	data := []byte{1, 2, 3}
	data2 := []byte{3, 4, 5}

	// Key[0]: ReadOnlySigner -> WritableSigner
	// Key[1]: ReadOnly       -> WritableSigner
	// Key[2]: Writable       -> Writable       (ReadOnly,noop)
	// Key[3]: WritableSigner -> WritableSigner (ReadOnly,noop)
	// Key[4]: n/a            -> WritableSigner
	// Key[5]: n/a            -> ReadOnly

	tx := NewLegacyTransaction(
		public(payer),
		NewInstruction(
			public(program2),
			data,
			NewReadonlyAccountMeta(public(keys[0]), true),
			NewReadonlyAccountMeta(public(keys[1]), false),
			NewAccountMeta(public(keys[2]), false),
			NewAccountMeta(public(keys[3]), true),
		),
		NewInstruction(
			public(program),
			data2,
			// Ensure that keys don't get downgraded in permissions
			NewReadonlyAccountMeta(public(keys[3]), false),
			NewReadonlyAccountMeta(public(keys[2]), false),
			// Ensure we can upgrade upgrading works
			NewAccountMeta(public(keys[0]), false),
			NewAccountMeta(public(keys[1]), true),
			// Ensure accounts get added
			NewAccountMeta(public(keys[4]), true),
			NewReadonlyAccountMeta(public(keys[5]), false),
		),
	)

	assert.NoError(t, tx.Sign(
		payer,
		keys[0],
		keys[1],
		keys[3],
		keys[4],
	))

	require.Len(t, tx.Signatures, 5)
	require.Len(t, tx.Message.Accounts, 9)

	assert.EqualValues(t, 5, tx.Message.Header.NumSignatures)
	assert.EqualValues(t, 0, tx.Message.Header.NumReadonlySigned)
	assert.EqualValues(t, 3, tx.Message.Header.NumReadOnly)

	message := mustMarshalMessage(t, tx.Message)

	assert.True(t, ed25519.Verify(public(payer), message, tx.Signatures[0][:]))
	assert.True(t, ed25519.Verify(public(keys[0]), message, tx.Signatures[1][:]))
	assert.True(t, ed25519.Verify(public(keys[1]), message, tx.Signatures[2][:]))
	assert.True(t, ed25519.Verify(public(keys[3]), message, tx.Signatures[3][:]))
	assert.True(t, ed25519.Verify(public(keys[4]), message, tx.Signatures[4][:]))

	assert.Equal(t, MessageVersionLegacy, tx.Message.Version)

	assert.Equal(t, public(payer), tx.Message.Accounts[0])
	assert.Equal(t, public(keys[0]), tx.Message.Accounts[1])
	assert.Equal(t, public(keys[1]), tx.Message.Accounts[2])
	assert.Equal(t, public(keys[3]), tx.Message.Accounts[3])
	assert.Equal(t, public(keys[4]), tx.Message.Accounts[4])
	assert.Equal(t, public(keys[2]), tx.Message.Accounts[5])
	assert.Equal(t, public(keys[5]), tx.Message.Accounts[6])
	assert.Equal(t, public(program), tx.Message.Accounts[7])
	assert.Equal(t, public(program2), tx.Message.Accounts[8])

	assert.Equal(t, byte(8), tx.Message.Instructions[0].ProgramIndex)
	assert.Equal(t, data, tx.Message.Instructions[0].Data)
	assert.Equal(t, []byte{1, 2, 5, 3}, tx.Message.Instructions[0].Accounts)

	assert.Equal(t, byte(7), tx.Message.Instructions[1].ProgramIndex)
	assert.Equal(t, data2, tx.Message.Instructions[1].Data)
	assert.Equal(t, []byte{0x3, 0x5, 0x1, 0x2, 0x4, 0x6}, tx.Message.Instructions[1].Accounts)
}

func TestV0Transaction_MultipleAlts(t *testing.T) {
	keys := generateKeys(t, 8)
	sort.Slice(keys, func(i, j int) bool {
		return bytes.Compare(public(keys[i]), public(keys[j])) < 0
	})

	payer := keys[0]
	program := keys[1]
	program2 := keys[2]
	accountSigner := keys[3]
	accountReadonly := keys[4]
	accountReadonly2 := keys[5]
	accountWriteable := keys[6]
	accountWriteable2 := keys[7]

	var bh Blockhash
	rand.Read(bh[:])

	ixns := []Instruction{
		NewInstruction(
			public(program),
			[]byte{0x1, 0x2, 0x3, 0x4},
			NewReadonlyAccountMeta(public(accountReadonly), false),
			NewReadonlyAccountMeta(public(accountReadonly2), false),
			NewReadonlyAccountMeta(public(accountWriteable), false),
			NewAccountMeta(public(accountWriteable2), false),
		),
		NewInstruction(
			public(program2),
			[]byte{0x5, 0x6, 0x7, 0x8},
			NewAccountMeta(public(accountWriteable), false),
			NewReadonlyAccountMeta(public(accountWriteable), false),
			NewReadonlyAccountMeta(public(accountReadonly), false),
			NewReadonlyAccountMeta(public(accountSigner), true),
		),
	}

	altKeys := generateKeys(t, 2)
	sort.Slice(altKeys, func(i, j int) bool {
		return bytes.Compare(public(altKeys[i]), public(altKeys[j])) < 0
	})

	alts := []AddressLookupTable{
		{
			PublicKey: public(altKeys[1]),
			Addresses: []ed25519.PublicKey{
				public(payer),
				public(program),
				public(program2),
				public(accountReadonly),
				public(accountReadonly2),
				public(accountWriteable),
				public(accountWriteable2),
			},
		},
		{
			PublicKey: public(altKeys[0]),
			Addresses: []ed25519.PublicKey{
				public(accountSigner),
				public(accountReadonly),
				public(accountReadonly),
				public(accountWriteable),
				public(accountWriteable),
			},
		},
	}

	tx := NewV0Transaction(
		public(payer),
		alts,
		ixns,
	)

	tx.SetBlockhash(bh)

	assert.NoError(t, tx.Sign(
		payer,
		accountSigner,
	))

	require.Len(t, tx.Signatures, 2)
	require.Len(t, tx.Message.Accounts, 4)
	require.Len(t, tx.Message.AddressTableLookups, 2)

	assert.EqualValues(t, 2, tx.Message.Header.NumSignatures)
	assert.EqualValues(t, 1, tx.Message.Header.NumReadonlySigned)
	assert.EqualValues(t, 2, tx.Message.Header.NumReadOnly)

	assert.Equal(t, bh, tx.Message.RecentBlockhash)

	message := mustMarshalMessage(t, tx.Message)

	assert.True(t, ed25519.Verify(public(payer), message, tx.Signatures[0][:]))
	assert.True(t, ed25519.Verify(public(accountSigner), message, tx.Signatures[1][:]))

	assert.Equal(t, MessageVersion0, tx.Message.Version)

	assert.Equal(t, public(payer), tx.Message.Accounts[0])
	assert.Equal(t, public(accountSigner), tx.Message.Accounts[1])
	assert.Equal(t, public(program), tx.Message.Accounts[2])
	assert.Equal(t, public(program2), tx.Message.Accounts[3])

	assert.Equal(t, byte(2), tx.Message.Instructions[0].ProgramIndex)
	assert.Equal(t, []byte{0x1, 0x2, 0x3, 0x4}, tx.Message.Instructions[0].Data)
	assert.Equal(t, []byte{6, 7, 4, 5}, tx.Message.Instructions[0].Accounts)

	assert.Equal(t, byte(3), tx.Message.Instructions[1].ProgramIndex)
	assert.Equal(t, []byte{0x5, 0x6, 0x7, 0x8}, tx.Message.Instructions[1].Data)
	assert.Equal(t, []byte{4, 4, 6, 1}, tx.Message.Instructions[1].Accounts)

	assert.Equal(t, public(altKeys[0]), tx.Message.AddressTableLookups[0].PublicKey)
	require.Len(t, tx.Message.AddressTableLookups[0].ReadonlyIndexes, 1)
	require.Len(t, tx.Message.AddressTableLookups[0].WritableIndexes, 1)
	assert.Equal(t, byte(1), tx.Message.AddressTableLookups[0].ReadonlyIndexes[0])
	assert.Equal(t, byte(3), tx.Message.AddressTableLookups[0].WritableIndexes[0])

	assert.Equal(t, public(altKeys[1]), tx.Message.AddressTableLookups[1].PublicKey)
	require.Len(t, tx.Message.AddressTableLookups[1].WritableIndexes, 1)
	require.Len(t, tx.Message.AddressTableLookups[1].ReadonlyIndexes, 1)
	assert.Equal(t, byte(4), tx.Message.AddressTableLookups[1].ReadonlyIndexes[0])
	assert.Equal(t, byte(6), tx.Message.AddressTableLookups[1].WritableIndexes[0])
}

func TestV0Transaction_MarshalRoundTrip(t *testing.T) {
	expected := "Abyp+nvyM7ZEdWoZTeADD5Cz8QJVVjhTr6CnzVj/CX2MwosyMNzT0tVNJ3gIUo8qxW8V+KclAAntCexlsvc2TQiAAQAEBYNezk00yE7eeJ8KVQSTMRnfgqKr2TuCkI2OvY6VqupmBqfVFxksVo7gioRfc9KXiM8DXDFFshqzRNgGLqlAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAMGRm/lIRcy/+ytunLDm+e8jOW7xfcSayxDmzpAAAAAmu3bzcyfl+oHt1b29uzQvgBqO8OA3K6s5S0u4S+oQYqcHxhrhTySMLI0fOjClaCEkXjCshHIi9E63Co6m/5ZfgQCAwcBAAQEAAAAAwAFAkANAwADAAkD6AMAAAAAAAAEBQUGCAkKCgABAgMEBQYHCAkBtCdbdeueeYQHgQ6Wzm4pItAtbgGigO5L8M2bbV6t3zoDAgMAAwQFBg=="
	decoded, err := base64.StdEncoding.DecodeString(expected)
	require.NoError(t, err)
	var txn Transaction
	require.NoError(t, txn.Unmarshal(decoded))
	assert.Equal(t, decoded, mustMarshal(t, txn))
}

// Generated with @solana/kit 8 (see TestV1Transaction_CrossImpl for the inputs)
var kitGeneratedV1 = map[string]string{
	"full":            "gQIBAR8AAAAFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQIEiojj3XQJ8ZX9UtstPLpdcspnCb8dlBIb83SIAbQPb1yBOXcOqH0XX1ajVGbDTH7My42KkbTuN6Jd9g9bj8mzlAMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBASIEwAAAAAAAEANAwCghgEAAAABAAMDAwADASwBAAECAQIDAgABAgMEBQYHCAkKCwwNDg8QERITFBUWFxgZGhscHR4fICEiIyQlJicoKSorLC0uLzAxMjM0NTY3ODk6Ozw9Pj9AQUJDREVGR0hJSktMTU5PUFFSU1RVVldYWVpbXF1eX2BhYmNkZWZnaGlqa2xtbm9wcXJzdHV2d3h5ent8fX5/gIGCg4SFhoeIiYqLjI2Oj5CRkpOUlZaXmJmam5ydnp+goaKjpKWmp6ipqqusra6vsLGys7S1tre4ubq7vL2+v8DBwsPExcbHyMnKy8zNzs/Q0dLT1NXW19jZ2tvc3d7f4OHi4+Tl5ufo6err7O3u7/Dx8vP09fb3+Pn6+/z9/v8AAQIDBAUGBwgJCgsMDQ4PEBESExQVFhcYGRobHB0eHyAhIiMkJSYnKCkqKwadBkWJOSTqIre0TpnD2/pEhHIliTVG9q0VmMxZ5Mz5+BhjNNIomISK6k39PbmWxc6/z2bFjYF5UKDjzbEHWQRYnYl9XMa2so4QHF4tQfq45lXaxyETBhfbL3V64qt3bvHqAomU9jRkhk89YPkuvmIZN+mJaoH/DIY8R4GcxygF",
	"limits-only":     "gQIBAQwAAAAFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQIEiojj3XQJ8ZX9UtstPLpdcspnCb8dlBIb83SIAbQPb1yBOXcOqH0XX1ajVGbDTH7My42KkbTuN6Jd9g9bj8mzlAMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBARADQMAoIYBAAMDAwADASwBAAECAQIDAgABAgMEBQYHCAkKCwwNDg8QERITFBUWFxgZGhscHR4fICEiIyQlJicoKSorLC0uLzAxMjM0NTY3ODk6Ozw9Pj9AQUJDREVGR0hJSktMTU5PUFFSU1RVVldYWVpbXF1eX2BhYmNkZWZnaGlqa2xtbm9wcXJzdHV2d3h5ent8fX5/gIGCg4SFhoeIiYqLjI2Oj5CRkpOUlZaXmJmam5ydnp+goaKjpKWmp6ipqqusra6vsLGys7S1tre4ubq7vL2+v8DBwsPExcbHyMnKy8zNzs/Q0dLT1NXW19jZ2tvc3d7f4OHi4+Tl5ufo6err7O3u7/Dx8vP09fb3+Pn6+/z9/v8AAQIDBAUGBwgJCgsMDQ4PEBESExQVFhcYGRobHB0eHyAhIiMkJSYnKCkqKwE8y4qiLSYJrKSBQnDToZidgFSVl7PhZv5Gwbq0tmlW4g6d42zt8uQLEsdSANu13g41ITmR+fooHRO2CB3w0AS2t7xsuZBA4mrDP7O+Aq52TyOMK2QTm+RSHX9IRdKNMEHLiASI3fj13ipTPUY5vrUBDL20WjXNFgwtrPuQ44QI",
	"limits-and-heap": "gQIBARwAAAAFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQIEiojj3XQJ8ZX9UtstPLpdcspnCb8dlBIb83SIAbQPb1yBOXcOqH0XX1ajVGbDTH7My42KkbTuN6Jd9g9bj8mzlAMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBARADQMAoIYBAAAAAQADAwMAAwEsAQABAgECAwIAAQIDBAUGBwgJCgsMDQ4PEBESExQVFhcYGRobHB0eHyAhIiMkJSYnKCkqKywtLi8wMTIzNDU2Nzg5Ojs8PT4/QEFCQ0RFRkdISUpLTE1OT1BRUlNUVVZXWFlaW1xdXl9gYWJjZGVmZ2hpamtsbW5vcHFyc3R1dnd4eXp7fH1+f4CBgoOEhYaHiImKi4yNjo+QkZKTlJWWl5iZmpucnZ6foKGio6SlpqeoqaqrrK2ur7CxsrO0tba3uLm6u7y9vr/AwcLDxMXGx8jJysvMzc7P0NHS09TV1tfY2drb3N3e3+Dh4uPk5ebn6Onq6+zt7u/w8fLz9PX29/j5+vv8/f7/AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8gISIjJCUmJygpKisjFDtun4YLWCyoXjNiGkdZqTNhrKcegvxZZJmLerDD3X4uzI+ShZm8rJghk5SBkFAwUG1UeLxc96xPZKCgysQLlaE5PLTk7rlpzjkfFni+07lAdfaB+lJYV+tyHMu/BGE+g3aTmHZO+xeYJg67HrNfhRJkAEA33w2dpkbuaFtaCQ==",
}

// Generated with @solana/kit 8 using the same inputs as kitGeneratedV1, but with
// limits unset. These can't be built with NewV1Transaction, but must decode.
var kitGeneratedV1WithoutLimits = map[string]string{
	"cu-limit-and-heap": "gQIBARQAAAAFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQIEiojj3XQJ8ZX9UtstPLpdcspnCb8dlBIb83SIAbQPb1yBOXcOqH0XX1ajVGbDTH7My42KkbTuN6Jd9g9bj8mzlAMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBARADQMAAAABAAMDAwADASwBAAECAQIDAgABAgMEBQYHCAkKCwwNDg8QERITFBUWFxgZGhscHR4fICEiIyQlJicoKSorLC0uLzAxMjM0NTY3ODk6Ozw9Pj9AQUJDREVGR0hJSktMTU5PUFFSU1RVVldYWVpbXF1eX2BhYmNkZWZnaGlqa2xtbm9wcXJzdHV2d3h5ent8fX5/gIGCg4SFhoeIiYqLjI2Oj5CRkpOUlZaXmJmam5ydnp+goaKjpKWmp6ipqqusra6vsLGys7S1tre4ubq7vL2+v8DBwsPExcbHyMnKy8zNzs/Q0dLT1NXW19jZ2tvc3d7f4OHi4+Tl5ufo6err7O3u7/Dx8vP09fb3+Pn6+/z9/v8AAQIDBAUGBwgJCgsMDQ4PEBESExQVFhcYGRobHB0eHyAhIiMkJSYnKCkqK5Vge5MRSqEEShsH3fq8JxeGd6NGbSH3emDtsNI38/tF1zwrIClU2Q4k/0LLCSU3JoUGDrZhOeody++FF+kMSAZHeHJIGGKcvfgZzlJFs86l4tBomCdR0LqdQVq+rTJHgawAvzFnXQgmrT8gOBbbR6nxnPDBlNNRYD+PmTI+WmAH",
	"none":              "gQIBAQAAAAAFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQIEiojj3XQJ8ZX9UtstPLpdcspnCb8dlBIb83SIAbQPb1yBOXcOqH0XX1ajVGbDTH7My42KkbTuN6Jd9g9bj8mzlAMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQDAwMAAwEsAQABAgECAwIAAQIDBAUGBwgJCgsMDQ4PEBESExQVFhcYGRobHB0eHyAhIiMkJSYnKCkqKywtLi8wMTIzNDU2Nzg5Ojs8PT4/QEFCQ0RFRkdISUpLTE1OT1BRUlNUVVZXWFlaW1xdXl9gYWJjZGVmZ2hpamtsbW5vcHFyc3R1dnd4eXp7fH1+f4CBgoOEhYaHiImKi4yNjo+QkZKTlJWWl5iZmpucnZ6foKGio6SlpqeoqaqrrK2ur7CxsrO0tba3uLm6u7y9vr/AwcLDxMXGx8jJysvMzc7P0NHS09TV1tfY2drb3N3e3+Dh4uPk5ebn6Onq6+zt7u/w8fLz9PX29/j5+vv8/f7/AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8gISIjJCUmJygpKivDPe8U3WKLcDb+WjfwvtMcYMg6Vb6h3gPBYfcmIpybp1b4FpmQF9XPqRjnOzi5YJCgcXeZrp5lSLWgsvWeJlQAZXRbwxrznjGCqvhnbzKRIAEp7jqpmMkmxWQg8uXHgacrBJOYhRn3s8A4q0xykHvuSTnPHLJq4uUiD5j4Xhv/Aw==",
}

func TestV1Transaction_CrossImpl(t *testing.T) {
	filled := func(b byte) []byte { return bytes.Repeat([]byte{b}, 32) }

	payer := ed25519.NewKeyFromSeed(filled(1))
	signer := ed25519.NewKeyFromSeed(filled(2))
	writable := ed25519.PublicKey(filled(3))
	program := ed25519.PublicKey(filled(4))

	var bh Blockhash
	copy(bh[:], filled(5))

	largeData := make([]byte, 300)
	for i := range largeData {
		largeData[i] = byte(i)
	}

	for name, config := range map[string]TransactionConfig{
		"full": {
			PriorityFeeLamports:         pointer.Uint64(5000),
			ComputeUnitLimit:            pointer.Uint32(200_000),
			LoadedAccountsDataSizeLimit: pointer.Uint32(100_000),
			HeapSize:                    pointer.Uint32(64 * 1024),
		},
		"limits-only": {
			ComputeUnitLimit:            pointer.Uint32(200_000),
			LoadedAccountsDataSizeLimit: pointer.Uint32(100_000),
		},
		"limits-and-heap": {
			ComputeUnitLimit:            pointer.Uint32(200_000),
			LoadedAccountsDataSizeLimit: pointer.Uint32(100_000),
			HeapSize:                    pointer.Uint32(64 * 1024),
		},
	} {
		t.Run(name, func(t *testing.T) {
			tx, err := NewV1Transaction(
				public(payer),
				config,
				NewInstruction(
					program,
					[]byte{1, 2, 3},
					NewAccountMeta(public(payer), true),
					NewReadonlyAccountMeta(public(signer), true),
					NewAccountMeta(writable, false),
				),
				NewInstruction(
					program,
					largeData,
					NewAccountMeta(writable, false),
				),
			)
			require.NoError(t, err)
			tx.SetBlockhash(bh)
			require.NoError(t, tx.Sign(payer, signer))

			assert.Equal(t, kitGeneratedV1[name], base64.StdEncoding.EncodeToString(mustMarshal(t, tx)))

			message := mustMarshalMessage(t, tx.Message)
			assert.True(t, ed25519.Verify(public(payer), message, tx.Signatures[0][:]))
			assert.True(t, ed25519.Verify(public(signer), message, tx.Signatures[1][:]))

			var decoded Transaction
			require.NoError(t, decoded.Unmarshal(mustMarshal(t, tx)))
			assert.Equal(t, tx, decoded)
		})
	}
}

func TestV1Transaction_MarshalRoundTripWithoutLimits(t *testing.T) {
	for name, encoded := range kitGeneratedV1WithoutLimits {
		t.Run(name, func(t *testing.T) {
			decoded, err := base64.StdEncoding.DecodeString(encoded)
			require.NoError(t, err)

			var tx Transaction
			require.NoError(t, tx.Unmarshal(decoded))
			assert.Nil(t, tx.Message.Config.LoadedAccountsDataSizeLimit)
			assert.Equal(t, decoded, mustMarshal(t, tx))
		})
	}
}

func TestV1Transaction_Builder(t *testing.T) {
	keys := generateKeys(t, 4)
	payer, program, writable, readonly := keys[0], keys[1], keys[2], keys[3]

	ixn := NewInstruction(
		public(program),
		[]byte{1},
		NewReadonlyAccountMeta(public(readonly), false),
		NewAccountMeta(public(writable), false),
		NewAccountMeta(public(writable), false),
	)

	// Both limits are required, since v1 treats unset limits as zero
	for _, config := range []TransactionConfig{
		{},
		{ComputeUnitLimit: pointer.Uint32(10_000)},
		{LoadedAccountsDataSizeLimit: pointer.Uint32(20_000)},
	} {
		_, err := NewV1Transaction(public(payer), config, ixn)
		assert.Error(t, err)
	}

	config := TransactionConfig{
		PriorityFeeLamports:         pointer.Uint64(1),
		ComputeUnitLimit:            pointer.Uint32(10_000),
		LoadedAccountsDataSizeLimit: pointer.Uint32(20_000),
	}
	tx, err := NewV1Transaction(public(payer), config, ixn)
	require.NoError(t, err)

	assert.Equal(t, MessageVersion1, tx.Message.Version)
	assert.Equal(t, config, tx.Message.Config)
	assert.Empty(t, tx.Message.AddressTableLookups)
	require.Len(t, tx.Signatures, 1)

	// Addresses are deduplicated, which v1 requires
	require.Len(t, tx.Message.Accounts, 4)
	assert.Equal(t, public(payer), tx.Message.Accounts[0])
	assert.Equal(t, public(writable), tx.Message.Accounts[1])
	assert.EqualValues(t, 1, tx.Message.Header.NumSignatures)
	assert.EqualValues(t, 0, tx.Message.Header.NumReadonlySigned)
	assert.EqualValues(t, 2, tx.Message.Header.NumReadOnly)

	// The wire format leads with the version byte and ends with signatures
	require.NoError(t, tx.Sign(payer))
	marshalled := mustMarshal(t, tx)
	assert.EqualValues(t, 129, marshalled[0])
	assert.Equal(t, tx.Signatures[0][:], marshalled[len(marshalled)-ed25519.SignatureSize:])
	assert.Len(t, marshalled, len(mustMarshalMessage(t, tx.Message))+ed25519.SignatureSize)

	// Mutating the caller's config doesn't alter the signed transaction
	*config.PriorityFeeLamports = 2
	*config.ComputeUnitLimit = 30_000
	*config.LoadedAccountsDataSizeLimit = 40_000
	assert.EqualValues(t, 1, *tx.Message.Config.PriorityFeeLamports)
	assert.EqualValues(t, 10_000, *tx.Message.Config.ComputeUnitLimit)
	assert.EqualValues(t, 20_000, *tx.Message.Config.LoadedAccountsDataSizeLimit)
	assert.Equal(t, marshalled, mustMarshal(t, tx))
	assert.True(t, ed25519.Verify(public(payer), mustMarshalMessage(t, tx.Message), tx.Signatures[0][:]))
}

func TestV1Transaction_MatchesLegacyAccountOrdering(t *testing.T) {
	keys := generateKeys(t, 9)
	payer, program, program2 := keys[0], keys[1], keys[2]
	writableSigner, readonlySigner, writable, readonly, upgraded, added := keys[3], keys[4], keys[5], keys[6], keys[7], keys[8]

	config := TransactionConfig{
		ComputeUnitLimit:            pointer.Uint32(10_000),
		LoadedAccountsDataSizeLimit: pointer.Uint32(20_000),
	}

	for name, instructions := range map[string][]Instruction{
		"single instruction": {
			NewInstruction(
				public(program),
				[]byte{1},
				NewReadonlyAccountMeta(public(readonly), false),
				NewAccountMeta(public(writable), false),
				NewReadonlyAccountMeta(public(readonlySigner), true),
				NewAccountMeta(public(writableSigner), true),
			),
		},
		"duplicate accounts with permission upgrades": {
			NewInstruction(
				public(program),
				[]byte{1},
				NewReadonlyAccountMeta(public(upgraded), false),
				NewReadonlyAccountMeta(public(readonly), false),
				NewAccountMeta(public(upgraded), true),
				NewReadonlyAccountMeta(public(payer), false),
			),
		},
		"multiple instructions": {
			NewInstruction(
				public(program2),
				[]byte{1, 2},
				NewReadonlyAccountMeta(public(readonlySigner), true),
				NewAccountMeta(public(writable), false),
			),
			NewInstruction(
				public(program),
				[]byte{3},
				NewReadonlyAccountMeta(public(writable), false),
				NewAccountMeta(public(readonlySigner), false),
				NewAccountMeta(public(added), true),
				NewReadonlyAccountMeta(public(readonly), false),
			),
			NewInstruction(
				public(program2),
				nil,
				NewReadonlyAccountMeta(public(program), false),
			),
		},
	} {
		t.Run(name, func(t *testing.T) {
			legacy := NewLegacyTransaction(public(payer), instructions...)
			v1, err := NewV1Transaction(public(payer), config, instructions...)
			require.NoError(t, err)

			assert.Equal(t, legacy.Message.Header, v1.Message.Header)
			assert.Equal(t, legacy.Message.Accounts, v1.Message.Accounts)
			assert.Equal(t, legacy.Message.Instructions, v1.Message.Instructions)
			assert.Len(t, v1.Signatures, len(legacy.Signatures))

			// Account ordering doesn't depend on the order instructions list them
			reversed := make([]Instruction, len(instructions))
			for i, ixn := range instructions {
				metas := slices.Clone(ixn.Accounts)
				slices.Reverse(metas)
				reversed[len(instructions)-1-i] = NewInstruction(ixn.Program, ixn.Data, metas...)
			}
			reordered, err := NewV1Transaction(public(payer), config, reversed...)
			require.NoError(t, err)
			assert.Equal(t, v1.Message.Header, reordered.Message.Header)
			assert.Equal(t, v1.Message.Accounts, reordered.Message.Accounts)
		})
	}
}

func TestV1Transaction_BuilderConstraints(t *testing.T) {
	keys := generateKeys(t, 2)
	payer, program := keys[0], keys[1]

	limits := TransactionConfig{
		ComputeUnitLimit:            pointer.Uint32(10_000),
		LoadedAccountsDataSizeLimit: pointer.Uint32(20_000),
	}
	withHeap := func(size uint32) TransactionConfig {
		config := limits
		config.HeapSize = pointer.Uint32(size)
		return config
	}
	withComputeUnitLimit := func(limit uint32) TransactionConfig {
		config := limits
		config.ComputeUnitLimit = pointer.Uint32(limit)
		return config
	}
	withLoadedAccountsDataSizeLimit := func(limit uint32) TransactionConfig {
		config := limits
		config.LoadedAccountsDataSizeLimit = pointer.Uint32(limit)
		return config
	}
	accountsInstruction := func(n int, signer bool) Instruction {
		metas := make([]AccountMeta, n)
		for i := range metas {
			metas[i] = NewAccountMeta(public(generateKeys(t, 1)[0]), signer)
		}
		return NewInstruction(public(program), []byte{1}, metas...)
	}
	repeatInstruction := func(n int) []Instruction {
		ixns := make([]Instruction, n)
		for i := range ixns {
			ixns[i] = NewInstruction(public(program), []byte{byte(i)})
		}
		return ixns
	}
	dataInstruction := func(n int) Instruction {
		return NewInstruction(public(program), make([]byte, n))
	}

	empty, err := NewV1Transaction(public(payer), limits, dataInstruction(0))
	require.NoError(t, err)
	maxData := MaxV1TransactionSize - len(mustMarshal(t, empty))

	// The payer and program each occupy an account, and the payer is a signer
	for name, tc := range map[string]struct {
		config       TransactionConfig
		instructions []Instruction
		valid        bool
	}{
		"max signatures":        {limits, []Instruction{accountsInstruction(maxV1Signatures-1, true)}, true},
		"too many signatures":   {limits, []Instruction{accountsInstruction(maxV1Signatures, true)}, false},
		"max accounts":          {limits, []Instruction{accountsInstruction(maxV1Accounts-2, false)}, true},
		"too many accounts":     {limits, []Instruction{accountsInstruction(maxV1Accounts-1, false)}, false},
		"max instructions":      {limits, repeatInstruction(maxV1Instructions), true},
		"too many instructions": {limits, repeatInstruction(maxV1Instructions + 1), false},
		"min heap size":         {withHeap(minV1HeapSize), repeatInstruction(1), true},
		"max heap size":         {withHeap(maxV1HeapSize), repeatInstruction(1), true},
		"heap size too small":   {withHeap(minV1HeapSize - v1HeapSizeMultiple), repeatInstruction(1), false},
		"heap size too large":   {withHeap(maxV1HeapSize + v1HeapSizeMultiple), repeatInstruction(1), false},
		"heap size unaligned":   {withHeap(minV1HeapSize + 1), repeatInstruction(1), false},

		"zero compute unit limit":     {withComputeUnitLimit(0), repeatInstruction(1), false},
		"max compute unit limit":      {withComputeUnitLimit(maxV1ComputeUnitLimit), repeatInstruction(1), true},
		"compute unit limit too high": {withComputeUnitLimit(maxV1ComputeUnitLimit + 1), repeatInstruction(1), false},

		"zero loaded accounts data size limit":     {withLoadedAccountsDataSizeLimit(0), repeatInstruction(1), false},
		"max loaded accounts data size limit":      {withLoadedAccountsDataSizeLimit(maxV1LoadedAccountsDataSizeLimit), repeatInstruction(1), true},
		"loaded accounts data size limit too high": {withLoadedAccountsDataSizeLimit(maxV1LoadedAccountsDataSizeLimit + 1), repeatInstruction(1), false},

		"max size":  {limits, []Instruction{dataInstruction(maxData)}, true},
		"too large": {limits, []Instruction{dataInstruction(maxData + 1)}, false},
	} {
		t.Run(name, func(t *testing.T) {
			tx, err := NewV1Transaction(public(payer), tc.config, tc.instructions...)
			if !tc.valid {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.LessOrEqual(t, len(mustMarshal(t, tx)), MaxV1TransactionSize)
		})
	}
}

func TestV1Transaction_UnmarshalInvalid(t *testing.T) {
	valid, err := base64.StdEncoding.DecodeString(kitGeneratedV1["full"])
	require.NoError(t, err)

	// version + header, then mask + lifetime + counts, then 4 addresses and
	// 20 bytes of config values
	const maskOffset = 1 + 3
	const programIndexOffset = maskOffset + 4 + 32 + 2 + 4*32 + 20

	for name, mutate := range map[string]func([]byte) []byte{
		"truncated signature": func(b []byte) []byte { return b[:len(b)-1] },
		"trailing data":       func(b []byte) []byte { return append(b, 0) },
		"truncated message":   func(b []byte) []byte { return b[:100] },
		"single priority fee bit": func(b []byte) []byte {
			b[maskOffset] &^= 0b10
			return b
		},
		"program index out of range": func(b []byte) []byte {
			b[programIndexOffset] = 4
			return b
		},
	} {
		t.Run(name, func(t *testing.T) {
			var tx Transaction
			assert.Error(t, tx.Unmarshal(mutate(bytes.Clone(valid))))
		})
	}
}

func TestV1Message_UnmarshalTrailingData(t *testing.T) {
	full, err := base64.StdEncoding.DecodeString(kitGeneratedV1["full"])
	require.NoError(t, err)

	var tx Transaction
	require.NoError(t, tx.Unmarshal(full))
	message := mustMarshalMessage(t, tx.Message)

	var decoded Message
	require.NoError(t, decoded.Unmarshal(message))
	assert.Equal(t, tx.Message, decoded)

	// A full transaction, or any extra bytes, isn't a valid message
	assert.Error(t, (&Message{}).Unmarshal(full))
	assert.Error(t, (&Message{}).Unmarshal(append(bytes.Clone(message), 0)))
}

func TestV1Transaction_UnknownConfigBits(t *testing.T) {
	valid, err := base64.StdEncoding.DecodeString(kitGeneratedV1["full"])
	require.NoError(t, err)

	// Every mask bit carries 4 bytes, so config fields from future SIMDs can
	// be carried through without understanding them. Set bits 5 and 31, and
	// insert their values after the 20 bytes of known config values.
	const maskOffset = 1 + 3
	const unknownValuesOffset = maskOffset + 4 + 32 + 2 + 4*32 + 20

	mutated := bytes.Clone(valid)
	mutated[maskOffset] |= 1 << 5
	mutated[maskOffset+3] |= 1 << 7
	unknownValues := []byte{0xa, 0xb, 0xc, 0xd, 0x1, 0x2, 0x3, 0x4}
	mutated = append(mutated[:unknownValuesOffset], append(unknownValues, mutated[unknownValuesOffset:]...)...)

	var tx Transaction
	require.NoError(t, tx.Unmarshal(mutated))
	assert.Equal(t, mutated, mustMarshal(t, tx))

	assert.EqualValues(t, 5000, *tx.Message.Config.PriorityFeeLamports)
	assert.EqualValues(t, 200_000, *tx.Message.Config.ComputeUnitLimit)
	assert.EqualValues(t, 100_000, *tx.Message.Config.LoadedAccountsDataSizeLimit)
	assert.EqualValues(t, 64*1024, *tx.Message.Config.HeapSize)
	assert.Equal(t, map[uint8][4]byte{5: {0xa, 0xb, 0xc, 0xd}, 31: {0x1, 0x2, 0x3, 0x4}}, tx.Message.Config.unknown)

	// Instructions and signatures are unaffected by the extra config values
	var expected Transaction
	require.NoError(t, expected.Unmarshal(valid))
	assert.Equal(t, expected.Message.Instructions, tx.Message.Instructions)
	assert.Equal(t, expected.Signatures, tx.Signatures)
}

func public(priv ed25519.PrivateKey) ed25519.PublicKey {
	return priv.Public().(ed25519.PublicKey)
}

func generateKeys(t *testing.T, amount int) []ed25519.PrivateKey {
	keys := make([]ed25519.PrivateKey, amount)

	for i := 0; i < amount; i++ {
		_, priv, err := ed25519.GenerateKey(nil)
		require.NoError(t, err)
		keys[i] = priv
	}

	return keys
}

func TestV1Transaction_MarshalOverflow(t *testing.T) {
	newMessage := func() Message {
		return Message{
			Version:  MessageVersion1,
			Header:   Header{NumSignatures: 1},
			Accounts: []ed25519.PublicKey{make([]byte, ed25519.PublicKeySize)},
			Instructions: []CompiledInstruction{
				{ProgramIndex: 0},
			},
		}
	}

	for name, tc := range map[string]struct {
		mutate func(m *Message, n int)
		max    int
	}{
		"instructions": {
			mutate: func(m *Message, n int) { m.Instructions = make([]CompiledInstruction, n) },
			max:    math.MaxUint8,
		},
		"accounts": {
			mutate: func(m *Message, n int) {
				m.Accounts = make([]ed25519.PublicKey, n)
				for i := range m.Accounts {
					m.Accounts[i] = make([]byte, ed25519.PublicKeySize)
				}
			},
			max: math.MaxUint8,
		},
		"instruction accounts": {
			mutate: func(m *Message, n int) { m.Instructions[0].Accounts = make([]byte, n) },
			max:    math.MaxUint8,
		},
		"instruction data": {
			mutate: func(m *Message, n int) { m.Instructions[0].Data = make([]byte, n) },
			max:    math.MaxUint16,
		},
	} {
		t.Run(name, func(t *testing.T) {
			m := newMessage()
			tc.mutate(&m, tc.max)
			_, err := m.Marshal()
			assert.NoError(t, err)

			m = newMessage()
			tc.mutate(&m, tc.max+1)
			_, err = m.Marshal()
			assert.Error(t, err)
		})
	}
}

func TestV1Transaction_MarshalSignatureCount(t *testing.T) {
	message := Message{
		Version:  MessageVersion1,
		Header:   Header{NumSignatures: 2},
		Accounts: []ed25519.PublicKey{make([]byte, ed25519.PublicKeySize), make([]byte, ed25519.PublicKeySize)},
		Instructions: []CompiledInstruction{
			{ProgramIndex: 1},
		},
	}

	// v1 has no signature count prefix, so it must match the header to decode
	for _, n := range []int{0, 1, 3} {
		_, err := Transaction{Signatures: make([]Signature, n), Message: message}.Marshal()
		assert.Error(t, err)
	}

	marshalled, err := Transaction{Signatures: make([]Signature, 2), Message: message}.Marshal()
	require.NoError(t, err)

	var decoded Transaction
	require.NoError(t, decoded.Unmarshal(marshalled))
	assert.Len(t, decoded.Signatures, 2)
}

func mustMarshal(t *testing.T, tx Transaction) []byte {
	b, err := tx.Marshal()
	require.NoError(t, err)
	return b
}

func mustMarshalMessage(t *testing.T, m Message) []byte {
	b, err := m.Marshal()
	require.NoError(t, err)
	return b
}
