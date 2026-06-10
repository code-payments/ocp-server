package common

import (
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"testing"

	slip10 "github.com/anyproto/go-slip10"
	"github.com/mr-tron/base58/base58"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bip39 "github.com/tyler-smith/go-bip39"

	commonpb "github.com/code-payments/ocp-protobuf-api/generated/go/common/v1"

	ocp_data "github.com/code-payments/ocp-server/ocp/data"
	timelock_token_v1 "github.com/code-payments/ocp-server/solana/timelock/v1"
	"github.com/code-payments/ocp-server/solana/token"
	"github.com/code-payments/ocp-server/solana/vm"
)

func TestAccountWithPublicKey(t *testing.T) {
	publicKey, _, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)

	var accounts []*Account

	account, err := NewAccountFromPublicKeyBytes(publicKey)
	require.NoError(t, err)
	accounts = append(accounts, account)

	account, err = NewAccountFromPublicKeyString(base58.Encode(publicKey))
	require.NoError(t, err)
	accounts = append(accounts, account)

	account, err = NewAccountFromProto(&commonpb.SolanaAccountId{
		Value: publicKey,
	})
	require.NoError(t, err)
	accounts = append(accounts, account)

	for _, account := range accounts {
		assert.EqualValues(t, publicKey, account.PublicKey().ToBytes())
		assert.Nil(t, account.PrivateKey())

		protoValue := account.ToProto()
		require.NoError(t, protoValue.Validate())
		assert.EqualValues(t, publicKey, protoValue.Value)

		_, err = account.Sign([]byte("message"))
		assert.Error(t, err)
	}
}

func TestAccountWithPrivateKey(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)

	var accounts []*Account

	account, err := NewAccountFromPrivateKeyBytes(privateKey)
	require.NoError(t, err)
	accounts = append(accounts, account)

	account, err = NewAccountFromPrivateKeyString(base58.Encode(privateKey))
	require.NoError(t, err)
	accounts = append(accounts, account)

	for _, account := range accounts {
		assert.EqualValues(t, publicKey, account.PublicKey().ToBytes())
		assert.EqualValues(t, privateKey, account.PrivateKey().ToBytes())

		protoValue := account.ToProto()
		require.NoError(t, protoValue.Validate())
		assert.EqualValues(t, publicKey, protoValue.Value)

		message := []byte("message")
		signature, err := account.Sign(message)
		require.NoError(t, err)
		assert.Equal(t, ed25519.Sign(privateKey, message), signature)
	}
}

func TestNewAccountFromEntropy(t *testing.T) {
	// All-zeros BIP-39 entropy maps to the canonical "abandon ... about"
	// mnemonic, whose Solana account at m/44'/501'/0'/0' (empty passphrase) is a
	// well-known address shared with standard wallets (Phantom, Solana CLI).
	entropy := make([]byte, 16)

	account, err := NewAccountFromEntropy(entropy)
	require.NoError(t, err)
	require.NotNil(t, account.PrivateKey())
	assert.Equal(t, "HAgk14JpMQLgt6rVgv7cBQFJWFto5Dqxi472uT3DKpqk", account.PublicKey().ToBase58())

	// The same entropy is deterministic; different entropy is unrelated.
	again, err := NewAccountFromEntropy(entropy)
	require.NoError(t, err)
	assert.Equal(t, account.PublicKey().ToBase58(), again.PublicKey().ToBase58())
	assert.Equal(t, account.PrivateKey().ToBase58(), again.PrivateKey().ToBase58())

	otherEntropy := make([]byte, 16)
	otherEntropy[15] = 1
	other, err := NewAccountFromEntropy(otherEntropy)
	require.NoError(t, err)
	assert.NotEqual(t, account.PublicKey().ToBase58(), other.PublicKey().ToBase58())

	// Only 16-byte entropy is accepted.
	for _, badLen := range []int{0, 15, 17, 32} {
		_, err := NewAccountFromEntropy(make([]byte, badLen))
		assert.Error(t, err)
	}
}

// TestNewAccountFromEntropy_SpecVectors anchors each stage of the derivation
// pipeline to its canonical specification test vector, guarding against a
// dependency behaving non-standardly.
func TestNewAccountFromEntropy_SpecVectors(t *testing.T) {
	// BIP-39: zero entropy -> mnemonic -> seed.
	mnemonic, err := bip39.NewMnemonic(make([]byte, 16))
	require.NoError(t, err)
	assert.Equal(t, "abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon about", mnemonic)
	assert.Equal(t,
		"5eb00bbddcf069084889a8ab9155568165f5c453ccb85e70811aaed6f6da5fc19a5ac40b389cd370d086206dec8aa6c43daea6690f20ad3d8d48b2d2ce9e38e4",
		hex.EncodeToString(bip39.NewSeed(mnemonic, "")),
	)

	// SLIP-0010 ed25519 test vector 1 (seed 000102...0f), deepest path.
	seed, err := hex.DecodeString("000102030405060708090a0b0c0d0e0f")
	require.NoError(t, err)
	node, err := slip10.DeriveForPath("m/0'/1'/2'/2'/1000000000'", seed)
	require.NoError(t, err)
	_, privateKey := node.Keypair()
	assert.Equal(t, "8f94d394a8e8fd6b1bc2f3f49f5c47e385281d5c17e65324b0f62483e37e8793", hex.EncodeToString(privateKey.Seed()))
}

func TestInvalidAccount(t *testing.T) {
	stringValue := "invalid-account"
	bytesValue := []byte(stringValue)
	protoValue := &commonpb.SolanaAccountId{
		Value: bytesValue,
	}

	_, err := NewAccountFromPublicKeyBytes(bytesValue)
	assert.Error(t, err)

	_, err = NewAccountFromPublicKeyString(stringValue)
	assert.Error(t, err)

	_, err = NewAccountFromPrivateKeyBytes(bytesValue)
	assert.Error(t, err)

	_, err = NewAccountFromPrivateKeyString(stringValue)
	assert.Error(t, err)

	_, err = NewAccountFromProto(protoValue)
	assert.Error(t, err)
}

func TestConvertToTimelockVault(t *testing.T) {
	vmConfig := newRandomVmConfig(t, true)

	ownerAccount := newRandomTestAccount(t)

	stateAddress, _, err := vm.GetVirtualTimelockAccountAddress(&vm.GetVirtualTimelockAccountAddressArgs{
		Mint:         vmConfig.Mint.PublicKey().ToBytes(),
		VmAuthority:  vmConfig.Authority.PublicKey().ToBytes(),
		Owner:        ownerAccount.PublicKey().ToBytes(),
		LockDuration: timelock_token_v1.DefaultNumDaysLocked,
	})
	require.NoError(t, err)

	expectedVaultAddress, _, err := vm.GetVirtualTimelockVaultAddress(&vm.GetVirtualTimelockVaultAddressArgs{
		VirtualTimelock: stateAddress,
	})
	require.NoError(t, err)

	tokenAccount, err := ownerAccount.ToTimelockVault(vmConfig)
	require.NoError(t, err)
	assert.EqualValues(t, expectedVaultAddress, tokenAccount.PublicKey().ToBytes())
}

func TestGetTimelockAccounts(t *testing.T) {
	vmConfig := newRandomVmConfig(t, true)

	ownerAccount := newRandomTestAccount(t)

	expectedStateAddress, expectedStateBump, err := vm.GetVirtualTimelockAccountAddress(&vm.GetVirtualTimelockAccountAddressArgs{
		Mint:         vmConfig.Mint.PublicKey().ToBytes(),
		VmAuthority:  vmConfig.Authority.PublicKey().ToBytes(),
		Owner:        ownerAccount.PublicKey().ToBytes(),
		LockDuration: timelock_token_v1.DefaultNumDaysLocked,
	})
	require.NoError(t, err)

	expectedVaultAddress, expectedVaultBump, err := vm.GetVirtualTimelockVaultAddress(&vm.GetVirtualTimelockVaultAddressArgs{
		VirtualTimelock: expectedStateAddress,
	})
	require.NoError(t, err)

	expectedUnlockAddress, expectedUnlockBump, err := vm.GetVmUnlockStateAccountAddress(&vm.GetVmUnlockStateAccountAddressArgs{
		VirtualAccountOwner: ownerAccount.PublicKey().ToBytes(),
		VirtualAccount:      expectedStateAddress,
		Vm:                  vmConfig.Vm.PublicKey().ToBytes(),
	})
	require.NoError(t, err)

	actual, err := ownerAccount.GetTimelockAccounts(vmConfig)
	require.NoError(t, err)
	assert.EqualValues(t, ownerAccount.PublicKey().ToBytes(), actual.VaultOwner.PublicKey().ToBytes())
	assert.EqualValues(t, expectedStateAddress, actual.State.PublicKey().ToBytes())
	assert.Equal(t, expectedStateBump, actual.StateBump)
	assert.EqualValues(t, expectedVaultAddress, actual.Vault.PublicKey().ToBytes())
	assert.Equal(t, expectedVaultBump, actual.VaultBump)
	assert.EqualValues(t, expectedUnlockAddress, actual.Unlock.PublicKey().ToBytes())
	assert.Equal(t, expectedUnlockBump, actual.UnlockBump)
	assert.EqualValues(t, vmConfig.Vm.PublicKey().ToBytes(), actual.Vm.PublicKey().ToBytes())
	assert.EqualValues(t, vmConfig.Mint.PublicKey().ToBytes(), actual.Mint.PublicKey().ToBytes())
}

func TestGetVmDepositAccounts(t *testing.T) {
	vmConfig := newRandomVmConfig(t, true)

	ownerAccount := newRandomTestAccount(t)

	expectedDepositPdaAddress, expectedDepositPdaBump, err := vm.GetVmDepositAddress(&vm.GetVmDepositAddressArgs{
		Depositor: ownerAccount.PublicKey().ToBytes(),
		Vm:        vmConfig.Vm.PublicKey().ToBytes(),
	})
	require.NoError(t, err)

	expectedDepositAtaAddress, err := token.GetAssociatedAccount(expectedDepositPdaAddress, vmConfig.Mint.PublicKey().ToBytes())
	require.NoError(t, err)

	actual, err := ownerAccount.GetVmDepositAccounts(vmConfig)
	require.NoError(t, err)
	assert.EqualValues(t, ownerAccount.PublicKey().ToBytes(), actual.VaultOwner.PublicKey().ToBytes())
	assert.EqualValues(t, expectedDepositPdaAddress, actual.Pda.PublicKey().ToBytes())
	assert.Equal(t, expectedDepositPdaBump, actual.PdaBump)
	assert.EqualValues(t, expectedDepositAtaAddress, actual.Ata.PublicKey().ToBytes())
	assert.EqualValues(t, vmConfig.Vm.PublicKey().ToBytes(), actual.Vm.PublicKey().ToBytes())
	assert.EqualValues(t, vmConfig.Mint.PublicKey().ToBytes(), actual.Mint.PublicKey().ToBytes())
}

func TestGetVmSwapAccounts(t *testing.T) {
	vmConfig := newRandomVmConfig(t, true)

	ownerAccount := newRandomTestAccount(t)

	expectedSwapPdaAddress, expectedSwapPdaBump, err := vm.GetVmSwapAddress(&vm.GetVmSwapAddressArgs{
		Swapper: ownerAccount.PublicKey().ToBytes(),
		Vm:      vmConfig.Vm.PublicKey().ToBytes(),
	})
	require.NoError(t, err)

	expectedSwapAtaAddress, err := token.GetAssociatedAccount(expectedSwapPdaAddress, vmConfig.Mint.PublicKey().ToBytes())
	require.NoError(t, err)

	actual, err := ownerAccount.GetVmSwapAccounts(vmConfig)
	require.NoError(t, err)
	assert.EqualValues(t, ownerAccount.PublicKey().ToBytes(), actual.VaultOwner.PublicKey().ToBytes())
	assert.EqualValues(t, expectedSwapPdaAddress, actual.Pda.PublicKey().ToBytes())
	assert.Equal(t, expectedSwapPdaBump, actual.PdaBump)
	assert.EqualValues(t, expectedSwapAtaAddress, actual.Ata.PublicKey().ToBytes())
	assert.EqualValues(t, vmConfig.Vm.PublicKey().ToBytes(), actual.Vm.PublicKey().ToBytes())
	assert.EqualValues(t, vmConfig.Mint.PublicKey().ToBytes(), actual.Mint.PublicKey().ToBytes())
}

func TestIsOnCurve(t *testing.T) {
	for _, tc := range []struct {
		expected  bool
		publicKey string
	}{
		{true, "codeHy87wGD5oMRLG75qKqsSi1vWE3oxNyYmXo5F9YR"},   // Example owner
		{false, "Cgx4Ff1mmdqRnUbMQ8SbM23hDEbaNaGuhxUHF4aDN8if"}, // Example ATA
		{false, "ChpRZqD1KKdbVsFkRGu85rqMa85MHKhWHuGwJXwAiRCN"}, // Example Timelock vault
	} {
		account, err := NewAccountFromPublicKeyString(tc.publicKey)
		require.NoError(t, err)
		assert.Equal(t, tc.expected, account.IsOnCurve())
	}
}

func TestIsAccountManagedByCode_TimelockState(t *testing.T) {
	ctx := context.Background()
	data := ocp_data.NewTestDataProvider()

	vmConfig := newRandomVmConfig(t, true)

	ownerAccount := newRandomTestAccount(t)

	timelockAccounts, err := ownerAccount.GetTimelockAccounts(vmConfig)
	require.NoError(t, err)

	// No record of the account anywhere
	result, err := timelockAccounts.Vault.IsManagedByCode(ctx, data)
	require.NoError(t, err)
	assert.False(t, result)

	// The account is a locked timelock account with Code as the time and close authority
	timelockRecord := timelockAccounts.ToDBRecord()
	require.NoError(t, data.SaveTimelock(ctx, timelockRecord))

	result, err = timelockAccounts.Vault.IsManagedByCode(ctx, data)
	require.NoError(t, err)
	assert.True(t, result)

	timelockRecord.VaultState = timelock_token_v1.StateLocked
	timelockRecord.Block += 1
	require.NoError(t, data.SaveTimelock(ctx, timelockRecord))

	result, err = timelockAccounts.Vault.IsManagedByCode(ctx, data)
	require.NoError(t, err)
	assert.True(t, result)

	// The timelock account is waiting for timeout
	timelockRecord.VaultState = timelock_token_v1.StateWaitingForTimeout
	timelockRecord.Block += 1
	require.NoError(t, data.SaveTimelock(ctx, timelockRecord))

	result, err = timelockAccounts.Vault.IsManagedByCode(ctx, data)
	require.NoError(t, err)
	assert.False(t, result)

	// The timelock account is unlocked
	timelockRecord.VaultState = timelock_token_v1.StateUnlocked
	timelockRecord.Block += 1
	require.NoError(t, data.SaveTimelock(ctx, timelockRecord))

	result, err = timelockAccounts.Vault.IsManagedByCode(ctx, data)
	require.NoError(t, err)
	assert.False(t, result)
}

func TestIsAccountManagedByCode_OtherAccounts(t *testing.T) {
	ctx := context.Background()
	data := ocp_data.NewTestDataProvider()

	vmConfig := newRandomVmConfig(t, true)

	ownerAccount := newRandomTestAccount(t)

	timelockAccounts, err := ownerAccount.GetTimelockAccounts(vmConfig)
	require.NoError(t, err)
	require.NoError(t, data.SaveTimelock(ctx, timelockAccounts.ToDBRecord()))

	result, err := timelockAccounts.Vault.IsManagedByCode(ctx, data)
	require.NoError(t, err)
	assert.True(t, result)

	result, err = timelockAccounts.VaultOwner.IsManagedByCode(ctx, data)
	require.NoError(t, err)
	assert.False(t, result)

	result, err = timelockAccounts.State.IsManagedByCode(ctx, data)
	require.NoError(t, err)
	assert.False(t, result)
}

func TestGetInitializeInstruction(t *testing.T) {
	// todo: implement me
}

func TestConvertToAssociatedTokenAccount(t *testing.T) {
	ownerAccount := newRandomTestAccount(t)
	mintAccount := newRandomTestAccount(t)

	expected, err := token.GetAssociatedAccount(ownerAccount.PublicKey().ToBytes(), mintAccount.PublicKey().ToBytes())
	require.NoError(t, err)

	actual, err := ownerAccount.ToAssociatedTokenAccount(mintAccount)
	require.NoError(t, err)

	assert.EqualValues(t, expected, actual.PublicKey().ToBytes())
}
