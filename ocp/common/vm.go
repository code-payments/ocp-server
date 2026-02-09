package common

import (
	"context"

	"github.com/code-payments/ocp-server/ocp/config"
	ocp_data "github.com/code-payments/ocp-server/ocp/data"
)

var (
	CoreMintVmAccount, _        = NewAccountFromPublicKeyString(config.CoreMintVmAccountPublicKey)
	CoreMintVmOmnibusAccount, _ = NewAccountFromPublicKeyString(config.CoreMintVmOmnibusPublicKey)

	// todo: DB store to track VM per mint

	badBoysAuthority, _        = NewAccountFromPublicKeyString(config.BadBoysAuthorityPublicKey)
	badBoysVmAccount, _        = NewAccountFromPublicKeyString(config.BadBoysVmAccountPublicKey)
	badBoysVmOmnibusAccount, _ = NewAccountFromPublicKeyString(config.BadBoysVmOmnibusPublicKey)

	bitsAuthority, _        = NewAccountFromPublicKeyString(config.BitsAuthorityPublicKey)
	bitsVmAccount, _        = NewAccountFromPublicKeyString(config.BitsVmAccountPublicKey)
	bitsVmOmnibusAccount, _ = NewAccountFromPublicKeyString(config.BitsVmOmnibusPublicKey)

	bogeyAuthority, _        = NewAccountFromPublicKeyString(config.BogeyAuthorityPublicKey)
	bogeyVmAccount, _        = NewAccountFromPublicKeyString(config.BogeyVmAccountPublicKey)
	bogeyVmOmnibusAccount, _ = NewAccountFromPublicKeyString(config.BogeyVmOmnibusPublicKey)

	floatAuthority, _        = NewAccountFromPublicKeyString(config.FloatAuthorityPublicKey)
	floatVmAccount, _        = NewAccountFromPublicKeyString(config.FloatVmAccountPublicKey)
	floatVmOmnibusAccount, _ = NewAccountFromPublicKeyString(config.FloatVmOmnibusPublicKey)

	jeffyAuthority, _        = NewAccountFromPublicKeyString(config.JeffyAuthorityPublicKey)
	jeffyVmAccount, _        = NewAccountFromPublicKeyString(config.JeffyVmAccountPublicKey)
	jeffyVmOmnibusAccount, _ = NewAccountFromPublicKeyString(config.JeffyVmOmnibusPublicKey)

	marketCoinAuthority, _        = NewAccountFromPublicKeyString(config.MarketCoinAuthorityPublicKey)
	marketCoinVmAccount, _        = NewAccountFromPublicKeyString(config.MarketCoinVmAccountPublicKey)
	marketCoinVmOmnibusAccount, _ = NewAccountFromPublicKeyString(config.MarketCoinVmOmnibusPublicKey)

	testAuthority, _        = NewAccountFromPublicKeyString(config.TestAuthorityPublicKey)
	testVmAccount, _        = NewAccountFromPublicKeyString(config.TestVmAccountPublicKey)
	testVmOmnibusAccount, _ = NewAccountFromPublicKeyString(config.TestVmOmnibusPublicKey)

	xpAuthority, _        = NewAccountFromPublicKeyString(config.XpAuthorityPublicKey)
	xpVmAccount, _        = NewAccountFromPublicKeyString(config.XpVmAccountPublicKey)
	xpVmOmnibusAccount, _ = NewAccountFromPublicKeyString(config.XpVmOmnibusPublicKey)
)

type VmConfig struct {
	Authority *Account
	Vm        *Account
	Omnibus   *Account
	Mint      *Account
}

func GetVmConfigForMint(ctx context.Context, data ocp_data.Provider, mintAccount *Account) (*VmConfig, error) {
	if !IsCoreMint(mintAccount) && !IsCoreMintUsdStableCoin() {
		return nil, ErrUnsupportedMint
	}

	switch mintAccount.PublicKey().ToBase58() {
	case CoreMintAccount.PublicKey().ToBase58():
		return &VmConfig{
			Authority: GetSubsidizer(),
			Vm:        CoreMintVmAccount,
			Omnibus:   CoreMintVmOmnibusAccount,
			Mint:      CoreMintAccount,
		}, nil
	case badBoysMintAccount.PublicKey().ToBase58():
		if badBoysAuthority.PrivateKey() == nil {
			vaultRecord, err := data.GetKey(ctx, badBoysAuthority.PublicKey().ToBase58())
			if err != nil {
				return nil, err
			}

			badBoysAuthority, err = NewAccountFromPrivateKeyString(vaultRecord.PrivateKey)
			if err != nil {
				return nil, err
			}
		}

		return &VmConfig{
			Authority: badBoysAuthority,
			Vm:        badBoysVmAccount,
			Omnibus:   badBoysVmOmnibusAccount,
			Mint:      mintAccount,
		}, nil
	case bitsMintAccount.PublicKey().ToBase58():
		if bitsAuthority.PrivateKey() == nil {
			vaultRecord, err := data.GetKey(ctx, bitsAuthority.PublicKey().ToBase58())
			if err != nil {
				return nil, err
			}

			bitsAuthority, err = NewAccountFromPrivateKeyString(vaultRecord.PrivateKey)
			if err != nil {
				return nil, err
			}
		}

		return &VmConfig{
			Authority: bitsAuthority,
			Vm:        bitsVmAccount,
			Omnibus:   bitsVmOmnibusAccount,
			Mint:      mintAccount,
		}, nil
	case bogeyMintAccount.PublicKey().ToBase58():
		if bogeyAuthority.PrivateKey() == nil {
			vaultRecord, err := data.GetKey(ctx, bogeyAuthority.PublicKey().ToBase58())
			if err != nil {
				return nil, err
			}

			bogeyAuthority, err = NewAccountFromPrivateKeyString(vaultRecord.PrivateKey)
			if err != nil {
				return nil, err
			}
		}

		return &VmConfig{
			Authority: bogeyAuthority,
			Vm:        bogeyVmAccount,
			Omnibus:   bogeyVmOmnibusAccount,
			Mint:      mintAccount,
		}, nil
	case floatMintAccount.PublicKey().ToBase58():
		if floatAuthority.PrivateKey() == nil {
			vaultRecord, err := data.GetKey(ctx, floatAuthority.PublicKey().ToBase58())
			if err != nil {
				return nil, err
			}

			floatAuthority, err = NewAccountFromPrivateKeyString(vaultRecord.PrivateKey)
			if err != nil {
				return nil, err
			}
		}

		return &VmConfig{
			Authority: floatAuthority,
			Vm:        floatVmAccount,
			Omnibus:   floatVmOmnibusAccount,
			Mint:      mintAccount,
		}, nil
	case jeffyMintAccount.PublicKey().ToBase58():
		if jeffyAuthority.PrivateKey() == nil {
			vaultRecord, err := data.GetKey(ctx, jeffyAuthority.PublicKey().ToBase58())
			if err != nil {
				return nil, err
			}

			jeffyAuthority, err = NewAccountFromPrivateKeyString(vaultRecord.PrivateKey)
			if err != nil {
				return nil, err
			}
		}

		return &VmConfig{
			Authority: jeffyAuthority,
			Vm:        jeffyVmAccount,
			Omnibus:   jeffyVmOmnibusAccount,
			Mint:      mintAccount,
		}, nil
	case marketCoinMintAccount.PublicKey().ToBase58():
		if marketCoinAuthority.PrivateKey() == nil {
			vaultRecord, err := data.GetKey(ctx, marketCoinAuthority.PublicKey().ToBase58())
			if err != nil {
				return nil, err
			}

			marketCoinAuthority, err = NewAccountFromPrivateKeyString(vaultRecord.PrivateKey)
			if err != nil {
				return nil, err
			}
		}

		return &VmConfig{
			Authority: marketCoinAuthority,
			Vm:        marketCoinVmAccount,
			Omnibus:   marketCoinVmOmnibusAccount,
			Mint:      mintAccount,
		}, nil
	case testMintAccount.PublicKey().ToBase58():
		if testAuthority.PrivateKey() == nil {
			vaultRecord, err := data.GetKey(ctx, testAuthority.PublicKey().ToBase58())
			if err != nil {
				return nil, err
			}

			testAuthority, err = NewAccountFromPrivateKeyString(vaultRecord.PrivateKey)
			if err != nil {
				return nil, err
			}
		}

		return &VmConfig{
			Authority: testAuthority,
			Vm:        testVmAccount,
			Omnibus:   testVmOmnibusAccount,
			Mint:      mintAccount,
		}, nil
	case xpMintAccount.PublicKey().ToBase58():
		if xpAuthority.PrivateKey() == nil {
			vaultRecord, err := data.GetKey(ctx, xpAuthority.PublicKey().ToBase58())
			if err != nil {
				return nil, err
			}

			xpAuthority, err = NewAccountFromPrivateKeyString(vaultRecord.PrivateKey)
			if err != nil {
				return nil, err
			}
		}

		return &VmConfig{
			Authority: xpAuthority,
			Vm:        xpVmAccount,
			Omnibus:   xpVmOmnibusAccount,
			Mint:      mintAccount,
		}, nil
	default:
		return nil, ErrUnsupportedMint
	}
}
