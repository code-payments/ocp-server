package config

import (
	"github.com/mr-tron/base58"

	"github.com/code-payments/ocp-server/pkg/usdf"
)

// todo: more things can be pulled into here to configure the open code protocol
// todo: make these environment configs

const (
	CoreMintPublicKeyString = usdf.Mint
	CoreMintQuarksPerUnit   = uint64(usdf.QuarksPerUsdf)
	CoreMintDecimals        = usdf.Decimals
	CoreMintName            = "USDF"
	CoreMintSymbol          = "USDF"
	CoreMintDescription     = "Your cash reserves are held in USDF, a fully backed digital dollar supported 1:1 by U.S. dollars. This ensures your funds retain the same value and stability as traditional USD, while benefiting from faster, more transparent transactions on modern financial infrastructure. You can deposit additional funds at any time, or withdraw your USDF for U.S. dollars whenever you like."
	CoreMintImageUrl        = "https://flipcash-currency-assets.s3.us-east-1.amazonaws.com/todo/icon.png"

	SubsidizerPublicKey = "cash11ndAmdKFEnG2wrQQ5Zqvr1kN9htxxLyoPLYFUV"

	// todo: replace with real VM
	VmAccountPublicKey = "BVMGLfRgr3nVFCH5DuW6VR2kfSDxq4EFEopXfwCDpYzb"
	VmOmnibusPublicKey = "GNw1t85VH8b1CcwB5933KBC7PboDPJ5EcQdGynbfN1Pb"

	// todo: replace with new Jeffy
	// todo: DB store to track VM per mint
	JeffyMintPublicKey      = "52MNGpgvydSwCtC2H4qeiZXZ1TxEuRVCRGa8LAfk2kSj"
	JeffyAuthorityPublicKey = "jfy1btcfsjSn2WCqLVaxiEjp4zgmemGyRsdCPbPwnZV"
	JeffyVmAccountPublicKey = "Bii3UFB9DzPq6UxgewF5iv9h1Gi8ZnP6mr7PtocHGNta"
	JeffyVmOmnibusPublicKey = "CQ5jni8XTXEcMFXS1ytNyTVbJBZHtHCzEtjBPowB3MLD"
)

var (
	CoreMintPublicKeyBytes []byte
)

func init() {
	decoded, err := base58.Decode(CoreMintPublicKeyString)
	if err != nil {
		panic(err)
	}
	CoreMintPublicKeyBytes = decoded
}
