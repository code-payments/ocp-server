package config

import (
	"fmt"

	"github.com/mr-tron/base58"

	"github.com/code-payments/ocp-server/usdf"
)

// todo: make these environment configs

const (
	CoreMintPublicKeyString = usdf.Mint
	CoreMintQuarksPerUnit   = uint64(usdf.QuarksPerUsdf)
	CoreMintDecimals        = usdf.Decimals
	CoreMintName            = "USD on Flipcash"
	CoreMintSymbol          = "USDF"
	CoreMintDescription     = "USDF is a fully backed digital dollar created in partnership with Coinbase. USDF can be used to buy currencies on Flipcash, or can be withdrawn to any other crypto wallet that supports the Solana blockchain. USDF can also be sold, allowing you to move your funds into a traditional bank account."

	SubsidizerPublicKey = "cash11ndAmdKFEnG2wrQQ5Zqvr1kN9htxxLyoPLYFUV"

	CoreMintVmAccountPublicKey = "JACkaKsm2Rd6TNJwH4UB7G6tHrWUATJPTgNNnRVsg4ip"
	CoreMintVmOmnibusPublicKey = "D8oUTXRvarxhx9cjYdFJqWAVj2rmzry58bS6JSTiQsv5"

	CoreMintFeesPublicKey = "HkL1my3dtsn6FVbcv7rHA4htg6zdVyn4fna3e941WomZ"

	CoreMintAltPublicKeyString = "4oLVyayQJCoPcrkKapE5Ry6pP6vTTTneLP5UPUSQZsvT"

	CurrencyAssetsBaseUrl      = "https://currency-assets.flipcash-infra.net"
	CurrencyAssetsS3BucketName = "flipcash-currency-assets"
)

var (
	CoreMintImageUrl       = fmt.Sprintf("%s/%s/icon.png", CurrencyAssetsBaseUrl, CoreMintPublicKeyString)
	CoreMintPublicKeyBytes []byte

	DefaultCurrencyIconImageUrl = fmt.Sprintf("%s/default/icon.jpg", CurrencyAssetsBaseUrl)
	DefaultBillColors           = []string{"#AAAAAA", "#2C2C2C"}
)

func init() {
	decoded, err := base58.Decode(CoreMintPublicKeyString)
	if err != nil {
		panic(err)
	}
	CoreMintPublicKeyBytes = decoded
}
