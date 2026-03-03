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
	CoreMintName            = "USDF"
	CoreMintSymbol          = "USDF"
	CoreMintDescription     = "Your USD Reserves are held in USDF, a fully backed digital dollar created in partnership with Coinbase. Your USD Reserves can be used to buy currencies on Flipcash, or can be withdrawn to any other crypto wallet that supports the Solana blockchain. USDF can also be sold, allowing you to move your funds into a traditional bank account."

	SubsidizerPublicKey = "cash11ndAmdKFEnG2wrQQ5Zqvr1kN9htxxLyoPLYFUV"

	CoreMintVmAccountPublicKey = "JACkaKsm2Rd6TNJwH4UB7G6tHrWUATJPTgNNnRVsg4ip"
	CoreMintVmOmnibusPublicKey = "D8oUTXRvarxhx9cjYdFJqWAVj2rmzry58bS6JSTiQsv5"

	DefaultCurrencyIconImageUrl = "https://flipcash-currency-assets.s3.us-east-1.amazonaws.com/default/icon.jpg"
)

var (
	CoreMintImageUrl       = fmt.Sprintf("https://flipcash-currency-assets.s3.us-east-1.amazonaws.com/%s/icon.png", CoreMintPublicKeyString)
	CoreMintPublicKeyBytes []byte

	DefaultBillColors = []string{"#AAAAAA", "#2C2C2C"}
)

func init() {
	decoded, err := base58.Decode(CoreMintPublicKeyString)
	if err != nil {
		panic(err)
	}
	CoreMintPublicKeyBytes = decoded
}
