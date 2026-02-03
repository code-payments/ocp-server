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

	// todo: DB store to track VM per mint

	BitsMintPublicKey      = "A3e8dzb1y4gqGP2cnCS3UU8dm5YNrFpZBpjjdoZdtfnB"
	BitsAuthorityPublicKey = "bit8rCyAcstm1ZiwLq22FHdz8wAigKU46hmtrigrGub"
	BitsVmAccountPublicKey = "5zDzL3CHb3wFxs7xnkxmWMGMR1gjNtYgV46PTBgSHmsJ"
	BitsVmOmnibusPublicKey = "872F9FFXnSMPahkYjB8uF1pz1CrgMrmbfKQMGNCdVBvc"

	BogeyMintPublicKey      = "3AhBb1fpDTp1F9hPkZjRPDejXBM9S5vfpVdvn66vLYnT"
	BogeyAuthorityPublicKey = "bgy6SZdBCjVbftagjp4Vv5rvajTHmmh4ccsZbD9F3m3"
	BogeyVmAccountPublicKey = "2KWN2ame8kZKfFc6iyNAWNZsTvByfo3Mrzue6NasNC5B"
	BogeyVmOmnibusPublicKey = "H52LuBfwtBNZ3qrX8gqteF2mLx3EscdeUiryUC4fFFfR"

	FloatMintPublicKey      = "5APqK9YUZupKt7rRUrpYy6WV3RPuxA71ZtKJffDUMdPP"
	FloatAuthorityPublicKey = "f1t57oRwzBvtcqfAkjGKsEAbXymDSbL7pp3iRmDw9iz"
	FloatVmAccountPublicKey = "5PKurz63VfSozHbkzwdudpDeA7mUuif8XTfNP932jRYy"
	FloatVmOmnibusPublicKey = "98W5qFv7jiFvNA5c72kj7ku2hhFgaAEUfBXcLw8SK1yY"

	JeffyMintPublicKey      = "54ggcQ23uen5b9QXMAns99MQNTKn7iyzq4wvCW6e8r25"
	JeffyAuthorityPublicKey = "jfy1btcfsjSn2WCqLVaxiEjp4zgmemGyRsdCPbPwnZV"
	JeffyVmAccountPublicKey = "8rwgUXsLSq1Pn51UJs4NGVA1cTkemwBgKyWPfFYgnm3B"
	JeffyVmOmnibusPublicKey = "9XiqBPYSG2cBwpb8MqJeuFmLaQaAAr6gwikyBrPZDQ8R"

	MarketCoinMintPublicKey      = "311m6Sb1814PfAxkEcqq6MNdBiVZLr8VWuAWDSC72euW"
	MarketCoinAuthorityPublicKey = "mrkthxqn4rKCZw1pDxhJTpCrvUBtK9m6aLvCeY3PyWR"
	MarketCoinVmAccountPublicKey = "AeJ6x6mtwdjUM2AqppQ7zG6m89sx5c8qegfFPzjmD2x6"
	MarketCoinVmOmnibusPublicKey = "CQMdG8AKtLP9JCU4GAzKunKnsfpXVFdMcSwQd3Hd5oZg"

	XpMintPublicKey      = "6oZnhB1FPrUaDfhRCVZnbVWNKVx9wgj84vKGH7eMpzXL"
	XpAuthorityPublicKey = "xpTXV7BNXwsdvCaFKfeT4h6rSnKck2Bv5iBAFFS5Uwk"
	XpVmAccountPublicKey = "4qnCaQkGxCnr66cmPfpNfM2rxaaTMFSSfbY9gXZgCYdS"
	XpVmOmnibusPublicKey = "5FCroyNvCXpLFkNuNQ4fw8Lk8xNGwGpwoTjzhRMbaBUo"
)

var (
	CoreMintImageUrl       = fmt.Sprintf("https://flipcash-currency-assets.s3.us-east-1.amazonaws.com/%s/icon.png", CoreMintPublicKeyString)
	CoreMintPublicKeyBytes []byte
)

func init() {
	decoded, err := base58.Decode(CoreMintPublicKeyString)
	if err != nil {
		panic(err)
	}
	CoreMintPublicKeyBytes = decoded
}
