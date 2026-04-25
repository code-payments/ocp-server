package coinbase_stable_swapper

// Account discriminators from the on-chain IDL
var (
	// LiquidityPool discriminator: [66, 38, 17, 64, 188, 80, 68, 129]
	LiquidityPoolAccountDiscriminator = []byte{66, 38, 17, 64, 188, 80, 68, 129}

	// AddressWhitelist discriminator: [10, 102, 46, 176, 154, 249, 160, 48]
	AddressWhitelistAccountDiscriminator = []byte{10, 102, 46, 176, 154, 249, 160, 48}

	// TokenVault discriminator: [121, 7, 84, 254, 151, 228, 43, 144]
	TokenVaultAccountDiscriminator = []byte{121, 7, 84, 254, 151, 228, 43, 144}
)
