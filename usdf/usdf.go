package usdf

import (
	"crypto/ed25519"
)

// todo: Update config once USDF is created

const (
	Mint          = "USDFmFa553nkGNSvFn2gXCnpPuWLkgre2aHmPMDyaWi"
	QuarksPerUsdf = 1000000
	Decimals      = 6
)

var (
	TokenMint = ed25519.PublicKey{7, 7, 48, 54, 200, 135, 41, 84, 87, 240, 35, 129, 5, 62, 49, 49, 241, 253, 42, 157, 138, 222, 175, 252, 65, 146, 180, 12, 103, 218, 59, 22}
)
