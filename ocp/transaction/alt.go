package transaction

import (
	"context"
	"crypto/ed25519"

	commonpb "github.com/code-payments/ocp-protobuf-api/generated/go/common/v1"

	"github.com/code-payments/ocp-server/ocp/common"
	ocp_data "github.com/code-payments/ocp-server/ocp/data"
	"github.com/code-payments/ocp-server/solana"
	"github.com/code-payments/ocp-server/solana/system"
)

// GetAltForMint gets an address lookup table to operate in a versioned
// transaction for the provided mint
func GetAltForMint(ctx context.Context, data ocp_data.Provider, mint *common.Account) (solana.AddressLookupTable, error) {
	metadataRecord, err := data.GetCurrencyMetadata(ctx, mint.PublicKey().ToBase58())
	if err != nil {
		return solana.AddressLookupTable{}, err
	}

	account, err := common.NewAccountFromPublicKeyString(metadataRecord.Alt)
	if err != nil {
		return solana.AddressLookupTable{}, err
	}

	vmConfig, err := common.GetVmConfigForMint(ctx, data, mint)
	if err != nil {
		return solana.AddressLookupTable{}, err
	}

	currencyAccounts, err := common.GetLaunchpadCurrencyAccounts(metadataRecord)
	if err != nil {
		return solana.AddressLookupTable{}, err
	}

	return solana.AddressLookupTable{
		PublicKey: account.PublicKey().ToBytes(),
		Addresses: []ed25519.PublicKey{
			vmConfig.Vm.PublicKey().ToBytes(),
			vmConfig.Omnibus.PublicKey().ToBytes(),
			mint.PublicKey().ToBytes(),
			currencyAccounts.LiquidityPool.PublicKey().ToBytes(),
			currencyAccounts.VaultBase.PublicKey().ToBytes(),
			currencyAccounts.VaultMint.PublicKey().ToBytes(),
			common.CoreMintAccount.PublicKey().ToBytes(),
			system.RentSysVar,
			system.RecentBlockhashesSysVar,
		},
	}, nil
}

func ToProtoAlt(alt solana.AddressLookupTable) *commonpb.SolanaAddressLookupTable {
	proto := &commonpb.SolanaAddressLookupTable{
		Address: &commonpb.SolanaAccountId{Value: alt.PublicKey},
		Entries: make([]*commonpb.SolanaAccountId, len(alt.Addresses)),
	}

	for i, address := range alt.Addresses {
		proto.Entries[i] = &commonpb.SolanaAccountId{Value: address}
	}

	return proto
}
