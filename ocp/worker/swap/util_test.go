package swap

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/code-payments/ocp-server/solana"
	"github.com/code-payments/ocp-server/solana/currencycreator"
	"github.com/code-payments/ocp-server/testutil"
)

func TestGetInitialPurchaseQuarksBought(t *testing.T) {
	vaultMint := testutil.NewRandomAccount(t)
	otherAccount := testutil.NewRandomAccount(t)

	// Measured from a mainnet launch purchase, which created the vault
	const quarksBought = 9_996_054_730_448
	const postPurchaseQuarks = currencycreator.DefaultMintMaxQuarkSupply - quarksBought

	balance := func(accountIndex uint64, quarks uint64) solana.TokenBalance {
		return solana.TokenBalance{
			AccountIndex: accountIndex,
			TokenAmount:  solana.TokenAmount{Amount: strconv.FormatUint(quarks, 10)},
		}
	}
	tokenBalances := func(pre, post []solana.TokenBalance) *solana.TransactionTokenBalances {
		return &solana.TransactionTokenBalances{
			Accounts: []string{
				otherAccount.PublicKey().ToBase58(),
				vaultMint.PublicKey().ToBase58(),
			},
			PreTokenBalances:  pre,
			PostTokenBalances: post,
		}
	}

	for _, tc := range []struct {
		name          string
		tokenBalances *solana.TransactionTokenBalances
		expected      uint64
		expectedErr   string
	}{
		{
			// v1 launches and v0 launches funded with the core mint
			name: "vault created by the purchase",
			tokenBalances: tokenBalances(
				[]solana.TokenBalance{balance(0, 20_000_000)},
				[]solana.TokenBalance{balance(0, 0), balance(1, postPurchaseQuarks)},
			),
			expected: quarksBought,
		},
		{
			// v0 launches funded with another currency
			name: "vault created ahead of the purchase",
			tokenBalances: tokenBalances(
				[]solana.TokenBalance{balance(0, 20_000_000), balance(1, currencycreator.DefaultMintMaxQuarkSupply)},
				[]solana.TokenBalance{balance(0, 0), balance(1, postPurchaseQuarks)},
			),
			expected: quarksBought,
		},
		{
			name: "vault missing after the transaction",
			tokenBalances: tokenBalances(
				nil,
				[]solana.TokenBalance{balance(0, postPurchaseQuarks)},
			),
			expectedErr: "pool vault balance not found after the transaction",
		},
		{
			name: "vault created by the transaction without a purchase",
			tokenBalances: tokenBalances(
				nil,
				[]solana.TokenBalance{balance(1, currencycreator.DefaultMintMaxQuarkSupply)},
			),
			expectedErr: "pool vault balance didn't decrease",
		},
		{
			name: "vault balance grew",
			tokenBalances: tokenBalances(
				[]solana.TokenBalance{balance(1, postPurchaseQuarks)},
				[]solana.TokenBalance{balance(1, postPurchaseQuarks+1)},
			),
			expectedErr: "pool vault balance didn't decrease",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			actual, err := getInitialPurchaseQuarksBought(vaultMint, tc.tokenBalances)
			if tc.expectedErr != "" {
				require.EqualError(t, err, tc.expectedErr)
				return
			}
			require.NoError(t, err)
			assert.EqualValues(t, tc.expected, actual)
		})
	}
}
