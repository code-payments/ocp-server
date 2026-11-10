package swap

import (
	"context"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/code-payments/ocp-server/ocp/common"
	ocp_data "github.com/code-payments/ocp-server/ocp/data"
	"github.com/code-payments/ocp-server/ocp/data/currency"
	holder_memory "github.com/code-payments/ocp-server/ocp/data/currency/holder/memory"
	reserve_memory "github.com/code-payments/ocp-server/ocp/data/currency/reserve/memory"
	"github.com/code-payments/ocp-server/ocp/data/swap"
	history_util "github.com/code-payments/ocp-server/ocp/history"
	"github.com/code-payments/ocp-server/pointer"
	"github.com/code-payments/ocp-server/solana"
	"github.com/code-payments/ocp-server/solana/currencycreator"
	"github.com/code-payments/ocp-server/solana/memo"
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

func TestGetCurrencyLaunchTerms(t *testing.T) {
	ctx := context.Background()
	data := ocp_data.NewTestDataProvider()
	p := &runtime{conf: WithEnvConfigs()(), data: data}

	owner := testutil.NewRandomAccount(t)
	launchingMint := testutil.SetupLaunchpadCurrencyInState(t, data, reserve_memory.New(), holder_memory.New(), currency.MetadataStateExecutingInitialPurchase, owner)
	availableMint := testutil.SetupLaunchpadCurrency(t, data, reserve_memory.New(), holder_memory.New())

	payer := testutil.NewRandomAccount(t).PublicKey().ToBytes()
	ixn := memo.Instruction("launch")
	v0Txn := solana.NewV0Transaction(payer, nil, []solana.Instruction{ixn})
	v1Txn, err := solana.NewV1Transaction(payer, solana.TransactionConfig{
		ComputeUnitLimit:            pointer.Uint32(1_000),
		LoadedAccountsDataSizeLimit: pointer.Uint32(1_000),
	}, ixn)
	require.NoError(t, err)
	marshal := func(txn solana.Transaction) []byte {
		blob, err := txn.Marshal()
		require.NoError(t, err)
		return blob
	}

	swapRecord := func(toMint *common.Account, blob []byte) *swap.Record {
		return &swap.Record{
			Kind:            swap.KindReserve,
			Owner:           owner.PublicKey().ToBase58(),
			FromMint:        common.CoreMintAccount.PublicKey().ToBase58(),
			ToMint:          toMint.PublicKey().ToBase58(),
			TransactionBlob: blob,
		}
	}

	for _, tc := range []struct {
		name        string
		swapRecord  *swap.Record
		expected    *history_util.CurrencyLaunchTerms
		expectedErr bool
	}{
		{
			name:       "v0 launch",
			swapRecord: swapRecord(launchingMint, marshal(v0Txn)),
			expected: &history_util.CurrencyLaunchTerms{
				PurchaseQuarks: 10 * common.CoreMintQuarksPerUnit,
				FeeQuarks:      10 * common.CoreMintQuarksPerUnit,
			},
		},
		{
			name:       "v1 launch",
			swapRecord: swapRecord(launchingMint, marshal(v1Txn)),
			expected: &history_util.CurrencyLaunchTerms{
				PurchaseQuarks: 20 * common.CoreMintQuarksPerUnit,
				FeeQuarks:      0,
			},
		},
		{
			name:        "launch without a transaction",
			swapRecord:  swapRecord(launchingMint, nil),
			expectedErr: true,
		},
		{
			name:       "buy of an available currency",
			swapRecord: swapRecord(availableMint, marshal(v1Txn)),
		},
		{
			name:       "sell into the core mint",
			swapRecord: swapRecord(common.CoreMintAccount, marshal(v1Txn)),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			actual, err := p.getCurrencyLaunchTerms(ctx, tc.swapRecord)
			if tc.expectedErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.expected, actual)
		})
	}
}
