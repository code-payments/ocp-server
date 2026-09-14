package balance

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	balancepb "github.com/code-payments/ocp-protobuf-api/generated/go/balance/v1"
	commonpb "github.com/code-payments/ocp-protobuf-api/generated/go/common/v1"

	balance_util "github.com/code-payments/ocp-server/ocp/balance"
	"github.com/code-payments/ocp-server/ocp/common"
	currency_util "github.com/code-payments/ocp-server/ocp/currency"
	ocp_data "github.com/code-payments/ocp-server/ocp/data"
	"github.com/code-payments/ocp-server/ocp/data/account"
	"github.com/code-payments/ocp-server/ocp/data/balance"
	exchange_memory "github.com/code-payments/ocp-server/ocp/data/currency/exchange/memory"
	"github.com/code-payments/ocp-server/ocp/data/currency/holder"
	holder_memory "github.com/code-payments/ocp-server/ocp/data/currency/holder/memory"
	"github.com/code-payments/ocp-server/ocp/data/currency/reserve"
	reserve_memory "github.com/code-payments/ocp-server/ocp/data/currency/reserve/memory"
	"github.com/code-payments/ocp-server/solana/currencycreator"
	timelock_token_v1 "github.com/code-payments/ocp-server/solana/timelock/v1"
	"github.com/code-payments/ocp-server/testutil"
)

type testEnv struct {
	ctx          context.Context
	client       balancepb.BalanceClient
	data         ocp_data.Provider
	reserveStore reserve.Store
	holderStore  holder.Store
}

func setup(t *testing.T) (env testEnv, cleanup func()) {
	return setupWithDustValue(t, 0)
}

func setupWithDustValue(t *testing.T, dustValue uint64) (env testEnv, cleanup func()) {
	log := zaptest.NewLogger(t)

	conn, serv, err := testutil.NewServer(log)
	require.NoError(t, err)

	env.ctx = context.Background()
	env.client = balancepb.NewBalanceClient(conn)
	env.data = ocp_data.NewTestDataProvider()
	env.reserveStore = reserve_memory.New()
	env.holderStore = holder_memory.New()
	testutil.SetupRandomSubsidizer(t, env.data)

	exchangeRateStore := exchange_memory.New()
	mintDataProvider := currency_util.NewMintDataProvider(log, env.data, exchangeRateStore, env.reserveStore, env.holderStore, 0, time.Second, time.Second)
	s := NewBalanceServer(log, env.data, mintDataProvider, dustValue)

	serv.RegisterService(func(server *grpc.Server) {
		balancepb.RegisterBalanceServer(server, s)
	})

	require.NoError(t, mintDataProvider.Start(env.ctx))

	serverCleanup, err := serv.Serve()
	require.NoError(t, err)

	cleanup = func() {
		mintDataProvider.Stop()
		serverCleanup()
	}
	return env, cleanup
}

func TestGetBalances_HappyPath(t *testing.T) {
	env, cleanup := setup(t)
	defer cleanup()

	coreVmConfig := testutil.NewRandomVmConfig(t, true)
	launchpadMint := testutil.SetupLaunchpadCurrency(t, env.data, env.reserveStore, env.holderStore)
	launchpadVmConfig, err := common.GetVmConfigForMint(env.ctx, env.data, launchpadMint)
	require.NoError(t, err)

	ownerAccount1 := testutil.NewRandomAccount(t)
	ownerAccount2 := testutil.NewRandomAccount(t)
	unknownOwnerAccount := testutil.NewRandomAccount(t)
	giftCardOwnerAccount := testutil.NewRandomAccount(t)

	// Duplicate owners collapse to a single entry in the response
	req := &balancepb.GetBalancesRequest{
		Owners: []*commonpb.SolanaAccountId{
			ownerAccount1.ToProto(),
			ownerAccount2.ToProto(),
			unknownOwnerAccount.ToProto(),
			giftCardOwnerAccount.ToProto(),
			ownerAccount1.ToProto(),
		},
	}

	// No owner has any account records yet
	resp, err := env.client.GetBalances(env.ctx, req)
	require.NoError(t, err)
	assert.Equal(t, balancepb.GetBalancesResponse_OK, resp.Result)
	require.Len(t, resp.BalancesByOwner, 4)
	for _, owner := range []*common.Account{ownerAccount1, ownerAccount2, unknownOwnerAccount, giftCardOwnerAccount} {
		ownerBalance := assertOwnerBalance(t, resp, owner, 0)
		assert.Empty(t, ownerBalance.BalancesByMint)
	}

	owner1CoreMintAccountRecords := setupAccountRecords(t, env, ownerAccount1, ownerAccount1, coreVmConfig, 0, commonpb.AccountType_PRIMARY)
	owner1LaunchpadMintAccountRecords := setupAccountRecords(t, env, ownerAccount1, ownerAccount1, launchpadVmConfig, 0, commonpb.AccountType_PRIMARY)
	owner2CoreMintAccountRecords := setupAccountRecords(t, env, ownerAccount2, ownerAccount2, coreVmConfig, 0, commonpb.AccountType_PRIMARY)
	giftCardAccountRecords := setupAccountRecords(t, env, giftCardOwnerAccount, giftCardOwnerAccount, coreVmConfig, 0, commonpb.AccountType_REMOTE_SEND_GIFT_CARD)

	// Account records exist, but hold nothing. Mints with a zero balance are
	// omitted from the per-mint breakdown rather than reported as zero.
	resp, err = env.client.GetBalances(env.ctx, req)
	require.NoError(t, err)
	assert.Equal(t, balancepb.GetBalancesResponse_OK, resp.Result)
	require.Len(t, resp.BalancesByOwner, 4)
	for _, owner := range []*common.Account{ownerAccount1, ownerAccount2, unknownOwnerAccount, giftCardOwnerAccount} {
		ownerBalance := assertOwnerBalance(t, resp, owner, 0)
		assert.Empty(t, ownerBalance.BalancesByMint)
	}

	setupCachedBalance(t, env, owner1CoreMintAccountRecords, common.ToCoreMintQuarks(42))
	setupCachedBalance(t, env, owner1LaunchpadMintAccountRecords, currencycreator.ToQuarks(100))
	setupCachedBalance(t, env, owner2CoreMintAccountRecords, common.ToCoreMintQuarks(7))
	setupCachedBalance(t, env, giftCardAccountRecords, common.ToCoreMintQuarks(1))

	// The launchpad currency's value is what the entire position would currently
	// sell for on the bonding curve.
	expectedLaunchpadMintValue := estimateLaunchpadSellValue(t, currencycreator.ToQuarks(100))

	resp, err = env.client.GetBalances(env.ctx, req)
	require.NoError(t, err)
	assert.Equal(t, balancepb.GetBalancesResponse_OK, resp.Result)
	require.Len(t, resp.BalancesByOwner, 4)

	ownerBalance1 := assertOwnerBalance(t, resp, ownerAccount1, common.ToCoreMintQuarks(42)+expectedLaunchpadMintValue)
	require.Len(t, ownerBalance1.BalancesByMint, 2)
	assertMintBalance(t, ownerBalance1.BalancesByMint, common.CoreMintAccount, common.ToCoreMintQuarks(42))
	assertMintBalance(t, ownerBalance1.BalancesByMint, launchpadMint, expectedLaunchpadMintValue)

	ownerBalance2 := assertOwnerBalance(t, resp, ownerAccount2, common.ToCoreMintQuarks(7))
	require.Len(t, ownerBalance2.BalancesByMint, 1)
	assertMintBalance(t, ownerBalance2.BalancesByMint, common.CoreMintAccount, common.ToCoreMintQuarks(7))

	unknownOwnerBalance := assertOwnerBalance(t, resp, unknownOwnerAccount, 0)
	assert.Empty(t, unknownOwnerBalance.BalancesByMint)

	giftCardOwnerBalance := assertOwnerBalance(t, resp, giftCardOwnerAccount, common.ToCoreMintQuarks(1))
	require.Len(t, giftCardOwnerBalance.BalancesByMint, 1)
	assertMintBalance(t, giftCardOwnerBalance.BalancesByMint, common.CoreMintAccount, common.ToCoreMintQuarks(1))
}

func TestGetBalances_MintFilter(t *testing.T) {
	env, cleanup := setup(t)
	defer cleanup()

	coreVmConfig := testutil.NewRandomVmConfig(t, true)
	launchpadMint := testutil.SetupLaunchpadCurrency(t, env.data, env.reserveStore, env.holderStore)
	launchpadVmConfig, err := common.GetVmConfigForMint(env.ctx, env.data, launchpadMint)
	require.NoError(t, err)

	ownerAccount1 := testutil.NewRandomAccount(t)
	ownerAccount2 := testutil.NewRandomAccount(t)

	owner1CoreMintAccountRecords := setupAccountRecords(t, env, ownerAccount1, ownerAccount1, coreVmConfig, 0, commonpb.AccountType_PRIMARY)
	owner1LaunchpadMintAccountRecords := setupAccountRecords(t, env, ownerAccount1, ownerAccount1, launchpadVmConfig, 0, commonpb.AccountType_PRIMARY)
	owner2CoreMintAccountRecords := setupAccountRecords(t, env, ownerAccount2, ownerAccount2, coreVmConfig, 0, commonpb.AccountType_PRIMARY)

	setupCachedBalance(t, env, owner1CoreMintAccountRecords, common.ToCoreMintQuarks(42))
	setupCachedBalance(t, env, owner1LaunchpadMintAccountRecords, currencycreator.ToQuarks(100))
	setupCachedBalance(t, env, owner2CoreMintAccountRecords, common.ToCoreMintQuarks(7))

	expectedLaunchpadMintValue := estimateLaunchpadSellValue(t, currencycreator.ToQuarks(100))

	owners := []*commonpb.SolanaAccountId{ownerAccount1.ToProto(), ownerAccount2.ToProto()}

	// Filtering to the core mint excludes the launchpad currency from both the
	// total and the per-mint breakdown
	resp, err := env.client.GetBalances(env.ctx, &balancepb.GetBalancesRequest{
		Owners: owners,
		Mints:  []*commonpb.SolanaAccountId{common.CoreMintAccount.ToProto()},
	})
	require.NoError(t, err)
	assert.Equal(t, balancepb.GetBalancesResponse_OK, resp.Result)
	require.Len(t, resp.BalancesByOwner, 2)

	ownerBalance1 := assertOwnerBalance(t, resp, ownerAccount1, common.ToCoreMintQuarks(42))
	require.Len(t, ownerBalance1.BalancesByMint, 1)
	assertMintBalance(t, ownerBalance1.BalancesByMint, common.CoreMintAccount, common.ToCoreMintQuarks(42))

	ownerBalance2 := assertOwnerBalance(t, resp, ownerAccount2, common.ToCoreMintQuarks(7))
	require.Len(t, ownerBalance2.BalancesByMint, 1)
	assertMintBalance(t, ownerBalance2.BalancesByMint, common.CoreMintAccount, common.ToCoreMintQuarks(7))

	// The filter applies to every owner. An owner holding nothing in the
	// filtered mints is still present with an empty balance.
	resp, err = env.client.GetBalances(env.ctx, &balancepb.GetBalancesRequest{
		Owners: owners,
		Mints:  []*commonpb.SolanaAccountId{launchpadMint.ToProto()},
	})
	require.NoError(t, err)
	assert.Equal(t, balancepb.GetBalancesResponse_OK, resp.Result)
	require.Len(t, resp.BalancesByOwner, 2)

	ownerBalance1 = assertOwnerBalance(t, resp, ownerAccount1, expectedLaunchpadMintValue)
	require.Len(t, ownerBalance1.BalancesByMint, 1)
	assertMintBalance(t, ownerBalance1.BalancesByMint, launchpadMint, expectedLaunchpadMintValue)

	ownerBalance2 = assertOwnerBalance(t, resp, ownerAccount2, 0)
	assert.Empty(t, ownerBalance2.BalancesByMint)

	// Duplicate mints in the filter don't double count
	resp, err = env.client.GetBalances(env.ctx, &balancepb.GetBalancesRequest{
		Owners: owners,
		Mints:  []*commonpb.SolanaAccountId{common.CoreMintAccount.ToProto(), launchpadMint.ToProto(), common.CoreMintAccount.ToProto()},
	})
	require.NoError(t, err)
	assert.Equal(t, balancepb.GetBalancesResponse_OK, resp.Result)
	require.Len(t, resp.BalancesByOwner, 2)

	ownerBalance1 = assertOwnerBalance(t, resp, ownerAccount1, common.ToCoreMintQuarks(42)+expectedLaunchpadMintValue)
	require.Len(t, ownerBalance1.BalancesByMint, 2)
	assertMintBalance(t, ownerBalance1.BalancesByMint, common.CoreMintAccount, common.ToCoreMintQuarks(42))
	assertMintBalance(t, ownerBalance1.BalancesByMint, launchpadMint, expectedLaunchpadMintValue)

	ownerBalance2 = assertOwnerBalance(t, resp, ownerAccount2, common.ToCoreMintQuarks(7))
	require.Len(t, ownerBalance2.BalancesByMint, 1)
	assertMintBalance(t, ownerBalance2.BalancesByMint, common.CoreMintAccount, common.ToCoreMintQuarks(7))

	// A mint no owner holds results in an OK response with no balances
	resp, err = env.client.GetBalances(env.ctx, &balancepb.GetBalancesRequest{
		Owners: owners,
		Mints:  []*commonpb.SolanaAccountId{testutil.NewRandomAccount(t).ToProto()},
	})
	require.NoError(t, err)
	assert.Equal(t, balancepb.GetBalancesResponse_OK, resp.Result)
	require.Len(t, resp.BalancesByOwner, 2)

	ownerBalance1 = assertOwnerBalance(t, resp, ownerAccount1, 0)
	assert.Empty(t, ownerBalance1.BalancesByMint)

	ownerBalance2 = assertOwnerBalance(t, resp, ownerAccount2, 0)
	assert.Empty(t, ownerBalance2.BalancesByMint)
}

func TestGetBalances_UnmanagedAccountsExcluded(t *testing.T) {
	env, cleanup := setup(t)
	defer cleanup()

	coreVmConfig := testutil.NewRandomVmConfig(t, true)
	launchpadMint := testutil.SetupLaunchpadCurrency(t, env.data, env.reserveStore, env.holderStore)
	launchpadVmConfig, err := common.GetVmConfigForMint(env.ctx, env.data, launchpadMint)
	require.NoError(t, err)

	ownerAccount1 := testutil.NewRandomAccount(t)
	ownerAccount2 := testutil.NewRandomAccount(t)

	owner1CoreMintAccountRecords := setupAccountRecords(t, env, ownerAccount1, ownerAccount1, coreVmConfig, 0, commonpb.AccountType_PRIMARY)
	owner1LaunchpadMintAccountRecords := setupAccountRecords(t, env, ownerAccount1, ownerAccount1, launchpadVmConfig, 0, commonpb.AccountType_PRIMARY)
	owner2CoreMintAccountRecords := setupAccountRecords(t, env, ownerAccount2, ownerAccount2, coreVmConfig, 0, commonpb.AccountType_PRIMARY)

	setupCachedBalance(t, env, owner1CoreMintAccountRecords, common.ToCoreMintQuarks(42))
	setupCachedBalance(t, env, owner1LaunchpadMintAccountRecords, currencycreator.ToQuarks(100))
	setupCachedBalance(t, env, owner2CoreMintAccountRecords, common.ToCoreMintQuarks(7))

	// Owner 1's launchpad mint account and owner 2's core mint account have
	// left the L2 system, so there isn't a cached balance that can be trusted
	// for them.
	//
	// The geyser worker moves both records in the same transaction, so the
	// ledger record's lock state can't disagree with the timelock record's
	for _, accountRecords := range []*common.AccountRecords{owner1LaunchpadMintAccountRecords, owner2CoreMintAccountRecords} {
		accountRecords.Timelock.VaultState = timelock_token_v1.StateUnlocked
		accountRecords.Timelock.Block += 1
		require.NoError(t, env.data.SaveTimelock(env.ctx, accountRecords.Timelock))
		require.NoError(t, env.data.MarkBalanceAsUnlocked(env.ctx, accountRecords.General.TokenAccount))
	}

	resp, err := env.client.GetBalances(env.ctx, &balancepb.GetBalancesRequest{
		Owners: []*commonpb.SolanaAccountId{ownerAccount1.ToProto(), ownerAccount2.ToProto()},
	})
	require.NoError(t, err)
	assert.Equal(t, balancepb.GetBalancesResponse_OK, resp.Result)
	require.Len(t, resp.BalancesByOwner, 2)

	// Only the account still managed by Code contributes to owner 1's balance
	ownerBalance1 := assertOwnerBalance(t, resp, ownerAccount1, common.ToCoreMintQuarks(42))
	require.Len(t, ownerBalance1.BalancesByMint, 1)
	assertMintBalance(t, ownerBalance1.BalancesByMint, common.CoreMintAccount, common.ToCoreMintQuarks(42))

	// An owner whose only account is unmanaged has an empty balance
	ownerBalance2 := assertOwnerBalance(t, resp, ownerAccount2, 0)
	assert.Empty(t, ownerBalance2.BalancesByMint)
}

func TestGetBalances_DustHidden(t *testing.T) {
	dustValue := common.ToCoreMintQuarks(1)

	env, cleanup := setupWithDustValue(t, dustValue)
	defer cleanup()

	coreVmConfig := testutil.NewRandomVmConfig(t, true)
	launchpadMint := testutil.SetupLaunchpadCurrency(t, env.data, env.reserveStore, env.holderStore)
	launchpadVmConfig, err := common.GetVmConfigForMint(env.ctx, env.data, launchpadMint)
	require.NoError(t, err)

	ownerAccount1 := testutil.NewRandomAccount(t)
	ownerAccount2 := testutil.NewRandomAccount(t)
	ownerAccount3 := testutil.NewRandomAccount(t)

	owner1CoreMintAccountRecords := setupAccountRecords(t, env, ownerAccount1, ownerAccount1, coreVmConfig, 0, commonpb.AccountType_PRIMARY)
	owner1LaunchpadMintAccountRecords := setupAccountRecords(t, env, ownerAccount1, ownerAccount1, launchpadVmConfig, 0, commonpb.AccountType_PRIMARY)
	owner2CoreMintAccountRecords := setupAccountRecords(t, env, ownerAccount2, ownerAccount2, coreVmConfig, 0, commonpb.AccountType_PRIMARY)
	owner3CoreMintAccountRecords := setupAccountRecords(t, env, ownerAccount3, ownerAccount3, coreVmConfig, 0, commonpb.AccountType_PRIMARY)

	// Owner 1 holds a core mint balance above the dust threshold and a launchpad
	// position that's worth less than it
	setupCachedBalance(t, env, owner1CoreMintAccountRecords, common.ToCoreMintQuarks(42))
	setupCachedBalance(t, env, owner1LaunchpadMintAccountRecords, currencycreator.ToQuarks(10))

	// Owner 2 holds nothing but dust
	setupCachedBalance(t, env, owner2CoreMintAccountRecords, dustValue-1)

	// Owner 3 holds exactly the dust threshold, which isn't dust
	setupCachedBalance(t, env, owner3CoreMintAccountRecords, dustValue)

	launchpadDustValue := estimateLaunchpadSellValue(t, currencycreator.ToQuarks(10))
	require.Less(t, launchpadDustValue, dustValue)

	resp, err := env.client.GetBalances(env.ctx, &balancepb.GetBalancesRequest{
		Owners: []*commonpb.SolanaAccountId{ownerAccount1.ToProto(), ownerAccount2.ToProto(), ownerAccount3.ToProto()},
	})
	require.NoError(t, err)
	assert.Equal(t, balancepb.GetBalancesResponse_OK, resp.Result)
	require.Len(t, resp.BalancesByOwner, 3)

	// Dust is excluded from both the total and the per-mint breakdown
	ownerBalance1 := assertOwnerBalance(t, resp, ownerAccount1, common.ToCoreMintQuarks(42))
	require.Len(t, ownerBalance1.BalancesByMint, 1)
	assertMintBalance(t, ownerBalance1.BalancesByMint, common.CoreMintAccount, common.ToCoreMintQuarks(42))

	ownerBalance2 := assertOwnerBalance(t, resp, ownerAccount2, 0)
	assert.Empty(t, ownerBalance2.BalancesByMint)

	ownerBalance3 := assertOwnerBalance(t, resp, ownerAccount3, dustValue)
	require.Len(t, ownerBalance3.BalancesByMint, 1)
	assertMintBalance(t, ownerBalance3.BalancesByMint, common.CoreMintAccount, dustValue)
}

func TestGetBalances_UnknownOwnerAccount(t *testing.T) {
	env, cleanup := setup(t)
	defer cleanup()

	unknownOwnerAccount := testutil.NewRandomAccount(t)

	resp, err := env.client.GetBalances(env.ctx, &balancepb.GetBalancesRequest{
		Owners: []*commonpb.SolanaAccountId{unknownOwnerAccount.ToProto()},
	})
	require.NoError(t, err)
	assert.Equal(t, balancepb.GetBalancesResponse_OK, resp.Result)
	require.Len(t, resp.BalancesByOwner, 1)

	unknownOwnerBalance := assertOwnerBalance(t, resp, unknownOwnerAccount, 0)
	assert.Empty(t, unknownOwnerBalance.BalancesByMint)
}

func TestGetBalances_InvalidRequest(t *testing.T) {
	env, cleanup := setup(t)
	defer cleanup()

	ownerAccount := testutil.NewRandomAccount(t)

	// At least one owner is required
	_, err := env.client.GetBalances(env.ctx, &balancepb.GetBalancesRequest{})
	testutil.AssertStatusErrorWithCode(t, err, codes.InvalidArgument)

	tooManyOwners := make([]*commonpb.SolanaAccountId, 0, 1025)
	for range cap(tooManyOwners) {
		tooManyOwners = append(tooManyOwners, testutil.NewRandomAccount(t).ToProto())
	}
	_, err = env.client.GetBalances(env.ctx, &balancepb.GetBalancesRequest{
		Owners: tooManyOwners,
	})
	testutil.AssertStatusErrorWithCode(t, err, codes.InvalidArgument)

	tooManyMints := make([]*commonpb.SolanaAccountId, 0, 1025)
	for range cap(tooManyMints) {
		tooManyMints = append(tooManyMints, testutil.NewRandomAccount(t).ToProto())
	}
	_, err = env.client.GetBalances(env.ctx, &balancepb.GetBalancesRequest{
		Owners: []*commonpb.SolanaAccountId{ownerAccount.ToProto()},
		Mints:  tooManyMints,
	})
	testutil.AssertStatusErrorWithCode(t, err, codes.InvalidArgument)

	// Accounts must be well-formed public keys
	_, err = env.client.GetBalances(env.ctx, &balancepb.GetBalancesRequest{
		Owners: []*commonpb.SolanaAccountId{{Value: []byte("invalid")}},
	})
	testutil.AssertStatusErrorWithCode(t, err, codes.InvalidArgument)

	_, err = env.client.GetBalances(env.ctx, &balancepb.GetBalancesRequest{
		Owners: []*commonpb.SolanaAccountId{ownerAccount.ToProto()},
		Mints:  []*commonpb.SolanaAccountId{{Value: []byte("invalid")}},
	})
	testutil.AssertStatusErrorWithCode(t, err, codes.InvalidArgument)
}

func setupAccountRecords(t *testing.T, env testEnv, ownerAccount, authorityAccount *common.Account, vmConfig *common.VmConfig, index uint64, accountType commonpb.AccountType) *common.AccountRecords {
	timelockAccounts, err := authorityAccount.GetTimelockAccounts(vmConfig)
	require.NoError(t, err)

	timelockRecord := timelockAccounts.ToDBRecord()
	timelockRecord.VaultState = timelock_token_v1.StateLocked
	timelockRecord.Block += 1

	accountInfoRecord := &account.Record{
		OwnerAccount:     ownerAccount.PublicKey().ToBase58(),
		AuthorityAccount: authorityAccount.PublicKey().ToBase58(),
		TokenAccount:     timelockAccounts.Vault.PublicKey().ToBase58(),
		MintAccount:      vmConfig.Mint.PublicKey().ToBase58(),

		AccountType: accountType,

		Index: index,
	}

	require.NoError(t, env.data.CreateAccountInfo(env.ctx, accountInfoRecord))
	require.NoError(t, env.data.SaveTimelock(env.ctx, timelockRecord))
	require.NoError(t, balance_util.CreateRecordInTx(env.ctx, env.data, accountInfoRecord))

	return &common.AccountRecords{
		General:  accountInfoRecord,
		Timelock: timelockRecord,
	}
}

func setupCachedBalance(t *testing.T, env testEnv, accountRecords *common.AccountRecords, quarks uint64) {
	require.NoError(t, balance_util.ApplyDeltasInTx(env.ctx, env.data, &balance.Delta{
		TokenAccount: accountRecords.General.TokenAccount,
		Kind:         balance.DeltaCredit,
		Quarks:       quarks,
	}))
}

// estimateLaunchpadSellValue is the core mint value of selling the entire
// position on the bonding curve of a currency set up by
// testutil.SetupLaunchpadCurrency
func estimateLaunchpadSellValue(t *testing.T, quarks uint64) uint64 {
	value, _ := currencycreator.EstimateSell(&currencycreator.EstimateSellArgs{
		CurrentSupplyInQuarks: currencycreator.ToQuarks(1_000),
		SellAmountInQuarks:    quarks,
		ValueMintDecimals:     uint8(common.CoreMintDecimals),
		SellFeeBps:            0,
	})
	require.NotZero(t, value)
	return value
}

func assertOwnerBalance(t *testing.T, resp *balancepb.GetBalancesResponse, owner *common.Account, expectedCoreMintValue uint64) *balancepb.OwnerBalance {
	ownerBalance, ok := resp.BalancesByOwner[owner.PublicKey().ToBase58()]
	require.True(t, ok)
	assert.Equal(t, owner.PublicKey().ToBytes(), ownerBalance.Owner.Value)
	assert.EqualValues(t, expectedCoreMintValue, ownerBalance.CoreMintValue)
	return ownerBalance
}

func assertMintBalance(t *testing.T, balancesByMint map[string]*balancepb.MintBalance, mint *common.Account, expectedCoreMintValue uint64) {
	mintBalance, ok := balancesByMint[mint.PublicKey().ToBase58()]
	require.True(t, ok)
	assert.Equal(t, mint.PublicKey().ToBytes(), mintBalance.Mint.Value)
	assert.EqualValues(t, expectedCoreMintValue, mintBalance.CoreMintValue)
}
