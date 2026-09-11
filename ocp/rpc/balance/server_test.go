package balance

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
	"google.golang.org/grpc"

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
	s := NewBalanceServer(log, env.data, mintDataProvider)

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

func TestGetBalance_HappyPath(t *testing.T) {
	env, cleanup := setup(t)
	defer cleanup()

	coreVmConfig := testutil.NewRandomVmConfig(t, true)
	launchpadMint := testutil.SetupLaunchpadCurrency(t, env.data, env.reserveStore, env.holderStore)
	launchpadVmConfig, err := common.GetVmConfigForMint(env.ctx, env.data, launchpadMint)
	require.NoError(t, err)

	ownerAccount := testutil.NewRandomAccount(t)

	req := &balancepb.GetBalanceRequest{
		Owner: ownerAccount.ToProto(),
	}

	resp, err := env.client.GetBalance(env.ctx, req)
	require.NoError(t, err)
	assert.Equal(t, balancepb.GetBalanceResponse_NOT_FOUND, resp.Result)
	assert.EqualValues(t, 0, resp.CoreMintValue)

	primaryCoreMintAccountRecords := setupAccountRecords(t, env, ownerAccount, ownerAccount, coreVmConfig, 0, commonpb.AccountType_PRIMARY)
	primaryLaunchpadMintAccountRecords := setupAccountRecords(t, env, ownerAccount, ownerAccount, launchpadVmConfig, 0, commonpb.AccountType_PRIMARY)

	resp, err = env.client.GetBalance(env.ctx, req)
	require.NoError(t, err)
	assert.Equal(t, balancepb.GetBalanceResponse_OK, resp.Result)
	assert.EqualValues(t, 0, resp.CoreMintValue)
	assert.Empty(t, resp.BalancesByMint)

	setupCachedBalance(t, env, primaryCoreMintAccountRecords, common.ToCoreMintQuarks(42))
	setupCachedBalance(t, env, primaryLaunchpadMintAccountRecords, currencycreator.ToQuarks(100))

	// The launchpad currency's value is what the entire position would currently
	// sell for on the bonding curve.
	expectedLaunchpadMintValue, _ := currencycreator.EstimateSell(&currencycreator.EstimateSellArgs{
		CurrentSupplyInQuarks: currencycreator.ToQuarks(1_000),
		SellAmountInQuarks:    currencycreator.ToQuarks(100),
		ValueMintDecimals:     uint8(common.CoreMintDecimals),
		SellFeeBps:            0,
	})
	require.NotZero(t, expectedLaunchpadMintValue)

	resp, err = env.client.GetBalance(env.ctx, req)
	require.NoError(t, err)
	assert.Equal(t, balancepb.GetBalanceResponse_OK, resp.Result)
	assert.EqualValues(t, common.ToCoreMintQuarks(42)+expectedLaunchpadMintValue, resp.CoreMintValue)
	require.Len(t, resp.BalancesByMint, 2)
	assertMintBalance(t, resp, common.CoreMintAccount, common.ToCoreMintQuarks(42))
	assertMintBalance(t, resp, launchpadMint, expectedLaunchpadMintValue)
}

func TestGetBalance_MintFilter(t *testing.T) {
	env, cleanup := setup(t)
	defer cleanup()

	coreVmConfig := testutil.NewRandomVmConfig(t, true)
	launchpadMint := testutil.SetupLaunchpadCurrency(t, env.data, env.reserveStore, env.holderStore)
	launchpadVmConfig, err := common.GetVmConfigForMint(env.ctx, env.data, launchpadMint)
	require.NoError(t, err)

	ownerAccount := testutil.NewRandomAccount(t)

	primaryCoreMintAccountRecords := setupAccountRecords(t, env, ownerAccount, ownerAccount, coreVmConfig, 0, commonpb.AccountType_PRIMARY)
	primaryLaunchpadMintAccountRecords := setupAccountRecords(t, env, ownerAccount, ownerAccount, launchpadVmConfig, 0, commonpb.AccountType_PRIMARY)

	setupCachedBalance(t, env, primaryCoreMintAccountRecords, common.ToCoreMintQuarks(42))
	setupCachedBalance(t, env, primaryLaunchpadMintAccountRecords, currencycreator.ToQuarks(100))

	expectedLaunchpadMintValue, _ := currencycreator.EstimateSell(&currencycreator.EstimateSellArgs{
		CurrentSupplyInQuarks: currencycreator.ToQuarks(1_000),
		SellAmountInQuarks:    currencycreator.ToQuarks(100),
		ValueMintDecimals:     uint8(common.CoreMintDecimals),
		SellFeeBps:            0,
	})
	require.NotZero(t, expectedLaunchpadMintValue)

	// Filtering to the core mint excludes the launchpad currency from both the
	// total and the per-mint breakdown
	resp, err := env.client.GetBalance(env.ctx, &balancepb.GetBalanceRequest{
		Owner: ownerAccount.ToProto(),
		Mints: []*commonpb.SolanaAccountId{common.CoreMintAccount.ToProto()},
	})
	require.NoError(t, err)
	assert.Equal(t, balancepb.GetBalanceResponse_OK, resp.Result)
	assert.EqualValues(t, common.ToCoreMintQuarks(42), resp.CoreMintValue)
	require.Len(t, resp.BalancesByMint, 1)
	assertMintBalance(t, resp, common.CoreMintAccount, common.ToCoreMintQuarks(42))

	resp, err = env.client.GetBalance(env.ctx, &balancepb.GetBalanceRequest{
		Owner: ownerAccount.ToProto(),
		Mints: []*commonpb.SolanaAccountId{launchpadMint.ToProto()},
	})
	require.NoError(t, err)
	assert.Equal(t, balancepb.GetBalanceResponse_OK, resp.Result)
	assert.EqualValues(t, expectedLaunchpadMintValue, resp.CoreMintValue)
	require.Len(t, resp.BalancesByMint, 1)
	assertMintBalance(t, resp, launchpadMint, expectedLaunchpadMintValue)

	// Duplicate mints in the filter don't double count
	resp, err = env.client.GetBalance(env.ctx, &balancepb.GetBalanceRequest{
		Owner: ownerAccount.ToProto(),
		Mints: []*commonpb.SolanaAccountId{common.CoreMintAccount.ToProto(), launchpadMint.ToProto(), common.CoreMintAccount.ToProto()},
	})
	require.NoError(t, err)
	assert.Equal(t, balancepb.GetBalanceResponse_OK, resp.Result)
	assert.EqualValues(t, common.ToCoreMintQuarks(42)+expectedLaunchpadMintValue, resp.CoreMintValue)
	require.Len(t, resp.BalancesByMint, 2)

	// A mint the owner doesn't hold results in an OK response with no balances
	resp, err = env.client.GetBalance(env.ctx, &balancepb.GetBalanceRequest{
		Owner: ownerAccount.ToProto(),
		Mints: []*commonpb.SolanaAccountId{testutil.NewRandomAccount(t).ToProto()},
	})
	require.NoError(t, err)
	assert.Equal(t, balancepb.GetBalanceResponse_OK, resp.Result)
	assert.EqualValues(t, 0, resp.CoreMintValue)
	assert.Empty(t, resp.BalancesByMint)
}

func TestGetBalance_UnmanagedAccountsExcluded(t *testing.T) {
	env, cleanup := setup(t)
	defer cleanup()

	coreVmConfig := testutil.NewRandomVmConfig(t, true)

	ownerAccount := testutil.NewRandomAccount(t)

	primaryCoreMintAccountRecords := setupAccountRecords(t, env, ownerAccount, ownerAccount, coreVmConfig, 0, commonpb.AccountType_PRIMARY)

	setupCachedBalance(t, env, primaryCoreMintAccountRecords, common.ToCoreMintQuarks(42))

	// The pool account has left the L2 system, so there isn't a cached balance that
	// can be trusted for it.
	// The geyser worker moves both records in the same transaction, so the
	// ledger record's lock state can't disagree with the timelock record's
	primaryCoreMintAccountRecords.Timelock.VaultState = timelock_token_v1.StateUnlocked
	primaryCoreMintAccountRecords.Timelock.Block += 1
	require.NoError(t, env.data.SaveTimelock(env.ctx, primaryCoreMintAccountRecords.Timelock))
	require.NoError(t, env.data.MarkBalanceAsUnlocked(env.ctx, primaryCoreMintAccountRecords.General.TokenAccount))

	resp, err := env.client.GetBalance(env.ctx, &balancepb.GetBalanceRequest{
		Owner: ownerAccount.ToProto(),
	})
	require.NoError(t, err)
	assert.Equal(t, balancepb.GetBalanceResponse_OK, resp.Result)
	assert.EqualValues(t, common.ToCoreMintQuarks(0), resp.CoreMintValue)
}

func TestGetBalance_GiftCardOwnerAccount(t *testing.T) {
	env, cleanup := setup(t)
	defer cleanup()

	coreVmConfig := testutil.NewRandomVmConfig(t, true)

	giftCardOwnerAccount := testutil.NewRandomAccount(t)

	giftCardAccountRecords := setupAccountRecords(t, env, giftCardOwnerAccount, giftCardOwnerAccount, coreVmConfig, 0, commonpb.AccountType_REMOTE_SEND_GIFT_CARD)
	setupCachedBalance(t, env, giftCardAccountRecords, common.ToCoreMintQuarks(42))

	resp, err := env.client.GetBalance(env.ctx, &balancepb.GetBalanceRequest{
		Owner: giftCardOwnerAccount.ToProto(),
	})
	require.NoError(t, err)
	assert.Equal(t, balancepb.GetBalanceResponse_NOT_FOUND, resp.Result)
	assert.EqualValues(t, 0, resp.CoreMintValue)
}

func TestGetBalance_UnknownOwnerAccount(t *testing.T) {
	env, cleanup := setup(t)
	defer cleanup()

	resp, err := env.client.GetBalance(env.ctx, &balancepb.GetBalanceRequest{
		Owner: testutil.NewRandomAccount(t).ToProto(),
	})
	require.NoError(t, err)
	assert.Equal(t, balancepb.GetBalanceResponse_NOT_FOUND, resp.Result)
	assert.EqualValues(t, 0, resp.CoreMintValue)
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

func assertMintBalance(t *testing.T, resp *balancepb.GetBalanceResponse, mint *common.Account, expectedCoreMintValue uint64) {
	mintBalance, ok := resp.BalancesByMint[mint.PublicKey().ToBase58()]
	require.True(t, ok)
	assert.Equal(t, mint.PublicKey().ToBytes(), mintBalance.Mint.Value)
	assert.EqualValues(t, expectedCoreMintValue, mintBalance.CoreMintValue)
}

func setupCachedBalance(t *testing.T, env testEnv, accountRecords *common.AccountRecords, quarks uint64) {
	require.NoError(t, balance_util.ApplyDeltasInTx(env.ctx, env.data, &balance.Delta{
		TokenAccount: accountRecords.General.TokenAccount,
		Kind:         balance.DeltaCredit,
		Quarks:       quarks,
	}))
}
