package transaction

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"math"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mr-tron/base58/base58"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonpb "github.com/code-payments/ocp-protobuf-api/generated/go/common/v1"
	currencypb "github.com/code-payments/ocp-protobuf-api/generated/go/currency/v1"
	transactionpb "github.com/code-payments/ocp-protobuf-api/generated/go/transaction/v1"

	currency_lib "github.com/code-payments/ocp-server/currency"
	"github.com/code-payments/ocp-server/database/query"
	noop_metrics "github.com/code-payments/ocp-server/metrics/noop"
	"github.com/code-payments/ocp-server/ocp/aml"
	"github.com/code-payments/ocp-server/ocp/antispam"
	"github.com/code-payments/ocp-server/ocp/auth"
	balance_util "github.com/code-payments/ocp-server/ocp/balance"
	"github.com/code-payments/ocp-server/ocp/common"
	currency_util "github.com/code-payments/ocp-server/ocp/currency"
	ocp_data "github.com/code-payments/ocp-server/ocp/data"
	"github.com/code-payments/ocp-server/ocp/data/account"
	"github.com/code-payments/ocp-server/ocp/data/action"
	"github.com/code-payments/ocp-server/ocp/data/balance"
	"github.com/code-payments/ocp-server/ocp/data/currency"
	holder_memory "github.com/code-payments/ocp-server/ocp/data/currency/holder/memory"
	reserve_memory "github.com/code-payments/ocp-server/ocp/data/currency/reserve/memory"
	"github.com/code-payments/ocp-server/ocp/data/fulfillment"
	"github.com/code-payments/ocp-server/ocp/data/history"
	"github.com/code-payments/ocp-server/ocp/data/intent"
	"github.com/code-payments/ocp-server/ocp/data/nonce"
	"github.com/code-payments/ocp-server/ocp/data/swap"
	"github.com/code-payments/ocp-server/ocp/data/task"
	"github.com/code-payments/ocp-server/ocp/data/timelock"
	"github.com/code-payments/ocp-server/ocp/data/vault"
	ocp_task "github.com/code-payments/ocp-server/ocp/task"
	"github.com/code-payments/ocp-server/ocp/transaction"
	"github.com/code-payments/ocp-server/pointer"
	"github.com/code-payments/ocp-server/solana"
	"github.com/code-payments/ocp-server/solana/currencycreator"
	timelock_token_v1 "github.com/code-payments/ocp-server/solana/timelock/v1"
	"github.com/code-payments/ocp-server/solana/vm"
	"github.com/code-payments/ocp-server/testutil"
)

//
// Test environment
//

const (
	testNodeId                 = "test-node"
	testNoncesPerVm            = 100
	defaultLaunchpadSupply     = 1_000_000 // in whole tokens
	defaultTestStreamTimeout   = 30 * time.Second
	defaultTestReceiveTimeout  = 5 * time.Second
	withdrawalFeeQuarks        = 500_000 // $0.50, matching defaultCreateOnSendWithdrawalFeeQuarks
	newCurrencyPurchaseAndFees = 20.0    // USD, matching defaults for the new currency purchase and fee
)

type testEnv struct {
	ctx    context.Context
	client transactionpb.TransactionClient
	server *transactionServer
	data   *testDataProvider

	subsidizer   *common.Account
	feeCollector *common.Account
	swapTreasury *common.Account

	submitIntentIntegration *testSubmitIntentIntegration
	antispamIntegration     *testAntispamIntegration
	taskExecutor            *testTaskExecutor

	keysMu sync.Mutex
	keys   map[string]*common.Account

	nonceVmsMu sync.Mutex
	nonceVms   map[string]struct{}
}

func setupTestEnv(t *testing.T, overrides ...func(*testOverrides)) *testEnv {
	log := zap.NewNop()

	conn, serv, err := testutil.NewServer(log)
	require.NoError(t, err)

	env := &testEnv{
		ctx:      context.Background(),
		data:     newTestDataProvider(),
		keys:     make(map[string]*common.Account),
		nonceVms: make(map[string]struct{}),
	}

	env.subsidizer = testutil.SetupRandomSubsidizer(t, env.data)
	env.feeCollector = env.newAccount(t)
	env.swapTreasury = env.newAccount(t)
	require.NoError(t, env.data.SaveKey(env.ctx, &vault.Record{
		PublicKey:  env.swapTreasury.PublicKey().ToBase58(),
		PrivateKey: env.swapTreasury.PrivateKey().ToBase58(),
		State:      vault.StateAvailable,
	}))

	env.saveNonces(t, common.CoreMintVmAccount, testNoncesPerVm)
	noncePool, err := transaction.NewLocalNoncePool(
		log,
		env.data,
		noop_metrics.NewProvider(),
		nonce.EnvironmentVm,
		common.CoreMintVmAccount.PublicKey().ToBase58(),
		nonce.PurposeClientIntent,
	)
	require.NoError(t, err)
	t.Cleanup(func() { noncePool.Close() })

	configOverrides := &testOverrides{
		disableSwaps:               true,
		clientReceiveTimeout:       defaultTestReceiveTimeout,
		feeCollectorOwnerPublicKey: env.feeCollector.PublicKey().ToBase58(),
		swapTreasuryOwnerPublicKey: env.swapTreasury.PublicKey().ToBase58(),
	}
	for _, override := range overrides {
		override(configOverrides)
	}

	env.submitIntentIntegration = &testSubmitIntentIntegration{}
	env.antispamIntegration = &testAntispamIntegration{allow: true}
	env.taskExecutor = &testTaskExecutor{executed: make(chan *task.Record, 100)}

	s, err := NewTransactionServer(
		log,
		env.data,
		nil,
		env.submitIntentIntegration,
		nil,
		ocp_task.NewScheduler(log, env.data, env.taskExecutor),
		antispam.NewGuard(env.antispamIntegration),
		aml.NewGuard(log, env.data),
		nil,
		testNodeId,
		[]*transaction.LocalNoncePool{noncePool},
		withManualTestOverrides(configOverrides),
	)
	require.NoError(t, err)
	env.server = s.(*transactionServer)

	serv.RegisterService(func(server *grpc.Server) {
		transactionpb.RegisterTransactionServer(server, s)
	})

	serverCleanup, err := serv.Serve()
	require.NoError(t, err)
	t.Cleanup(serverCleanup)

	env.client = transactionpb.NewTransactionClient(conn)

	return env
}

func withAntispamChecks() func(*testOverrides) {
	return func(o *testOverrides) {
		o.enableAntispamChecks = true
	}
}

func withAmlChecks() func(*testOverrides) {
	return func(o *testOverrides) {
		o.enableAmlChecks = true
	}
}

func withSubmitIntentDisabled() func(*testOverrides) {
	return func(o *testOverrides) {
		o.disableSubmitIntent = true
	}
}

func withClientReceiveTimeout(timeout time.Duration) func(*testOverrides) {
	return func(o *testOverrides) {
		o.clientReceiveTimeout = timeout
	}
}

// testDataProvider wraps the test data provider to avoid live blockchain calls
// and to inject failures into the intent DB transaction.
type testDataProvider struct {
	ocp_data.Provider

	mu                       sync.Mutex
	blockchainAccounts       map[string]struct{}
	applyBalanceDeltasErr    error
	applyBalanceDeltasCalled int
}

func newTestDataProvider() *testDataProvider {
	return &testDataProvider{
		Provider:           ocp_data.NewTestDataProvider(),
		blockchainAccounts: make(map[string]struct{}),
	}
}

func (p *testDataProvider) GetBlockchainAccountInfo(ctx context.Context, account string, commitment solana.Commitment) (*solana.AccountInfo, uint64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if _, ok := p.blockchainAccounts[account]; ok {
		return &solana.AccountInfo{}, 1, nil
	}
	return nil, 0, solana.ErrNoAccountInfo
}

func (p *testDataProvider) ApplyBalanceDeltas(ctx context.Context, deltas ...*balance.Delta) error {
	p.mu.Lock()
	p.applyBalanceDeltasCalled++
	err := p.applyBalanceDeltasErr
	p.mu.Unlock()

	if err != nil {
		return err
	}
	return p.Provider.ApplyBalanceDeltas(ctx, deltas...)
}

func (p *testDataProvider) setBlockchainAccountExists(account *common.Account) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.blockchainAccounts[account.PublicKey().ToBase58()] = struct{}{}
}

func (p *testDataProvider) setApplyBalanceDeltasErr(err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.applyBalanceDeltasErr = err
	p.applyBalanceDeltasCalled = 0
}

type testSubmitIntentIntegration struct {
	mu sync.Mutex

	allowCreationErr  error
	tasksToSchedule   func(intentRecord *intent.Record) []*task.Record
	getTasksErr       error
	allowCreationArgs []*intent.Record
	successes         []string
}

func (i *testSubmitIntentIntegration) AllowCreation(ctx context.Context, intentRecord *intent.Record, metadata *transactionpb.Metadata, actions []*transactionpb.Action) error {
	i.mu.Lock()
	defer i.mu.Unlock()

	cloned := intentRecord.Clone()
	i.allowCreationArgs = append(i.allowCreationArgs, &cloned)
	return i.allowCreationErr
}

func (i *testSubmitIntentIntegration) GetTasksToSchedule(ctx context.Context, intentRecord *intent.Record) ([]*task.Record, error) {
	i.mu.Lock()
	defer i.mu.Unlock()

	if i.getTasksErr != nil {
		return nil, i.getTasksErr
	}
	if i.tasksToSchedule == nil {
		return nil, nil
	}
	return i.tasksToSchedule(intentRecord), nil
}

func (i *testSubmitIntentIntegration) OnSuccess(ctx context.Context, intentRecord *intent.Record) error {
	i.mu.Lock()
	defer i.mu.Unlock()

	i.successes = append(i.successes, intentRecord.IntentId)
	return nil
}

func (i *testSubmitIntentIntegration) setAllowCreationErr(err error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.allowCreationErr = err
}

func (i *testSubmitIntentIntegration) getSuccesses() []string {
	i.mu.Lock()
	defer i.mu.Unlock()
	return append([]string(nil), i.successes...)
}

type testAntispamIntegration struct {
	mu    sync.Mutex
	allow bool
	calls []string
}

func (i *testAntispamIntegration) record(call string) (bool, string, error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.calls = append(i.calls, call)
	if !i.allow {
		return false, "denied by test", nil
	}
	return true, "", nil
}

func (i *testAntispamIntegration) setAllow(allow bool) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.allow = allow
}

func (i *testAntispamIntegration) getCalls() []string {
	i.mu.Lock()
	defer i.mu.Unlock()
	return append([]string(nil), i.calls...)
}

func (i *testAntispamIntegration) AllowOpenAccounts(ctx context.Context, owner *common.Account, accountSet transactionpb.OpenAccountsMetadata_AccountSet) (bool, string, error) {
	return i.record("open_accounts:" + accountSet.String())
}

func (i *testAntispamIntegration) AllowSendPayment(ctx context.Context, owner, destination *common.Account, isPublic bool) (bool, string, error) {
	return i.record("send_payment:" + destination.PublicKey().ToBase58())
}

func (i *testAntispamIntegration) AllowReceivePayments(ctx context.Context, owner *common.Account, isPublic bool) (bool, string, error) {
	return i.record("receive_payments")
}

func (i *testAntispamIntegration) AllowDistribution(ctx context.Context, owner *common.Account, isPublic bool) (bool, string, error) {
	return i.record("distribution")
}

func (i *testAntispamIntegration) AllowSwap(ctx context.Context, kind swap.Kind, fundingSource swap.FundingSource, owner, fromMint, toMint *common.Account, swapAmount, feeAmount uint64, initializesMint bool) (bool, string, error) {
	return i.record("swap")
}

func (i *testAntispamIntegration) AllowCurrencyLaunch(ctx context.Context, owner *common.Account, name, symbol string) (bool, string, error) {
	return i.record("currency_launch")
}

type testTaskExecutor struct {
	executed chan *task.Record
}

func (e *testTaskExecutor) Execute(ctx context.Context, record *task.Record) error {
	cloned := record.Clone()
	e.executed <- &cloned
	return nil
}

//
// Account and currency setup
//

func (e *testEnv) newAccount(t *testing.T) *common.Account {
	account := testutil.NewRandomAccount(t)

	e.keysMu.Lock()
	e.keys[account.PublicKey().ToBase58()] = account
	e.keysMu.Unlock()

	return account
}

func (e *testEnv) getKey(t *testing.T, publicKey []byte) *common.Account {
	e.keysMu.Lock()
	defer e.keysMu.Unlock()

	key, ok := e.keys[base58.Encode(publicKey)]
	require.True(t, ok, "no private key registered for %s", base58.Encode(publicKey))
	return key
}

func (e *testEnv) saveNonces(t *testing.T, vmAccount *common.Account, count int) {
	e.nonceVmsMu.Lock()
	e.nonceVms[vmAccount.PublicKey().ToBase58()] = struct{}{}
	e.nonceVmsMu.Unlock()

	for range count {
		var bh solana.Blockhash
		_, err := rand.Read(bh[:])
		require.NoError(t, err)

		require.NoError(t, e.data.SaveNonce(e.ctx, &nonce.Record{
			Address:             testutil.NewRandomAccount(t).PublicKey().ToBase58(),
			Authority:           common.GetSubsidizer().PublicKey().ToBase58(),
			Blockhash:           base58.Encode(bh[:]),
			Environment:         nonce.EnvironmentVm,
			EnvironmentInstance: vmAccount.PublicKey().ToBase58(),
			Purpose:             nonce.PurposeClientIntent,
			State:               nonce.StateAvailable,
		}))
	}
}

// newLaunchpadMint sets up an available launchpad currency with nonces for its VM
func (e *testEnv) newLaunchpadMint(t *testing.T) *common.Account {
	mint := testutil.SetupLaunchpadCurrency(t, e.data, reserve_memory.New(), holder_memory.New())

	vmConfig, err := common.GetVmConfigForMint(e.ctx, e.data, mint)
	require.NoError(t, err)
	e.saveNonces(t, vmConfig.Vm, testNoncesPerVm)

	return mint
}

func (e *testEnv) getTimelockAccounts(t *testing.T, authority, mint *common.Account) *common.TimelockAccounts {
	vmConfig, err := common.GetVmConfigForMint(e.ctx, e.data, mint)
	require.NoError(t, err)

	timelockAccounts, err := authority.GetTimelockAccounts(vmConfig)
	require.NoError(t, err)
	return timelockAccounts
}

func (e *testEnv) getVault(t *testing.T, authority, mint *common.Account) *common.Account {
	return e.getTimelockAccounts(t, authority, mint).Vault
}

// newUser creates a new user with an opened core mint primary account
func (e *testEnv) newUser(t *testing.T) *common.Account {
	owner := e.newAccount(t)
	e.openAccount(t, owner, owner, common.CoreMintAccount, commonpb.AccountType_PRIMARY, 0)
	return owner
}

// newFundedUser creates a new user with a funded primary account for each
// provided mint
func (e *testEnv) newFundedUser(t *testing.T, quarks uint64, mints ...*common.Account) *common.Account {
	owner := e.newUser(t)
	e.credit(t, e.getVault(t, owner, common.CoreMintAccount), quarks)
	for _, mint := range mints {
		if common.IsCoreMint(mint) {
			continue
		}
		e.openAccount(t, owner, owner, mint, commonpb.AccountType_PRIMARY, 0)
		e.credit(t, e.getVault(t, owner, mint), quarks)
	}
	return owner
}

func (e *testEnv) openAccount(t *testing.T, owner, authority, mint *common.Account, accountType commonpb.AccountType, index uint64) *common.AccountRecords {
	timelockAccounts := e.getTimelockAccounts(t, authority, mint)

	accountInfoRecord := &account.Record{
		OwnerAccount:     owner.PublicKey().ToBase58(),
		AuthorityAccount: authority.PublicKey().ToBase58(),
		TokenAccount:     timelockAccounts.Vault.PublicKey().ToBase58(),
		MintAccount:      mint.PublicKey().ToBase58(),
		AccountType:      accountType,
		Index:            index,
	}
	require.NoError(t, e.data.CreateAccountInfo(e.ctx, accountInfoRecord))
	require.NoError(t, balance_util.CreateRecordInTx(e.ctx, e.data, accountInfoRecord))

	timelockRecord := timelockAccounts.ToDBRecord()
	timelockRecord.VaultState = timelock_token_v1.StateLocked
	timelockRecord.Block = 1
	require.NoError(t, e.data.SaveTimelock(e.ctx, timelockRecord))

	return &common.AccountRecords{
		General:  accountInfoRecord,
		Timelock: timelockRecord,
	}
}

func (e *testEnv) credit(t *testing.T, tokenAccount *common.Account, quarks uint64) {
	if quarks == 0 {
		return
	}
	require.NoError(t, balance_util.ApplyDeltasInTx(e.ctx, e.data, &balance.Delta{
		TokenAccount: tokenAccount.PublicKey().ToBase58(),
		Kind:         balance.DeltaCredit,
		Quarks:       quarks,
	}))
}

func (e *testEnv) setTimelockState(t *testing.T, vault *common.Account, state timelock_token_v1.TimelockState) {
	timelockRecord, err := e.data.GetTimelockByVault(e.ctx, vault.PublicKey().ToBase58())
	require.NoError(t, err)

	timelockRecord.VaultState = state
	timelockRecord.Block += 1
	require.NoError(t, e.data.SaveTimelock(e.ctx, timelockRecord))

	if !timelockRecord.IsLocked() {
		require.NoError(t, e.data.MarkBalanceAsUnlocked(e.ctx, vault.PublicKey().ToBase58()))
	}
}

func (e *testEnv) getBalance(t *testing.T, tokenAccount *common.Account) *balance.Record {
	record, err := e.data.GetBalance(e.ctx, tokenAccount.PublicKey().ToBase58())
	require.NoError(t, err)
	return record
}

func (e *testEnv) requireBalance(t *testing.T, tokenAccount *common.Account, expectedQuarks uint64) {
	assert.Equal(t, expectedQuarks, e.getBalance(t, tokenAccount).Quarks)
}

//
// Exchange data
//

func (e *testEnv) signedExchangeRate(t *testing.T, currencyCode currency_lib.Code, rate float64, ts time.Time) *currencypb.VerifiedCoreMintFiatExchangeRate {
	exchangeRate := &currencypb.CoreMintFiatExchangeRate{
		CurrencyCode: string(currencyCode),
		ExchangeRate: rate,
		Timestamp:    timestamppb.New(ts),
	}

	marshalled, err := auth.ForceConsistentMarshal(exchangeRate)
	require.NoError(t, err)

	return &currencypb.VerifiedCoreMintFiatExchangeRate{
		ExchangeRate: exchangeRate,
		Signature:    &commonpb.Signature{Value: ed25519.Sign(e.subsidizer.PrivateKey().ToBytes(), marshalled)},
	}
}

func (e *testEnv) signedReserveState(t *testing.T, mint *common.Account, supplyFromBonding uint64, ts time.Time) *currencypb.VerifiedLaunchpadCurrencyReserveState {
	reserveState := &currencypb.LaunchpadCurrencyReserveState{
		Mint:              mint.ToProto(),
		SupplyFromBonding: supplyFromBonding,
		Timestamp:         timestamppb.New(ts),
	}

	marshalled, err := auth.ForceConsistentMarshal(reserveState)
	require.NoError(t, err)

	return &currencypb.VerifiedLaunchpadCurrencyReserveState{
		ReserveState: reserveState,
		Signature:    &commonpb.Signature{Value: ed25519.Sign(e.subsidizer.PrivateKey().ToBytes(), marshalled)},
	}
}

// exchangeData builds valid verified exchange data for the mint in USD at the
// default reserve state for launchpad currencies
func (e *testEnv) exchangeData(t *testing.T, mint *common.Account, quarks uint64) *transactionpb.VerifiedExchangeData {
	exchangeRate := e.signedExchangeRate(t, currency_lib.USD, 1.0, time.Now())
	if common.IsCoreMint(mint) {
		return e.coreMintExchangeData(t, quarks, exchangeRate)
	}
	reserveState := e.signedReserveState(t, mint, currencycreator.ToQuarks(defaultLaunchpadSupply), time.Now())
	return e.launchpadExchangeData(t, mint, quarks, exchangeRate, reserveState)
}

func (e *testEnv) coreMintExchangeData(t *testing.T, quarks uint64, exchangeRate *currencypb.VerifiedCoreMintFiatExchangeRate) *transactionpb.VerifiedExchangeData {
	return &transactionpb.VerifiedExchangeData{
		Mint:                     common.CoreMintAccount.ToProto(),
		Quarks:                   quarks,
		NativeAmount:             exchangeRate.ExchangeRate.ExchangeRate * float64(quarks) / float64(common.CoreMintQuarksPerUnit),
		CoreMintFiatExchangeRate: exchangeRate,
	}
}

func (e *testEnv) launchpadExchangeData(t *testing.T, mint *common.Account, quarks uint64, exchangeRate *currencypb.VerifiedCoreMintFiatExchangeRate, reserveState *currencypb.VerifiedLaunchpadCurrencyReserveState) *transactionpb.VerifiedExchangeData {
	return &transactionpb.VerifiedExchangeData{
		Mint:                          mint.ToProto(),
		Quarks:                        quarks,
		NativeAmount:                  exchangeRate.ExchangeRate.ExchangeRate * launchpadSellValue(reserveState.ReserveState.SupplyFromBonding, quarks),
		CoreMintFiatExchangeRate:      exchangeRate,
		LaunchpadCurrencyReserveState: reserveState,
	}
}

// launchpadSellValue is the USD value of selling quarks of a launchpad
// currency against the bonding curve, without fees
func launchpadSellValue(supplyFromBonding, quarks uint64) float64 {
	sellValueInQuarks, _ := currencycreator.EstimateSell(&currencycreator.EstimateSellArgs{
		CurrentSupplyInQuarks: supplyFromBonding,
		SellAmountInQuarks:    quarks,
		ValueMintDecimals:     uint8(common.CoreMintDecimals),
		SellFeeBps:            0,
	})
	return float64(sellValueInQuarks) / float64(common.CoreMintQuarksPerUnit)
}

// launchpadQuarksForUsdValue finds the quark amount of a launchpad currency
// whose sell value is closest to the target USD value
func launchpadQuarksForUsdValue(supplyFromBonding uint64, usdValue float64) uint64 {
	low, high := uint64(1), supplyFromBonding
	for low < high {
		mid := low + (high-low)/2
		if launchpadSellValue(supplyFromBonding, mid) < usdValue {
			low = mid + 1
		} else {
			high = mid
		}
	}
	return low
}

//
// Proto builders
//

func (e *testEnv) newIntentId(t *testing.T) *common.Account {
	return testutil.NewRandomAccount(t)
}

func (e *testEnv) openAccountAction(t *testing.T, id uint32, accountType commonpb.AccountType, owner, authority, mint *common.Account, index uint64) *transactionpb.Action {
	return &transactionpb.Action{
		Id: id,
		Type: &transactionpb.Action_OpenAccount{
			OpenAccount: &transactionpb.OpenAccountAction{
				AccountType: accountType,
				Owner:       owner.ToProto(),
				Index:       index,
				Authority:   authority.ToProto(),
				Token:       e.getVault(t, authority, mint).ToProto(),
				Mint:        mint.ToProto(),
			},
		},
	}
}

func (e *testEnv) transferAction(t *testing.T, id uint32, authority, destination, mint *common.Account, amount uint64) *transactionpb.Action {
	return &transactionpb.Action{
		Id: id,
		Type: &transactionpb.Action_NoPrivacyTransfer{
			NoPrivacyTransfer: &transactionpb.NoPrivacyTransferAction{
				Authority:   authority.ToProto(),
				Source:      e.getVault(t, authority, mint).ToProto(),
				Destination: destination.ToProto(),
				Amount:      amount,
				Mint:        mint.ToProto(),
			},
		},
	}
}

func (e *testEnv) withdrawAction(t *testing.T, id uint32, authority, destination, mint *common.Account, amount uint64, isAutoReturn bool) *transactionpb.Action {
	return &transactionpb.Action{
		Id: id,
		Type: &transactionpb.Action_NoPrivacyWithdraw{
			NoPrivacyWithdraw: &transactionpb.NoPrivacyWithdrawAction{
				Authority:    authority.ToProto(),
				Source:       e.getVault(t, authority, mint).ToProto(),
				Destination:  destination.ToProto(),
				Amount:       amount,
				ShouldClose:  true,
				IsAutoReturn: isAutoReturn,
				Mint:         mint.ToProto(),
			},
		},
	}
}

func (e *testEnv) feePaymentAction(t *testing.T, id uint32, authority, mint *common.Account, amount uint64) *transactionpb.Action {
	return &transactionpb.Action{
		Id: id,
		Type: &transactionpb.Action_FeePayment{
			FeePayment: &transactionpb.FeePaymentAction{
				Type:      transactionpb.FeePaymentAction_CREATE_ON_SEND_WITHDRAWAL,
				Authority: authority.ToProto(),
				Source:    e.getVault(t, authority, mint).ToProto(),
				Amount:    amount,
				Mint:      mint.ToProto(),
			},
		},
	}
}

func openAccountsMetadata(accountSet transactionpb.OpenAccountsMetadata_AccountSet, mint *common.Account) *transactionpb.Metadata {
	return &transactionpb.Metadata{
		Type: &transactionpb.Metadata_OpenAccounts{
			OpenAccounts: &transactionpb.OpenAccountsMetadata{
				AccountSet: accountSet,
				Mint:       mint.ToProto(),
			},
		},
	}
}

//
// Intent submission
//

type submitIntentRequest struct {
	intentId *common.Account
	owner    *common.Account
	metadata *transactionpb.Metadata
	actions  []*transactionpb.Action

	// Optional overrides for negative tests
	submitActionsSigner *common.Account
	mutateSignatures    func([]*commonpb.Signature) []*commonpb.Signature
	withholdSignatures  bool
}

type submitIntentResult struct {
	serverParameters []*transactionpb.ServerParameter
	resp             *transactionpb.SubmitIntentResponse
	err              error

	// The virtual instruction signature the client submitted for each action,
	// keyed by action ID
	signaturesByAction map[uint32]*commonpb.Signature
}

// submitIntent drives the SubmitIntent stream as a client would. A Send that
// fails with io.EOF means the server already ended the stream, in which case
// the status is read from the following Recv.
func (e *testEnv) submitIntent(t *testing.T, req *submitIntentRequest) *submitIntentResult {
	ctx, cancel := context.WithTimeout(e.ctx, defaultTestStreamTimeout)
	defer cancel()

	stream, err := e.client.SubmitIntent(ctx)
	require.NoError(t, err)

	for _, protoAction := range req.actions {
		openAction := protoAction.GetOpenAccount()
		if openAction == nil || openAction.AuthoritySignature != nil {
			continue
		}
		openAction.AuthoritySignature = e.signProto(t, e.getKey(t, openAction.Authority.Value), openAction)
	}

	submitActions := &transactionpb.SubmitIntentRequest_SubmitActions{
		Id:       &commonpb.IntentId{Value: req.intentId.PublicKey().ToBytes()},
		Owner:    req.owner.ToProto(),
		Metadata: req.metadata,
		Actions:  req.actions,
	}
	signer := req.owner
	if req.submitActionsSigner != nil {
		signer = req.submitActionsSigner
	}
	submitActions.Signature = e.signProto(t, signer, submitActions)

	err = stream.Send(&transactionpb.SubmitIntentRequest{
		Request: &transactionpb.SubmitIntentRequest_SubmitActions_{
			SubmitActions: submitActions,
		},
	})
	if err != nil && err != io.EOF {
		return &submitIntentResult{err: err}
	}

	resp, err := stream.Recv()
	if err != nil {
		return &submitIntentResult{err: err}
	}

	result := &submitIntentResult{}
	if serverParameters := resp.GetServerParameters(); serverParameters != nil {
		result.serverParameters = serverParameters.ServerParameters

		if !req.withholdSignatures {
			signatures, signedActionIds := e.signVirtualInstructions(t, req.actions, serverParameters.ServerParameters)
			if req.mutateSignatures != nil {
				signatures = req.mutateSignatures(signatures)
			} else {
				result.signaturesByAction = make(map[uint32]*commonpb.Signature)
				for i, actionId := range signedActionIds {
					result.signaturesByAction[actionId] = signatures[i]
				}
			}

			err = stream.Send(&transactionpb.SubmitIntentRequest{
				Request: &transactionpb.SubmitIntentRequest_SubmitSignatures_{
					SubmitSignatures: &transactionpb.SubmitIntentRequest_SubmitSignatures{
						Signatures: signatures,
					},
				},
			})
			if err != nil && err != io.EOF {
				result.err = err
				return result
			}
		}

		resp, err = stream.Recv()
		if err != nil {
			result.err = err
			return result
		}
	}

	result.resp = resp
	return result
}

func (e *testEnv) signProto(t *testing.T, signer *common.Account, msg proto.Message) *commonpb.Signature {
	marshalled, err := proto.Marshal(msg)
	require.NoError(t, err)
	return &commonpb.Signature{Value: ed25519.Sign(signer.PrivateKey().ToBytes(), marshalled)}
}

// signVirtualInstructions independently derives the virtual instruction hash
// for each action requiring a client signature and signs it with the action's
// authority. The IDs of the signed actions are returned alongside, in order.
func (e *testEnv) signVirtualInstructions(t *testing.T, actions []*transactionpb.Action, serverParameters []*transactionpb.ServerParameter) ([]*commonpb.Signature, []uint32) {
	require.Len(t, serverParameters, len(actions))

	var signatures []*commonpb.Signature
	var signedActionIds []uint32
	for i, protoAction := range actions {
		serverParameter := serverParameters[i]
		require.EqualValues(t, protoAction.Id, serverParameter.ActionId)
		if len(serverParameter.Nonces) == 0 {
			continue
		}
		require.Len(t, serverParameter.Nonces, 1)

		var nonceValue vm.Hash
		copy(nonceValue[:], serverParameter.Nonces[0].Blockhash.Value)
		nonceAddress := serverParameter.Nonces[0].Nonce.Value

		var authority, mint *commonpb.SolanaAccountId
		var message vm.CompactMessage
		switch typed := protoAction.Type.(type) {
		case *transactionpb.Action_NoPrivacyTransfer:
			authority, mint = typed.NoPrivacyTransfer.Authority, typed.NoPrivacyTransfer.Mint
			message = vm.GetCompactTransferMessage(&vm.GetCompactTransferMessageArgs{
				Source:       e.getVaultFromProto(t, authority, mint).PublicKey().ToBytes(),
				Destination:  typed.NoPrivacyTransfer.Destination.Value,
				Amount:       typed.NoPrivacyTransfer.Amount,
				NonceAddress: nonceAddress,
				NonceValue:   nonceValue,
			})
		case *transactionpb.Action_FeePayment:
			authority, mint = typed.FeePayment.Authority, typed.FeePayment.Mint
			require.NotNil(t, serverParameter.GetFeePayment())
			message = vm.GetCompactTransferMessage(&vm.GetCompactTransferMessageArgs{
				Source:       e.getVaultFromProto(t, authority, mint).PublicKey().ToBytes(),
				Destination:  serverParameter.GetFeePayment().Destination.Value,
				Amount:       typed.FeePayment.Amount,
				NonceAddress: nonceAddress,
				NonceValue:   nonceValue,
			})
		case *transactionpb.Action_NoPrivacyWithdraw:
			authority, mint = typed.NoPrivacyWithdraw.Authority, typed.NoPrivacyWithdraw.Mint
			message = vm.GetCompactWithdrawMessage(&vm.GetCompactWithdrawMessageArgs{
				Source:       e.getVaultFromProto(t, authority, mint).PublicKey().ToBytes(),
				Destination:  typed.NoPrivacyWithdraw.Destination.Value,
				NonceAddress: nonceAddress,
				NonceValue:   nonceValue,
			})
		default:
			require.Fail(t, "unexpected action requiring a signature")
		}

		signatures = append(signatures, &commonpb.Signature{
			Value: ed25519.Sign(e.getKey(t, authority.Value).PrivateKey().ToBytes(), message[:]),
		})
		signedActionIds = append(signedActionIds, protoAction.Id)
	}
	return signatures, signedActionIds
}

func (e *testEnv) getVaultFromProto(t *testing.T, authority, mint *commonpb.SolanaAccountId) *common.Account {
	vault, err := getExpectedTimelockVaultFromProtoAccounts(e.ctx, e.data, authority, mint)
	require.NoError(t, err)
	return vault
}

func (r *submitIntentResult) requireSuccess(t *testing.T) {
	require.NoError(t, r.err)
	require.NotNil(t, r.resp)
	require.NotNil(t, r.resp.GetSuccess(), "expected success, got %v", r.resp)
	assert.Equal(t, transactionpb.SubmitIntentResponse_Success_OK, r.resp.GetSuccess().Code)
}

func (r *submitIntentResult) requireError(t *testing.T, code transactionpb.SubmitIntentResponse_Error_Code, reasonSubstr string) {
	require.NoError(t, r.err)
	require.NotNil(t, r.resp)
	require.NotNil(t, r.resp.GetError(), "expected error, got %v", r.resp)
	assert.Equal(t, code, r.resp.GetError().Code, "unexpected error response: %v", r.resp.GetError())

	if len(reasonSubstr) == 0 {
		return
	}

	var reasons []string
	for _, errorDetails := range r.resp.GetError().ErrorDetails {
		switch typed := errorDetails.Type.(type) {
		case *transactionpb.ErrorDetails_ReasonString:
			reasons = append(reasons, typed.ReasonString.Reason)
		case *transactionpb.ErrorDetails_Denied:
			reasons = append(reasons, typed.Denied.Reason)
		}
	}
	for _, reason := range reasons {
		if strings.Contains(reason, reasonSubstr) {
			return
		}
	}
	assert.Fail(t, "error reason not found", "expected a reason containing %q, got %v", reasonSubstr, reasons)
}

func (r *submitIntentResult) requireInvalidIntent(t *testing.T, reasonSubstr string) {
	r.requireError(t, transactionpb.SubmitIntentResponse_Error_INVALID_INTENT, reasonSubstr)
}

func (r *submitIntentResult) requireDenied(t *testing.T, reasonSubstr string) {
	r.requireError(t, transactionpb.SubmitIntentResponse_Error_DENIED, reasonSubstr)
}

func (r *submitIntentResult) requireStaleState(t *testing.T, reasonSubstr string) {
	r.requireError(t, transactionpb.SubmitIntentResponse_Error_STALE_STATE, reasonSubstr)
}

func (r *submitIntentResult) requireStatusCode(t *testing.T, code codes.Code) {
	testutil.AssertStatusErrorWithCode(t, r.err, code)
}

//
// Persisted state assertions
//

func (e *testEnv) requireIntent(t *testing.T, intentId *common.Account) *intent.Record {
	intentRecord, err := e.data.GetIntent(e.ctx, intentId.PublicKey().ToBase58())
	require.NoError(t, err)
	return intentRecord
}

func (e *testEnv) requireNoIntent(t *testing.T, intentId *common.Account) {
	_, err := e.data.GetIntent(e.ctx, intentId.PublicKey().ToBase58())
	assert.Equal(t, intent.ErrIntentNotFound, err)
}

func (e *testEnv) getActions(t *testing.T, intentId *common.Account) []*action.Record {
	actionRecords, err := e.data.GetAllActionsByIntent(e.ctx, intentId.PublicKey().ToBase58())
	if err == action.ErrActionNotFound {
		return nil
	}
	require.NoError(t, err)
	return actionRecords
}

func (e *testEnv) getFulfillments(t *testing.T, intentId *common.Account) []*fulfillment.Record {
	fulfillmentRecords, err := e.data.GetAllFulfillmentsByIntent(e.ctx, intentId.PublicKey().ToBase58())
	if err == fulfillment.ErrFulfillmentNotFound {
		return nil
	}
	require.NoError(t, err)
	return fulfillmentRecords
}

func (e *testEnv) getHistory(t *testing.T, intentId *common.Account) map[string]*history.Record {
	historyRecords, err := e.data.GetAllTransactionHistoryByReference(e.ctx, history.IntentReference, intentId.PublicKey().ToBase58())
	if errors.Is(err, history.ErrNotFound) {
		return nil
	}
	require.NoError(t, err)

	res := make(map[string]*history.Record)
	for _, historyRecord := range historyRecords {
		res[historyRecord.OwnerAccount] = historyRecord
	}
	return res
}

// requireReservedNonces verifies the nonce for every action the client signed
// is reserved with exactly the signature the client submitted
func (e *testEnv) requireReservedNonces(t *testing.T, result *submitIntentResult) {
	require.NotEmpty(t, result.signaturesByAction)

	for actionId, signature := range result.signaturesByAction {
		protoNonce := result.nonceForAction(t, actionId)

		nonceRecord, err := e.data.GetNonce(e.ctx, base58.Encode(protoNonce.Nonce.Value))
		require.NoError(t, err)
		assert.Equal(t, nonce.StateReserved, nonceRecord.State)
		assert.Equal(t, base58.Encode(signature.Value), nonceRecord.Signature)
		assert.Equal(t, base58.Encode(protoNonce.Blockhash.Value), nonceRecord.Blockhash)
		assert.Nil(t, nonceRecord.ClaimNodeID)
		assert.Nil(t, nonceRecord.ClaimExpiresAt)
	}
}

func (r *submitIntentResult) nonceForAction(t *testing.T, actionId uint32) *transactionpb.NoncedTransactionMetadata {
	for _, serverParameter := range r.serverParameters {
		if serverParameter.ActionId == actionId {
			require.Len(t, serverParameter.Nonces, 1)
			return serverParameter.Nonces[0]
		}
	}
	require.Fail(t, "no server parameter for action")
	return nil
}

func requireServerParameterNonces(t *testing.T, serverParameters []*transactionpb.ServerParameter, expectedNonceCounts ...int) {
	require.Len(t, serverParameters, len(expectedNonceCounts))
	for i, expected := range expectedNonceCounts {
		assert.EqualValues(t, i, serverParameters[i].ActionId)
		assert.Len(t, serverParameters[i].Nonces, expected)
	}
}

//
// Full persisted state verification
//

// dbSnapshot captures every record SubmitIntent can write that's reachable
// from a set of owners, along with global record counts that catch writes the
// owners can't reach
type dbSnapshot struct {
	owners []*common.Account

	intents      map[string]*intent.Record
	actions      map[string]*action.Record
	fulfillments map[string]*fulfillment.Record
	accountInfos map[string]*account.Record
	timelocks    map[string]*timelock.Record
	balances     map[string]*balance.Record
	history      map[uint64]*history.Record

	fulfillmentCount   uint64
	timelockCount      uint64
	reservedNonceCount uint64
	taskCount          uint64
}

func actionKey(record *action.Record) string {
	return fmt.Sprintf("%s:%d", record.Intent, record.ActionId)
}

func fulfillmentKey(record *fulfillment.Record) string {
	return fmt.Sprintf("%s:%d:%d", record.Intent, record.ActionId, record.FulfillmentOrderingIndex)
}

func historyKey(record *history.Record) string {
	return fmt.Sprintf("%s:%s:%d", record.ReferenceId, record.OwnerAccount, record.Type)
}

func (e *testEnv) snapshot(t *testing.T, owners ...*common.Account) *dbSnapshot {
	snapshot := &dbSnapshot{
		owners:       owners,
		intents:      make(map[string]*intent.Record),
		actions:      make(map[string]*action.Record),
		fulfillments: make(map[string]*fulfillment.Record),
		accountInfos: make(map[string]*account.Record),
		timelocks:    make(map[string]*timelock.Record),
		balances:     make(map[string]*balance.Record),
		history:      make(map[uint64]*history.Record),
	}

	for _, owner := range owners {
		ownerAddress := owner.PublicKey().ToBase58()

		intentRecords, err := e.data.GetAllIntentsByOwner(e.ctx, ownerAddress, query.WithLimit(1000))
		if err != intent.ErrIntentNotFound {
			require.NoError(t, err)
		}
		for _, intentRecord := range intentRecords {
			snapshot.intents[intentRecord.IntentId] = intentRecord

			intentId, err := common.NewAccountFromPublicKeyString(intentRecord.IntentId)
			require.NoError(t, err)
			for _, actionRecord := range e.getActions(t, intentId) {
				snapshot.actions[actionKey(actionRecord)] = actionRecord
			}
			for _, fulfillmentRecord := range e.getFulfillments(t, intentId) {
				snapshot.fulfillments[fulfillmentKey(fulfillmentRecord)] = fulfillmentRecord
			}
		}

		tokenAccounts := make(map[string]struct{})
		balanceRecords, err := e.data.GetAllBalancesByOwner(e.ctx, ownerAddress)
		if err != balance.ErrRecordNotFound {
			require.NoError(t, err)
		}
		for _, balanceRecord := range balanceRecords {
			snapshot.balances[balanceRecord.TokenAccount] = balanceRecord
			tokenAccounts[balanceRecord.TokenAccount] = struct{}{}
		}
		accountInfoRecordsByMintAndType, err := e.data.GetLatestAccountInfosByOwnerAddress(e.ctx, ownerAddress)
		if err != account.ErrAccountInfoNotFound {
			require.NoError(t, err)
		}
		for _, accountInfoRecordsByType := range accountInfoRecordsByMintAndType {
			for _, accountInfoRecords := range accountInfoRecordsByType {
				for _, accountInfoRecord := range accountInfoRecords {
					tokenAccounts[accountInfoRecord.TokenAccount] = struct{}{}
				}
			}
		}
		for tokenAccount := range tokenAccounts {
			accountInfoRecord, err := e.data.GetAccountInfoByTokenAddress(e.ctx, tokenAccount)
			require.NoError(t, err)
			snapshot.accountInfos[tokenAccount] = accountInfoRecord

			timelockRecord, err := e.data.GetTimelockByVault(e.ctx, tokenAccount)
			if err == timelock.ErrTimelockNotFound {
				continue
			}
			require.NoError(t, err)
			snapshot.timelocks[tokenAccount] = timelockRecord
		}

		historyRecords, err := e.data.GetAllTransactionHistoryByOwner(e.ctx, ownerAddress, query.WithLimit(1000))
		if !errors.Is(err, history.ErrNotFound) {
			require.NoError(t, err)
		}
		for _, historyRecord := range historyRecords {
			snapshot.history[historyRecord.Id] = historyRecord
		}
	}

	for _, state := range []fulfillment.State{fulfillment.StateUnknown, fulfillment.StatePending, fulfillment.StateConfirmed, fulfillment.StateFailed, fulfillment.StateRevoked} {
		count, err := e.data.GetFulfillmentCountByState(e.ctx, state)
		require.NoError(t, err)
		snapshot.fulfillmentCount += count
	}
	for _, state := range []timelock_token_v1.TimelockState{timelock_token_v1.StateUnknown, timelock_token_v1.StateUnlocked, timelock_token_v1.StateWaitingForTimeout, timelock_token_v1.StateLocked, timelock_token_v1.StateClosed} {
		count, err := e.data.GetTimelockCountByState(e.ctx, state)
		require.NoError(t, err)
		snapshot.timelockCount += count
	}
	e.nonceVmsMu.Lock()
	for vmAddress := range e.nonceVms {
		count, err := e.data.GetNonceCountByState(e.ctx, nonce.EnvironmentVm, vmAddress, nonce.StateReserved)
		require.NoError(t, err)
		snapshot.reservedNonceCount += count
	}
	e.nonceVmsMu.Unlock()
	for _, state := range []task.State{task.StateUnknown, task.StatePending, task.StateConfirmed, task.StateFailed} {
		count, err := e.data.GetTaskCountByState(e.ctx, state)
		require.NoError(t, err)
		snapshot.taskCount += count
	}

	return snapshot
}

// dbChanges is the complete set of records created or modified by a
// submission. Expected records leave DB-assigned fields (IDs, versions and
// timestamps) unset.
type dbChanges struct {
	intents      []*intent.Record
	actions      []*action.Record
	fulfillments []*fulfillment.Record
	accountInfos []*account.Record
	timelocks    []*timelock.Record
	balances     []*balance.Record
	history      []*history.Record
	newTasks     int
}

// changedRecords returns records in after that are new or differ from before,
// failing on any record that disappeared
func changedRecords[K comparable, V any](t *testing.T, kind string, before, after map[K]V) (changed []V, created int) {
	for key := range before {
		_, ok := after[key]
		assert.True(t, ok, "%s record %v was deleted", kind, key)
	}
	for key, afterRecord := range after {
		beforeRecord, ok := before[key]
		if !ok {
			created++
			changed = append(changed, afterRecord)
		} else if !reflect.DeepEqual(beforeRecord, afterRecord) {
			changed = append(changed, afterRecord)
		}
	}
	return changed, created
}

func sortedBy[V any](records []V, key func(V) string) []V {
	sorted := append([]V(nil), records...)
	sort.Slice(sorted, func(i, j int) bool {
		return key(sorted[i]) < key(sorted[j])
	})
	return sorted
}

func requireRecent(t *testing.T, ts time.Time, field string) {
	assert.False(t, ts.IsZero(), "%s is not set", field)
	assert.WithinDuration(t, time.Now(), ts, time.Minute, "%s is not recent", field)
}

// requireChanges verifies the records created or modified since the snapshot
// are exactly the expected ones, field for field. DB-assigned fields are
// verified to be set and are then excluded from the comparison.
func (e *testEnv) requireChanges(t *testing.T, before *dbSnapshot, expected *dbChanges) {
	after := e.snapshot(t, before.owners...)

	intents, _ := changedRecords(t, "intent", before.intents, after.intents)
	for _, record := range intents {
		assert.NotZero(t, record.Id)
		assert.NotZero(t, record.Version)
		requireRecent(t, record.CreatedAt, "intent created at")
	}
	intentsById := make(map[string]*intent.Record)
	for _, record := range after.intents {
		intentsById[record.IntentId] = record
	}

	actions, _ := changedRecords(t, "action", before.actions, after.actions)
	fulfillments, createdFulfillments := changedRecords(t, "fulfillment", before.fulfillments, after.fulfillments)
	var createdClientSignedFulfillments uint64
	for _, record := range fulfillments {
		assert.NotZero(t, record.Id)
		if _, ok := before.fulfillments[fulfillmentKey(record)]; !ok && record.VirtualSignature != nil {
			createdClientSignedFulfillments++
		}
	}

	accountInfos, _ := changedRecords(t, "account info", before.accountInfos, after.accountInfos)
	for _, record := range accountInfos {
		assert.NotZero(t, record.Id)
		requireRecent(t, record.CreatedAt, "account info created at")
		requireRecent(t, record.DepositsLastSyncedAt, "account info deposits last synced at")
	}

	timelocks, createdTimelocks := changedRecords(t, "timelock", before.timelocks, after.timelocks)
	balances, _ := changedRecords(t, "balance", before.balances, after.balances)

	historyRecords, _ := changedRecords(t, "history", before.history, after.history)
	for _, record := range historyRecords {
		assert.NotZero(t, record.Id)
		if record.ReferenceType == history.IntentReference {
			// Gift card claims transition the issuer's record, which is
			// timestamped by the issuing intent
			if intentRecord, ok := intentsById[record.ReferenceId]; ok {
				assert.True(t, intentRecord.CreatedAt.Equal(record.CreatedAt), "history record isn't timestamped by its intent")
			}
		}
	}

	normalizedIntents := make([]*intent.Record, len(intents))
	for i, record := range intents {
		cloned := record.Clone()
		cloned.Id, cloned.Version, cloned.CreatedAt = 0, 0, time.Time{}
		normalizedIntents[i] = &cloned
	}
	normalizedActions := make([]*action.Record, len(actions))
	for i, record := range actions {
		cloned := record.Clone()
		cloned.Id, cloned.Version, cloned.CreatedAt = 0, 0, time.Time{}
		normalizedActions[i] = &cloned
	}
	normalizedFulfillments := make([]*fulfillment.Record, len(fulfillments))
	for i, record := range fulfillments {
		cloned := record.Clone()
		cloned.Id, cloned.Version, cloned.CreatedAt = 0, 0, time.Time{}
		normalizedFulfillments[i] = &cloned
	}
	normalizedAccountInfos := make([]*account.Record, len(accountInfos))
	for i, record := range accountInfos {
		cloned := record.Clone()
		cloned.Id, cloned.CreatedAt, cloned.DepositsLastSyncedAt = 0, time.Time{}, time.Time{}
		normalizedAccountInfos[i] = &cloned
	}
	normalizedTimelocks := make([]*timelock.Record, len(timelocks))
	for i, record := range timelocks {
		cloned := record.Clone()
		cloned.Id, cloned.LastUpdatedAt = 0, time.Time{}
		normalizedTimelocks[i] = cloned
	}
	normalizedBalances := make([]*balance.Record, len(balances))
	for i, record := range balances {
		cloned := record.Clone()
		cloned.Id, cloned.UpdatedAt = 0, time.Time{}
		normalizedBalances[i] = &cloned
	}
	normalizedHistory := make([]*history.Record, len(historyRecords))
	for i, record := range historyRecords {
		cloned := record.Clone()
		cloned.Id, cloned.Version, cloned.CreatedAt, cloned.UpdatedAt = 0, 0, time.Time{}, time.Time{}
		normalizedHistory[i] = &cloned
	}

	intentKey := func(record *intent.Record) string { return record.IntentId }
	accountInfoKey := func(record *account.Record) string { return record.TokenAccount }
	timelockKey := func(record *timelock.Record) string { return record.VaultAddress }
	balanceKey := func(record *balance.Record) string { return record.TokenAccount }

	assert.Equal(t, sortedBy(expected.intents, intentKey), sortedBy(normalizedIntents, intentKey), "intent records")
	assert.Equal(t, sortedBy(expected.actions, actionKey), sortedBy(normalizedActions, actionKey), "action records")
	assert.Equal(t, sortedBy(expected.fulfillments, fulfillmentKey), sortedBy(normalizedFulfillments, fulfillmentKey), "fulfillment records")
	assert.Equal(t, sortedBy(expected.accountInfos, accountInfoKey), sortedBy(normalizedAccountInfos, accountInfoKey), "account info records")
	assert.Equal(t, sortedBy(expected.timelocks, timelockKey), sortedBy(normalizedTimelocks, timelockKey), "timelock records")
	assert.Equal(t, sortedBy(expected.balances, balanceKey), sortedBy(normalizedBalances, balanceKey), "balance records")
	assert.Equal(t, sortedBy(expected.history, historyKey), sortedBy(normalizedHistory, historyKey), "history records")

	// Nothing was written outside of what's reachable from the owners
	assert.EqualValues(t, createdFulfillments, after.fulfillmentCount-before.fulfillmentCount, "unexpected fulfillment records")
	assert.EqualValues(t, createdTimelocks, after.timelockCount-before.timelockCount, "unexpected timelock records")
	assert.Equal(t, createdClientSignedFulfillments, after.reservedNonceCount-before.reservedNonceCount, "unexpected nonce reservations")
	assert.EqualValues(t, expected.newTasks, after.taskCount-before.taskCount, "unexpected task records")
}

// requireNoChanges verifies nothing was persisted since the snapshot
func (e *testEnv) requireNoChanges(t *testing.T, before *dbSnapshot) {
	e.requireChanges(t, before, &dbChanges{})
}

//
// Expected records
//

func expectedOpenAccountAction(intentRecord *intent.Record, actionId uint32, vault *common.Account) *action.Record {
	return &action.Record{
		Intent:     intentRecord.IntentId,
		IntentType: intentRecord.IntentType,
		ActionId:   actionId,
		ActionType: action.OpenAccount,
		Source:     vault.PublicKey().ToBase58(),
		State:      action.StatePending,
	}
}

func expectedTransferAction(intentRecord *intent.Record, actionId uint32, source, destination *common.Account, quantity uint64) *action.Record {
	return &action.Record{
		Intent:      intentRecord.IntentId,
		IntentType:  intentRecord.IntentType,
		ActionId:    actionId,
		ActionType:  action.NoPrivacyTransfer,
		Source:      source.PublicKey().ToBase58(),
		Destination: pointer.String(destination.PublicKey().ToBase58()),
		Quantity:    pointer.Uint64(quantity),
		State:       action.StatePending,
	}
}

func expectedFeePaymentAction(intentRecord *intent.Record, actionId uint32, source, destination *common.Account, quantity uint64) *action.Record {
	record := expectedTransferAction(intentRecord, actionId, source, destination, quantity)
	feeType := transactionpb.FeePaymentAction_CREATE_ON_SEND_WITHDRAWAL
	record.FeeType = &feeType
	return record
}

func expectedWithdrawAction(intentRecord *intent.Record, actionId uint32, source, destination *common.Account, quantity uint64) *action.Record {
	record := expectedTransferAction(intentRecord, actionId, source, destination, quantity)
	record.ActionType = action.NoPrivacyWithdraw
	return record
}

// expectedAutoReturnAction is deferred, with no quantity until it's scheduled
func expectedAutoReturnAction(intentRecord *intent.Record, actionId uint32, source, destination *common.Account) *action.Record {
	record := expectedWithdrawAction(intentRecord, actionId, source, destination, 0)
	record.Quantity = nil
	record.State = action.StateUnknown
	return record
}

func expectedOpenAccountFulfillment(intentRecord *intent.Record, actionId uint32, vault *common.Account) *fulfillment.Record {
	return &fulfillment.Record{
		Intent:                  intentRecord.IntentId,
		IntentType:              intentRecord.IntentType,
		ActionId:                actionId,
		ActionType:              action.OpenAccount,
		FulfillmentType:         fulfillment.InitializeLockedTimelockAccount,
		Source:                  vault.PublicKey().ToBase58(),
		IntentOrderingIndex:     intentRecord.Id,
		ActionOrderingIndex:     actionId,
		DisableActiveScheduling: true,
		State:                   fulfillment.StateUnknown,
	}
}

// expectedClientSignedFulfillment uses the nonce the server provided and the
// signature the client submitted for the action
func expectedClientSignedFulfillment(t *testing.T, intentRecord *intent.Record, result *submitIntentResult, actionId uint32, actionType action.Type, fulfillmentType fulfillment.Type, source, destination *common.Account) *fulfillment.Record {
	protoNonce := result.nonceForAction(t, actionId)
	signature, ok := result.signaturesByAction[actionId]
	require.True(t, ok)

	return &fulfillment.Record{
		Intent:              intentRecord.IntentId,
		IntentType:          intentRecord.IntentType,
		ActionId:            actionId,
		ActionType:          actionType,
		FulfillmentType:     fulfillmentType,
		VirtualSignature:    pointer.String(base58.Encode(signature.Value)),
		VirtualNonce:        pointer.String(base58.Encode(protoNonce.Nonce.Value)),
		VirtualBlockhash:    pointer.String(base58.Encode(protoNonce.Blockhash.Value)),
		Source:              source.PublicKey().ToBase58(),
		Destination:         pointer.String(destination.PublicKey().ToBase58()),
		IntentOrderingIndex: intentRecord.Id,
		ActionOrderingIndex: actionId,
		State:               fulfillment.StateUnknown,
	}
}

func expectedTransferFulfillment(t *testing.T, intentRecord *intent.Record, result *submitIntentResult, actionId uint32, source, destination *common.Account) *fulfillment.Record {
	return expectedClientSignedFulfillment(t, intentRecord, result, actionId, action.NoPrivacyTransfer, fulfillment.NoPrivacyTransferWithAuthority, source, destination)
}

func expectedWithdrawFulfillment(t *testing.T, intentRecord *intent.Record, result *submitIntentResult, actionId uint32, source, destination *common.Account) *fulfillment.Record {
	return expectedClientSignedFulfillment(t, intentRecord, result, actionId, action.NoPrivacyWithdraw, fulfillment.NoPrivacyWithdraw, source, destination)
}

// expectedAutoReturnFulfillment is ordered after everything else and is only
// scheduled on demand
func expectedAutoReturnFulfillment(t *testing.T, intentRecord *intent.Record, result *submitIntentResult, actionId uint32, source, destination *common.Account) *fulfillment.Record {
	record := expectedWithdrawFulfillment(t, intentRecord, result, actionId, source, destination)
	record.IntentOrderingIndex = math.MaxInt64
	record.ActionOrderingIndex = 0
	record.DisableActiveScheduling = true
	return record
}

func (e *testEnv) expectedAccountInfo(t *testing.T, owner, authority, mint *common.Account, accountType commonpb.AccountType, index uint64) *account.Record {
	return &account.Record{
		OwnerAccount:            owner.PublicKey().ToBase58(),
		AuthorityAccount:        authority.PublicKey().ToBase58(),
		TokenAccount:            e.getVault(t, authority, mint).PublicKey().ToBase58(),
		MintAccount:             mint.PublicKey().ToBase58(),
		AccountType:             accountType,
		Index:                   index,
		RequiresAutoReturnCheck: accountType == commonpb.AccountType_REMOTE_SEND_GIFT_CARD,
	}
}

// expectedTimelock is a newly opened timelock with no known blockchain state
func (e *testEnv) expectedTimelock(t *testing.T, authority, mint *common.Account) *timelock.Record {
	timelockAccounts := e.getTimelockAccounts(t, authority, mint)
	return &timelock.Record{
		Address:           timelockAccounts.State.PublicKey().ToBase58(),
		Bump:              timelockAccounts.StateBump,
		VaultAddress:      timelockAccounts.Vault.PublicKey().ToBase58(),
		VaultBump:         timelockAccounts.VaultBump,
		VaultOwner:        authority.PublicKey().ToBase58(),
		VaultState:        timelock_token_v1.StateUnknown,
		DepositPdaAddress: timelockAccounts.VmDepositAccounts.Pda.PublicKey().ToBase58(),
		DepositPdaBump:    timelockAccounts.VmDepositAccounts.PdaBump,
		SwapPdaAddress:    timelockAccounts.VmSwapAccounts.Pda.PublicKey().ToBase58(),
		SwapPdaBump:       timelockAccounts.VmSwapAccounts.PdaBump,
	}
}

func expectedBalance(tokenAccount, owner, mint *common.Account, quarks uint64, usdCostBasis int64, isOpen bool) *balance.Record {
	return &balance.Record{
		TokenAccount: tokenAccount.PublicKey().ToBase58(),
		OwnerAccount: owner.PublicKey().ToBase58(),
		MintAccount:  mint.PublicKey().ToBase58(),
		Quarks:       quarks,
		UsdCostBasis: usdCostBasis,
		IsOpen:       isOpen,
		IsLocked:     true,
	}
}

func usdCostBasis(usd float64) int64 {
	return balance.UsdCostBasisFromFloat(usd)
}

func expectedHistory(intentRecord *intent.Record, historyType history.Type, owner, counterparty *common.Account, exchangeCurrency currency_lib.Code, nativeAmount float64, quantity uint64) *history.Record {
	record := &history.Record{
		ReferenceId:      intentRecord.IntentId,
		ReferenceType:    history.IntentReference,
		Type:             historyType,
		OwnerAccount:     owner.PublicKey().ToBase58(),
		ExchangeCurrency: exchangeCurrency,
		NativeAmount:     nativeAmount,
		MintAccount:      intentRecord.MintAccount,
		Quantity:         quantity,
		State:            history.StateCompleted,
	}
	if counterparty != nil {
		record.CounterpartyOwnerAccount = pointer.String(counterparty.PublicKey().ToBase58())
	}
	return record
}

// requireAutoOpenIntent finds the single synthetic OpenAccounts intent that
// opened the owner's primary account for the mint
func (e *testEnv) requireAutoOpenIntent(t *testing.T, owner, mint *common.Account) *intent.Record {
	intentRecords, err := e.data.GetAllIntentsByOwner(e.ctx, owner.PublicKey().ToBase58(), query.WithLimit(1000))
	require.NoError(t, err)

	var res []*intent.Record
	for _, intentRecord := range intentRecords {
		if intentRecord.IntentType == intent.OpenAccounts && intentRecord.MintAccount == mint.PublicKey().ToBase58() {
			res = append(res, intentRecord)
		}
	}
	require.Len(t, res, 1)
	return res[0]
}

// expectedAutoOpenChanges are the records for a primary account auto-opened by
// a synthetic OpenAccounts intent
func (e *testEnv) expectedAutoOpenChanges(t *testing.T, openIntentRecord *intent.Record, owner, mint *common.Account) (*intent.Record, *action.Record, *fulfillment.Record, *account.Record, *timelock.Record) {
	vault := e.getVault(t, owner, mint)
	return &intent.Record{
			IntentId:              openIntentRecord.IntentId,
			IntentType:            intent.OpenAccounts,
			MintAccount:           mint.PublicKey().ToBase58(),
			InitiatorOwnerAccount: owner.PublicKey().ToBase58(),
			OpenAccountsMetadata:  &intent.OpenAccountsMetadata{},
			State:                 intent.StatePending,
		},
		expectedOpenAccountAction(openIntentRecord, 0, vault),
		expectedOpenAccountFulfillment(openIntentRecord, 0, vault),
		e.expectedAccountInfo(t, owner, owner, mint, commonpb.AccountType_PRIMARY, 0),
		e.expectedTimelock(t, owner, mint)
}

//
// Intent request builders
//

func sendPublicPaymentMetadata(typed *transactionpb.SendPublicPaymentMetadata) *transactionpb.Metadata {
	return &transactionpb.Metadata{
		Type: &transactionpb.Metadata_SendPublicPayment{
			SendPublicPayment: typed,
		},
	}
}

func receivePaymentsPubliclyMetadata(typed *transactionpb.ReceivePaymentsPubliclyMetadata) *transactionpb.Metadata {
	return &transactionpb.Metadata{
		Type: &transactionpb.Metadata_ReceivePaymentsPublicly{
			ReceivePaymentsPublicly: typed,
		},
	}
}

func sendPublicMultiPaymentMetadata(typed *transactionpb.SendPublicMultiPaymentMetadata) *transactionpb.Metadata {
	return &transactionpb.Metadata{
		Type: &transactionpb.Metadata_SendPublicMultiPayment{
			SendPublicMultiPayment: typed,
		},
	}
}

func paymentNativeAmount(req *submitIntentRequest) float64 {
	return req.metadata.GetSendPublicPayment().GetClientExchangeData().NativeAmount
}

func clientExchangeData(exchangeData *transactionpb.VerifiedExchangeData) *transactionpb.SendPublicPaymentMetadata_ClientExchangeData {
	return &transactionpb.SendPublicPaymentMetadata_ClientExchangeData{
		ClientExchangeData: exchangeData,
	}
}

func (e *testEnv) paymentMetadata(t *testing.T, sender, destination, mint *common.Account, exchangeData *transactionpb.VerifiedExchangeData) *transactionpb.SendPublicPaymentMetadata {
	return &transactionpb.SendPublicPaymentMetadata{
		Source:       e.getVault(t, sender, mint).ToProto(),
		Destination:  destination.ToProto(),
		ExchangeData: clientExchangeData(exchangeData),
		Mint:         mint.ToProto(),
	}
}

// directPaymentRequest builds a valid direct payment from the sender's primary
// account to the destination
func (e *testEnv) directPaymentRequest(t *testing.T, sender, destination, mint *common.Account, quarks uint64) *submitIntentRequest {
	return &submitIntentRequest{
		intentId: e.newIntentId(t),
		owner:    sender,
		metadata: sendPublicPaymentMetadata(e.paymentMetadata(t, sender, destination, mint, e.exchangeData(t, mint, quarks))),
		actions: []*transactionpb.Action{
			e.transferAction(t, 0, sender, destination, mint, quarks),
		},
	}
}

// withdrawalRequest builds a valid withdrawal from the sender's primary account
// to the destination, with an optional fee payment taken out of quarks
func (e *testEnv) withdrawalRequest(t *testing.T, sender, destination, destinationOwner, mint *common.Account, quarks, feeQuarks uint64) *submitIntentRequest {
	typedMetadata := e.paymentMetadata(t, sender, destination, mint, e.exchangeData(t, mint, quarks))
	typedMetadata.IsWithdrawal = true
	if destinationOwner != nil {
		typedMetadata.DestinationOwner = destinationOwner.ToProto()
	}

	actions := []*transactionpb.Action{
		e.transferAction(t, 0, sender, destination, mint, quarks-feeQuarks),
	}
	if feeQuarks > 0 {
		actions = append(actions, e.feePaymentAction(t, 1, sender, mint, feeQuarks))
	}

	return &submitIntentRequest{
		intentId: e.newIntentId(t),
		owner:    sender,
		metadata: sendPublicPaymentMetadata(typedMetadata),
		actions:  actions,
	}
}

// giftCardRequest builds a valid indirect send from the sender's primary account
// to a new remote send gift card
func (e *testEnv) giftCardRequest(t *testing.T, sender, giftCard, mint *common.Account, quarks uint64) *submitIntentRequest {
	senderVault := e.getVault(t, sender, mint)
	giftCardVault := e.getVault(t, giftCard, mint)

	typedMetadata := e.paymentMetadata(t, sender, giftCardVault, mint, e.exchangeData(t, mint, quarks))
	typedMetadata.IsIndirectSend = true

	return &submitIntentRequest{
		intentId: e.newIntentId(t),
		owner:    sender,
		metadata: sendPublicPaymentMetadata(typedMetadata),
		actions: []*transactionpb.Action{
			e.openAccountAction(t, 0, commonpb.AccountType_REMOTE_SEND_GIFT_CARD, giftCard, giftCard, mint, 0),
			e.transferAction(t, 1, sender, giftCardVault, mint, quarks),
			e.withdrawAction(t, 2, giftCard, senderVault, mint, quarks, true),
		},
	}
}

// issueGiftCard issues a new gift card funded by the sender, returning the
// gift card owner and the issuing intent ID
func (e *testEnv) issueGiftCard(t *testing.T, sender, mint *common.Account, quarks uint64) (*common.Account, *common.Account) {
	giftCard := e.newAccount(t)
	req := e.giftCardRequest(t, sender, giftCard, mint, quarks)
	e.submitIntent(t, req).requireSuccess(t)
	return giftCard, req.intentId
}

// claimGiftCardRequest builds a valid gift card claim into the receiver's
// primary account
func (e *testEnv) claimGiftCardRequest(t *testing.T, receiver, giftCard, mint *common.Account, quarks uint64) *submitIntentRequest {
	return &submitIntentRequest{
		intentId: e.newIntentId(t),
		owner:    receiver,
		metadata: receivePaymentsPubliclyMetadata(&transactionpb.ReceivePaymentsPubliclyMetadata{
			Source:         e.getVault(t, giftCard, mint).ToProto(),
			Quarks:         quarks,
			IsIndirectSend: true,
			Mint:           mint.ToProto(),
		}),
		actions: []*transactionpb.Action{
			e.withdrawAction(t, 0, giftCard, e.getVault(t, receiver, mint), mint, quarks, false),
		},
	}
}

type testMultiPayment struct {
	destination      *common.Account
	destinationOwner *common.Account
	quarks           uint64
}

// multiPaymentRequest builds a valid multi payment from the sender's primary
// account, where every payment uses the same verified exchange data
func (e *testEnv) multiPaymentRequest(t *testing.T, sender, mint *common.Account, payments ...testMultiPayment) *submitIntentRequest {
	exchangeRate := e.signedExchangeRate(t, currency_lib.USD, 1.0, time.Now())
	reserveState := e.signedReserveState(t, mint, currencycreator.ToQuarks(defaultLaunchpadSupply), time.Now())

	typedMetadata := &transactionpb.SendPublicMultiPaymentMetadata{
		Source: e.getVault(t, sender, mint).ToProto(),
		Mint:   mint.ToProto(),
	}

	var actions []*transactionpb.Action
	for i, payment := range payments {
		var exchangeData *transactionpb.VerifiedExchangeData
		if common.IsCoreMint(mint) {
			exchangeData = e.coreMintExchangeData(t, payment.quarks, exchangeRate)
		} else {
			exchangeData = e.launchpadExchangeData(t, mint, payment.quarks, exchangeRate, reserveState)
		}

		protoPayment := &transactionpb.SendPublicMultiPaymentMetadata_Payment{
			Destination: payment.destination.ToProto(),
			ExchangeData: &transactionpb.SendPublicMultiPaymentMetadata_Payment_ClientExchangeData{
				ClientExchangeData: exchangeData,
			},
		}
		if payment.destinationOwner != nil {
			protoPayment.DestinationOwner = payment.destinationOwner.ToProto()
		}
		typedMetadata.Payments = append(typedMetadata.Payments, protoPayment)

		actions = append(actions, e.transferAction(t, uint32(i), sender, payment.destination, mint, payment.quarks))
	}

	return &submitIntentRequest{
		intentId: e.newIntentId(t),
		owner:    sender,
		metadata: sendPublicMultiPaymentMetadata(typedMetadata),
		actions:  actions,
	}
}

// newSwapRecord saves a swap that is pending funding by the provided intent
func (e *testEnv) newSwapRecord(t *testing.T, owner, fromMint, toMint, fundingId *common.Account, quarks uint64, state swap.State) *swap.Record {
	swapRecord := &swap.Record{
		SwapId:               testutil.NewRandomAccount(t).PublicKey().ToBase58(),
		Kind:                 swap.KindReserve,
		Owner:                owner.PublicKey().ToBase58(),
		FromMint:             fromMint.PublicKey().ToBase58(),
		ToMint:               toMint.PublicKey().ToBase58(),
		SwapAmount:           quarks,
		FeeAmount:            0,
		FundingId:            fundingId.PublicKey().ToBase58(),
		FundingSource:        swap.FundingSourceSubmitIntent,
		Nonce:                testutil.NewRandomAccount(t).PublicKey().ToBase58(),
		Blockhash:            testutil.NewRandomAccount(t).PublicKey().ToBase58(),
		ProofSignature:       testutil.NewRandomAccount(t).PublicKey().ToBase58(),
		TransactionSignature: testutil.NewRandomAccount(t).PublicKey().ToBase58(),
		State:                state,
		CreatedAt:            time.Now(),
	}
	require.NoError(t, e.data.SaveSwap(e.ctx, swapRecord))
	return swapRecord
}

// getVmSwapPda gets the owner's VM swap PDA for the mint, which is the
// destination owner for swap funding withdrawals
func (e *testEnv) getVmSwapPda(t *testing.T, owner, mint *common.Account) *common.Account {
	return e.getTimelockAccounts(t, owner, mint).VmSwapAccounts.Pda
}

func toAta(t *testing.T, owner, mint *common.Account) *common.Account {
	ata, err := owner.ToAssociatedTokenAccount(mint)
	require.NoError(t, err)
	return ata
}

//
// Generic SubmitIntent behaviour, using payments as the vehicle
//

func TestSubmitIntent_Disabled(t *testing.T) {
	env := setupTestEnv(t, withSubmitIntentDisabled())

	sender := env.newFundedUser(t, common.ToCoreMintQuarks(100))
	receiver := env.newUser(t)

	req := env.directPaymentRequest(t, sender, env.getVault(t, receiver, common.CoreMintAccount), common.CoreMintAccount, common.ToCoreMintQuarks(1))
	before := env.snapshot(t, sender, receiver)
	env.submitIntent(t, req).requireStatusCode(t, codes.Unavailable)
	env.requireNoChanges(t, before)
}

func TestSubmitIntent_FirstRequestMustSubmitActions(t *testing.T) {
	env := setupTestEnv(t)

	ctx, cancel := context.WithTimeout(env.ctx, defaultTestStreamTimeout)
	defer cancel()

	stream, err := env.client.SubmitIntent(ctx)
	require.NoError(t, err)

	err = stream.Send(&transactionpb.SubmitIntentRequest{
		Request: &transactionpb.SubmitIntentRequest_SubmitSignatures_{
			SubmitSignatures: &transactionpb.SubmitIntentRequest_SubmitSignatures{
				Signatures: []*commonpb.Signature{{Value: make([]byte, ed25519.SignatureSize)}},
			},
		},
	})
	if err != io.EOF {
		require.NoError(t, err)
	}

	_, err = stream.Recv()
	testutil.AssertStatusErrorWithCode(t, err, codes.InvalidArgument)
}

func TestSubmitIntent_ActionIdsMustMatchIndex(t *testing.T) {
	env := setupTestEnv(t)

	sender := env.newFundedUser(t, common.ToCoreMintQuarks(100))
	receiver := env.newUser(t)

	req := env.directPaymentRequest(t, sender, env.getVault(t, receiver, common.CoreMintAccount), common.CoreMintAccount, common.ToCoreMintQuarks(1))
	req.actions[0].Id = 1

	before := env.snapshot(t, sender, receiver)
	env.submitIntent(t, req).requireStatusCode(t, codes.InvalidArgument)
	env.requireNoChanges(t, before)
}

func TestSubmitIntent_ProtoValidation(t *testing.T) {
	env := setupTestEnv(t)

	sender := env.newFundedUser(t, common.ToCoreMintQuarks(100))
	receiver1 := env.newUser(t)
	receiver2 := env.newUser(t)

	for _, tc := range []struct {
		name  string
		build func() *submitIntentRequest
	}{
		{
			name: "missing metadata type",
			build: func() *submitIntentRequest {
				req := env.directPaymentRequest(t, sender, env.getVault(t, receiver1, common.CoreMintAccount), common.CoreMintAccount, common.ToCoreMintQuarks(1))
				req.metadata = &transactionpb.Metadata{}
				return req
			},
		},
		{
			name: "no actions",
			build: func() *submitIntentRequest {
				req := env.directPaymentRequest(t, sender, env.getVault(t, receiver1, common.CoreMintAccount), common.CoreMintAccount, common.ToCoreMintQuarks(1))
				req.actions = nil
				return req
			},
		},
		{
			name: "zero quark transfer",
			build: func() *submitIntentRequest {
				req := env.directPaymentRequest(t, sender, env.getVault(t, receiver1, common.CoreMintAccount), common.CoreMintAccount, common.ToCoreMintQuarks(1))
				req.actions[0].GetNoPrivacyTransfer().Amount = 0
				return req
			},
		},
		{
			name: "receive payments publicly that isn't an indirect send",
			build: func() *submitIntentRequest {
				req := env.claimGiftCardRequest(t, receiver1, env.newAccount(t), common.CoreMintAccount, common.ToCoreMintQuarks(1))
				req.metadata.GetReceivePaymentsPublicly().IsIndirectSend = false
				return req
			},
		},
		{
			name: "multi payment with a single payment",
			build: func() *submitIntentRequest {
				req := env.multiPaymentRequest(t, sender, common.CoreMintAccount,
					testMultiPayment{destination: env.getVault(t, receiver1, common.CoreMintAccount), quarks: common.ToCoreMintQuarks(1)},
				)
				return req
			},
		},
		{
			name: "multi payment with too many payments",
			build: func() *submitIntentRequest {
				var payments []testMultiPayment
				for range 11 {
					payments = append(payments, testMultiPayment{destination: env.getVault(t, receiver2, common.CoreMintAccount), quarks: common.ToCoreMintQuarks(1)})
				}
				return env.multiPaymentRequest(t, sender, common.CoreMintAccount, payments...)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := tc.build()
			before := env.snapshot(t, sender, receiver1, receiver2)
			env.submitIntent(t, req).requireStatusCode(t, codes.InvalidArgument)
			env.requireNoChanges(t, before)
		})
	}
}

func TestSubmitIntent_InvalidOwnerSignature(t *testing.T) {
	env := setupTestEnv(t)

	sender := env.newFundedUser(t, common.ToCoreMintQuarks(100))
	receiver := env.newUser(t)

	req := env.directPaymentRequest(t, sender, env.getVault(t, receiver, common.CoreMintAccount), common.CoreMintAccount, common.ToCoreMintQuarks(1))
	req.submitActionsSigner = env.newAccount(t)

	before := env.snapshot(t, sender, receiver)
	env.submitIntent(t, req).requireStatusCode(t, codes.Unauthenticated)
	env.requireNoChanges(t, before)
	env.requireBalance(t, env.getVault(t, sender, common.CoreMintAccount), common.ToCoreMintQuarks(100))
}

func TestSubmitIntent_InvalidOpenAccountAuthoritySignature(t *testing.T) {
	env := setupTestEnv(t)

	owner := env.newAccount(t)
	openAction := env.openAccountAction(t, 0, commonpb.AccountType_PRIMARY, owner, owner, common.CoreMintAccount, 0)
	openAction.GetOpenAccount().AuthoritySignature = env.signProto(t, env.newAccount(t), openAction.GetOpenAccount())

	req := &submitIntentRequest{
		intentId: env.newIntentId(t),
		owner:    owner,
		metadata: openAccountsMetadata(transactionpb.OpenAccountsMetadata_USER, common.CoreMintAccount),
		actions:  []*transactionpb.Action{openAction},
	}
	before := env.snapshot(t, owner)
	env.submitIntent(t, req).requireStatusCode(t, codes.Unauthenticated)
	env.requireNoChanges(t, before)
}

func TestSubmitIntent_UnknownOwner(t *testing.T) {
	env := setupTestEnv(t)

	sender := env.newAccount(t)
	receiver := env.newUser(t)

	req := env.directPaymentRequest(t, sender, env.getVault(t, receiver, common.CoreMintAccount), common.CoreMintAccount, common.ToCoreMintQuarks(1))
	before := env.snapshot(t, sender, receiver)
	env.submitIntent(t, req).requireDenied(t, "unexpected owner account")
	env.requireNoChanges(t, before)
}

func TestSubmitIntent_GiftCardOwnerCannotSubmitNonReceiveIntents(t *testing.T) {
	env := setupTestEnv(t)

	sender := env.newFundedUser(t, common.ToCoreMintQuarks(100))
	receiver := env.newUser(t)
	giftCard, _ := env.issueGiftCard(t, sender, common.CoreMintAccount, common.ToCoreMintQuarks(10))

	// The gift card owner attempts to pay out of the gift card directly
	req := &submitIntentRequest{
		intentId: env.newIntentId(t),
		owner:    giftCard,
		metadata: sendPublicPaymentMetadata(env.paymentMetadata(
			t,
			giftCard,
			env.getVault(t, receiver, common.CoreMintAccount),
			common.CoreMintAccount,
			env.exchangeData(t, common.CoreMintAccount, common.ToCoreMintQuarks(10)),
		)),
		actions: []*transactionpb.Action{
			env.transferAction(t, 0, giftCard, env.getVault(t, receiver, common.CoreMintAccount), common.CoreMintAccount, common.ToCoreMintQuarks(10)),
		},
	}

	before := env.snapshot(t, sender, receiver, giftCard)

	// todo: SubmitIntent returns this validation error directly instead of via
	//       handleSubmitIntentError, so it surfaces as an unknown gRPC error
	//       rather than an INVALID_INTENT response
	result := env.submitIntent(t, req)
	if result.err == nil {
		require.NotNil(t, result.resp)
		assert.Nil(t, result.resp.GetSuccess())
	}
	env.requireNoChanges(t, before)
	env.requireBalance(t, env.getVault(t, giftCard, common.CoreMintAccount), common.ToCoreMintQuarks(10))
}

func TestSubmitIntent_IntentIdReuse(t *testing.T) {
	env := setupTestEnv(t)

	sender := env.newFundedUser(t, common.ToCoreMintQuarks(100))
	receiver := env.newUser(t)
	receiverVault := env.getVault(t, receiver, common.CoreMintAccount)

	req := env.directPaymentRequest(t, sender, receiverVault, common.CoreMintAccount, common.ToCoreMintQuarks(1))
	env.submitIntent(t, req).requireSuccess(t)

	before := env.snapshot(t, sender, receiver)

	// Resubmitting the exact same intent
	resubmitted := env.directPaymentRequest(t, sender, receiverVault, common.CoreMintAccount, common.ToCoreMintQuarks(1))
	resubmitted.intentId = req.intentId
	env.submitIntent(t, resubmitted).requireStaleState(t, "intent already exists")

	// Reusing the intent ID for a different intent type
	openReq := &submitIntentRequest{
		intentId: req.intentId,
		owner:    sender,
		metadata: openAccountsMetadata(transactionpb.OpenAccountsMetadata_POOL, common.CoreMintAccount),
		actions: []*transactionpb.Action{
			env.openAccountAction(t, 0, commonpb.AccountType_POOL, sender, env.newAccount(t), common.CoreMintAccount, 0),
		},
	}
	env.submitIntent(t, openReq).requireStaleState(t, "intent already exists")

	env.requireNoChanges(t, before)

	intentRecord := env.requireIntent(t, req.intentId)
	assert.Equal(t, intent.SendPublicPayment, intentRecord.IntentType)
	env.requireBalance(t, env.getVault(t, sender, common.CoreMintAccount), common.ToCoreMintQuarks(99))
	env.requireBalance(t, receiverVault, common.ToCoreMintQuarks(1))
}

func TestSubmitIntent_UnsupportedMint(t *testing.T) {
	env := setupTestEnv(t)

	sender := env.newFundedUser(t, common.ToCoreMintQuarks(100))
	receiver := env.newUser(t)

	unavailableMint := testutil.SetupLaunchpadCurrency(t, env.data, reserve_memory.New(), holder_memory.New())
	metadataRecord, err := env.data.GetCurrencyMetadata(env.ctx, unavailableMint.PublicKey().ToBase58())
	require.NoError(t, err)
	metadataRecord.State = currency.MetadataStateWaitingForInitialPurchase
	require.NoError(t, env.data.SaveCurrencyMetadata(env.ctx, metadataRecord))

	for _, mint := range []*common.Account{
		env.newAccount(t),
		unavailableMint,
	} {
		req := &submitIntentRequest{
			intentId: env.newIntentId(t),
			owner:    sender,
			metadata: sendPublicPaymentMetadata(&transactionpb.SendPublicPaymentMetadata{
				Source:       env.getVault(t, sender, common.CoreMintAccount).ToProto(),
				Destination:  env.getVault(t, receiver, common.CoreMintAccount).ToProto(),
				ExchangeData: clientExchangeData(env.exchangeData(t, common.CoreMintAccount, common.ToCoreMintQuarks(1))),
				Mint:         mint.ToProto(),
			}),
			actions: []*transactionpb.Action{
				env.transferAction(t, 0, sender, env.getVault(t, receiver, common.CoreMintAccount), common.CoreMintAccount, common.ToCoreMintQuarks(1)),
			},
		}
		before := env.snapshot(t, sender, receiver)
		env.submitIntent(t, req).requireInvalidIntent(t, "mint account must be the core mint or a launchpad currency")
		env.requireNoChanges(t, before)
	}
}

func TestSubmitIntent_AppMetadata(t *testing.T) {
	env := setupTestEnv(t)

	sender := env.newFundedUser(t, common.ToCoreMintQuarks(100))
	receiver := env.newUser(t)
	senderVault := env.getVault(t, sender, common.CoreMintAccount)
	receiverVault := env.getVault(t, receiver, common.CoreMintAccount)
	quarks := common.ToCoreMintQuarks(1)

	for i, appMetadata := range [][]byte{
		[]byte("app-specific metadata"),
		nil,
	} {
		req := env.directPaymentRequest(t, sender, receiverVault, common.CoreMintAccount, quarks)
		if appMetadata != nil {
			req.metadata.AppMetadata = &transactionpb.AppMetadata{Value: appMetadata}
		}

		before := env.snapshot(t, sender, receiver)
		result := env.submitIntent(t, req)
		result.requireSuccess(t)

		intentRecord := env.requireIntent(t, req.intentId)
		sent := expectedHistory(intentRecord, history.DirectlySent, sender, receiver, currency_lib.USD, 1.0, quarks)
		sent.AppMetadata = appMetadata
		received := expectedHistory(intentRecord, history.DirectlyReceived, receiver, sender, currency_lib.USD, 1.0, quarks)
		received.AppMetadata = appMetadata

		env.requireChanges(t, before, &dbChanges{
			intents: []*intent.Record{{
				IntentId:              req.intentId.PublicKey().ToBase58(),
				IntentType:            intent.SendPublicPayment,
				MintAccount:           common.CoreMintAccount.PublicKey().ToBase58(),
				InitiatorOwnerAccount: sender.PublicKey().ToBase58(),
				SendPublicPaymentMetadata: &intent.SendPublicPaymentMetadata{
					DestinationOwnerAccount: receiver.PublicKey().ToBase58(),
					DestinationTokenAccount: receiverVault.PublicKey().ToBase58(),
					Quantity:                quarks,
					ExchangeCurrency:        currency_lib.USD,
					ExchangeRate:            1.0,
					NativeAmount:            1.0,
					UsdMarketValue:          1.0,
				},
				AppMetadata: appMetadata,
				State:       intent.StatePending,
			}},
			actions:      []*action.Record{expectedTransferAction(intentRecord, 0, senderVault, receiverVault, quarks)},
			fulfillments: []*fulfillment.Record{expectedTransferFulfillment(t, intentRecord, result, 0, senderVault, receiverVault)},
			balances: []*balance.Record{
				expectedBalance(senderVault, sender, common.CoreMintAccount, common.ToCoreMintQuarks(99-uint64(i)), -usdCostBasis(float64(i+1)), true),
				expectedBalance(receiverVault, receiver, common.CoreMintAccount, quarks*uint64(i+1), usdCostBasis(float64(i+1)), true),
			},
			history: []*history.Record{sent, received},
		})
	}
}

func TestSubmitIntent_IntegrationAllowCreation(t *testing.T) {
	for _, tc := range []struct {
		name          string
		err           error
		expectedCode  *transactionpb.SubmitIntentResponse_Error_Code
		expectedGrpc  codes.Code
		expectedError string
	}{
		{
			name:          "denied",
			err:           NewIntentDeniedError("denied by app"),
			expectedCode:  transactionpb.SubmitIntentResponse_Error_DENIED.Enum(),
			expectedError: "denied by app",
		},
		{
			name:          "invalid",
			err:           NewIntentValidationError("invalid for app"),
			expectedCode:  transactionpb.SubmitIntentResponse_Error_INVALID_INTENT.Enum(),
			expectedError: "invalid for app",
		},
		{
			name:          "stale state",
			err:           NewStaleStateError("stale for app"),
			expectedCode:  transactionpb.SubmitIntentResponse_Error_STALE_STATE.Enum(),
			expectedError: "stale for app",
		},
		{
			name:         "unexpected failure",
			err:          errors.New("unexpected failure"),
			expectedGrpc: codes.Internal,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env := setupTestEnv(t)

			sender := env.newFundedUser(t, common.ToCoreMintQuarks(100))
			receiver := env.newUser(t)
			senderVault := env.getVault(t, sender, common.CoreMintAccount)

			env.submitIntentIntegration.setAllowCreationErr(tc.err)

			req := env.directPaymentRequest(t, sender, env.getVault(t, receiver, common.CoreMintAccount), common.CoreMintAccount, common.ToCoreMintQuarks(1))
			before := env.snapshot(t, sender, receiver)
			result := env.submitIntent(t, req)
			if tc.expectedCode != nil {
				result.requireError(t, *tc.expectedCode, tc.expectedError)
			} else {
				result.requireStatusCode(t, tc.expectedGrpc)
			}
			assert.Empty(t, result.serverParameters)

			env.requireNoChanges(t, before)
			env.requireBalance(t, senderVault, common.ToCoreMintQuarks(100))
			assert.Empty(t, env.submitIntentIntegration.getSuccesses())

			// The integration sees the fully populated intent
			env.submitIntentIntegration.mu.Lock()
			require.Len(t, env.submitIntentIntegration.allowCreationArgs, 1)
			seen := env.submitIntentIntegration.allowCreationArgs[0]
			env.submitIntentIntegration.mu.Unlock()
			assert.Equal(t, req.intentId.PublicKey().ToBase58(), seen.IntentId)
			assert.Equal(t, intent.SendPublicPayment, seen.IntentType)
			assert.Equal(t, sender.PublicKey().ToBase58(), seen.InitiatorOwnerAccount)
			require.NotNil(t, seen.SendPublicPaymentMetadata)
			assert.Equal(t, common.ToCoreMintQuarks(1), seen.SendPublicPaymentMetadata.Quantity)

			// Once the integration allows it, the intent goes through
			env.submitIntentIntegration.setAllowCreationErr(nil)
			req = env.directPaymentRequest(t, sender, env.getVault(t, receiver, common.CoreMintAccount), common.CoreMintAccount, common.ToCoreMintQuarks(1))
			env.submitIntent(t, req).requireSuccess(t)
		})
	}
}

func TestSubmitIntent_IntegrationOnSuccess(t *testing.T) {
	env := setupTestEnv(t)

	sender := env.newFundedUser(t, common.ToCoreMintQuarks(100))
	receiver := env.newUser(t)

	req := env.directPaymentRequest(t, sender, env.getVault(t, receiver, common.CoreMintAccount), common.CoreMintAccount, common.ToCoreMintQuarks(1))
	env.submitIntent(t, req).requireSuccess(t)

	require.Eventually(t, func() bool {
		successes := env.submitIntentIntegration.getSuccesses()
		return len(successes) == 1 && successes[0] == req.intentId.PublicKey().ToBase58()
	}, time.Second, 10*time.Millisecond)

	// Failed intents never trigger the callback
	failed := env.directPaymentRequest(t, sender, env.getVault(t, receiver, common.CoreMintAccount), common.CoreMintAccount, common.ToCoreMintQuarks(1000))
	env.submitIntent(t, failed).requireInvalidIntent(t, "insufficient balance")
	time.Sleep(100 * time.Millisecond)
	assert.Len(t, env.submitIntentIntegration.getSuccesses(), 1)
}

func TestSubmitIntent_ScheduledTasks(t *testing.T) {
	env := setupTestEnv(t)

	sender := env.newFundedUser(t, common.ToCoreMintQuarks(100))
	receiver := env.newUser(t)

	env.submitIntentIntegration.tasksToSchedule = func(intentRecord *intent.Record) []*task.Record {
		return []*task.Record{
			{TaskId: intentRecord.IntentId + "-1", Type: 1, Data: []byte("first"), ReferenceId: &intentRecord.IntentId},
			{TaskId: intentRecord.IntentId + "-2", Type: 2, Data: []byte("second"), ReferenceId: &intentRecord.IntentId},
		}
	}

	req := env.directPaymentRequest(t, sender, env.getVault(t, receiver, common.CoreMintAccount), common.CoreMintAccount, common.ToCoreMintQuarks(1))
	before := env.snapshot(t, sender, receiver)
	result := env.submitIntent(t, req)
	result.requireSuccess(t)

	intentId := req.intentId.PublicKey().ToBase58()
	expectedTasks := map[string]*task.Record{
		intentId + "-1": {TaskId: intentId + "-1", Type: 1, Data: []byte("first"), ReferenceId: pointer.String(intentId)},
		intentId + "-2": {TaskId: intentId + "-2", Type: 2, Data: []byte("second"), ReferenceId: pointer.String(intentId)},
	}

	// Executed on the fast path with the scheduled contents
	for range expectedTasks {
		select {
		case executed := <-env.taskExecutor.executed:
			expected, ok := expectedTasks[executed.TaskId]
			require.True(t, ok)
			assert.Equal(t, expected.Type, executed.Type)
			assert.Equal(t, expected.Data, executed.Data)
			assert.Equal(t, expected.ReferenceId, executed.ReferenceId)
		case <-time.After(time.Second):
			require.Fail(t, "timed out waiting for task execution")
		}
	}

	// Persisted exactly as scheduled, and confirmed after execution
	for taskId, expected := range expectedTasks {
		var taskRecord *task.Record
		require.Eventually(t, func() bool {
			var err error
			taskRecord, err = env.data.GetTaskById(env.ctx, taskId)
			return err == nil && taskRecord.State == task.StateConfirmed
		}, time.Second, 10*time.Millisecond)

		assert.NotZero(t, taskRecord.Id)
		requireRecent(t, taskRecord.CreatedAt, "task created at")
		actual := taskRecord.Clone()
		actual.Id, actual.Version, actual.CreatedAt, actual.NextAttemptAt = 0, 0, time.Time{}, time.Time{}
		expected.State = task.StateConfirmed
		assert.Equal(t, expected, &actual)
	}

	intentRecord := env.requireIntent(t, req.intentId)
	senderVault := env.getVault(t, sender, common.CoreMintAccount)
	receiverVault := env.getVault(t, receiver, common.CoreMintAccount)
	env.requireChanges(t, before, &dbChanges{
		intents: []*intent.Record{{
			IntentId:              intentId,
			IntentType:            intent.SendPublicPayment,
			MintAccount:           common.CoreMintAccount.PublicKey().ToBase58(),
			InitiatorOwnerAccount: sender.PublicKey().ToBase58(),
			SendPublicPaymentMetadata: &intent.SendPublicPaymentMetadata{
				DestinationOwnerAccount: receiver.PublicKey().ToBase58(),
				DestinationTokenAccount: receiverVault.PublicKey().ToBase58(),
				Quantity:                common.ToCoreMintQuarks(1),
				ExchangeCurrency:        currency_lib.USD,
				ExchangeRate:            1.0,
				NativeAmount:            1.0,
				UsdMarketValue:          1.0,
			},
			State: intent.StatePending,
		}},
		actions:      []*action.Record{expectedTransferAction(intentRecord, 0, senderVault, receiverVault, common.ToCoreMintQuarks(1))},
		fulfillments: []*fulfillment.Record{expectedTransferFulfillment(t, intentRecord, result, 0, senderVault, receiverVault)},
		balances: []*balance.Record{
			expectedBalance(senderVault, sender, common.CoreMintAccount, common.ToCoreMintQuarks(99), -usdCostBasis(1), true),
			expectedBalance(receiverVault, receiver, common.CoreMintAccount, common.ToCoreMintQuarks(1), usdCostBasis(1), true),
		},
		history: []*history.Record{
			expectedHistory(intentRecord, history.DirectlySent, sender, receiver, currency_lib.USD, 1.0, common.ToCoreMintQuarks(1)),
			expectedHistory(intentRecord, history.DirectlyReceived, receiver, sender, currency_lib.USD, 1.0, common.ToCoreMintQuarks(1)),
		},
		newTasks: 2,
	})
}

func TestSubmitIntent_ScheduledTasksFailure(t *testing.T) {
	env := setupTestEnv(t)

	sender := env.newFundedUser(t, common.ToCoreMintQuarks(100))
	receiver := env.newUser(t)

	env.submitIntentIntegration.getTasksErr = errors.New("failure getting tasks")

	req := env.directPaymentRequest(t, sender, env.getVault(t, receiver, common.CoreMintAccount), common.CoreMintAccount, common.ToCoreMintQuarks(1))
	env.submitIntent(t, req).requireStatusCode(t, codes.Internal)

	select {
	case <-env.taskExecutor.executed:
		require.Fail(t, "unexpected task execution")
	case <-time.After(100 * time.Millisecond):
	}
	assert.Empty(t, env.submitIntentIntegration.getSuccesses())
}

func TestSubmitIntent_InvalidScheduledTask(t *testing.T) {
	env := setupTestEnv(t)

	sender := env.newFundedUser(t, common.ToCoreMintQuarks(100))
	receiver := env.newUser(t)

	env.submitIntentIntegration.tasksToSchedule = func(intentRecord *intent.Record) []*task.Record {
		return []*task.Record{{TaskId: intentRecord.IntentId}} // Missing type
	}

	req := env.directPaymentRequest(t, sender, env.getVault(t, receiver, common.CoreMintAccount), common.CoreMintAccount, common.ToCoreMintQuarks(1))
	env.submitIntent(t, req).requireStatusCode(t, codes.Internal)
	assert.Empty(t, env.submitIntentIntegration.getSuccesses())
}

func TestSubmitIntent_SignatureSubmission(t *testing.T) {
	for _, tc := range []struct {
		name             string
		mutateSignatures func([]*commonpb.Signature) []*commonpb.Signature
		expectedReason   string
	}{
		{
			name: "missing signature",
			mutateSignatures: func(signatures []*commonpb.Signature) []*commonpb.Signature {
				return signatures[:1]
			},
			expectedReason: ErrMissingSignature.Error(),
		},
		{
			name: "too many signatures",
			mutateSignatures: func(signatures []*commonpb.Signature) []*commonpb.Signature {
				return append(signatures, signatures[0])
			},
			expectedReason: ErrTooManySignatures.Error(),
		},
		{
			name: "out of order signatures",
			mutateSignatures: func(signatures []*commonpb.Signature) []*commonpb.Signature {
				return []*commonpb.Signature{signatures[1], signatures[0]}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env := setupTestEnv(t)

			sender := env.newFundedUser(t, common.ToCoreMintQuarks(100))
			senderVault := env.getVault(t, sender, common.CoreMintAccount)
			giftCard := env.newAccount(t)

			req := env.giftCardRequest(t, sender, giftCard, common.CoreMintAccount, common.ToCoreMintQuarks(10))
			req.mutateSignatures = tc.mutateSignatures

			before := env.snapshot(t, sender, giftCard)
			result := env.submitIntent(t, req)
			result.requireError(t, transactionpb.SubmitIntentResponse_Error_SIGNATURE_ERROR, tc.expectedReason)
			requireServerParameterNonces(t, result.serverParameters, 0, 1, 1)

			env.requireNoChanges(t, before)
			env.requireBalance(t, senderVault, common.ToCoreMintQuarks(100))
			_, err := env.data.GetAccountInfoByTokenAddress(env.ctx, env.getVault(t, giftCard, common.CoreMintAccount).PublicKey().ToBase58())
			assert.Equal(t, account.ErrAccountInfoNotFound, err)

			// Nonces are released for reuse
			for _, serverParameter := range result.serverParameters {
				for _, protoNonce := range serverParameter.Nonces {
					nonceRecord, err := env.data.GetNonce(env.ctx, base58.Encode(protoNonce.Nonce.Value))
					require.NoError(t, err)
					assert.NotEqual(t, nonce.StateReserved, nonceRecord.State)
					assert.Empty(t, nonceRecord.Signature)
				}
			}
		})
	}
}

func TestSubmitIntent_InvalidVirtualInstructionSignature(t *testing.T) {
	env := setupTestEnv(t)

	sender := env.newFundedUser(t, common.ToCoreMintQuarks(100))
	giftCard := env.newAccount(t)
	maliciousAccount := env.newAccount(t)

	req := env.giftCardRequest(t, sender, giftCard, common.CoreMintAccount, common.ToCoreMintQuarks(10))
	var original []*commonpb.Signature
	req.mutateSignatures = func(signatures []*commonpb.Signature) []*commonpb.Signature {
		original = signatures
		return []*commonpb.Signature{
			signatures[0],
			{Value: ed25519.Sign(maliciousAccount.PrivateKey().ToBytes(), []byte("not the virtual instruction"))},
		}
	}

	before := env.snapshot(t, sender, giftCard)
	result := env.submitIntent(t, req)
	result.requireError(t, transactionpb.SubmitIntentResponse_Error_SIGNATURE_ERROR, "")

	errorDetails := result.resp.GetError().ErrorDetails
	require.Len(t, errorDetails, 1)
	invalidSignature := errorDetails[0].GetInvalidSignature()
	require.NotNil(t, invalidSignature)
	assert.EqualValues(t, 2, invalidSignature.ActionId)
	require.NotNil(t, invalidSignature.GetExpectedVixnHash())
	assert.NotEqual(t, original[1].Value, invalidSignature.ProvidedSignature.Value)

	// The expected hash is what the gift card authority signed
	assert.True(t, ed25519.Verify(giftCard.PublicKey().ToBytes(), invalidSignature.GetExpectedVixnHash().Value, original[1].Value))

	env.requireNoChanges(t, before)
	env.requireBalance(t, env.getVault(t, sender, common.CoreMintAccount), common.ToCoreMintQuarks(100))
}

func TestSubmitIntent_ClientReceiveTimeout(t *testing.T) {
	env := setupTestEnv(t, withClientReceiveTimeout(250*time.Millisecond))

	sender := env.newFundedUser(t, common.ToCoreMintQuarks(100))
	receiver := env.newUser(t)

	req := env.directPaymentRequest(t, sender, env.getVault(t, receiver, common.CoreMintAccount), common.CoreMintAccount, common.ToCoreMintQuarks(1))
	req.withholdSignatures = true

	before := env.snapshot(t, sender, receiver)
	result := env.submitIntent(t, req)
	requireServerParameterNonces(t, result.serverParameters, 1)
	result.requireStatusCode(t, codes.DeadlineExceeded)

	env.requireNoChanges(t, before)
	env.requireBalance(t, env.getVault(t, sender, common.CoreMintAccount), common.ToCoreMintQuarks(100))
}

func TestSubmitIntent_NoAvailableNonces(t *testing.T) {
	env := setupTestEnv(t)

	// Launchpad currency VM without any nonces
	mint := testutil.SetupLaunchpadCurrency(t, env.data, reserve_memory.New(), holder_memory.New())

	sender := env.newFundedUser(t, common.ToCoreMintQuarks(100))
	env.openAccount(t, sender, sender, mint, commonpb.AccountType_PRIMARY, 0)
	env.credit(t, env.getVault(t, sender, mint), currencycreator.ToQuarks(100))
	receiver := env.newUser(t)
	env.openAccount(t, receiver, receiver, mint, commonpb.AccountType_PRIMARY, 0)

	req := env.directPaymentRequest(t, sender, env.getVault(t, receiver, mint), mint, currencycreator.ToQuarks(10))
	before := env.snapshot(t, sender, receiver)
	env.submitIntent(t, req).requireStatusCode(t, codes.Unavailable)
	env.requireNoChanges(t, before)

	// Nonces become available, and the intent can be submitted
	vmConfig, err := common.GetVmConfigForMint(env.ctx, env.data, mint)
	require.NoError(t, err)
	env.saveNonces(t, vmConfig.Vm, 1)

	req = env.directPaymentRequest(t, sender, env.getVault(t, receiver, mint), mint, currencycreator.ToQuarks(10))
	result := env.submitIntent(t, req)
	result.requireSuccess(t)
	env.requireReservedNonces(t, result)
}

func TestSubmitIntent_BalanceLedgerRejection(t *testing.T) {
	for _, tc := range []struct {
		err          error
		expectStale  bool
		expectedGrpc codes.Code
	}{
		{err: balance.ErrInsufficientBalance, expectStale: true},
		{err: balance.ErrBalanceChanged, expectStale: true},
		{err: balance.ErrAccountClosed, expectStale: true},
		{err: balance.ErrAccountUnlocked, expectedGrpc: codes.Internal},
	} {
		t.Run(tc.err.Error(), func(t *testing.T) {
			env := setupTestEnv(t)

			sender := env.newFundedUser(t, common.ToCoreMintQuarks(100))
			receiver := env.newUser(t)

			env.data.setApplyBalanceDeltasErr(tc.err)

			req := env.directPaymentRequest(t, sender, env.getVault(t, receiver, common.CoreMintAccount), common.CoreMintAccount, common.ToCoreMintQuarks(1))
			result := env.submitIntent(t, req)
			if tc.expectStale {
				result.requireStaleState(t, "race detected")
			} else {
				result.requireStatusCode(t, tc.expectedGrpc)
			}
			assert.Empty(t, env.submitIntentIntegration.getSuccesses())

			env.data.mu.Lock()
			assert.Equal(t, 1, env.data.applyBalanceDeltasCalled)
			env.data.mu.Unlock()
		})
	}
}

//
// OpenAccounts
//

func TestSubmitIntent_OpenAccounts_User_HappyPath(t *testing.T) {
	env := setupTestEnv(t)

	owner := env.newAccount(t)
	intentId := env.newIntentId(t)
	vault := env.getVault(t, owner, common.CoreMintAccount)

	_, err := common.GetOwnerMetadata(env.ctx, env.data, owner)
	require.Equal(t, common.ErrOwnerNotFound, err)

	before := env.snapshot(t, owner)
	result := env.submitIntent(t, &submitIntentRequest{
		intentId: intentId,
		owner:    owner,
		metadata: openAccountsMetadata(transactionpb.OpenAccountsMetadata_USER, common.CoreMintAccount),
		actions: []*transactionpb.Action{
			env.openAccountAction(t, 0, commonpb.AccountType_PRIMARY, owner, owner, common.CoreMintAccount, 0),
		},
	})
	result.requireSuccess(t)
	assert.Empty(t, result.serverParameters)

	intentRecord := env.requireIntent(t, intentId)
	env.requireChanges(t, before, &dbChanges{
		intents: []*intent.Record{{
			IntentId:              intentId.PublicKey().ToBase58(),
			IntentType:            intent.OpenAccounts,
			MintAccount:           common.CoreMintAccount.PublicKey().ToBase58(),
			InitiatorOwnerAccount: owner.PublicKey().ToBase58(),
			OpenAccountsMetadata:  &intent.OpenAccountsMetadata{},
			State:                 intent.StatePending,
		}},
		actions:      []*action.Record{expectedOpenAccountAction(intentRecord, 0, vault)},
		fulfillments: []*fulfillment.Record{expectedOpenAccountFulfillment(intentRecord, 0, vault)},
		accountInfos: []*account.Record{env.expectedAccountInfo(t, owner, owner, common.CoreMintAccount, commonpb.AccountType_PRIMARY, 0)},
		timelocks:    []*timelock.Record{env.expectedTimelock(t, owner, common.CoreMintAccount)},
		balances:     []*balance.Record{expectedBalance(vault, owner, common.CoreMintAccount, 0, 0, true)},
	})

	ownerMetadata, err := common.GetOwnerMetadata(env.ctx, env.data, owner)
	require.NoError(t, err)
	assert.Equal(t, common.OwnerTypeUser12Words, ownerMetadata.Type)
	assert.Equal(t, common.OwnerManagementStateOcpAccount, ownerMetadata.State)
}
func TestSubmitIntent_OpenAccounts_User_LaunchpadMint(t *testing.T) {
	env := setupTestEnv(t)

	mint := env.newLaunchpadMint(t)
	owner := env.newUser(t)
	vault := env.getVault(t, owner, mint)

	intentId := env.newIntentId(t)
	before := env.snapshot(t, owner)
	env.submitIntent(t, &submitIntentRequest{
		intentId: intentId,
		owner:    owner,
		metadata: openAccountsMetadata(transactionpb.OpenAccountsMetadata_USER, mint),
		actions: []*transactionpb.Action{
			env.openAccountAction(t, 0, commonpb.AccountType_PRIMARY, owner, owner, mint, 0),
		},
	}).requireSuccess(t)

	intentRecord := env.requireIntent(t, intentId)
	env.requireChanges(t, before, &dbChanges{
		intents: []*intent.Record{{
			IntentId:              intentId.PublicKey().ToBase58(),
			IntentType:            intent.OpenAccounts,
			MintAccount:           mint.PublicKey().ToBase58(),
			InitiatorOwnerAccount: owner.PublicKey().ToBase58(),
			OpenAccountsMetadata:  &intent.OpenAccountsMetadata{},
			State:                 intent.StatePending,
		}},
		actions:      []*action.Record{expectedOpenAccountAction(intentRecord, 0, vault)},
		fulfillments: []*fulfillment.Record{expectedOpenAccountFulfillment(intentRecord, 0, vault)},
		accountInfos: []*account.Record{env.expectedAccountInfo(t, owner, owner, mint, commonpb.AccountType_PRIMARY, 0)},
		timelocks:    []*timelock.Record{env.expectedTimelock(t, owner, mint)},
		balances:     []*balance.Record{expectedBalance(vault, owner, mint, 0, 0, true)},
	})
}
func TestSubmitIntent_OpenAccounts_User_Noop(t *testing.T) {
	env := setupTestEnv(t)

	mint := env.newLaunchpadMint(t)
	owner := env.newAccount(t)

	for _, mint := range []*common.Account{common.CoreMintAccount, mint} {
		newOpenRequest := func() *submitIntentRequest {
			return &submitIntentRequest{
				intentId: env.newIntentId(t),
				owner:    owner,
				metadata: openAccountsMetadata(transactionpb.OpenAccountsMetadata_USER, mint),
				actions: []*transactionpb.Action{
					env.openAccountAction(t, 0, commonpb.AccountType_PRIMARY, owner, owner, mint, 0),
				},
			}
		}

		req := newOpenRequest()
		env.submitIntent(t, req).requireSuccess(t)
		env.requireIntent(t, req.intentId)

		// Resubmitting with a new intent ID is a no-op that succeeds without
		// persisting anything
		for range 2 {
			before := env.snapshot(t, owner)
			noopReq := newOpenRequest()
			env.submitIntent(t, noopReq).requireSuccess(t)
			env.requireNoIntent(t, noopReq.intentId)
			env.requireNoChanges(t, before)
		}
	}
}
func TestSubmitIntent_OpenAccounts_Pool_HappyPath(t *testing.T) {
	env := setupTestEnv(t)

	owner := env.newUser(t)

	for index := range uint64(3) {
		poolAuthority := env.newAccount(t)
		poolVault := env.getVault(t, poolAuthority, common.CoreMintAccount)

		intentId := env.newIntentId(t)
		before := env.snapshot(t, owner, poolAuthority)
		result := env.submitIntent(t, &submitIntentRequest{
			intentId: intentId,
			owner:    owner,
			metadata: openAccountsMetadata(transactionpb.OpenAccountsMetadata_POOL, common.CoreMintAccount),
			actions: []*transactionpb.Action{
				env.openAccountAction(t, 0, commonpb.AccountType_POOL, owner, poolAuthority, common.CoreMintAccount, index),
			},
		})
		result.requireSuccess(t)
		assert.Empty(t, result.serverParameters)

		intentRecord := env.requireIntent(t, intentId)
		env.requireChanges(t, before, &dbChanges{
			intents: []*intent.Record{{
				IntentId:              intentId.PublicKey().ToBase58(),
				IntentType:            intent.OpenAccounts,
				MintAccount:           common.CoreMintAccount.PublicKey().ToBase58(),
				InitiatorOwnerAccount: owner.PublicKey().ToBase58(),
				OpenAccountsMetadata:  &intent.OpenAccountsMetadata{},
				State:                 intent.StatePending,
			}},
			actions:      []*action.Record{expectedOpenAccountAction(intentRecord, 0, poolVault)},
			fulfillments: []*fulfillment.Record{expectedOpenAccountFulfillment(intentRecord, 0, poolVault)},
			accountInfos: []*account.Record{env.expectedAccountInfo(t, owner, poolAuthority, common.CoreMintAccount, commonpb.AccountType_POOL, index)},
			timelocks:    []*timelock.Record{env.expectedTimelock(t, poolAuthority, common.CoreMintAccount)},
			balances:     []*balance.Record{expectedBalance(poolVault, owner, common.CoreMintAccount, 0, 0, true)},
		})
	}
}
func TestSubmitIntent_OpenAccounts_Pool_Noop(t *testing.T) {
	env := setupTestEnv(t)

	owner := env.newUser(t)
	poolAuthority := env.newAccount(t)

	newOpenRequest := func() *submitIntentRequest {
		return &submitIntentRequest{
			intentId: env.newIntentId(t),
			owner:    owner,
			metadata: openAccountsMetadata(transactionpb.OpenAccountsMetadata_POOL, common.CoreMintAccount),
			actions: []*transactionpb.Action{
				env.openAccountAction(t, 0, commonpb.AccountType_POOL, owner, poolAuthority, common.CoreMintAccount, 0),
			},
		}
	}

	req := newOpenRequest()
	env.submitIntent(t, req).requireSuccess(t)
	env.requireIntent(t, req.intentId)

	before := env.snapshot(t, owner, poolAuthority)
	noopReq := newOpenRequest()
	env.submitIntent(t, noopReq).requireSuccess(t)
	env.requireNoIntent(t, noopReq.intentId)
	env.requireNoChanges(t, before)
}
func TestSubmitIntent_OpenAccounts_Antispam(t *testing.T) {
	env := setupTestEnv(t, withAntispamChecks())

	owner := env.newAccount(t)
	newOpenRequest := func() *submitIntentRequest {
		return &submitIntentRequest{
			intentId: env.newIntentId(t),
			owner:    owner,
			metadata: openAccountsMetadata(transactionpb.OpenAccountsMetadata_USER, common.CoreMintAccount),
			actions: []*transactionpb.Action{
				env.openAccountAction(t, 0, commonpb.AccountType_PRIMARY, owner, owner, common.CoreMintAccount, 0),
			},
		}
	}

	env.antispamIntegration.setAllow(false)
	req := newOpenRequest()
	before := env.snapshot(t, owner)
	env.submitIntent(t, req).requireDenied(t, "antispam guard denied account creation")
	env.requireNoChanges(t, before)
	assert.Equal(t, []string{"open_accounts:USER"}, env.antispamIntegration.getCalls())

	env.antispamIntegration.setAllow(true)
	req = newOpenRequest()
	env.submitIntent(t, req).requireSuccess(t)
	env.requireIntent(t, req.intentId)
}

func TestSubmitIntent_OpenAccounts_Validation(t *testing.T) {
	type fixture struct {
		env   *testEnv
		owner *common.Account
	}

	newUserRequest := func(t *testing.T, f *fixture, actions ...*transactionpb.Action) *submitIntentRequest {
		return &submitIntentRequest{
			intentId: f.env.newIntentId(t),
			owner:    f.owner,
			metadata: openAccountsMetadata(transactionpb.OpenAccountsMetadata_USER, common.CoreMintAccount),
			actions:  actions,
		}
	}

	newPoolRequest := func(t *testing.T, f *fixture, mint *common.Account, actions ...*transactionpb.Action) *submitIntentRequest {
		return &submitIntentRequest{
			intentId: f.env.newIntentId(t),
			owner:    f.owner,
			metadata: openAccountsMetadata(transactionpb.OpenAccountsMetadata_POOL, mint),
			actions:  actions,
		}
	}

	for _, tc := range []struct {
		name           string
		existingUser   bool
		build          func(t *testing.T, f *fixture) *submitIntentRequest
		expectedCode   transactionpb.SubmitIntentResponse_Error_Code
		expectedReason string
	}{
		{
			name: "user: action is not an open account action",
			build: func(t *testing.T, f *fixture) *submitIntentRequest {
				return newUserRequest(t, f, f.env.transferAction(t, 0, f.owner, f.env.newAccount(t), common.CoreMintAccount, 1))
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_INVALID_INTENT,
			expectedReason: "actions[0]: expected an open account action",
		},
		{
			name: "user: too many actions",
			build: func(t *testing.T, f *fixture) *submitIntentRequest {
				return newUserRequest(t, f,
					f.env.openAccountAction(t, 0, commonpb.AccountType_PRIMARY, f.owner, f.owner, common.CoreMintAccount, 0),
					f.env.openAccountAction(t, 1, commonpb.AccountType_POOL, f.owner, f.env.newAccount(t), common.CoreMintAccount, 0),
				)
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_INVALID_INTENT,
			expectedReason: "expected 1 total actions",
		},
		{
			name: "user: account type isn't primary",
			build: func(t *testing.T, f *fixture) *submitIntentRequest {
				return newUserRequest(t, f, f.env.openAccountAction(t, 0, commonpb.AccountType_POOL, f.owner, f.owner, common.CoreMintAccount, 0))
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_INVALID_INTENT,
			expectedReason: "actions[0]: account type must be PRIMARY",
		},
		{
			name: "user: invalid index",
			build: func(t *testing.T, f *fixture) *submitIntentRequest {
				return newUserRequest(t, f, f.env.openAccountAction(t, 0, commonpb.AccountType_PRIMARY, f.owner, f.owner, common.CoreMintAccount, 1))
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_INVALID_INTENT,
			expectedReason: "actions[0]: index must be 0",
		},
		{
			name: "user: authority isn't the owner",
			build: func(t *testing.T, f *fixture) *submitIntentRequest {
				return newUserRequest(t, f, f.env.openAccountAction(t, 0, commonpb.AccountType_PRIMARY, f.owner, f.env.newAccount(t), common.CoreMintAccount, 0))
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_INVALID_INTENT,
			expectedReason: "actions[0]: authority must be",
		},
		{
			name: "user: owner isn't the intent owner",
			build: func(t *testing.T, f *fixture) *submitIntentRequest {
				other := f.env.newAccount(t)
				return newUserRequest(t, f, f.env.openAccountAction(t, 0, commonpb.AccountType_PRIMARY, other, other, common.CoreMintAccount, 0))
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_INVALID_INTENT,
			expectedReason: "actions[0]: owner must be",
		},
		{
			name: "user: token isn't the derived timelock vault",
			build: func(t *testing.T, f *fixture) *submitIntentRequest {
				openAction := f.env.openAccountAction(t, 0, commonpb.AccountType_PRIMARY, f.owner, f.owner, common.CoreMintAccount, 0)
				openAction.GetOpenAccount().Token = f.env.newAccount(t).ToProto()
				return newUserRequest(t, f, openAction)
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_INVALID_INTENT,
			expectedReason: "actions[0]: token must be",
		},
		{
			name: "user: action mint doesn't match intent mint",
			build: func(t *testing.T, f *fixture) *submitIntentRequest {
				mint := f.env.newLaunchpadMint(t)
				return newUserRequest(t, f, f.env.openAccountAction(t, 0, commonpb.AccountType_PRIMARY, f.owner, f.owner, mint, 0))
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_INVALID_INTENT,
			expectedReason: "actions[0]: mint must be " + common.CoreMintAccount.PublicKey().ToBase58(),
		},
		{
			name: "user: account already initiated an unlock",
			build: func(t *testing.T, f *fixture) *submitIntentRequest {
				f.env.data.setBlockchainAccountExists(f.env.getTimelockAccounts(t, f.owner, common.CoreMintAccount).Unlock)
				return newUserRequest(t, f, f.env.openAccountAction(t, 0, commonpb.AccountType_PRIMARY, f.owner, f.owner, common.CoreMintAccount, 0))
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_DENIED,
			expectedReason: "an account being opened has already initiated an unlock",
		},
		{
			name: "user: timelock account already exists",
			build: func(t *testing.T, f *fixture) *submitIntentRequest {
				timelockRecord := f.env.getTimelockAccounts(t, f.owner, common.CoreMintAccount).ToDBRecord()
				require.NoError(t, f.env.data.SaveTimelock(f.env.ctx, timelockRecord))
				return newUserRequest(t, f, f.env.openAccountAction(t, 0, commonpb.AccountType_PRIMARY, f.owner, f.owner, common.CoreMintAccount, 0))
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_STALE_STATE,
			expectedReason: "actions[0]: account is already opened",
		},
		{
			name: "user: closed timelock account cannot be reopened",
			build: func(t *testing.T, f *fixture) *submitIntentRequest {
				timelockRecord := f.env.getTimelockAccounts(t, f.owner, common.CoreMintAccount).ToDBRecord()
				timelockRecord.VaultState = timelock_token_v1.StateClosed
				timelockRecord.Block = 1
				require.NoError(t, f.env.data.SaveTimelock(f.env.ctx, timelockRecord))
				return newUserRequest(t, f, f.env.openAccountAction(t, 0, commonpb.AccountType_PRIMARY, f.owner, f.owner, common.CoreMintAccount, 0))
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_STALE_STATE,
			expectedReason: "actions[0]: account is already closed and won't be reused",
		},
		{
			name: "user: intent ID is reserved to fund a swap",
			build: func(t *testing.T, f *fixture) *submitIntentRequest {
				req := newUserRequest(t, f, f.env.openAccountAction(t, 0, commonpb.AccountType_PRIMARY, f.owner, f.owner, common.CoreMintAccount, 0))
				f.env.newSwapRecord(t, f.owner, common.CoreMintAccount, f.env.newLaunchpadMint(t), req.intentId, 1, swap.StateCreated)
				return req
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_DENIED,
			expectedReason: "intent id reserved to fund swap",
		},
		{
			name: "user: launchpad mint for a new owner",
			build: func(t *testing.T, f *fixture) *submitIntentRequest {
				mint := f.env.newLaunchpadMint(t)
				return &submitIntentRequest{
					intentId: f.env.newIntentId(t),
					owner:    f.owner,
					metadata: openAccountsMetadata(transactionpb.OpenAccountsMetadata_USER, mint),
					actions: []*transactionpb.Action{
						f.env.openAccountAction(t, 0, commonpb.AccountType_PRIMARY, f.owner, f.owner, mint, 0),
					},
				}
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_DENIED,
			expectedReason: "unexpected owner account",
		},
		{
			name: "pool: new owner",
			build: func(t *testing.T, f *fixture) *submitIntentRequest {
				return newPoolRequest(t, f, common.CoreMintAccount, f.env.openAccountAction(t, 0, commonpb.AccountType_POOL, f.owner, f.env.newAccount(t), common.CoreMintAccount, 0))
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_DENIED,
			expectedReason: "unexpected owner account",
		},
		{
			name:         "pool: launchpad mint",
			existingUser: true,
			build: func(t *testing.T, f *fixture) *submitIntentRequest {
				mint := f.env.newLaunchpadMint(t)
				return newPoolRequest(t, f, mint, f.env.openAccountAction(t, 0, commonpb.AccountType_POOL, f.owner, f.env.newAccount(t), mint, 0))
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_DENIED,
			expectedReason: "non-core mint account opening is only supported for the user account set",
		},
		{
			name:         "pool: account type isn't pool",
			existingUser: true,
			build: func(t *testing.T, f *fixture) *submitIntentRequest {
				return newPoolRequest(t, f, common.CoreMintAccount, f.env.openAccountAction(t, 0, commonpb.AccountType_PRIMARY, f.owner, f.env.newAccount(t), common.CoreMintAccount, 0))
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_INVALID_INTENT,
			expectedReason: "actions[0]: account type must be POOL",
		},
		{
			name:         "pool: invalid index",
			existingUser: true,
			build: func(t *testing.T, f *fixture) *submitIntentRequest {
				return newPoolRequest(t, f, common.CoreMintAccount, f.env.openAccountAction(t, 0, commonpb.AccountType_POOL, f.owner, f.env.newAccount(t), common.CoreMintAccount, 1))
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_INVALID_INTENT,
			expectedReason: "actions[0]: index must be 0",
		},
		{
			name:         "pool: index doesn't follow the latest pool",
			existingUser: true,
			build: func(t *testing.T, f *fixture) *submitIntentRequest {
				f.env.openAccount(t, f.owner, f.env.newAccount(t), common.CoreMintAccount, commonpb.AccountType_POOL, 0)
				f.env.openAccount(t, f.owner, f.env.newAccount(t), common.CoreMintAccount, commonpb.AccountType_POOL, 1)
				return newPoolRequest(t, f, common.CoreMintAccount, f.env.openAccountAction(t, 0, commonpb.AccountType_POOL, f.owner, f.env.newAccount(t), common.CoreMintAccount, 1))
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_INVALID_INTENT,
			expectedReason: "actions[0]: index must be 2",
		},
		{
			name:         "pool: authority is the owner",
			existingUser: true,
			build: func(t *testing.T, f *fixture) *submitIntentRequest {
				// The owner's primary account would also be the pool's vault
				openAction := f.env.openAccountAction(t, 0, commonpb.AccountType_POOL, f.owner, f.owner, common.CoreMintAccount, 0)
				return newPoolRequest(t, f, common.CoreMintAccount, openAction)
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_INVALID_INTENT,
			expectedReason: "actions[0]: authority cannot be",
		},
		{
			name:         "pool: unlock already initiated",
			existingUser: true,
			build: func(t *testing.T, f *fixture) *submitIntentRequest {
				poolAuthority := f.env.newAccount(t)
				f.env.data.setBlockchainAccountExists(f.env.getTimelockAccounts(t, poolAuthority, common.CoreMintAccount).Unlock)
				return newPoolRequest(t, f, common.CoreMintAccount, f.env.openAccountAction(t, 0, commonpb.AccountType_POOL, f.owner, poolAuthority, common.CoreMintAccount, 0))
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_DENIED,
			expectedReason: "an account being opened has already initiated an unlock",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env := setupTestEnv(t)

			f := &fixture{env: env}
			if tc.existingUser {
				f.owner = env.newUser(t)
			} else {
				f.owner = env.newAccount(t)
			}

			req := tc.build(t, f)

			before := env.snapshot(t, f.owner)
			env.submitIntent(t, req).requireError(t, tc.expectedCode, tc.expectedReason)
			env.requireNoChanges(t, before)
		})
	}
}

//
// SendPublicPayment
//

func TestSubmitIntent_SendPublicPayment_Direct_HappyPath(t *testing.T) {
	env := setupTestEnv(t)

	sender := env.newFundedUser(t, common.ToCoreMintQuarks(100))
	receiver := env.newUser(t)
	senderVault := env.getVault(t, sender, common.CoreMintAccount)
	receiverVault := env.getVault(t, receiver, common.CoreMintAccount)

	quarks := common.ToCoreMintQuarks(42)
	req := env.directPaymentRequest(t, sender, receiverVault, common.CoreMintAccount, quarks)

	before := env.snapshot(t, sender, receiver)
	result := env.submitIntent(t, req)
	result.requireSuccess(t)
	requireServerParameterNonces(t, result.serverParameters, 1)
	assert.NotNil(t, result.serverParameters[0].GetNoPrivacyTransfer())

	intentRecord := env.requireIntent(t, req.intentId)
	env.requireChanges(t, before, &dbChanges{
		intents: []*intent.Record{{
			IntentId:              req.intentId.PublicKey().ToBase58(),
			IntentType:            intent.SendPublicPayment,
			MintAccount:           common.CoreMintAccount.PublicKey().ToBase58(),
			InitiatorOwnerAccount: sender.PublicKey().ToBase58(),
			SendPublicPaymentMetadata: &intent.SendPublicPaymentMetadata{
				DestinationOwnerAccount: receiver.PublicKey().ToBase58(),
				DestinationTokenAccount: receiverVault.PublicKey().ToBase58(),
				Quantity:                quarks,
				ExchangeCurrency:        currency_lib.USD,
				ExchangeRate:            1.0,
				NativeAmount:            42.0,
				UsdMarketValue:          42.0,
			},
			State: intent.StatePending,
		}},
		actions: []*action.Record{
			expectedTransferAction(intentRecord, 0, senderVault, receiverVault, quarks),
		},
		fulfillments: []*fulfillment.Record{
			expectedTransferFulfillment(t, intentRecord, result, 0, senderVault, receiverVault),
		},
		balances: []*balance.Record{
			expectedBalance(senderVault, sender, common.CoreMintAccount, common.ToCoreMintQuarks(58), -usdCostBasis(42), true),
			expectedBalance(receiverVault, receiver, common.CoreMintAccount, quarks, usdCostBasis(42), true),
		},
		history: []*history.Record{
			expectedHistory(intentRecord, history.DirectlySent, sender, receiver, currency_lib.USD, 42.0, quarks),
			expectedHistory(intentRecord, history.DirectlyReceived, receiver, sender, currency_lib.USD, 42.0, quarks),
		},
	})
	env.requireReservedNonces(t, result)

	require.Eventually(t, func() bool {
		return len(env.submitIntentIntegration.getSuccesses()) == 1
	}, time.Second, 10*time.Millisecond)
}
func TestSubmitIntent_SendPublicPayment_Direct_NonUsdCurrency(t *testing.T) {
	env := setupTestEnv(t)

	sender := env.newFundedUser(t, common.ToCoreMintQuarks(100))
	receiver := env.newUser(t)
	senderVault := env.getVault(t, sender, common.CoreMintAccount)
	receiverVault := env.getVault(t, receiver, common.CoreMintAccount)

	quarks := common.ToCoreMintQuarks(10)
	exchangeRate := env.signedExchangeRate(t, currency_lib.CAD, 1.35, time.Now())

	req := env.directPaymentRequest(t, sender, receiverVault, common.CoreMintAccount, quarks)
	req.metadata.GetSendPublicPayment().ExchangeData = clientExchangeData(env.coreMintExchangeData(t, quarks, exchangeRate))
	nativeAmount := paymentNativeAmount(req)
	usdValue := nativeAmount / 1.35
	require.InDelta(t, 13.5, nativeAmount, 1e-9)
	require.InDelta(t, 10.0, usdValue, 1e-9)

	before := env.snapshot(t, sender, receiver)
	result := env.submitIntent(t, req)
	result.requireSuccess(t)

	intentRecord := env.requireIntent(t, req.intentId)
	env.requireChanges(t, before, &dbChanges{
		intents: []*intent.Record{{
			IntentId:              req.intentId.PublicKey().ToBase58(),
			IntentType:            intent.SendPublicPayment,
			MintAccount:           common.CoreMintAccount.PublicKey().ToBase58(),
			InitiatorOwnerAccount: sender.PublicKey().ToBase58(),
			SendPublicPaymentMetadata: &intent.SendPublicPaymentMetadata{
				DestinationOwnerAccount: receiver.PublicKey().ToBase58(),
				DestinationTokenAccount: receiverVault.PublicKey().ToBase58(),
				Quantity:                quarks,
				ExchangeCurrency:        currency_lib.CAD,
				ExchangeRate:            1.35,
				NativeAmount:            nativeAmount,
				UsdMarketValue:          usdValue,
			},
			State: intent.StatePending,
		}},
		actions: []*action.Record{
			expectedTransferAction(intentRecord, 0, senderVault, receiverVault, quarks),
		},
		fulfillments: []*fulfillment.Record{
			expectedTransferFulfillment(t, intentRecord, result, 0, senderVault, receiverVault),
		},
		balances: []*balance.Record{
			expectedBalance(senderVault, sender, common.CoreMintAccount, common.ToCoreMintQuarks(90), -usdCostBasis(usdValue), true),
			expectedBalance(receiverVault, receiver, common.CoreMintAccount, quarks, usdCostBasis(usdValue), true),
		},
		history: []*history.Record{
			expectedHistory(intentRecord, history.DirectlySent, sender, receiver, currency_lib.CAD, nativeAmount, quarks),
			expectedHistory(intentRecord, history.DirectlyReceived, receiver, sender, currency_lib.CAD, nativeAmount, quarks),
		},
	})
	env.requireReservedNonces(t, result)
}
func TestSubmitIntent_SendPublicPayment_Direct_LaunchpadMint(t *testing.T) {
	env := setupTestEnv(t)

	mint := env.newLaunchpadMint(t)
	sender := env.newFundedUser(t, currencycreator.ToQuarks(1_000), mint)
	receiver := env.newFundedUser(t, 0, mint)
	senderVault := env.getVault(t, sender, mint)
	receiverVault := env.getVault(t, receiver, mint)

	quarks := currencycreator.ToQuarks(123)
	exchangeRate := env.signedExchangeRate(t, currency_lib.CAD, 1.35, time.Now())
	reserveState := env.signedReserveState(t, mint, currencycreator.ToQuarks(defaultLaunchpadSupply), time.Now())
	exchangeData := env.launchpadExchangeData(t, mint, quarks, exchangeRate, reserveState)
	require.Greater(t, exchangeData.NativeAmount, 0.01)

	req := env.directPaymentRequest(t, sender, receiverVault, mint, quarks)
	req.metadata.GetSendPublicPayment().ExchangeData = clientExchangeData(exchangeData)
	nativeAmount := exchangeData.NativeAmount
	usdValue := nativeAmount / 1.35

	before := env.snapshot(t, sender, receiver)
	result := env.submitIntent(t, req)
	result.requireSuccess(t)
	requireServerParameterNonces(t, result.serverParameters, 1)

	intentRecord := env.requireIntent(t, req.intentId)
	env.requireChanges(t, before, &dbChanges{
		intents: []*intent.Record{{
			IntentId:              req.intentId.PublicKey().ToBase58(),
			IntentType:            intent.SendPublicPayment,
			MintAccount:           mint.PublicKey().ToBase58(),
			InitiatorOwnerAccount: sender.PublicKey().ToBase58(),
			SendPublicPaymentMetadata: &intent.SendPublicPaymentMetadata{
				DestinationOwnerAccount: receiver.PublicKey().ToBase58(),
				DestinationTokenAccount: receiverVault.PublicKey().ToBase58(),
				Quantity:                quarks,
				ExchangeCurrency:        currency_lib.CAD,
				ExchangeRate:            nativeAmount / 123, // Per whole token of the launchpad currency
				NativeAmount:            nativeAmount,
				UsdMarketValue:          usdValue,
			},
			State: intent.StatePending,
		}},
		actions: []*action.Record{
			expectedTransferAction(intentRecord, 0, senderVault, receiverVault, quarks),
		},
		fulfillments: []*fulfillment.Record{
			expectedTransferFulfillment(t, intentRecord, result, 0, senderVault, receiverVault),
		},
		balances: []*balance.Record{
			expectedBalance(senderVault, sender, mint, currencycreator.ToQuarks(877), -usdCostBasis(usdValue), true),
			expectedBalance(receiverVault, receiver, mint, quarks, usdCostBasis(usdValue), true),
		},
		history: []*history.Record{
			expectedHistory(intentRecord, history.DirectlySent, sender, receiver, currency_lib.CAD, nativeAmount, quarks),
			expectedHistory(intentRecord, history.DirectlyReceived, receiver, sender, currency_lib.CAD, nativeAmount, quarks),
		},
	})
	env.requireReservedNonces(t, result)
}
func TestSubmitIntent_SendPublicPayment_Direct_ToPool(t *testing.T) {
	env := setupTestEnv(t)

	sender := env.newFundedUser(t, common.ToCoreMintQuarks(100))
	senderVault := env.getVault(t, sender, common.CoreMintAccount)

	// Another user's pool
	receiver := env.newUser(t)
	receiverPoolAuthority := env.newAccount(t)
	env.openAccount(t, receiver, receiverPoolAuthority, common.CoreMintAccount, commonpb.AccountType_POOL, 0)
	receiverPoolVault := env.getVault(t, receiverPoolAuthority, common.CoreMintAccount)

	quarks := common.ToCoreMintQuarks(10)
	req := env.directPaymentRequest(t, sender, receiverPoolVault, common.CoreMintAccount, quarks)
	before := env.snapshot(t, sender, receiver)
	result := env.submitIntent(t, req)
	result.requireSuccess(t)

	intentRecord := env.requireIntent(t, req.intentId)
	env.requireChanges(t, before, &dbChanges{
		intents: []*intent.Record{{
			IntentId:              req.intentId.PublicKey().ToBase58(),
			IntentType:            intent.SendPublicPayment,
			MintAccount:           common.CoreMintAccount.PublicKey().ToBase58(),
			InitiatorOwnerAccount: sender.PublicKey().ToBase58(),
			SendPublicPaymentMetadata: &intent.SendPublicPaymentMetadata{
				DestinationOwnerAccount: receiver.PublicKey().ToBase58(),
				DestinationTokenAccount: receiverPoolVault.PublicKey().ToBase58(),
				Quantity:                quarks,
				ExchangeCurrency:        currency_lib.USD,
				ExchangeRate:            1.0,
				NativeAmount:            10.0,
				UsdMarketValue:          10.0,
			},
			State: intent.StatePending,
		}},
		actions:      []*action.Record{expectedTransferAction(intentRecord, 0, senderVault, receiverPoolVault, quarks)},
		fulfillments: []*fulfillment.Record{expectedTransferFulfillment(t, intentRecord, result, 0, senderVault, receiverPoolVault)},
		balances: []*balance.Record{
			expectedBalance(senderVault, sender, common.CoreMintAccount, common.ToCoreMintQuarks(90), -usdCostBasis(10), true),
			expectedBalance(receiverPoolVault, receiver, common.CoreMintAccount, quarks, usdCostBasis(10), true),
		},
		history: []*history.Record{
			expectedHistory(intentRecord, history.DirectlySent, sender, receiver, currency_lib.USD, 10.0, quarks),
			expectedHistory(intentRecord, history.DirectlyReceived, receiver, sender, currency_lib.USD, 10.0, quarks),
		},
	})

	// The sender's own pool, which is a payment to self with a single history
	// record
	senderPoolAuthority := env.newAccount(t)
	env.openAccount(t, sender, senderPoolAuthority, common.CoreMintAccount, commonpb.AccountType_POOL, 0)
	senderPoolVault := env.getVault(t, senderPoolAuthority, common.CoreMintAccount)

	quarks = common.ToCoreMintQuarks(20)
	req = env.directPaymentRequest(t, sender, senderPoolVault, common.CoreMintAccount, quarks)
	before = env.snapshot(t, sender, receiver)
	result = env.submitIntent(t, req)
	result.requireSuccess(t)

	intentRecord = env.requireIntent(t, req.intentId)
	env.requireChanges(t, before, &dbChanges{
		intents: []*intent.Record{{
			IntentId:              req.intentId.PublicKey().ToBase58(),
			IntentType:            intent.SendPublicPayment,
			MintAccount:           common.CoreMintAccount.PublicKey().ToBase58(),
			InitiatorOwnerAccount: sender.PublicKey().ToBase58(),
			SendPublicPaymentMetadata: &intent.SendPublicPaymentMetadata{
				DestinationOwnerAccount: sender.PublicKey().ToBase58(),
				DestinationTokenAccount: senderPoolVault.PublicKey().ToBase58(),
				Quantity:                quarks,
				ExchangeCurrency:        currency_lib.USD,
				ExchangeRate:            1.0,
				NativeAmount:            20.0,
				UsdMarketValue:          20.0,
			},
			State: intent.StatePending,
		}},
		actions:      []*action.Record{expectedTransferAction(intentRecord, 0, senderVault, senderPoolVault, quarks)},
		fulfillments: []*fulfillment.Record{expectedTransferFulfillment(t, intentRecord, result, 0, senderVault, senderPoolVault)},
		balances: []*balance.Record{
			expectedBalance(senderVault, sender, common.CoreMintAccount, common.ToCoreMintQuarks(70), -usdCostBasis(30), true),
			expectedBalance(senderPoolVault, sender, common.CoreMintAccount, quarks, usdCostBasis(20), true),
		},
		history: []*history.Record{
			expectedHistory(intentRecord, history.DirectlySent, sender, sender, currency_lib.USD, 20.0, quarks),
		},
	})
}
func TestSubmitIntent_SendPublicPayment_Direct_AutoOpensPrimary(t *testing.T) {
	env := setupTestEnv(t)

	mint := env.newLaunchpadMint(t)
	sender := env.newFundedUser(t, currencycreator.ToQuarks(1_000), mint)
	senderVault := env.getVault(t, sender, mint)

	// The receiver is an existing user without a primary account for the mint
	receiver := env.newUser(t)
	receiverVault := env.getVault(t, receiver, mint)

	quarks := currencycreator.ToQuarks(10)
	req := env.directPaymentRequest(t, sender, receiverVault, mint, quarks)
	req.metadata.GetSendPublicPayment().DestinationOwner = receiver.ToProto()
	nativeAmount := paymentNativeAmount(req)

	before := env.snapshot(t, sender, receiver)
	result := env.submitIntent(t, req)
	result.requireSuccess(t)

	intentRecord := env.requireIntent(t, req.intentId)
	openIntentRecord := env.requireAutoOpenIntent(t, receiver, mint)
	assert.Less(t, openIntentRecord.Id, intentRecord.Id)
	openIntent, openAction, openFulfillment, openAccountInfo, openTimelock := env.expectedAutoOpenChanges(t, openIntentRecord, receiver, mint)

	env.requireChanges(t, before, &dbChanges{
		intents: []*intent.Record{
			openIntent,
			{
				IntentId:              req.intentId.PublicKey().ToBase58(),
				IntentType:            intent.SendPublicPayment,
				MintAccount:           mint.PublicKey().ToBase58(),
				InitiatorOwnerAccount: sender.PublicKey().ToBase58(),
				SendPublicPaymentMetadata: &intent.SendPublicPaymentMetadata{
					DestinationOwnerAccount: receiver.PublicKey().ToBase58(),
					DestinationTokenAccount: receiverVault.PublicKey().ToBase58(),
					Quantity:                quarks,
					ExchangeCurrency:        currency_lib.USD,
					ExchangeRate:            nativeAmount / 10,
					NativeAmount:            nativeAmount,
					UsdMarketValue:          nativeAmount,
				},
				State: intent.StatePending,
			},
		},
		actions: []*action.Record{
			openAction,
			expectedTransferAction(intentRecord, 0, senderVault, receiverVault, quarks),
		},
		fulfillments: []*fulfillment.Record{
			openFulfillment,
			expectedTransferFulfillment(t, intentRecord, result, 0, senderVault, receiverVault),
		},
		accountInfos: []*account.Record{openAccountInfo},
		timelocks:    []*timelock.Record{openTimelock},
		balances: []*balance.Record{
			expectedBalance(senderVault, sender, mint, currencycreator.ToQuarks(990), -usdCostBasis(nativeAmount), true),
			expectedBalance(receiverVault, receiver, mint, quarks, usdCostBasis(nativeAmount), true),
		},
		history: []*history.Record{
			expectedHistory(intentRecord, history.DirectlySent, sender, receiver, currency_lib.USD, nativeAmount, quarks),
			expectedHistory(intentRecord, history.DirectlyReceived, receiver, sender, currency_lib.USD, nativeAmount, quarks),
		},
	})
	env.requireReservedNonces(t, result)

	// Subsequent payments go to the now existing primary account without an
	// additional auto-open
	req = env.directPaymentRequest(t, sender, receiverVault, mint, quarks)
	req.metadata.GetSendPublicPayment().DestinationOwner = receiver.ToProto()
	before = env.snapshot(t, sender, receiver)
	result = env.submitIntent(t, req)
	result.requireSuccess(t)

	intentRecord = env.requireIntent(t, req.intentId)
	env.requireChanges(t, before, &dbChanges{
		intents: []*intent.Record{{
			IntentId:              req.intentId.PublicKey().ToBase58(),
			IntentType:            intent.SendPublicPayment,
			MintAccount:           mint.PublicKey().ToBase58(),
			InitiatorOwnerAccount: sender.PublicKey().ToBase58(),
			SendPublicPaymentMetadata: &intent.SendPublicPaymentMetadata{
				DestinationOwnerAccount: receiver.PublicKey().ToBase58(),
				DestinationTokenAccount: receiverVault.PublicKey().ToBase58(),
				Quantity:                quarks,
				ExchangeCurrency:        currency_lib.USD,
				ExchangeRate:            nativeAmount / 10,
				NativeAmount:            nativeAmount,
				UsdMarketValue:          nativeAmount,
			},
			State: intent.StatePending,
		}},
		actions:      []*action.Record{expectedTransferAction(intentRecord, 0, senderVault, receiverVault, quarks)},
		fulfillments: []*fulfillment.Record{expectedTransferFulfillment(t, intentRecord, result, 0, senderVault, receiverVault)},
		balances: []*balance.Record{
			expectedBalance(senderVault, sender, mint, currencycreator.ToQuarks(980), -2*usdCostBasis(nativeAmount), true),
			expectedBalance(receiverVault, receiver, mint, 2*quarks, 2*usdCostBasis(nativeAmount), true),
		},
		history: []*history.Record{
			expectedHistory(intentRecord, history.DirectlySent, sender, receiver, currency_lib.USD, nativeAmount, quarks),
			expectedHistory(intentRecord, history.DirectlyReceived, receiver, sender, currency_lib.USD, nativeAmount, quarks),
		},
	})
}
func TestSubmitIntent_SendPublicPayment_Direct_AutoOpenIneligible(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup func(t *testing.T, env *testEnv, mint *common.Account) (destination, destinationOwner *common.Account)
	}{
		{
			name: "destination owner isn't an ocp user",
			setup: func(t *testing.T, env *testEnv, mint *common.Account) (*common.Account, *common.Account) {
				destinationOwner := env.newAccount(t)
				return env.getVault(t, destinationOwner, mint), destinationOwner
			},
		},
		{
			name: "destination isn't the owner's derived primary vault",
			setup: func(t *testing.T, env *testEnv, mint *common.Account) (*common.Account, *common.Account) {
				destinationOwner := env.newUser(t)
				return env.getVault(t, env.newAccount(t), mint), destinationOwner
			},
		},
		{
			name: "destination owner is a gift card",
			setup: func(t *testing.T, env *testEnv, mint *common.Account) (*common.Account, *common.Account) {
				giftCard := env.newAccount(t)
				env.openAccount(t, giftCard, giftCard, common.CoreMintAccount, commonpb.AccountType_REMOTE_SEND_GIFT_CARD, 0)
				return env.getVault(t, giftCard, mint), giftCard
			},
		},
		{
			name: "destination is the ata of an ocp user",
			setup: func(t *testing.T, env *testEnv, mint *common.Account) (*common.Account, *common.Account) {
				destinationOwner := env.newUser(t)
				return toAta(t, destinationOwner, mint), destinationOwner
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env := setupTestEnv(t)

			mint := env.newLaunchpadMint(t)
			sender := env.newFundedUser(t, currencycreator.ToQuarks(1_000), mint)
			destination, destinationOwner := tc.setup(t, env, mint)

			req := env.directPaymentRequest(t, sender, destination, mint, currencycreator.ToQuarks(10))
			req.metadata.GetSendPublicPayment().DestinationOwner = destinationOwner.ToProto()
			before := env.snapshot(t, sender, destinationOwner)
			env.submitIntent(t, req).requireInvalidIntent(t, "payments to external destinations must be withdrawals")

			env.requireNoChanges(t, before)
			_, err := env.data.GetAccountInfoByTokenAddress(env.ctx, destination.PublicKey().ToBase58())
			assert.Equal(t, account.ErrAccountInfoNotFound, err)
		})
	}
}

func TestSubmitIntent_SendPublicPayment_GiftCard_HappyPath(t *testing.T) {
	for _, useLaunchpadMint := range []bool{false, true} {
		t.Run(fmt.Sprintf("launchpad=%v", useLaunchpadMint), func(t *testing.T) {
			env := setupTestEnv(t)

			mint := common.CoreMintAccount
			quarks := common.ToCoreMintQuarks(25)
			if useLaunchpadMint {
				mint = env.newLaunchpadMint(t)
				quarks = currencycreator.ToQuarks(25)
			}

			sender := env.newFundedUser(t, 4*quarks, mint)
			senderVault := env.getVault(t, sender, mint)
			giftCard := env.newAccount(t)
			giftCardVault := env.getVault(t, giftCard, mint)

			req := env.giftCardRequest(t, sender, giftCard, mint, quarks)
			nativeAmount := paymentNativeAmount(req)
			exchangeRate := 1.0
			if useLaunchpadMint {
				exchangeRate = nativeAmount / 25
			}

			before := env.snapshot(t, sender, giftCard)
			result := env.submitIntent(t, req)
			result.requireSuccess(t)
			requireServerParameterNonces(t, result.serverParameters, 0, 1, 1)
			assert.NotNil(t, result.serverParameters[0].GetOpenAccount())
			assert.NotNil(t, result.serverParameters[1].GetNoPrivacyTransfer())
			assert.NotNil(t, result.serverParameters[2].GetNoPrivacyWithdraw())

			intentRecord := env.requireIntent(t, req.intentId)
			giftCardIssuance := expectedHistory(intentRecord, history.IndirectlySent, sender, nil, currency_lib.USD, nativeAmount, quarks)
			giftCardIssuance.GiftCardVault = pointer.String(giftCardVault.PublicKey().ToBase58())
			giftCardIssuance.State = history.StatePending

			env.requireChanges(t, before, &dbChanges{
				intents: []*intent.Record{{
					IntentId:              req.intentId.PublicKey().ToBase58(),
					IntentType:            intent.SendPublicPayment,
					MintAccount:           mint.PublicKey().ToBase58(),
					InitiatorOwnerAccount: sender.PublicKey().ToBase58(),
					SendPublicPaymentMetadata: &intent.SendPublicPaymentMetadata{
						DestinationTokenAccount: giftCardVault.PublicKey().ToBase58(),
						Quantity:                quarks,
						ExchangeCurrency:        currency_lib.USD,
						ExchangeRate:            exchangeRate,
						NativeAmount:            nativeAmount,
						UsdMarketValue:          nativeAmount,
						IsIndirectSend:          true,
					},
					State: intent.StatePending,
				}},
				actions: []*action.Record{
					expectedOpenAccountAction(intentRecord, 0, giftCardVault),
					expectedTransferAction(intentRecord, 1, senderVault, giftCardVault, quarks),
					expectedAutoReturnAction(intentRecord, 2, giftCardVault, senderVault),
				},
				fulfillments: []*fulfillment.Record{
					expectedOpenAccountFulfillment(intentRecord, 0, giftCardVault),
					expectedTransferFulfillment(t, intentRecord, result, 1, senderVault, giftCardVault),
					expectedAutoReturnFulfillment(t, intentRecord, result, 2, giftCardVault, senderVault),
				},
				accountInfos: []*account.Record{env.expectedAccountInfo(t, giftCard, giftCard, mint, commonpb.AccountType_REMOTE_SEND_GIFT_CARD, 0)},
				timelocks:    []*timelock.Record{env.expectedTimelock(t, giftCard, mint)},
				balances: []*balance.Record{
					expectedBalance(senderVault, sender, mint, 3*quarks, -usdCostBasis(nativeAmount), true),
					expectedBalance(giftCardVault, giftCard, mint, quarks, usdCostBasis(nativeAmount), true),
				},
				history: []*history.Record{giftCardIssuance},
			})
			env.requireReservedNonces(t, result)

			ownerMetadata, err := common.GetOwnerMetadata(env.ctx, env.data, giftCard)
			require.NoError(t, err)
			assert.Equal(t, common.OwnerTypeRemoteSendGiftCard, ownerMetadata.Type)

			autoReturnActionRecord, err := env.data.GetGiftCardAutoReturnAction(env.ctx, giftCardVault.PublicKey().ToBase58())
			require.NoError(t, err)
			assert.Equal(t, req.intentId.PublicKey().ToBase58(), autoReturnActionRecord.Intent)
		})
	}
}
func TestSubmitIntent_SendPublicPayment_Withdrawal_CodeToCode(t *testing.T) {
	env := setupTestEnv(t)

	sender := env.newFundedUser(t, common.ToCoreMintQuarks(100))
	receiver := env.newUser(t)
	senderVault := env.getVault(t, sender, common.CoreMintAccount)
	receiverVault := env.getVault(t, receiver, common.CoreMintAccount)

	quarks := common.ToCoreMintQuarks(100) // Full balance
	req := env.withdrawalRequest(t, sender, receiverVault, nil, common.CoreMintAccount, quarks, 0)

	before := env.snapshot(t, sender, receiver)
	result := env.submitIntent(t, req)
	result.requireSuccess(t)
	requireServerParameterNonces(t, result.serverParameters, 1)

	intentRecord := env.requireIntent(t, req.intentId)
	env.requireChanges(t, before, &dbChanges{
		intents: []*intent.Record{{
			IntentId:              req.intentId.PublicKey().ToBase58(),
			IntentType:            intent.SendPublicPayment,
			MintAccount:           common.CoreMintAccount.PublicKey().ToBase58(),
			InitiatorOwnerAccount: sender.PublicKey().ToBase58(),
			SendPublicPaymentMetadata: &intent.SendPublicPaymentMetadata{
				DestinationOwnerAccount: receiver.PublicKey().ToBase58(),
				DestinationTokenAccount: receiverVault.PublicKey().ToBase58(),
				Quantity:                quarks,
				ExchangeCurrency:        currency_lib.USD,
				ExchangeRate:            1.0,
				NativeAmount:            100.0,
				UsdMarketValue:          100.0,
				IsWithdrawal:            true,
			},
			State: intent.StatePending,
		}},
		actions:      []*action.Record{expectedTransferAction(intentRecord, 0, senderVault, receiverVault, quarks)},
		fulfillments: []*fulfillment.Record{expectedTransferFulfillment(t, intentRecord, result, 0, senderVault, receiverVault)},
		balances: []*balance.Record{
			expectedBalance(senderVault, sender, common.CoreMintAccount, 0, -usdCostBasis(100), true),
			expectedBalance(receiverVault, receiver, common.CoreMintAccount, quarks, usdCostBasis(100), true),
		},
		history: []*history.Record{
			expectedHistory(intentRecord, history.Withdrawn, sender, receiver, currency_lib.USD, 100.0, quarks),
			expectedHistory(intentRecord, history.Deposited, receiver, sender, currency_lib.USD, 100.0, quarks),
		},
	})
	env.requireReservedNonces(t, result)
}
func TestSubmitIntent_SendPublicPayment_Withdrawal_External(t *testing.T) {
	env := setupTestEnv(t)

	sender := env.newFundedUser(t, common.ToCoreMintQuarks(100))
	senderVault := env.getVault(t, sender, common.CoreMintAccount)
	externalOwner := env.newAccount(t)
	destination := toAta(t, externalOwner, common.CoreMintAccount)
	quarks := common.ToCoreMintQuarks(30)

	for i, destinationOwner := range []*common.Account{
		nil, // Assumes the destination already exists
		externalOwner,
	} {
		req := env.withdrawalRequest(t, sender, destination, destinationOwner, common.CoreMintAccount, quarks, 0)

		before := env.snapshot(t, sender, externalOwner)
		result := env.submitIntent(t, req)
		result.requireSuccess(t)

		var destinationOwnerAccount string
		if destinationOwner != nil {
			destinationOwnerAccount = destinationOwner.PublicKey().ToBase58()
		}

		intentRecord := env.requireIntent(t, req.intentId)
		env.requireChanges(t, before, &dbChanges{
			intents: []*intent.Record{{
				IntentId:              req.intentId.PublicKey().ToBase58(),
				IntentType:            intent.SendPublicPayment,
				MintAccount:           common.CoreMintAccount.PublicKey().ToBase58(),
				InitiatorOwnerAccount: sender.PublicKey().ToBase58(),
				SendPublicPaymentMetadata: &intent.SendPublicPaymentMetadata{
					DestinationOwnerAccount: destinationOwnerAccount,
					DestinationTokenAccount: destination.PublicKey().ToBase58(),
					Quantity:                quarks,
					ExchangeCurrency:        currency_lib.USD,
					ExchangeRate:            1.0,
					NativeAmount:            30.0,
					UsdMarketValue:          30.0,
					IsWithdrawal:            true,
				},
				State: intent.StatePending,
			}},
			actions:      []*action.Record{expectedTransferAction(intentRecord, 0, senderVault, destination, quarks)},
			fulfillments: []*fulfillment.Record{expectedTransferFulfillment(t, intentRecord, result, 0, senderVault, destination)},
			// The external destination isn't tracked by the ledger
			balances: []*balance.Record{
				expectedBalance(senderVault, sender, common.CoreMintAccount, common.ToCoreMintQuarks(70-30*uint64(i)), -usdCostBasis(30*float64(i+1)), true),
			},
			history: []*history.Record{
				expectedHistory(intentRecord, history.Withdrawn, sender, destinationOwner, currency_lib.USD, 30.0, quarks),
			},
		})
		env.requireReservedNonces(t, result)
	}

	assert.Empty(t, env.antispamIntegration.getCalls())
}
func TestSubmitIntent_SendPublicPayment_Withdrawal_WithFee(t *testing.T) {
	for _, tc := range []struct {
		name         string
		currencyCode currency_lib.Code
		rate         float64
	}{
		{name: "usd", currencyCode: currency_lib.USD, rate: 1.0},
		{name: "cad", currencyCode: currency_lib.CAD, rate: 1.35},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env := setupTestEnv(t)

			sender := env.newFundedUser(t, common.ToCoreMintQuarks(100))
			senderVault := env.getVault(t, sender, common.CoreMintAccount)
			externalOwner := env.newAccount(t)
			destination := toAta(t, externalOwner, common.CoreMintAccount)
			feeDestination := toAta(t, env.feeCollector, common.CoreMintAccount)

			quarks := common.ToCoreMintQuarks(20)
			exchangeRate := env.signedExchangeRate(t, tc.currencyCode, tc.rate, time.Now())
			req := env.withdrawalRequest(t, sender, destination, externalOwner, common.CoreMintAccount, quarks, withdrawalFeeQuarks)
			req.metadata.GetSendPublicPayment().ExchangeData = clientExchangeData(env.coreMintExchangeData(t, quarks, exchangeRate))
			nativeAmount := paymentNativeAmount(req)
			usdValue := nativeAmount / tc.rate

			before := env.snapshot(t, sender, externalOwner, env.feeCollector)
			result := env.submitIntent(t, req)
			result.requireSuccess(t)
			requireServerParameterNonces(t, result.serverParameters, 1, 1)
			require.NotNil(t, result.serverParameters[1].GetFeePayment())
			assert.Equal(t, feeDestination.PublicKey().ToBytes(), result.serverParameters[1].GetFeePayment().Destination.Value)

			intentRecord := env.requireIntent(t, req.intentId)
			withdrawn := expectedHistory(intentRecord, history.Withdrawn, sender, externalOwner, tc.currencyCode, nativeAmount, quarks)
			withdrawn.Fees = []history.Fee{{Type: history.WithdrawalAccountCreationFee, NativeAmount: tc.rate * 0.5}}

			env.requireChanges(t, before, &dbChanges{
				intents: []*intent.Record{{
					IntentId:              req.intentId.PublicKey().ToBase58(),
					IntentType:            intent.SendPublicPayment,
					MintAccount:           common.CoreMintAccount.PublicKey().ToBase58(),
					InitiatorOwnerAccount: sender.PublicKey().ToBase58(),
					SendPublicPaymentMetadata: &intent.SendPublicPaymentMetadata{
						DestinationOwnerAccount: externalOwner.PublicKey().ToBase58(),
						DestinationTokenAccount: destination.PublicKey().ToBase58(),
						Quantity:                quarks,
						ExchangeCurrency:        tc.currencyCode,
						ExchangeRate:            tc.rate,
						NativeAmount:            nativeAmount,
						UsdMarketValue:          usdValue,
						IsWithdrawal:            true,
					},
					State: intent.StatePending,
				}},
				actions: []*action.Record{
					expectedTransferAction(intentRecord, 0, senderVault, destination, quarks-withdrawalFeeQuarks),
					expectedFeePaymentAction(intentRecord, 1, senderVault, feeDestination, withdrawalFeeQuarks),
				},
				fulfillments: []*fulfillment.Record{
					expectedTransferFulfillment(t, intentRecord, result, 0, senderVault, destination),
					expectedTransferFulfillment(t, intentRecord, result, 1, senderVault, feeDestination),
				},
				// The fee carries its fixed USD value, and the principal carries the
				// remainder of the gross
				balances: []*balance.Record{
					expectedBalance(senderVault, sender, common.CoreMintAccount, common.ToCoreMintQuarks(80), -usdCostBasis(usdValue), true),
				},
				history: []*history.Record{withdrawn},
			})
			env.requireReservedNonces(t, result)
		})
	}
}
func TestSubmitIntent_SendPublicPayment_Withdrawal_WithFee_LaunchpadMint(t *testing.T) {
	env := setupTestEnv(t)

	mint := env.newLaunchpadMint(t)
	sender := env.newFundedUser(t, currencycreator.ToQuarks(10_000), mint)
	senderVault := env.getVault(t, sender, mint)
	externalOwner := env.newAccount(t)
	destination := toAta(t, externalOwner, mint)
	feeDestination := toAta(t, env.feeCollector, mint)

	supply := currencycreator.ToQuarks(defaultLaunchpadSupply)
	feeQuarks := launchpadQuarksForUsdValue(supply, 0.5)
	quarks := currencycreator.ToQuarks(1_000)
	require.Greater(t, quarks, feeQuarks)

	req := env.withdrawalRequest(t, sender, destination, externalOwner, mint, quarks, feeQuarks)
	nativeAmount := paymentNativeAmount(req)

	before := env.snapshot(t, sender, externalOwner, env.feeCollector)
	result := env.submitIntent(t, req)
	result.requireSuccess(t)

	intentRecord := env.requireIntent(t, req.intentId)
	withdrawn := expectedHistory(intentRecord, history.Withdrawn, sender, externalOwner, currency_lib.USD, nativeAmount, quarks)
	withdrawn.Fees = []history.Fee{{Type: history.WithdrawalAccountCreationFee, NativeAmount: 0.5}}

	env.requireChanges(t, before, &dbChanges{
		intents: []*intent.Record{{
			IntentId:              req.intentId.PublicKey().ToBase58(),
			IntentType:            intent.SendPublicPayment,
			MintAccount:           mint.PublicKey().ToBase58(),
			InitiatorOwnerAccount: sender.PublicKey().ToBase58(),
			SendPublicPaymentMetadata: &intent.SendPublicPaymentMetadata{
				DestinationOwnerAccount: externalOwner.PublicKey().ToBase58(),
				DestinationTokenAccount: destination.PublicKey().ToBase58(),
				Quantity:                quarks,
				ExchangeCurrency:        currency_lib.USD,
				ExchangeRate:            nativeAmount / 1_000,
				NativeAmount:            nativeAmount,
				UsdMarketValue:          nativeAmount,
				IsWithdrawal:            true,
			},
			State: intent.StatePending,
		}},
		actions: []*action.Record{
			expectedTransferAction(intentRecord, 0, senderVault, destination, quarks-feeQuarks),
			expectedFeePaymentAction(intentRecord, 1, senderVault, feeDestination, feeQuarks),
		},
		fulfillments: []*fulfillment.Record{
			expectedTransferFulfillment(t, intentRecord, result, 0, senderVault, destination),
			expectedTransferFulfillment(t, intentRecord, result, 1, senderVault, feeDestination),
		},
		balances: []*balance.Record{
			expectedBalance(senderVault, sender, mint, currencycreator.ToQuarks(9_000), -usdCostBasis(nativeAmount), true),
		},
		history: []*history.Record{withdrawn},
	})
	env.requireReservedNonces(t, result)

	// A fee not valued at $0.50 is rejected
	req = env.withdrawalRequest(t, sender, destination, externalOwner, mint, quarks, launchpadQuarksForUsdValue(supply, 0.4))
	before = env.snapshot(t, sender, externalOwner, env.feeCollector)
	env.submitIntent(t, req).requireInvalidIntent(t, "CREATE_ON_SEND_WITHDRAWAL fee payment amount must be 0.50 USD")
	env.requireNoChanges(t, before)
}
func TestSubmitIntent_SendPublicPayment_SwapFunding_HappyPath(t *testing.T) {
	for _, useLaunchpadMint := range []bool{false, true} {
		t.Run(fmt.Sprintf("launchpad=%v", useLaunchpadMint), func(t *testing.T) {
			env := setupTestEnv(t)

			fromMint := common.CoreMintAccount
			toMint := env.newLaunchpadMint(t)
			quarks := common.ToCoreMintQuarks(15)
			if useLaunchpadMint {
				fromMint, toMint = toMint, common.CoreMintAccount
				quarks = currencycreator.ToQuarks(15)
			}

			owner := env.newFundedUser(t, 2*quarks, fromMint)
			ownerVault := env.getVault(t, owner, fromMint)
			swapPda := env.getVmSwapPda(t, owner, fromMint)
			destination := toAta(t, swapPda, fromMint)

			req := env.withdrawalRequest(t, owner, destination, swapPda, fromMint, quarks, 0)
			swapRecord := env.newSwapRecord(t, owner, fromMint, toMint, req.intentId, quarks, swap.StateCreated)
			nativeAmount := paymentNativeAmount(req)
			exchangeRate := 1.0
			if useLaunchpadMint {
				exchangeRate = nativeAmount / 15
			}

			before := env.snapshot(t, owner, swapPda)
			result := env.submitIntent(t, req)
			result.requireSuccess(t)

			intentRecord := env.requireIntent(t, req.intentId)
			env.requireChanges(t, before, &dbChanges{
				intents: []*intent.Record{{
					IntentId:              req.intentId.PublicKey().ToBase58(),
					IntentType:            intent.SendPublicPayment,
					MintAccount:           fromMint.PublicKey().ToBase58(),
					InitiatorOwnerAccount: owner.PublicKey().ToBase58(),
					SendPublicPaymentMetadata: &intent.SendPublicPaymentMetadata{
						DestinationOwnerAccount: swapPda.PublicKey().ToBase58(),
						DestinationTokenAccount: destination.PublicKey().ToBase58(),
						Quantity:                quarks,
						ExchangeCurrency:        currency_lib.USD,
						ExchangeRate:            exchangeRate,
						NativeAmount:            nativeAmount,
						UsdMarketValue:          nativeAmount,
						IsWithdrawal:            true,
						IsSwapSell:              true,
					},
					State: intent.StatePending,
				}},
				actions:      []*action.Record{expectedTransferAction(intentRecord, 0, ownerVault, destination, quarks)},
				fulfillments: []*fulfillment.Record{expectedTransferFulfillment(t, intentRecord, result, 0, ownerVault, destination)},
				balances: []*balance.Record{
					expectedBalance(ownerVault, owner, fromMint, quarks, -usdCostBasis(nativeAmount), true),
				},
				// History for swaps is owned by the swap flow
			})
			env.requireReservedNonces(t, result)

			// Only the swap's state moves to funding
			expectedSwapRecord := swapRecord.Clone()
			expectedSwapRecord.State = swap.StateFunding
			updatedSwapRecord, err := env.data.GetSwapById(env.ctx, swapRecord.SwapId)
			require.NoError(t, err)
			assert.Greater(t, updatedSwapRecord.Version, swapRecord.Version)
			expectedSwapRecord.Version = updatedSwapRecord.Version
			assert.Equal(t, &expectedSwapRecord, updatedSwapRecord)
		})
	}
}
func TestSubmitIntent_SendPublicPayment_SwapFunding_NewCurrencyInitialization(t *testing.T) {
	newEnv := func(t *testing.T) (*testEnv, *common.Account, *common.Account, *common.Account) {
		env := setupTestEnv(t)

		fromMint := env.newLaunchpadMint(t)
		toMint := testutil.SetupLaunchpadCurrency(t, env.data, reserve_memory.New(), holder_memory.New())
		metadataRecord, err := env.data.GetCurrencyMetadata(env.ctx, toMint.PublicKey().ToBase58())
		require.NoError(t, err)
		metadataRecord.State = currency.MetadataStateWaitingForInitialPurchase
		require.NoError(t, env.data.SaveCurrencyMetadata(env.ctx, metadataRecord))

		owner := env.newFundedUser(t, currencycreator.ToQuarks(100_000), fromMint)
		return env, owner, fromMint, toMint
	}

	supply := currencycreator.ToQuarks(defaultLaunchpadSupply)

	t.Run("valued at the new currency purchase and fee in usd", func(t *testing.T) {
		env, owner, fromMint, toMint := newEnv(t)
		ownerVault := env.getVault(t, owner, fromMint)

		quarks := launchpadQuarksForUsdValue(supply, newCurrencyPurchaseAndFees)
		swapPda := env.getVmSwapPda(t, owner, fromMint)
		destination := toAta(t, swapPda, fromMint)
		req := env.withdrawalRequest(t, owner, destination, swapPda, fromMint, quarks, 0)
		req.metadata.GetSendPublicPayment().GetClientExchangeData().NativeAmount = newCurrencyPurchaseAndFees
		swapRecord := env.newSwapRecord(t, owner, fromMint, toMint, req.intentId, quarks, swap.StateCreated)

		before := env.snapshot(t, owner, swapPda)
		result := env.submitIntent(t, req)
		result.requireSuccess(t)

		intentRecord := env.requireIntent(t, req.intentId)
		env.requireChanges(t, before, &dbChanges{
			intents: []*intent.Record{{
				IntentId:              req.intentId.PublicKey().ToBase58(),
				IntentType:            intent.SendPublicPayment,
				MintAccount:           fromMint.PublicKey().ToBase58(),
				InitiatorOwnerAccount: owner.PublicKey().ToBase58(),
				SendPublicPaymentMetadata: &intent.SendPublicPaymentMetadata{
					DestinationOwnerAccount: swapPda.PublicKey().ToBase58(),
					DestinationTokenAccount: destination.PublicKey().ToBase58(),
					Quantity:                quarks,
					ExchangeCurrency:        currency_lib.USD,
					// The quarks aren't a whole number of tokens, so derive the rate
					// with the same precision as the server
					ExchangeRate:   currency_util.CalculateExchangeRate(fromMint, quarks, newCurrencyPurchaseAndFees),
					NativeAmount:   newCurrencyPurchaseAndFees,
					UsdMarketValue: newCurrencyPurchaseAndFees,
					IsWithdrawal:   true,
					IsSwapSell:     true,
				},
				State: intent.StatePending,
			}},
			actions:      []*action.Record{expectedTransferAction(intentRecord, 0, ownerVault, destination, quarks)},
			fulfillments: []*fulfillment.Record{expectedTransferFulfillment(t, intentRecord, result, 0, ownerVault, destination)},
			balances: []*balance.Record{
				expectedBalance(ownerVault, owner, fromMint, currencycreator.ToQuarks(100_000)-quarks, -usdCostBasis(newCurrencyPurchaseAndFees), true),
			},
		})
		env.requireReservedNonces(t, result)

		updatedSwapRecord, err := env.data.GetSwapById(env.ctx, swapRecord.SwapId)
		require.NoError(t, err)
		assert.Equal(t, swap.StateFunding, updatedSwapRecord.State)
	})

	t.Run("not valued in usd", func(t *testing.T) {
		env, owner, fromMint, toMint := newEnv(t)

		exchangeRate := env.signedExchangeRate(t, currency_lib.CAD, 1.35, time.Now())
		reserveState := env.signedReserveState(t, fromMint, supply, time.Now())
		quarks := launchpadQuarksForUsdValue(supply, newCurrencyPurchaseAndFees)
		swapPda := env.getVmSwapPda(t, owner, fromMint)
		req := env.withdrawalRequest(t, owner, toAta(t, swapPda, fromMint), swapPda, fromMint, quarks, 0)
		req.metadata.GetSendPublicPayment().ExchangeData = clientExchangeData(env.launchpadExchangeData(t, fromMint, quarks, exchangeRate, reserveState))
		swapRecord := env.newSwapRecord(t, owner, fromMint, toMint, req.intentId, quarks, swap.StateCreated)

		before := env.snapshot(t, owner, swapPda)
		env.submitIntent(t, req).requireInvalidIntent(t, "swap funding must be valued in usd")
		env.requireNoChanges(t, before)

		updatedSwapRecord, err := env.data.GetSwapById(env.ctx, swapRecord.SwapId)
		require.NoError(t, err)
		assert.Equal(t, swap.StateCreated, updatedSwapRecord.State)
	})

	t.Run("not valued at the new currency purchase and fee", func(t *testing.T) {
		env, owner, fromMint, toMint := newEnv(t)

		quarks := launchpadQuarksForUsdValue(supply, 15)
		swapPda := env.getVmSwapPda(t, owner, fromMint)
		req := env.withdrawalRequest(t, owner, toAta(t, swapPda, fromMint), swapPda, fromMint, quarks, 0)
		env.newSwapRecord(t, owner, fromMint, toMint, req.intentId, quarks, swap.StateCreated)

		before := env.snapshot(t, owner, swapPda)
		env.submitIntent(t, req).requireInvalidIntent(t, "swap funding must be valued at 20.00 usd")
		env.requireNoChanges(t, before)
	})
}

type paymentFixture struct {
	env      *testEnv
	sender   *common.Account
	receiver *common.Account
}

const paymentFixtureSenderBalance = 100 // core mint units

func newPaymentFixture(t *testing.T, overrides ...func(*testOverrides)) *paymentFixture {
	env := setupTestEnv(t, overrides...)
	return &paymentFixture{
		env:      env,
		sender:   env.newFundedUser(t, common.ToCoreMintQuarks(paymentFixtureSenderBalance)),
		receiver: env.newUser(t),
	}
}

func (f *paymentFixture) senderVault(t *testing.T) *common.Account {
	return f.env.getVault(t, f.sender, common.CoreMintAccount)
}

func (f *paymentFixture) receiverVault(t *testing.T) *common.Account {
	return f.env.getVault(t, f.receiver, common.CoreMintAccount)
}

type paymentValidationTestCase struct {
	name           string
	build          func(t *testing.T, f *paymentFixture) *submitIntentRequest
	expectedCode   transactionpb.SubmitIntentResponse_Error_Code
	expectedReason string
}

func runPaymentValidationTestCases(t *testing.T, testCases []paymentValidationTestCase) {
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			f := newPaymentFixture(t)

			req := tc.build(t, f)

			before := f.env.snapshot(t, f.sender, f.receiver)
			result := f.env.submitIntent(t, req)
			result.requireError(t, tc.expectedCode, tc.expectedReason)
			assert.Empty(t, result.serverParameters)

			f.env.requireNoChanges(t, before)
			assert.Equal(t, common.ToCoreMintQuarks(paymentFixtureSenderBalance), f.env.getBalance(t, f.senderVault(t)).Quarks)
			assert.Empty(t, f.env.submitIntentIntegration.getSuccesses())
		})
	}
}

func TestSubmitIntent_SendPublicPayment_Direct_Validation(t *testing.T) {
	runPaymentValidationTestCases(t, []paymentValidationTestCase{
		{
			name: "server exchange data",
			build: func(t *testing.T, f *paymentFixture) *submitIntentRequest {
				req := f.env.directPaymentRequest(t, f.sender, f.receiverVault(t), common.CoreMintAccount, common.ToCoreMintQuarks(1))
				req.metadata.GetSendPublicPayment().ExchangeData = &transactionpb.SendPublicPaymentMetadata_ServerExchangeData{
					ServerExchangeData: &transactionpb.ExchangeData{
						Currency:     "usd",
						ExchangeRate: 1.0,
						NativeAmount: 1.0,
						Quarks:       common.ToCoreMintQuarks(1),
						Mint:         common.CoreMintAccount.ToProto(),
					},
				}
				return req
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_DENIED,
			expectedReason: "client exchange data not provided",
		},
		{
			name: "stale exchange rate",
			build: func(t *testing.T, f *paymentFixture) *submitIntentRequest {
				exchangeRate := f.env.signedExchangeRate(t, currency_lib.USD, 1.0, time.Now().Add(-11*time.Minute))
				req := f.env.directPaymentRequest(t, f.sender, f.receiverVault(t), common.CoreMintAccount, common.ToCoreMintQuarks(1))
				req.metadata.GetSendPublicPayment().ExchangeData = clientExchangeData(f.env.coreMintExchangeData(t, common.ToCoreMintQuarks(1), exchangeRate))
				return req
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_STALE_STATE,
			expectedReason: "exchange rate is stale",
		},
		{
			name: "exchange rate not signed by server",
			build: func(t *testing.T, f *paymentFixture) *submitIntentRequest {
				exchangeRate := f.env.signedExchangeRate(t, currency_lib.USD, 1.0, time.Now())
				exchangeRate.Signature = f.env.signProto(t, f.env.newAccount(t), exchangeRate.ExchangeRate)
				req := f.env.directPaymentRequest(t, f.sender, f.receiverVault(t), common.CoreMintAccount, common.ToCoreMintQuarks(1))
				req.metadata.GetSendPublicPayment().ExchangeData = clientExchangeData(f.env.coreMintExchangeData(t, common.ToCoreMintQuarks(1), exchangeRate))
				return req
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_INVALID_INTENT,
			expectedReason: "exchange rate validation failed: invalid signature",
		},
		{
			name: "exchange rate tampered after signing",
			build: func(t *testing.T, f *paymentFixture) *submitIntentRequest {
				exchangeRate := f.env.signedExchangeRate(t, currency_lib.USD, 1.0, time.Now())
				exchangeRate.ExchangeRate.ExchangeRate = 2.0
				req := f.env.directPaymentRequest(t, f.sender, f.receiverVault(t), common.CoreMintAccount, common.ToCoreMintQuarks(1))
				req.metadata.GetSendPublicPayment().ExchangeData = clientExchangeData(f.env.coreMintExchangeData(t, common.ToCoreMintQuarks(1), exchangeRate))
				return req
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_INVALID_INTENT,
			expectedReason: "exchange rate validation failed: invalid signature",
		},
		{
			name: "exchange data mint doesn't match intent mint",
			build: func(t *testing.T, f *paymentFixture) *submitIntentRequest {
				mint := f.env.newLaunchpadMint(t)
				req := f.env.directPaymentRequest(t, f.sender, f.receiverVault(t), common.CoreMintAccount, common.ToCoreMintQuarks(1))
				req.metadata.GetSendPublicPayment().ExchangeData = clientExchangeData(f.env.exchangeData(t, mint, common.ToCoreMintQuarks(1)))
				return req
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_INVALID_INTENT,
			expectedReason: "expected exchange data mint to be " + common.CoreMintAccount.PublicKey().ToBase58(),
		},
		{
			name: "native amount doesn't match quarks",
			build: func(t *testing.T, f *paymentFixture) *submitIntentRequest {
				req := f.env.directPaymentRequest(t, f.sender, f.receiverVault(t), common.CoreMintAccount, common.ToCoreMintQuarks(1))
				req.metadata.GetSendPublicPayment().GetClientExchangeData().NativeAmount = 1.01
				return req
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_INVALID_INTENT,
			expectedReason: "native amount and quark value mismatch",
		},
		{
			name: "reserve state provided for the core mint",
			build: func(t *testing.T, f *paymentFixture) *submitIntentRequest {
				req := f.env.directPaymentRequest(t, f.sender, f.receiverVault(t), common.CoreMintAccount, common.ToCoreMintQuarks(1))
				req.metadata.GetSendPublicPayment().GetClientExchangeData().LaunchpadCurrencyReserveState = f.env.signedReserveState(t, common.CoreMintAccount, 1, time.Now())
				return req
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_INVALID_INTENT,
			expectedReason: "launchpad currency reserve state is not required for core mint",
		},
		{
			name: "launchpad mint without a reserve state",
			build: func(t *testing.T, f *paymentFixture) *submitIntentRequest {
				mint := f.env.newLaunchpadMint(t)
				f.env.openAccount(t, f.sender, f.sender, mint, commonpb.AccountType_PRIMARY, 0)
				f.env.credit(t, f.env.getVault(t, f.sender, mint), currencycreator.ToQuarks(100))
				f.env.openAccount(t, f.receiver, f.receiver, mint, commonpb.AccountType_PRIMARY, 0)
				req := f.env.directPaymentRequest(t, f.sender, f.env.getVault(t, f.receiver, mint), mint, currencycreator.ToQuarks(10))
				req.metadata.GetSendPublicPayment().GetClientExchangeData().LaunchpadCurrencyReserveState = nil
				return req
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_INVALID_INTENT,
			expectedReason: "reserve state is required for launchpad currencies",
		},
		{
			name: "launchpad mint with a stale reserve state",
			build: func(t *testing.T, f *paymentFixture) *submitIntentRequest {
				mint := f.env.newLaunchpadMint(t)
				f.env.openAccount(t, f.sender, f.sender, mint, commonpb.AccountType_PRIMARY, 0)
				f.env.credit(t, f.env.getVault(t, f.sender, mint), currencycreator.ToQuarks(100))
				f.env.openAccount(t, f.receiver, f.receiver, mint, commonpb.AccountType_PRIMARY, 0)
				exchangeRate := f.env.signedExchangeRate(t, currency_lib.USD, 1.0, time.Now())
				reserveState := f.env.signedReserveState(t, mint, currencycreator.ToQuarks(defaultLaunchpadSupply), time.Now().Add(-11*time.Minute))
				req := f.env.directPaymentRequest(t, f.sender, f.env.getVault(t, f.receiver, mint), mint, currencycreator.ToQuarks(10))
				req.metadata.GetSendPublicPayment().ExchangeData = clientExchangeData(f.env.launchpadExchangeData(t, mint, currencycreator.ToQuarks(10), exchangeRate, reserveState))
				return req
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_STALE_STATE,
			expectedReason: "reserve state is stale",
		},
		{
			name: "launchpad mint with a reserve state for another mint",
			build: func(t *testing.T, f *paymentFixture) *submitIntentRequest {
				mint := f.env.newLaunchpadMint(t)
				f.env.openAccount(t, f.sender, f.sender, mint, commonpb.AccountType_PRIMARY, 0)
				f.env.credit(t, f.env.getVault(t, f.sender, mint), currencycreator.ToQuarks(100))
				f.env.openAccount(t, f.receiver, f.receiver, mint, commonpb.AccountType_PRIMARY, 0)
				exchangeRate := f.env.signedExchangeRate(t, currency_lib.USD, 1.0, time.Now())
				reserveState := f.env.signedReserveState(t, f.env.newLaunchpadMint(t), currencycreator.ToQuarks(defaultLaunchpadSupply), time.Now())
				req := f.env.directPaymentRequest(t, f.sender, f.env.getVault(t, f.receiver, mint), mint, currencycreator.ToQuarks(10))
				exchangeData := f.env.launchpadExchangeData(t, mint, currencycreator.ToQuarks(10), exchangeRate, reserveState)
				req.metadata.GetSendPublicPayment().ExchangeData = clientExchangeData(exchangeData)
				return req
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_INVALID_INTENT,
			expectedReason: "reserve state mint does not match expected mint",
		},
		{
			name: "launchpad mint native amount doesn't match sell value",
			build: func(t *testing.T, f *paymentFixture) *submitIntentRequest {
				mint := f.env.newLaunchpadMint(t)
				f.env.openAccount(t, f.sender, f.sender, mint, commonpb.AccountType_PRIMARY, 0)
				f.env.credit(t, f.env.getVault(t, f.sender, mint), currencycreator.ToQuarks(100))
				f.env.openAccount(t, f.receiver, f.receiver, mint, commonpb.AccountType_PRIMARY, 0)
				req := f.env.directPaymentRequest(t, f.sender, f.env.getVault(t, f.receiver, mint), mint, currencycreator.ToQuarks(10))
				req.metadata.GetSendPublicPayment().GetClientExchangeData().NativeAmount += 0.01
				return req
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_INVALID_INTENT,
			expectedReason: "native amount does not match expected sell value",
		},
		{
			name: "action amount doesn't match exchange data",
			build: func(t *testing.T, f *paymentFixture) *submitIntentRequest {
				req := f.env.directPaymentRequest(t, f.sender, f.receiverVault(t), common.CoreMintAccount, common.ToCoreMintQuarks(1))
				req.actions[0].GetNoPrivacyTransfer().Amount = common.ToCoreMintQuarks(2)
				return req
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_INVALID_INTENT,
			expectedReason: fmt.Sprintf("actions[0]: must send %d quarks to destination account", common.ToCoreMintQuarks(1)),
		},
		{
			name: "multiple actions",
			build: func(t *testing.T, f *paymentFixture) *submitIntentRequest {
				req := f.env.directPaymentRequest(t, f.sender, f.receiverVault(t), common.CoreMintAccount, common.ToCoreMintQuarks(2))
				req.actions = []*transactionpb.Action{
					f.env.transferAction(t, 0, f.sender, f.receiverVault(t), common.CoreMintAccount, common.ToCoreMintQuarks(1)),
					f.env.transferAction(t, 1, f.sender, f.receiverVault(t), common.CoreMintAccount, common.ToCoreMintQuarks(1)),
				}
				return req
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_INVALID_INTENT,
			expectedReason: "expected 1 action for payment",
		},
		{
			name: "fee payment",
			build: func(t *testing.T, f *paymentFixture) *submitIntentRequest {
				req := f.env.directPaymentRequest(t, f.sender, f.receiverVault(t), common.CoreMintAccount, common.ToCoreMintQuarks(1))
				req.actions = append(req.actions, f.env.feePaymentAction(t, 1, f.sender, common.CoreMintAccount, withdrawalFeeQuarks))
				return req
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_INVALID_INTENT,
			expectedReason: "intent doesn't require a fee payment",
		},
		{
			name: "withdraw action",
			build: func(t *testing.T, f *paymentFixture) *submitIntentRequest {
				quarks := common.ToCoreMintQuarks(paymentFixtureSenderBalance)
				req := f.env.directPaymentRequest(t, f.sender, f.receiverVault(t), common.CoreMintAccount, quarks)
				req.actions[0] = f.env.withdrawAction(t, 0, f.sender, f.receiverVault(t), common.CoreMintAccount, quarks, false)
				return req
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_INVALID_INTENT,
			expectedReason: "actions[0]: payment sent to destination must be a public transfer",
		},
		{
			name: "action mint doesn't match intent mint",
			build: func(t *testing.T, f *paymentFixture) *submitIntentRequest {
				mint := f.env.newLaunchpadMint(t)
				f.env.openAccount(t, f.sender, f.sender, mint, commonpb.AccountType_PRIMARY, 0)
				f.env.credit(t, f.env.getVault(t, f.sender, mint), currencycreator.ToQuarks(100))
				f.env.openAccount(t, f.receiver, f.receiver, mint, commonpb.AccountType_PRIMARY, 0)
				req := f.env.directPaymentRequest(t, f.sender, f.receiverVault(t), common.CoreMintAccount, common.ToCoreMintQuarks(1))
				req.actions[0] = f.env.transferAction(t, 0, f.sender, f.env.getVault(t, f.receiver, mint), mint, common.ToCoreMintQuarks(1))
				return req
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_INVALID_INTENT,
			expectedReason: "actions[0]: mint must be " + common.CoreMintAccount.PublicKey().ToBase58(),
		},
		{
			name: "source isn't a primary account",
			build: func(t *testing.T, f *paymentFixture) *submitIntentRequest {
				poolAuthority := f.env.newAccount(t)
				f.env.openAccount(t, f.sender, poolAuthority, common.CoreMintAccount, commonpb.AccountType_POOL, 0)
				f.env.credit(t, f.env.getVault(t, poolAuthority, common.CoreMintAccount), common.ToCoreMintQuarks(10))
				req := f.env.directPaymentRequest(t, poolAuthority, f.receiverVault(t), common.CoreMintAccount, common.ToCoreMintQuarks(1))
				req.owner = f.sender
				return req
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_INVALID_INTENT,
			expectedReason: "source account must be a PRIMARY account",
		},
		{
			name: "source isn't owned by the intent owner",
			build: func(t *testing.T, f *paymentFixture) *submitIntentRequest {
				f.env.credit(t, f.receiverVault(t), common.ToCoreMintQuarks(10))
				req := f.env.directPaymentRequest(t, f.receiver, f.senderVault(t), common.CoreMintAccount, common.ToCoreMintQuarks(1))
				req.owner = f.sender
				return req
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_INVALID_INTENT,
			expectedReason: "source account must be a PRIMARY account",
		},
		{
			name: "action source doesn't match intent source",
			build: func(t *testing.T, f *paymentFixture) *submitIntentRequest {
				poolAuthority := f.env.newAccount(t)
				f.env.openAccount(t, f.sender, poolAuthority, common.CoreMintAccount, commonpb.AccountType_POOL, 0)
				f.env.credit(t, f.env.getVault(t, poolAuthority, common.CoreMintAccount), common.ToCoreMintQuarks(10))
				req := f.env.directPaymentRequest(t, f.sender, f.receiverVault(t), common.CoreMintAccount, common.ToCoreMintQuarks(1))
				req.actions[0] = f.env.transferAction(t, 0, poolAuthority, f.receiverVault(t), common.CoreMintAccount, common.ToCoreMintQuarks(1))
				return req
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_INVALID_INTENT,
			expectedReason: "must send payment from source account",
		},
		{
			name: "action authority doesn't own the source",
			build: func(t *testing.T, f *paymentFixture) *submitIntentRequest {
				req := f.env.directPaymentRequest(t, f.sender, f.receiverVault(t), common.CoreMintAccount, common.ToCoreMintQuarks(1))
				req.actions[0].GetNoPrivacyTransfer().Authority = f.env.newAccount(t).ToProto()
				return req
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_INVALID_INTENT,
			expectedReason: "actions[0]: token must be",
		},
		{
			name: "sending to an account other than the destination",
			build: func(t *testing.T, f *paymentFixture) *submitIntentRequest {
				other := f.env.newUser(t)
				req := f.env.directPaymentRequest(t, f.sender, f.receiverVault(t), common.CoreMintAccount, common.ToCoreMintQuarks(1))
				req.actions[0] = f.env.transferAction(t, 0, f.sender, f.env.getVault(t, other, common.CoreMintAccount), common.CoreMintAccount, common.ToCoreMintQuarks(1))
				return req
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_INVALID_INTENT,
			expectedReason: "must send payment to destination account",
		},
		{
			name: "payment to self",
			build: func(t *testing.T, f *paymentFixture) *submitIntentRequest {
				return f.env.directPaymentRequest(t, f.sender, f.senderVault(t), common.CoreMintAccount, common.ToCoreMintQuarks(1))
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_INVALID_INTENT,
			expectedReason: "payment is a no-op",
		},
		{
			name: "insufficient balance",
			build: func(t *testing.T, f *paymentFixture) *submitIntentRequest {
				return f.env.directPaymentRequest(t, f.sender, f.receiverVault(t), common.CoreMintAccount, common.ToCoreMintQuarks(paymentFixtureSenderBalance)+1)
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_INVALID_INTENT,
			expectedReason: "has insufficient balance to perform action",
		},
		{
			name: "source primary account isn't managed by code",
			build: func(t *testing.T, f *paymentFixture) *submitIntentRequest {
				f.env.setTimelockState(t, f.senderVault(t), timelock_token_v1.StateWaitingForTimeout)
				return f.env.directPaymentRequest(t, f.sender, f.receiverVault(t), common.CoreMintAccount, common.ToCoreMintQuarks(1))
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_DENIED,
			expectedReason: ErrSourceNotManagedByCode.Error(),
		},
		{
			name: "another of the owner's accounts isn't managed by code",
			build: func(t *testing.T, f *paymentFixture) *submitIntentRequest {
				poolAuthority := f.env.newAccount(t)
				f.env.openAccount(t, f.sender, poolAuthority, common.CoreMintAccount, commonpb.AccountType_POOL, 0)
				f.env.setTimelockState(t, f.env.getVault(t, poolAuthority, common.CoreMintAccount), timelock_token_v1.StateUnlocked)
				return f.env.directPaymentRequest(t, f.sender, f.receiverVault(t), common.CoreMintAccount, common.ToCoreMintQuarks(1))
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_DENIED,
			expectedReason: ErrSourceNotManagedByCode.Error(),
		},
		{
			name: "destination isn't managed by code",
			build: func(t *testing.T, f *paymentFixture) *submitIntentRequest {
				f.env.setTimelockState(t, f.receiverVault(t), timelock_token_v1.StateUnlocked)
				return f.env.directPaymentRequest(t, f.sender, f.receiverVault(t), common.CoreMintAccount, common.ToCoreMintQuarks(1))
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_DENIED,
			expectedReason: ErrDestinationNotManagedByCode.Error(),
		},
		{
			name: "destination is closed",
			build: func(t *testing.T, f *paymentFixture) *submitIntentRequest {
				f.env.setTimelockState(t, f.receiverVault(t), timelock_token_v1.StateClosed)
				return f.env.directPaymentRequest(t, f.sender, f.receiverVault(t), common.CoreMintAccount, common.ToCoreMintQuarks(1))
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_STALE_STATE,
			expectedReason: "destination account has been closed",
		},
		{
			name: "destination is a different mint",
			build: func(t *testing.T, f *paymentFixture) *submitIntentRequest {
				mint := f.env.newLaunchpadMint(t)
				f.env.openAccount(t, f.receiver, f.receiver, mint, commonpb.AccountType_PRIMARY, 0)
				return f.env.directPaymentRequest(t, f.sender, f.env.getVault(t, f.receiver, mint), common.CoreMintAccount, common.ToCoreMintQuarks(1))
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_INVALID_INTENT,
			expectedReason: "mint is invalid",
		},
		{
			name: "destination is a gift card",
			build: func(t *testing.T, f *paymentFixture) *submitIntentRequest {
				giftCard := f.env.newAccount(t)
				f.env.openAccount(t, giftCard, giftCard, common.CoreMintAccount, commonpb.AccountType_REMOTE_SEND_GIFT_CARD, 0)
				return f.env.directPaymentRequest(t, f.sender, f.env.getVault(t, giftCard, common.CoreMintAccount), common.CoreMintAccount, common.ToCoreMintQuarks(1))
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_INVALID_INTENT,
			expectedReason: "destination account must be a PRIMARY or POOL account",
		},
		{
			name: "external destination",
			build: func(t *testing.T, f *paymentFixture) *submitIntentRequest {
				return f.env.directPaymentRequest(t, f.sender, toAta(t, f.env.newAccount(t), common.CoreMintAccount), common.CoreMintAccount, common.ToCoreMintQuarks(1))
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_INVALID_INTENT,
			expectedReason: "payments to external destinations must be withdrawals",
		},
		{
			name: "external destination with an owner",
			build: func(t *testing.T, f *paymentFixture) *submitIntentRequest {
				externalOwner := f.env.newAccount(t)
				req := f.env.directPaymentRequest(t, f.sender, toAta(t, externalOwner, common.CoreMintAccount), common.CoreMintAccount, common.ToCoreMintQuarks(1))
				req.metadata.GetSendPublicPayment().DestinationOwner = externalOwner.ToProto()
				return req
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_INVALID_INTENT,
			expectedReason: "payments to external destinations must be withdrawals",
		},
		{
			name: "intent ID reserved for a swap",
			build: func(t *testing.T, f *paymentFixture) *submitIntentRequest {
				req := f.env.directPaymentRequest(t, f.sender, f.receiverVault(t), common.CoreMintAccount, common.ToCoreMintQuarks(1))
				f.env.newSwapRecord(t, f.sender, common.CoreMintAccount, f.env.newLaunchpadMint(t), req.intentId, common.ToCoreMintQuarks(1), swap.StateCreated)
				return req
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_INVALID_INTENT,
			expectedReason: "swap funding must be an external withdrawal",
		},
		{
			name: "payment to a vm swap pda without a reserved intent ID",
			build: func(t *testing.T, f *paymentFixture) *submitIntentRequest {
				swapPda := f.env.getVmSwapPda(t, f.sender, common.CoreMintAccount)
				req := f.env.directPaymentRequest(t, f.sender, toAta(t, swapPda, common.CoreMintAccount), common.CoreMintAccount, common.ToCoreMintQuarks(1))
				req.metadata.GetSendPublicPayment().IsWithdrawal = true
				req.metadata.GetSendPublicPayment().DestinationOwner = swapPda.ToProto()
				return req
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_DENIED,
			expectedReason: "intent id is not reserved for a swap",
		},
	})
}

func TestSubmitIntent_SendPublicPayment_GiftCard_Validation(t *testing.T) {
	quarks := common.ToCoreMintQuarks(10)

	runPaymentValidationTestCases(t, []paymentValidationTestCase{
		{
			name: "missing auto-return action",
			build: func(t *testing.T, f *paymentFixture) *submitIntentRequest {
				req := f.env.giftCardRequest(t, f.sender, f.env.newAccount(t), common.CoreMintAccount, quarks)
				req.actions = req.actions[:2]
				return req
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_INVALID_INTENT,
			expectedReason: "expected 3 actions for indirect send",
		},
		{
			name: "indirect send is also a withdrawal",
			build: func(t *testing.T, f *paymentFixture) *submitIntentRequest {
				req := f.env.giftCardRequest(t, f.sender, f.env.newAccount(t), common.CoreMintAccount, quarks)
				req.metadata.GetSendPublicPayment().IsWithdrawal = true
				return req
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_INVALID_INTENT,
			expectedReason: "indirect send cannot be a withdraw",
		},
		{
			name: "destination is an existing account",
			build: func(t *testing.T, f *paymentFixture) *submitIntentRequest {
				req := f.env.giftCardRequest(t, f.sender, f.env.newAccount(t), common.CoreMintAccount, quarks)
				req.metadata.GetSendPublicPayment().Destination = f.receiverVault(t).ToProto()
				return req
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_INVALID_INTENT,
			expectedReason: "destination must be a brand new gift card account",
		},
		{
			name: "withdraw isn't an auto-return",
			build: func(t *testing.T, f *paymentFixture) *submitIntentRequest {
				req := f.env.giftCardRequest(t, f.sender, f.env.newAccount(t), common.CoreMintAccount, quarks)
				req.actions[2].GetNoPrivacyWithdraw().IsAutoReturn = false
				return req
			},
			expectedCode: transactionpb.SubmitIntentResponse_Error_INVALID_INTENT,
			// The withdraw counts against the gift card's net balance change when it
			// isn't an auto-return
			expectedReason: fmt.Sprintf("actions[1]: must send %d quarks to destination account", quarks),
		},
		{
			name: "auto-return to another user's primary account",
			build: func(t *testing.T, f *paymentFixture) *submitIntentRequest {
				giftCard := f.env.newAccount(t)
				req := f.env.giftCardRequest(t, f.sender, giftCard, common.CoreMintAccount, quarks)
				req.actions[2] = f.env.withdrawAction(t, 2, giftCard, f.receiverVault(t), common.CoreMintAccount, quarks, true)
				return req
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_INVALID_INTENT,
			expectedReason: "actions[2]: source account must be the PRIMARY account",
		},
		{
			name: "auto-return amount doesn't match",
			build: func(t *testing.T, f *paymentFixture) *submitIntentRequest {
				req := f.env.giftCardRequest(t, f.sender, f.env.newAccount(t), common.CoreMintAccount, quarks)
				req.actions[2].GetNoPrivacyWithdraw().Amount = quarks - 1
				return req
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_INVALID_INTENT,
			expectedReason: fmt.Sprintf("actions[2]: must auto-return %d quarks from remote send gift card", quarks),
		},
		{
			name: "transfer amount doesn't match",
			build: func(t *testing.T, f *paymentFixture) *submitIntentRequest {
				req := f.env.giftCardRequest(t, f.sender, f.env.newAccount(t), common.CoreMintAccount, quarks)
				req.actions[1].GetNoPrivacyTransfer().Amount = quarks + 1
				req.actions[2].GetNoPrivacyWithdraw().Amount = quarks + 1
				return req
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_INVALID_INTENT,
			expectedReason: fmt.Sprintf("actions[1]: must send %d quarks to destination account", quarks),
		},
		{
			name: "gift card index isn't zero",
			build: func(t *testing.T, f *paymentFixture) *submitIntentRequest {
				req := f.env.giftCardRequest(t, f.sender, f.env.newAccount(t), common.CoreMintAccount, quarks)
				req.actions[0].GetOpenAccount().Index = 1
				return req
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_INVALID_INTENT,
			expectedReason: "actions[0]: index must be 0",
		},
		{
			name: "gift card owner isn't its authority",
			build: func(t *testing.T, f *paymentFixture) *submitIntentRequest {
				req := f.env.giftCardRequest(t, f.sender, f.env.newAccount(t), common.CoreMintAccount, quarks)
				req.actions[0].GetOpenAccount().Owner = f.sender.ToProto()
				return req
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_INVALID_INTENT,
			expectedReason: "actions[0]: owner must be",
		},
		{
			name: "opened account isn't a gift card",
			build: func(t *testing.T, f *paymentFixture) *submitIntentRequest {
				giftCard := f.env.newAccount(t)
				req := f.env.giftCardRequest(t, f.sender, giftCard, common.CoreMintAccount, quarks)
				req.actions[0] = f.env.openAccountAction(t, 0, commonpb.AccountType_POOL, f.sender, giftCard, common.CoreMintAccount, 0)
				return req
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_INVALID_INTENT,
			expectedReason: "open account action for REMOTE_SEND_GIFT_CARD account type missing",
		},
		{
			name: "gift card token isn't the derived timelock vault",
			build: func(t *testing.T, f *paymentFixture) *submitIntentRequest {
				req := f.env.giftCardRequest(t, f.sender, f.env.newAccount(t), common.CoreMintAccount, quarks)
				req.actions[0].GetOpenAccount().Token = f.env.newAccount(t).ToProto()
				return req
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_INVALID_INTENT,
			expectedReason: "actions[0]: token must be",
		},
		{
			name: "gift card account already exists",
			build: func(t *testing.T, f *paymentFixture) *submitIntentRequest {
				giftCard := f.env.newAccount(t)
				require.NoError(t, f.env.data.SaveTimelock(f.env.ctx, f.env.getTimelockAccounts(t, giftCard, common.CoreMintAccount).ToDBRecord()))
				return f.env.giftCardRequest(t, f.sender, giftCard, common.CoreMintAccount, quarks)
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_STALE_STATE,
			expectedReason: "actions[0]: account is already opened",
		},
		{
			name: "gift card already initiated an unlock",
			build: func(t *testing.T, f *paymentFixture) *submitIntentRequest {
				giftCard := f.env.newAccount(t)
				f.env.data.setBlockchainAccountExists(f.env.getTimelockAccounts(t, giftCard, common.CoreMintAccount).Unlock)
				return f.env.giftCardRequest(t, f.sender, giftCard, common.CoreMintAccount, quarks)
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_DENIED,
			expectedReason: "an account being opened has already initiated an unlock",
		},
		{
			name: "insufficient balance",
			build: func(t *testing.T, f *paymentFixture) *submitIntentRequest {
				return f.env.giftCardRequest(t, f.sender, f.env.newAccount(t), common.CoreMintAccount, common.ToCoreMintQuarks(paymentFixtureSenderBalance+1))
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_INVALID_INTENT,
			expectedReason: "has insufficient balance to perform action",
		},
	})
}

func TestSubmitIntent_SendPublicPayment_Withdrawal_Validation(t *testing.T) {
	quarks := common.ToCoreMintQuarks(10)

	runPaymentValidationTestCases(t, []paymentValidationTestCase{
		{
			name: "too many actions",
			build: func(t *testing.T, f *paymentFixture) *submitIntentRequest {
				destination := toAta(t, f.env.newAccount(t), common.CoreMintAccount)
				req := f.env.withdrawalRequest(t, f.sender, destination, nil, common.CoreMintAccount, quarks, 0)
				req.actions = []*transactionpb.Action{
					f.env.transferAction(t, 0, f.sender, destination, common.CoreMintAccount, quarks/2),
					f.env.transferAction(t, 1, f.sender, destination, common.CoreMintAccount, quarks/4),
					f.env.transferAction(t, 2, f.sender, destination, common.CoreMintAccount, quarks/4),
				}
				return req
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_INVALID_INTENT,
			expectedReason: "expected 1 or 2 actions for withdrawal",
		},
		{
			name: "fee payment to an ocp account",
			build: func(t *testing.T, f *paymentFixture) *submitIntentRequest {
				return f.env.withdrawalRequest(t, f.sender, f.receiverVault(t), f.receiver, common.CoreMintAccount, quarks, withdrawalFeeQuarks)
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_INVALID_INTENT,
			expectedReason: "CREATE_ON_SEND_WITHDRAWAL fee payment not required for code destination",
		},
		{
			name: "withdrawal to an ocp pool account",
			build: func(t *testing.T, f *paymentFixture) *submitIntentRequest {
				poolAuthority := f.env.newAccount(t)
				f.env.openAccount(t, f.receiver, poolAuthority, common.CoreMintAccount, commonpb.AccountType_POOL, 0)
				return f.env.withdrawalRequest(t, f.sender, f.env.getVault(t, poolAuthority, common.CoreMintAccount), nil, common.CoreMintAccount, quarks, 0)
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_INVALID_INTENT,
			expectedReason: "destination account must be a PRIMARY account",
		},
		{
			name: "withdrawal to a gift card",
			build: func(t *testing.T, f *paymentFixture) *submitIntentRequest {
				giftCard := f.env.newAccount(t)
				f.env.openAccount(t, giftCard, giftCard, common.CoreMintAccount, commonpb.AccountType_REMOTE_SEND_GIFT_CARD, 0)
				return f.env.withdrawalRequest(t, f.sender, f.env.getVault(t, giftCard, common.CoreMintAccount), nil, common.CoreMintAccount, quarks, 0)
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_INVALID_INTENT,
			expectedReason: "destination account must be a PRIMARY or POOL account",
		},
		{
			name: "withdrawal to a closed ocp account",
			build: func(t *testing.T, f *paymentFixture) *submitIntentRequest {
				f.env.setTimelockState(t, f.receiverVault(t), timelock_token_v1.StateClosed)
				return f.env.withdrawalRequest(t, f.sender, f.receiverVault(t), nil, common.CoreMintAccount, quarks, 0)
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_STALE_STATE,
			expectedReason: "destination account has been closed",
		},
		{
			name: "withdrawal to self",
			build: func(t *testing.T, f *paymentFixture) *submitIntentRequest {
				return f.env.withdrawalRequest(t, f.sender, f.senderVault(t), nil, common.CoreMintAccount, quarks, 0)
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_INVALID_INTENT,
			expectedReason: "payment is a no-op",
		},
		{
			name: "fee payment without a destination owner",
			build: func(t *testing.T, f *paymentFixture) *submitIntentRequest {
				return f.env.withdrawalRequest(t, f.sender, toAta(t, f.env.newAccount(t), common.CoreMintAccount), nil, common.CoreMintAccount, quarks, withdrawalFeeQuarks)
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_INVALID_INTENT,
			expectedReason: "destination owner account is required when paying CREATE_ON_SEND_WITHDRAWAL fee",
		},
		{
			name: "destination isn't the ata of the destination owner",
			build: func(t *testing.T, f *paymentFixture) *submitIntentRequest {
				return f.env.withdrawalRequest(t, f.sender, toAta(t, f.env.newAccount(t), common.CoreMintAccount), f.env.newAccount(t), common.CoreMintAccount, quarks, withdrawalFeeQuarks)
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_INVALID_INTENT,
			expectedReason: "destination is not the ata for",
		},
		{
			name: "destination is the ata of another mint",
			build: func(t *testing.T, f *paymentFixture) *submitIntentRequest {
				externalOwner := f.env.newAccount(t)
				return f.env.withdrawalRequest(t, f.sender, toAta(t, externalOwner, f.env.newLaunchpadMint(t)), externalOwner, common.CoreMintAccount, quarks, 0)
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_INVALID_INTENT,
			expectedReason: "destination is not the ata for",
		},
		{
			name: "fee isn't the required amount",
			build: func(t *testing.T, f *paymentFixture) *submitIntentRequest {
				externalOwner := f.env.newAccount(t)
				return f.env.withdrawalRequest(t, f.sender, toAta(t, externalOwner, common.CoreMintAccount), externalOwner, common.CoreMintAccount, quarks, withdrawalFeeQuarks-common.ToCoreMintQuarks(1)/100)
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_INVALID_INTENT,
			expectedReason: "actions[1]: CREATE_ON_SEND_WITHDRAWAL fee payment amount must be 0.50 USD",
		},
		{
			name: "multiple fee payments",
			build: func(t *testing.T, f *paymentFixture) *submitIntentRequest {
				externalOwner := f.env.newAccount(t)
				req := f.env.withdrawalRequest(t, f.sender, toAta(t, externalOwner, common.CoreMintAccount), externalOwner, common.CoreMintAccount, quarks, withdrawalFeeQuarks)
				req.actions[0].GetNoPrivacyTransfer().Amount -= withdrawalFeeQuarks
				req.actions = append(req.actions, f.env.feePaymentAction(t, 2, f.sender, common.CoreMintAccount, withdrawalFeeQuarks))
				return req
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_INVALID_INTENT,
			expectedReason: "expected at most 1 fee payment",
		},
		{
			name: "destination payment doesn't account for the fee",
			build: func(t *testing.T, f *paymentFixture) *submitIntentRequest {
				externalOwner := f.env.newAccount(t)
				req := f.env.withdrawalRequest(t, f.sender, toAta(t, externalOwner, common.CoreMintAccount), externalOwner, common.CoreMintAccount, quarks, withdrawalFeeQuarks)
				req.actions[0].GetNoPrivacyTransfer().Amount = quarks
				return req
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_INVALID_INTENT,
			expectedReason: fmt.Sprintf("actions[0]: must send %d quarks to destination account", quarks-withdrawalFeeQuarks),
		},
		{
			name: "fee payment that isn't from the source",
			build: func(t *testing.T, f *paymentFixture) *submitIntentRequest {
				poolAuthority := f.env.newAccount(t)
				f.env.openAccount(t, f.sender, poolAuthority, common.CoreMintAccount, commonpb.AccountType_POOL, 0)
				f.env.credit(t, f.env.getVault(t, poolAuthority, common.CoreMintAccount), common.ToCoreMintQuarks(10))
				externalOwner := f.env.newAccount(t)
				req := f.env.withdrawalRequest(t, f.sender, toAta(t, externalOwner, common.CoreMintAccount), externalOwner, common.CoreMintAccount, quarks, withdrawalFeeQuarks)
				req.actions[1] = f.env.feePaymentAction(t, 1, poolAuthority, common.CoreMintAccount, withdrawalFeeQuarks)
				return req
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_INVALID_INTENT,
			expectedReason: "must send",
		},
	})
}

func TestSubmitIntent_SendPublicPayment_SwapFunding_Validation(t *testing.T) {
	quarks := common.ToCoreMintQuarks(10)

	swapFundingRequest := func(t *testing.T, f *paymentFixture) *submitIntentRequest {
		swapPda := f.env.getVmSwapPda(t, f.sender, common.CoreMintAccount)
		return f.env.withdrawalRequest(t, f.sender, toAta(t, swapPda, common.CoreMintAccount), swapPda, common.CoreMintAccount, quarks, 0)
	}

	runPaymentValidationTestCases(t, []paymentValidationTestCase{
		{
			name: "swap isn't pending funding",
			build: func(t *testing.T, f *paymentFixture) *submitIntentRequest {
				req := swapFundingRequest(t, f)
				f.env.newSwapRecord(t, f.sender, common.CoreMintAccount, f.env.newLaunchpadMint(t), req.intentId, quarks, swap.StateFunding)
				return req
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_DENIED,
			expectedReason: "swap state is funding",
		},
		{
			name: "not the owner of the swap",
			build: func(t *testing.T, f *paymentFixture) *submitIntentRequest {
				req := swapFundingRequest(t, f)
				f.env.newSwapRecord(t, f.receiver, common.CoreMintAccount, f.env.newLaunchpadMint(t), req.intentId, quarks, swap.StateCreated)
				return req
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_DENIED,
			expectedReason: "not the owner of the swap",
		},
		{
			name: "not a withdrawal",
			build: func(t *testing.T, f *paymentFixture) *submitIntentRequest {
				req := swapFundingRequest(t, f)
				req.metadata.GetSendPublicPayment().IsWithdrawal = false
				f.env.newSwapRecord(t, f.sender, common.CoreMintAccount, f.env.newLaunchpadMint(t), req.intentId, quarks, swap.StateCreated)
				return req
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_INVALID_INTENT,
			expectedReason: "swap funding must be an external withdrawal",
		},
		{
			name: "missing destination owner",
			build: func(t *testing.T, f *paymentFixture) *submitIntentRequest {
				req := swapFundingRequest(t, f)
				req.metadata.GetSendPublicPayment().DestinationOwner = nil
				f.env.newSwapRecord(t, f.sender, common.CoreMintAccount, f.env.newLaunchpadMint(t), req.intentId, quarks, swap.StateCreated)
				return req
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_INVALID_INTENT,
			expectedReason: "destination owner must be a vm swap pda",
		},
		{
			name: "destination owner isn't a vm swap pda",
			build: func(t *testing.T, f *paymentFixture) *submitIntentRequest {
				externalOwner := f.env.newAccount(t)
				req := f.env.withdrawalRequest(t, f.sender, toAta(t, externalOwner, common.CoreMintAccount), externalOwner, common.CoreMintAccount, quarks, 0)
				f.env.newSwapRecord(t, f.sender, common.CoreMintAccount, f.env.newLaunchpadMint(t), req.intentId, quarks, swap.StateCreated)
				return req
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_INVALID_INTENT,
			expectedReason: "destination owner must be a vm swap pda",
		},
		{
			name: "wrong swap mint",
			build: func(t *testing.T, f *paymentFixture) *submitIntentRequest {
				req := swapFundingRequest(t, f)
				fromMint := f.env.newLaunchpadMint(t)
				f.env.newSwapRecord(t, f.sender, fromMint, common.CoreMintAccount, req.intentId, quarks, swap.StateCreated)
				return req
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_INVALID_INTENT,
			expectedReason: "must fund swap with",
		},
		{
			name: "wrong swap amount",
			build: func(t *testing.T, f *paymentFixture) *submitIntentRequest {
				req := swapFundingRequest(t, f)
				f.env.newSwapRecord(t, f.sender, common.CoreMintAccount, f.env.newLaunchpadMint(t), req.intentId, quarks+1, swap.StateCreated)
				return req
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_INVALID_INTENT,
			expectedReason: fmt.Sprintf("must fund swap with %d quarks", quarks+1),
		},
		{
			name: "another user's vm swap pda",
			build: func(t *testing.T, f *paymentFixture) *submitIntentRequest {
				swapPda := f.env.getVmSwapPda(t, f.receiver, common.CoreMintAccount)
				req := f.env.withdrawalRequest(t, f.sender, toAta(t, swapPda, common.CoreMintAccount), swapPda, common.CoreMintAccount, quarks, 0)
				f.env.newSwapRecord(t, f.sender, common.CoreMintAccount, f.env.newLaunchpadMint(t), req.intentId, quarks, swap.StateCreated)
				return req
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_DENIED,
			expectedReason: "can only send to your own vm swap pda",
		},
		{
			name: "another user's vm swap pda without a reserved intent ID",
			build: func(t *testing.T, f *paymentFixture) *submitIntentRequest {
				swapPda := f.env.getVmSwapPda(t, f.receiver, common.CoreMintAccount)
				return f.env.withdrawalRequest(t, f.sender, toAta(t, swapPda, common.CoreMintAccount), swapPda, common.CoreMintAccount, quarks, 0)
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_DENIED,
			expectedReason: "intent id is not reserved for a swap",
		},
		{
			name: "vm swap pda for another mint",
			build: func(t *testing.T, f *paymentFixture) *submitIntentRequest {
				mint := f.env.newLaunchpadMint(t)
				f.env.openAccount(t, f.sender, f.sender, mint, commonpb.AccountType_PRIMARY, 0)
				swapPda := f.env.getVmSwapPda(t, f.sender, mint)
				req := f.env.withdrawalRequest(t, f.sender, toAta(t, swapPda, common.CoreMintAccount), swapPda, common.CoreMintAccount, quarks, 0)
				f.env.newSwapRecord(t, f.sender, common.CoreMintAccount, mint, req.intentId, quarks, swap.StateCreated)
				return req
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_INVALID_INTENT,
			expectedReason: "vm swap pda is for a different mint",
		},
		{
			name: "own vm swap pda without a reserved intent ID",
			build: func(t *testing.T, f *paymentFixture) *submitIntentRequest {
				return swapFundingRequest(t, f)
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_DENIED,
			expectedReason: "intent id is not reserved for a swap",
		},
	})
}

func TestSubmitIntent_SendPublicPayment_Antispam(t *testing.T) {
	f := newPaymentFixture(t, withAntispamChecks())

	f.env.antispamIntegration.setAllow(false)
	for _, req := range []*submitIntentRequest{
		f.env.directPaymentRequest(t, f.sender, f.receiverVault(t), common.CoreMintAccount, common.ToCoreMintQuarks(1)),
		f.env.giftCardRequest(t, f.sender, f.env.newAccount(t), common.CoreMintAccount, common.ToCoreMintQuarks(1)),
		f.env.withdrawalRequest(t, f.sender, toAta(t, f.env.newAccount(t), common.CoreMintAccount), nil, common.CoreMintAccount, common.ToCoreMintQuarks(1), 0),
	} {
		before := f.env.snapshot(t, f.sender, f.receiver)
		f.env.submitIntent(t, req).requireDenied(t, ErrTooManyPayments.Error())
		f.env.requireNoChanges(t, before)
	}
	assert.Len(t, f.env.antispamIntegration.getCalls(), 3)

	f.env.antispamIntegration.setAllow(true)
	req := f.env.directPaymentRequest(t, f.sender, f.receiverVault(t), common.CoreMintAccount, common.ToCoreMintQuarks(1))
	f.env.submitIntent(t, req).requireSuccess(t)
	calls := f.env.antispamIntegration.getCalls()
	require.Len(t, calls, 4)
	assert.Equal(t, "send_payment:"+f.receiverVault(t).PublicKey().ToBase58(), calls[3])
}

func TestSubmitIntent_SendPublicPayment_Aml(t *testing.T) {
	t.Run("per transaction limit", func(t *testing.T) {
		env := setupTestEnv(t, withAmlChecks())
		sender := env.newFundedUser(t, common.ToCoreMintQuarks(1_000))
		receiver := env.newUser(t)

		req := env.directPaymentRequest(t, sender, env.getVault(t, receiver, common.CoreMintAccount), common.CoreMintAccount, common.ToCoreMintQuarks(251))
		before := env.snapshot(t, sender, receiver)
		env.submitIntent(t, req).requireDenied(t, ErrTransactionLimitExceeded.Error())
		env.requireNoChanges(t, before)

		req = env.giftCardRequest(t, sender, env.newAccount(t), common.CoreMintAccount, common.ToCoreMintQuarks(251))
		env.submitIntent(t, req).requireDenied(t, ErrTransactionLimitExceeded.Error())

		// Limits are per currency
		exchangeRate := env.signedExchangeRate(t, currency_lib.CAD, 2.0, time.Now())
		req = env.directPaymentRequest(t, sender, env.getVault(t, receiver, common.CoreMintAccount), common.CoreMintAccount, common.ToCoreMintQuarks(126))
		req.metadata.GetSendPublicPayment().ExchangeData = clientExchangeData(env.coreMintExchangeData(t, common.ToCoreMintQuarks(126), exchangeRate))
		env.submitIntent(t, req).requireDenied(t, ErrTransactionLimitExceeded.Error())

		req = env.directPaymentRequest(t, sender, env.getVault(t, receiver, common.CoreMintAccount), common.CoreMintAccount, common.ToCoreMintQuarks(250))
		env.submitIntent(t, req).requireSuccess(t)

		// Withdrawals are always allowed
		req = env.withdrawalRequest(t, sender, toAta(t, env.newAccount(t), common.CoreMintAccount), nil, common.CoreMintAccount, common.ToCoreMintQuarks(500), 0)
		env.submitIntent(t, req).requireSuccess(t)
	})

	t.Run("unsupported currency", func(t *testing.T) {
		env := setupTestEnv(t, withAmlChecks())
		sender := env.newFundedUser(t, common.ToCoreMintQuarks(1_000))
		receiver := env.newUser(t)

		exchangeRate := env.signedExchangeRate(t, currency_lib.Code("xyz"), 1.0, time.Now())
		req := env.directPaymentRequest(t, sender, env.getVault(t, receiver, common.CoreMintAccount), common.CoreMintAccount, common.ToCoreMintQuarks(1))
		req.metadata.GetSendPublicPayment().ExchangeData = clientExchangeData(env.coreMintExchangeData(t, common.ToCoreMintQuarks(1), exchangeRate))
		before := env.snapshot(t, sender, receiver)
		env.submitIntent(t, req).requireDenied(t, ErrTransactionLimitExceeded.Error())
		env.requireNoChanges(t, before)
	})

	t.Run("daily limit", func(t *testing.T) {
		env := setupTestEnv(t, withAmlChecks())
		sender := env.newFundedUser(t, common.ToCoreMintQuarks(10_000))
		receiver := env.newUser(t)
		receiverVault := env.getVault(t, receiver, common.CoreMintAccount)

		// Payments and gift cards count towards the limit
		for range 4 {
			req := env.directPaymentRequest(t, sender, receiverVault, common.CoreMintAccount, common.ToCoreMintQuarks(240))
			env.submitIntent(t, req).requireSuccess(t)
		}
		env.issueGiftCard(t, sender, common.CoreMintAccount, common.ToCoreMintQuarks(240))

		// Withdrawals don't count towards the limit, and are always allowed
		req := env.withdrawalRequest(t, sender, toAta(t, env.newAccount(t), common.CoreMintAccount), nil, common.CoreMintAccount, common.ToCoreMintQuarks(240), 0)
		env.submitIntent(t, req).requireSuccess(t)

		req = env.directPaymentRequest(t, sender, receiverVault, common.CoreMintAccount, common.ToCoreMintQuarks(1))
		before := env.snapshot(t, sender, receiver)
		env.submitIntent(t, req).requireDenied(t, ErrTransactionLimitExceeded.Error())
		env.requireNoChanges(t, before)

		// The limit is per owner
		other := env.newFundedUser(t, common.ToCoreMintQuarks(100))
		req = env.directPaymentRequest(t, other, receiverVault, common.CoreMintAccount, common.ToCoreMintQuarks(1))
		env.submitIntent(t, req).requireSuccess(t)
	})
}

//
// ReceivePaymentsPublicly
//

// setupGiftCard directly saves the records for a core mint gift card issued at
// the provided time, bypassing SubmitIntent
func (e *testEnv) setupGiftCard(t *testing.T, issuer *common.Account, quarks uint64, issuedAt time.Time) *common.Account {
	giftCard := e.newAccount(t)
	timelockAccounts := e.getTimelockAccounts(t, giftCard, common.CoreMintAccount)
	usdValue := float64(quarks) / float64(common.CoreMintQuarksPerUnit)

	require.NoError(t, e.data.SaveIntent(e.ctx, &intent.Record{
		IntentId:              testutil.NewRandomAccount(t).PublicKey().ToBase58(),
		IntentType:            intent.SendPublicPayment,
		MintAccount:           common.CoreMintAccount.PublicKey().ToBase58(),
		InitiatorOwnerAccount: issuer.PublicKey().ToBase58(),
		SendPublicPaymentMetadata: &intent.SendPublicPaymentMetadata{
			DestinationTokenAccount: timelockAccounts.Vault.PublicKey().ToBase58(),
			Quantity:                quarks,
			ExchangeCurrency:        currency_lib.USD,
			ExchangeRate:            1.0,
			NativeAmount:            usdValue,
			UsdMarketValue:          usdValue,
			IsIndirectSend:          true,
		},
		State:     intent.StatePending,
		CreatedAt: issuedAt,
	}))

	accountInfoRecord := &account.Record{
		OwnerAccount:            giftCard.PublicKey().ToBase58(),
		AuthorityAccount:        giftCard.PublicKey().ToBase58(),
		TokenAccount:            timelockAccounts.Vault.PublicKey().ToBase58(),
		MintAccount:             common.CoreMintAccount.PublicKey().ToBase58(),
		AccountType:             commonpb.AccountType_REMOTE_SEND_GIFT_CARD,
		RequiresAutoReturnCheck: true,
		CreatedAt:               issuedAt,
	}
	require.NoError(t, e.data.CreateAccountInfo(e.ctx, accountInfoRecord))
	require.NoError(t, balance_util.CreateRecordInTx(e.ctx, e.data, accountInfoRecord))
	e.credit(t, timelockAccounts.Vault, quarks)

	timelockRecord := timelockAccounts.ToDBRecord()
	timelockRecord.VaultState = timelock_token_v1.StateLocked
	timelockRecord.Block = 1
	require.NoError(t, e.data.SaveTimelock(e.ctx, timelockRecord))

	return giftCard
}

// expectedClaimChanges are the records for claiming a gift card into the
// claimant's primary account, including the issuer's completed issuance
func (e *testEnv) expectedClaimChanges(t *testing.T, result *submitIntentResult, claimIntentId, issueIntentId, claimant, issuer, giftCard, mint *common.Account, quarks uint64, claimantBalanceBefore *balance.Record) *dbChanges {
	claimIntentRecord := e.requireIntent(t, claimIntentId)
	issueIntentRecord := e.requireIntent(t, issueIntentId)
	issueMetadata := issueIntentRecord.SendPublicPaymentMetadata

	giftCardVault := e.getVault(t, giftCard, mint)
	claimantVault := e.getVault(t, claimant, mint)

	received := expectedHistory(claimIntentRecord, history.IndirectlyReceived, claimant, nil, issueMetadata.ExchangeCurrency, issueMetadata.NativeAmount, quarks)
	received.GiftCardVault = pointer.String(giftCardVault.PublicKey().ToBase58())

	issued := expectedHistory(issueIntentRecord, history.IndirectlySent, issuer, nil, issueMetadata.ExchangeCurrency, issueMetadata.NativeAmount, quarks)
	issued.GiftCardVault = pointer.String(giftCardVault.PublicKey().ToBase58())

	return &dbChanges{
		intents: []*intent.Record{{
			IntentId:              claimIntentId.PublicKey().ToBase58(),
			IntentType:            intent.ReceivePaymentsPublicly,
			MintAccount:           mint.PublicKey().ToBase58(),
			InitiatorOwnerAccount: claimant.PublicKey().ToBase58(),
			ReceivePaymentsPubliclyMetadata: &intent.ReceivePaymentsPubliclyMetadata{
				Source:                   giftCardVault.PublicKey().ToBase58(),
				Quantity:                 quarks,
				IsIndirectSend:           true,
				OriginalExchangeCurrency: issueMetadata.ExchangeCurrency,
				OriginalExchangeRate:     issueMetadata.ExchangeRate,
				OriginalNativeAmount:     issueMetadata.NativeAmount,
				UsdMarketValue:           issueMetadata.UsdMarketValue,
			},
			State: intent.StatePending,
		}},
		actions: []*action.Record{
			expectedWithdrawAction(claimIntentRecord, 0, giftCardVault, claimantVault, quarks),
		},
		fulfillments: []*fulfillment.Record{
			expectedWithdrawFulfillment(t, claimIntentRecord, result, 0, giftCardVault, claimantVault),
		},
		balances: []*balance.Record{
			// Drained and closed
			expectedBalance(giftCardVault, giftCard, mint, 0, 0, false),
			expectedBalance(claimantVault, claimant, mint, claimantBalanceBefore.Quarks+quarks, claimantBalanceBefore.UsdCostBasis+usdCostBasis(issueMetadata.UsdMarketValue), true),
		},
		history: []*history.Record{received, issued},
	}
}

func TestSubmitIntent_ReceivePaymentsPublicly_HappyPath(t *testing.T) {
	for _, useLaunchpadMint := range []bool{false, true} {
		t.Run(fmt.Sprintf("launchpad=%v", useLaunchpadMint), func(t *testing.T) {
			env := setupTestEnv(t)

			mint := common.CoreMintAccount
			quarks := common.ToCoreMintQuarks(25)
			if useLaunchpadMint {
				mint = env.newLaunchpadMint(t)
				quarks = currencycreator.ToQuarks(25)
			}

			sender := env.newFundedUser(t, 2*quarks, mint)
			receiver := env.newFundedUser(t, 0, mint)
			receiverVault := env.getVault(t, receiver, mint)

			giftCard, issueIntentId := env.issueGiftCard(t, sender, mint, quarks)
			giftCardVault := env.getVault(t, giftCard, mint)
			receiverBalanceBefore := env.getBalance(t, receiverVault)

			req := env.claimGiftCardRequest(t, receiver, giftCard, mint, quarks)
			before := env.snapshot(t, sender, receiver, giftCard)
			result := env.submitIntent(t, req)
			result.requireSuccess(t)
			requireServerParameterNonces(t, result.serverParameters, 1)
			assert.NotNil(t, result.serverParameters[0].GetNoPrivacyWithdraw())

			env.requireChanges(t, before, env.expectedClaimChanges(t, result, req.intentId, issueIntentId, receiver, sender, giftCard, mint, quarks, receiverBalanceBefore))
			env.requireReservedNonces(t, result)

			claimedActionRecord, err := env.data.GetGiftCardClaimedAction(env.ctx, giftCardVault.PublicKey().ToBase58())
			require.NoError(t, err)
			assert.Equal(t, req.intentId.PublicKey().ToBase58(), claimedActionRecord.Intent)
		})
	}
}

func TestSubmitIntent_ReceivePaymentsPublicly_ClaimByIssuer(t *testing.T) {
	env := setupTestEnv(t)

	sender := env.newFundedUser(t, common.ToCoreMintQuarks(100))
	senderVault := env.getVault(t, sender, common.CoreMintAccount)
	quarks := common.ToCoreMintQuarks(10)

	giftCard, issueIntentId := env.issueGiftCard(t, sender, common.CoreMintAccount, quarks)
	senderBalanceBefore := env.getBalance(t, senderVault)
	require.Equal(t, common.ToCoreMintQuarks(90), senderBalanceBefore.Quarks)

	req := env.claimGiftCardRequest(t, sender, giftCard, common.CoreMintAccount, quarks)
	before := env.snapshot(t, sender, giftCard)
	result := env.submitIntent(t, req)
	result.requireSuccess(t)

	env.requireChanges(t, before, env.expectedClaimChanges(t, result, req.intentId, issueIntentId, sender, sender, giftCard, common.CoreMintAccount, quarks, senderBalanceBefore))
	env.requireBalance(t, senderVault, common.ToCoreMintQuarks(100))
}

func TestSubmitIntent_ReceivePaymentsPublicly_SubmittedByGiftCardOwner(t *testing.T) {
	env := setupTestEnv(t)

	sender := env.newFundedUser(t, common.ToCoreMintQuarks(100))
	receiver := env.newUser(t)
	receiverVault := env.getVault(t, receiver, common.CoreMintAccount)
	quarks := common.ToCoreMintQuarks(10)

	giftCard, issueIntentId := env.issueGiftCard(t, sender, common.CoreMintAccount, quarks)

	// Destinations other than a primary account are rejected
	receiverPoolAuthority := env.newAccount(t)
	env.openAccount(t, receiver, receiverPoolAuthority, common.CoreMintAccount, commonpb.AccountType_POOL, 0)
	for _, destination := range []*common.Account{
		toAta(t, env.newAccount(t), common.CoreMintAccount),
		env.getVault(t, receiverPoolAuthority, common.CoreMintAccount),
	} {
		req := env.claimGiftCardRequest(t, receiver, giftCard, common.CoreMintAccount, quarks)
		req.owner = giftCard
		req.actions[0] = env.withdrawAction(t, 0, giftCard, destination, common.CoreMintAccount, quarks, false)

		before := env.snapshot(t, sender, receiver, giftCard)
		env.submitIntent(t, req).requireInvalidIntent(t, "actions[0]: destination must be a primary account")
		env.requireNoChanges(t, before)
	}

	// The gift card owner signs the intent on behalf of the receiver, who is
	// the initiator via their primary account
	receiverBalanceBefore := env.getBalance(t, receiverVault)
	req := env.claimGiftCardRequest(t, receiver, giftCard, common.CoreMintAccount, quarks)
	req.owner = giftCard

	before := env.snapshot(t, sender, receiver, giftCard)
	result := env.submitIntent(t, req)
	result.requireSuccess(t)

	env.requireChanges(t, before, env.expectedClaimChanges(t, result, req.intentId, issueIntentId, receiver, sender, giftCard, common.CoreMintAccount, quarks, receiverBalanceBefore))
	env.requireReservedNonces(t, result)
}

func TestSubmitIntent_ReceivePaymentsPublicly_Antispam(t *testing.T) {
	env := setupTestEnv(t, withAntispamChecks())

	sender := env.newFundedUser(t, common.ToCoreMintQuarks(100))
	receiver := env.newUser(t)
	quarks := common.ToCoreMintQuarks(10)
	giftCard, _ := env.issueGiftCard(t, sender, common.CoreMintAccount, quarks)

	env.antispamIntegration.setAllow(false)
	req := env.claimGiftCardRequest(t, receiver, giftCard, common.CoreMintAccount, quarks)
	before := env.snapshot(t, sender, receiver, giftCard)
	env.submitIntent(t, req).requireDenied(t, ErrTooManyPayments.Error())
	env.requireNoChanges(t, before)

	env.antispamIntegration.setAllow(true)
	req = env.claimGiftCardRequest(t, receiver, giftCard, common.CoreMintAccount, quarks)
	env.submitIntent(t, req).requireSuccess(t)

	calls := env.antispamIntegration.getCalls()
	assert.Equal(t, "receive_payments", calls[len(calls)-1])
}

func TestSubmitIntent_ReceivePaymentsPublicly_AmlDoesNotApply(t *testing.T) {
	env := setupTestEnv(t, withAmlChecks())

	sender := env.newFundedUser(t, common.ToCoreMintQuarks(1_000))
	receiver := env.newUser(t)

	// Max out the receiver's daily limit
	env.credit(t, env.getVault(t, receiver, common.CoreMintAccount), common.ToCoreMintQuarks(1_200))
	for range 5 {
		req := env.directPaymentRequest(t, receiver, env.getVault(t, sender, common.CoreMintAccount), common.CoreMintAccount, common.ToCoreMintQuarks(240))
		env.submitIntent(t, req).requireSuccess(t)
	}

	giftCard, _ := env.issueGiftCard(t, sender, common.CoreMintAccount, common.ToCoreMintQuarks(200))
	req := env.claimGiftCardRequest(t, receiver, giftCard, common.CoreMintAccount, common.ToCoreMintQuarks(200))
	env.submitIntent(t, req).requireSuccess(t)
}

func TestSubmitIntent_ReceivePaymentsPublicly_ConcurrentClaims(t *testing.T) {
	env := setupTestEnv(t)

	sender := env.newFundedUser(t, common.ToCoreMintQuarks(100))
	quarks := common.ToCoreMintQuarks(10)
	giftCard, issueIntentId := env.issueGiftCard(t, sender, common.CoreMintAccount, quarks)

	const claimants = 8
	var receivers []*common.Account
	var reqs []*submitIntentRequest
	for range claimants {
		receiver := env.newUser(t)
		receivers = append(receivers, receiver)
		reqs = append(reqs, env.claimGiftCardRequest(t, receiver, giftCard, common.CoreMintAccount, quarks))
	}

	before := env.snapshot(t, append([]*common.Account{sender, giftCard}, receivers...)...)

	results := make([]*submitIntentResult, claimants)
	var wg sync.WaitGroup
	for i := range claimants {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i] = env.submitIntent(t, reqs[i])
		}(i)
	}
	wg.Wait()

	winner := -1
	for i, result := range results {
		require.NoError(t, result.err)
		require.NotNil(t, result.resp)
		if result.resp.GetSuccess() != nil {
			require.Equal(t, -1, winner, "multiple claims succeeded")
			winner = i
			continue
		}
		result.requireStaleState(t, "gift card balance has already been claimed")
	}
	require.NotEqual(t, -1, winner)

	// Exactly the winning claim was persisted, and the losing claims left
	// nothing behind
	winnerBalanceBefore := before.balances[env.getVault(t, receivers[winner], common.CoreMintAccount).PublicKey().ToBase58()]
	require.NotNil(t, winnerBalanceBefore)
	env.requireChanges(t, before, env.expectedClaimChanges(t, results[winner], reqs[winner].intentId, issueIntentId, receivers[winner], sender, giftCard, common.CoreMintAccount, quarks, winnerBalanceBefore))
	env.requireReservedNonces(t, results[winner])
}

func TestSubmitIntent_ReceivePaymentsPublicly_Validation(t *testing.T) {
	type fixture struct {
		env      *testEnv
		sender   *common.Account
		receiver *common.Account
		giftCard *common.Account
		quarks   uint64
	}

	giftCardVault := func(t *testing.T, f *fixture) *common.Account {
		return f.env.getVault(t, f.giftCard, common.CoreMintAccount)
	}

	for _, tc := range []struct {
		name           string
		build          func(t *testing.T, f *fixture) *submitIntentRequest
		expectedCode   transactionpb.SubmitIntentResponse_Error_Code
		expectedReason string
	}{
		{
			name: "already claimed",
			build: func(t *testing.T, f *fixture) *submitIntentRequest {
				other := f.env.newUser(t)
				f.env.submitIntent(t, f.env.claimGiftCardRequest(t, other, f.giftCard, common.CoreMintAccount, f.quarks)).requireSuccess(t)
				return f.env.claimGiftCardRequest(t, f.receiver, f.giftCard, common.CoreMintAccount, f.quarks)
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_STALE_STATE,
			expectedReason: "gift card balance has already been claimed",
		},
		{
			name: "partial claim",
			build: func(t *testing.T, f *fixture) *submitIntentRequest {
				return f.env.claimGiftCardRequest(t, f.receiver, f.giftCard, common.CoreMintAccount, f.quarks-1)
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_INVALID_INTENT,
			expectedReason: fmt.Sprintf("must receive entire gift card balance of %d quarks", common.ToCoreMintQuarks(10)),
		},
		{
			name: "action amount doesn't match intent",
			build: func(t *testing.T, f *fixture) *submitIntentRequest {
				req := f.env.claimGiftCardRequest(t, f.receiver, f.giftCard, common.CoreMintAccount, f.quarks)
				req.actions[0].GetNoPrivacyWithdraw().Amount = f.quarks - 1
				return req
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_INVALID_INTENT,
			expectedReason: fmt.Sprintf("actions[0]: must receive %d quarks from source account", common.ToCoreMintQuarks(10)),
		},
		{
			name: "auto-return is being scheduled",
			build: func(t *testing.T, f *fixture) *submitIntentRequest {
				autoReturnActionRecord, err := f.env.data.GetGiftCardAutoReturnAction(f.env.ctx, giftCardVault(t, f).PublicKey().ToBase58())
				require.NoError(t, err)
				autoReturnActionRecord.State = action.StatePending
				require.NoError(t, f.env.data.UpdateAction(f.env.ctx, autoReturnActionRecord))
				return f.env.claimGiftCardRequest(t, f.receiver, f.giftCard, common.CoreMintAccount, f.quarks)
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_STALE_STATE,
			expectedReason: "gift card is expired",
		},
		{
			name: "expired by age",
			build: func(t *testing.T, f *fixture) *submitIntentRequest {
				expired := f.env.setupGiftCard(t, f.sender, f.quarks, time.Now().Add(-7*24*time.Hour+30*time.Second))
				return f.env.claimGiftCardRequest(t, f.receiver, expired, common.CoreMintAccount, f.quarks)
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_STALE_STATE,
			expectedReason: "gift card is expired",
		},
		{
			name: "source isn't a gift card",
			build: func(t *testing.T, f *fixture) *submitIntentRequest {
				return f.env.claimGiftCardRequest(t, f.receiver, f.sender, common.CoreMintAccount, f.quarks)
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_INVALID_INTENT,
			expectedReason: "source is not a remote send gift card",
		},
		{
			name: "gift card isn't managed by code",
			build: func(t *testing.T, f *fixture) *submitIntentRequest {
				f.env.setTimelockState(t, giftCardVault(t, f), timelock_token_v1.StateWaitingForTimeout)
				return f.env.claimGiftCardRequest(t, f.receiver, f.giftCard, common.CoreMintAccount, f.quarks)
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_DENIED,
			expectedReason: ErrSourceNotManagedByCode.Error(),
		},
		{
			name: "gift card is closed",
			build: func(t *testing.T, f *fixture) *submitIntentRequest {
				f.env.setTimelockState(t, giftCardVault(t, f), timelock_token_v1.StateClosed)
				return f.env.claimGiftCardRequest(t, f.receiver, f.giftCard, common.CoreMintAccount, f.quarks)
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_STALE_STATE,
			expectedReason: "gift card balance has already been claimed",
		},
		{
			name: "receiver account isn't managed by code",
			build: func(t *testing.T, f *fixture) *submitIntentRequest {
				f.env.setTimelockState(t, f.env.getVault(t, f.receiver, common.CoreMintAccount), timelock_token_v1.StateUnlocked)
				return f.env.claimGiftCardRequest(t, f.receiver, f.giftCard, common.CoreMintAccount, f.quarks)
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_DENIED,
			expectedReason: ErrSourceNotManagedByCode.Error(),
		},
		{
			name: "exchange data provided",
			build: func(t *testing.T, f *fixture) *submitIntentRequest {
				req := f.env.claimGiftCardRequest(t, f.receiver, f.giftCard, common.CoreMintAccount, f.quarks)
				req.metadata.GetReceivePaymentsPublicly().ExchangeData = &transactionpb.ExchangeData{
					Currency:     "usd",
					ExchangeRate: 1.0,
					NativeAmount: 10.0,
					Quarks:       f.quarks,
					Mint:         common.CoreMintAccount.ToProto(),
				}
				return req
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_INVALID_INTENT,
			expectedReason: "exchange data cannot be set",
		},
		{
			name: "multiple actions",
			build: func(t *testing.T, f *fixture) *submitIntentRequest {
				req := f.env.claimGiftCardRequest(t, f.receiver, f.giftCard, common.CoreMintAccount, f.quarks)
				req.actions = append(req.actions, f.env.transferAction(t, 1, f.receiver, f.env.getVault(t, f.sender, common.CoreMintAccount), common.CoreMintAccount, 1))
				return req
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_INVALID_INTENT,
			expectedReason: "expected 1 action",
		},
		{
			name: "destination isn't the receiver's primary account",
			build: func(t *testing.T, f *fixture) *submitIntentRequest {
				req := f.env.claimGiftCardRequest(t, f.receiver, f.giftCard, common.CoreMintAccount, f.quarks)
				req.actions[0] = f.env.withdrawAction(t, 0, f.giftCard, f.env.getVault(t, f.sender, common.CoreMintAccount), common.CoreMintAccount, f.quarks, false)
				return req
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_INVALID_INTENT,
			expectedReason: "actions[0]: must send payment to PRIMARY account",
		},
		{
			name: "transfer instead of a withdraw",
			build: func(t *testing.T, f *fixture) *submitIntentRequest {
				req := f.env.claimGiftCardRequest(t, f.receiver, f.giftCard, common.CoreMintAccount, f.quarks)
				req.actions[0] = f.env.transferAction(t, 0, f.giftCard, f.env.getVault(t, f.receiver, common.CoreMintAccount), common.CoreMintAccount, f.quarks)
				return req
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_INVALID_INTENT,
			expectedReason: "actions[0]: transfer must be a public withdraw",
		},
		{
			name: "withdraw from another account",
			build: func(t *testing.T, f *fixture) *submitIntentRequest {
				req := f.env.claimGiftCardRequest(t, f.receiver, f.giftCard, common.CoreMintAccount, f.quarks)
				req.actions[0] = f.env.withdrawAction(t, 0, f.sender, f.env.getVault(t, f.receiver, common.CoreMintAccount), common.CoreMintAccount, f.quarks, false)
				return req
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_INVALID_INTENT,
			expectedReason: "must receive payments from source account",
		},
		{
			name: "intent ID reserved for a swap",
			build: func(t *testing.T, f *fixture) *submitIntentRequest {
				req := f.env.claimGiftCardRequest(t, f.receiver, f.giftCard, common.CoreMintAccount, f.quarks)
				f.env.newSwapRecord(t, f.receiver, common.CoreMintAccount, f.env.newLaunchpadMint(t), req.intentId, f.quarks, swap.StateCreated)
				return req
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_DENIED,
			expectedReason: "intent id reserved to fund swap",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env := setupTestEnv(t)

			f := &fixture{
				env:      env,
				sender:   env.newFundedUser(t, common.ToCoreMintQuarks(100)),
				receiver: env.newUser(t),
				quarks:   common.ToCoreMintQuarks(10),
			}
			f.giftCard, _ = env.issueGiftCard(t, f.sender, common.CoreMintAccount, f.quarks)

			req := tc.build(t, f)

			before := env.snapshot(t, f.sender, f.receiver, f.giftCard)
			result := env.submitIntent(t, req)
			result.requireError(t, tc.expectedCode, tc.expectedReason)
			assert.Empty(t, result.serverParameters)

			env.requireNoChanges(t, before)
		})
	}
}

//
// SendPublicMultiPayment
//

func TestSubmitIntent_SendPublicMultiPayment_HappyPath(t *testing.T) {
	env := setupTestEnv(t)

	sender := env.newFundedUser(t, common.ToCoreMintQuarks(100))
	receiver1 := env.newUser(t)
	receiver2 := env.newUser(t)
	senderVault := env.getVault(t, sender, common.CoreMintAccount)
	receiver1Vault := env.getVault(t, receiver1, common.CoreMintAccount)
	receiver2Vault := env.getVault(t, receiver2, common.CoreMintAccount)

	req := env.multiPaymentRequest(t, sender, common.CoreMintAccount,
		testMultiPayment{destination: receiver1Vault, quarks: common.ToCoreMintQuarks(10)},
		testMultiPayment{destination: receiver2Vault, destinationOwner: receiver2, quarks: common.ToCoreMintQuarks(15)},
	)

	before := env.snapshot(t, sender, receiver1, receiver2)
	result := env.submitIntent(t, req)
	result.requireSuccess(t)
	requireServerParameterNonces(t, result.serverParameters, 1, 1)
	for _, serverParameter := range result.serverParameters {
		assert.NotNil(t, serverParameter.GetNoPrivacyTransfer())
	}

	intentRecord := env.requireIntent(t, req.intentId)
	env.requireChanges(t, before, &dbChanges{
		intents: []*intent.Record{{
			IntentId:              req.intentId.PublicKey().ToBase58(),
			IntentType:            intent.SendPublicMultiPayment,
			MintAccount:           common.CoreMintAccount.PublicKey().ToBase58(),
			InitiatorOwnerAccount: sender.PublicKey().ToBase58(),
			SendPublicMultiPaymentMetadata: &intent.SendPublicMultiPaymentMetadata{
				Source: senderVault.PublicKey().ToBase58(),
				Payments: []*intent.Payment{
					{
						DestinationOwnerAccount: receiver1.PublicKey().ToBase58(),
						DestinationTokenAccount: receiver1Vault.PublicKey().ToBase58(),
						Quantity:                common.ToCoreMintQuarks(10),
						NativeAmount:            10.0,
						UsdMarketValue:          10.0,
					},
					{
						DestinationOwnerAccount: receiver2.PublicKey().ToBase58(),
						DestinationTokenAccount: receiver2Vault.PublicKey().ToBase58(),
						Quantity:                common.ToCoreMintQuarks(15),
						NativeAmount:            15.0,
						UsdMarketValue:          15.0,
					},
				},
				Quantity:         common.ToCoreMintQuarks(25),
				ExchangeCurrency: currency_lib.USD,
				ExchangeRate:     1.0,
				NativeAmount:     25.0,
				UsdMarketValue:   25.0,
			},
			State: intent.StatePending,
		}},
		actions: []*action.Record{
			expectedTransferAction(intentRecord, 0, senderVault, receiver1Vault, common.ToCoreMintQuarks(10)),
			expectedTransferAction(intentRecord, 1, senderVault, receiver2Vault, common.ToCoreMintQuarks(15)),
		},
		fulfillments: []*fulfillment.Record{
			expectedTransferFulfillment(t, intentRecord, result, 0, senderVault, receiver1Vault),
			expectedTransferFulfillment(t, intentRecord, result, 1, senderVault, receiver2Vault),
		},
		balances: []*balance.Record{
			expectedBalance(senderVault, sender, common.CoreMintAccount, common.ToCoreMintQuarks(75), -usdCostBasis(25), true),
			expectedBalance(receiver1Vault, receiver1, common.CoreMintAccount, common.ToCoreMintQuarks(10), usdCostBasis(10), true),
			expectedBalance(receiver2Vault, receiver2, common.CoreMintAccount, common.ToCoreMintQuarks(15), usdCostBasis(15), true),
		},
		history: []*history.Record{
			// The sender has a single record for the total, with no counterparty
			expectedHistory(intentRecord, history.DirectlySent, sender, nil, currency_lib.USD, 25.0, common.ToCoreMintQuarks(25)),
			expectedHistory(intentRecord, history.DirectlyReceived, receiver1, sender, currency_lib.USD, 10.0, common.ToCoreMintQuarks(10)),
			expectedHistory(intentRecord, history.DirectlyReceived, receiver2, sender, currency_lib.USD, 15.0, common.ToCoreMintQuarks(15)),
		},
	})
	env.requireReservedNonces(t, result)
}

func TestSubmitIntent_SendPublicMultiPayment_MaxPayments(t *testing.T) {
	env := setupTestEnv(t)

	const count = 10

	sender := env.newFundedUser(t, common.ToCoreMintQuarks(100))
	senderVault := env.getVault(t, sender, common.CoreMintAccount)

	receivers := make([]*common.Account, count)
	receiverVaults := make([]*common.Account, count)
	payments := make([]testMultiPayment, count)
	for i := range count {
		receivers[i] = env.newUser(t)
		receiverVaults[i] = env.getVault(t, receivers[i], common.CoreMintAccount)
		payments[i] = testMultiPayment{destination: receiverVaults[i], quarks: common.ToCoreMintQuarks(uint64(i + 1))}
	}

	req := env.multiPaymentRequest(t, sender, common.CoreMintAccount, payments...)

	before := env.snapshot(t, append([]*common.Account{sender}, receivers...)...)
	result := env.submitIntent(t, req)
	result.requireSuccess(t)
	nonceCounts := make([]int, count)
	for i := range nonceCounts {
		nonceCounts[i] = 1
	}
	requireServerParameterNonces(t, result.serverParameters, nonceCounts...)

	intentRecord := env.requireIntent(t, req.intentId)

	// 1 + 2 + ... + 10
	const total = 55

	expected := &dbChanges{
		intents: []*intent.Record{{
			IntentId:              req.intentId.PublicKey().ToBase58(),
			IntentType:            intent.SendPublicMultiPayment,
			MintAccount:           common.CoreMintAccount.PublicKey().ToBase58(),
			InitiatorOwnerAccount: sender.PublicKey().ToBase58(),
			SendPublicMultiPaymentMetadata: &intent.SendPublicMultiPaymentMetadata{
				Source:           senderVault.PublicKey().ToBase58(),
				Quantity:         common.ToCoreMintQuarks(total),
				ExchangeCurrency: currency_lib.USD,
				ExchangeRate:     1.0,
				NativeAmount:     total,
				UsdMarketValue:   total,
			},
			State: intent.StatePending,
		}},
		balances: []*balance.Record{
			expectedBalance(senderVault, sender, common.CoreMintAccount, common.ToCoreMintQuarks(100-total), -usdCostBasis(total), true),
		},
		history: []*history.Record{
			// The sender has a single record for the total, with no counterparty
			expectedHistory(intentRecord, history.DirectlySent, sender, nil, currency_lib.USD, total, common.ToCoreMintQuarks(total)),
		},
	}
	for i := range count {
		amount := float64(i + 1)
		quarks := common.ToCoreMintQuarks(uint64(i + 1))

		expected.intents[0].SendPublicMultiPaymentMetadata.Payments = append(expected.intents[0].SendPublicMultiPaymentMetadata.Payments, &intent.Payment{
			DestinationOwnerAccount: receivers[i].PublicKey().ToBase58(),
			DestinationTokenAccount: receiverVaults[i].PublicKey().ToBase58(),
			Quantity:                quarks,
			NativeAmount:            amount,
			UsdMarketValue:          amount,
		})
		expected.actions = append(expected.actions, expectedTransferAction(intentRecord, uint32(i), senderVault, receiverVaults[i], quarks))
		expected.fulfillments = append(expected.fulfillments, expectedTransferFulfillment(t, intentRecord, result, uint32(i), senderVault, receiverVaults[i]))
		expected.balances = append(expected.balances, expectedBalance(receiverVaults[i], receivers[i], common.CoreMintAccount, quarks, usdCostBasis(amount), true))
		expected.history = append(expected.history, expectedHistory(intentRecord, history.DirectlyReceived, receivers[i], sender, currency_lib.USD, amount, quarks))
	}
	env.requireChanges(t, before, expected)
	env.requireReservedNonces(t, result)
}

// The number of payments in a multi payment, 2 to 10, is bounded by the
// metadata's validation rules alone, which refuse a request before the intent
// handler sees it
func TestSubmitIntent_SendPublicMultiPayment_PaymentCountBounds(t *testing.T) {
	env := setupTestEnv(t)

	sender := env.newFundedUser(t, common.ToCoreMintQuarks(100))

	payments := make([]testMultiPayment, 11)
	for i := range payments {
		payments[i] = testMultiPayment{destination: env.getVault(t, env.newUser(t), common.CoreMintAccount), quarks: common.ToCoreMintQuarks(1)}
	}

	for _, tc := range []struct {
		count int
		valid bool
	}{
		{count: 1, valid: false},
		{count: 2, valid: true},
		{count: 10, valid: true},
		{count: 11, valid: false},
	} {
		t.Run(fmt.Sprintf("%d payments", tc.count), func(t *testing.T) {
			req := env.multiPaymentRequest(t, sender, common.CoreMintAccount, payments[:tc.count]...)
			err := req.metadata.Validate()
			if tc.valid {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}

func TestSubmitIntent_SendPublicMultiPayment_LaunchpadMint(t *testing.T) {
	env := setupTestEnv(t)

	mint := env.newLaunchpadMint(t)
	sender := env.newFundedUser(t, currencycreator.ToQuarks(1_000), mint)
	receiver1 := env.newFundedUser(t, 0, mint)
	receiver2 := env.newFundedUser(t, 0, mint)
	senderVault := env.getVault(t, sender, mint)
	receiver1Vault := env.getVault(t, receiver1, mint)
	receiver2Vault := env.getVault(t, receiver2, mint)

	req := env.multiPaymentRequest(t, sender, mint,
		testMultiPayment{destination: receiver1Vault, quarks: currencycreator.ToQuarks(100)},
		testMultiPayment{destination: receiver2Vault, quarks: currencycreator.ToQuarks(250)},
	)

	// Value everything in CAD with the same rate and reserve state
	exchangeRate := env.signedExchangeRate(t, currency_lib.CAD, 1.35, time.Now())
	reserveState := env.signedReserveState(t, mint, currencycreator.ToQuarks(defaultLaunchpadSupply), time.Now())
	var nativeAmounts, usdValues []float64
	var totalNative, totalUsd float64
	for _, payment := range req.metadata.GetSendPublicMultiPayment().Payments {
		exchangeData := env.launchpadExchangeData(t, mint, payment.GetClientExchangeData().Quarks, exchangeRate, reserveState)
		payment.ExchangeData = &transactionpb.SendPublicMultiPaymentMetadata_Payment_ClientExchangeData{ClientExchangeData: exchangeData}

		nativeAmounts = append(nativeAmounts, exchangeData.NativeAmount)
		usdValues = append(usdValues, exchangeData.NativeAmount/1.35)
		totalNative += exchangeData.NativeAmount
		totalUsd += exchangeData.NativeAmount / 1.35
	}

	before := env.snapshot(t, sender, receiver1, receiver2)
	result := env.submitIntent(t, req)
	result.requireSuccess(t)

	intentRecord := env.requireIntent(t, req.intentId)
	env.requireChanges(t, before, &dbChanges{
		intents: []*intent.Record{{
			IntentId:              req.intentId.PublicKey().ToBase58(),
			IntentType:            intent.SendPublicMultiPayment,
			MintAccount:           mint.PublicKey().ToBase58(),
			InitiatorOwnerAccount: sender.PublicKey().ToBase58(),
			SendPublicMultiPaymentMetadata: &intent.SendPublicMultiPaymentMetadata{
				Source: senderVault.PublicKey().ToBase58(),
				Payments: []*intent.Payment{
					{
						DestinationOwnerAccount: receiver1.PublicKey().ToBase58(),
						DestinationTokenAccount: receiver1Vault.PublicKey().ToBase58(),
						Quantity:                currencycreator.ToQuarks(100),
						NativeAmount:            nativeAmounts[0],
						UsdMarketValue:          usdValues[0],
					},
					{
						DestinationOwnerAccount: receiver2.PublicKey().ToBase58(),
						DestinationTokenAccount: receiver2Vault.PublicKey().ToBase58(),
						Quantity:                currencycreator.ToQuarks(250),
						NativeAmount:            nativeAmounts[1],
						UsdMarketValue:          usdValues[1],
					},
				},
				Quantity:         currencycreator.ToQuarks(350),
				ExchangeCurrency: currency_lib.CAD,
				ExchangeRate:     totalNative / 350, // Per whole token across all payments
				NativeAmount:     totalNative,
				UsdMarketValue:   totalUsd,
			},
			State: intent.StatePending,
		}},
		actions: []*action.Record{
			expectedTransferAction(intentRecord, 0, senderVault, receiver1Vault, currencycreator.ToQuarks(100)),
			expectedTransferAction(intentRecord, 1, senderVault, receiver2Vault, currencycreator.ToQuarks(250)),
		},
		fulfillments: []*fulfillment.Record{
			expectedTransferFulfillment(t, intentRecord, result, 0, senderVault, receiver1Vault),
			expectedTransferFulfillment(t, intentRecord, result, 1, senderVault, receiver2Vault),
		},
		balances: []*balance.Record{
			expectedBalance(senderVault, sender, mint, currencycreator.ToQuarks(650), -(usdCostBasis(usdValues[0]) + usdCostBasis(usdValues[1])), true),
			expectedBalance(receiver1Vault, receiver1, mint, currencycreator.ToQuarks(100), usdCostBasis(usdValues[0]), true),
			expectedBalance(receiver2Vault, receiver2, mint, currencycreator.ToQuarks(250), usdCostBasis(usdValues[1]), true),
		},
		history: []*history.Record{
			expectedHistory(intentRecord, history.DirectlySent, sender, nil, currency_lib.CAD, totalNative, currencycreator.ToQuarks(350)),
			expectedHistory(intentRecord, history.DirectlyReceived, receiver1, sender, currency_lib.CAD, nativeAmounts[0], currencycreator.ToQuarks(100)),
			expectedHistory(intentRecord, history.DirectlyReceived, receiver2, sender, currency_lib.CAD, nativeAmounts[1], currencycreator.ToQuarks(250)),
		},
	})
	env.requireReservedNonces(t, result)
}

func TestSubmitIntent_SendPublicMultiPayment_AutoOpensPrimary(t *testing.T) {
	for _, autoOpenBoth := range []bool{false, true} {
		t.Run(fmt.Sprintf("both=%v", autoOpenBoth), func(t *testing.T) {
			env := setupTestEnv(t)

			mint := env.newLaunchpadMint(t)
			sender := env.newFundedUser(t, currencycreator.ToQuarks(1_000), mint)
			senderVault := env.getVault(t, sender, mint)

			receiver1 := env.newUser(t)
			if !autoOpenBoth {
				env.openAccount(t, receiver1, receiver1, mint, commonpb.AccountType_PRIMARY, 0)
			}
			receiver2 := env.newUser(t)
			receiver1Vault := env.getVault(t, receiver1, mint)
			receiver2Vault := env.getVault(t, receiver2, mint)

			req := env.multiPaymentRequest(t, sender, mint,
				testMultiPayment{destination: receiver1Vault, destinationOwner: receiver1, quarks: currencycreator.ToQuarks(10)},
				testMultiPayment{destination: receiver2Vault, destinationOwner: receiver2, quarks: currencycreator.ToQuarks(20)},
			)
			payments := req.metadata.GetSendPublicMultiPayment().Payments
			native1 := payments[0].GetClientExchangeData().NativeAmount
			native2 := payments[1].GetClientExchangeData().NativeAmount

			before := env.snapshot(t, sender, receiver1, receiver2)
			result := env.submitIntent(t, req)
			result.requireSuccess(t)
			intentRecord := env.requireIntent(t, req.intentId)

			expected := &dbChanges{
				intents: []*intent.Record{{
					IntentId:              req.intentId.PublicKey().ToBase58(),
					IntentType:            intent.SendPublicMultiPayment,
					MintAccount:           mint.PublicKey().ToBase58(),
					InitiatorOwnerAccount: sender.PublicKey().ToBase58(),
					SendPublicMultiPaymentMetadata: &intent.SendPublicMultiPaymentMetadata{
						Source: senderVault.PublicKey().ToBase58(),
						Payments: []*intent.Payment{
							{
								DestinationOwnerAccount: receiver1.PublicKey().ToBase58(),
								DestinationTokenAccount: receiver1Vault.PublicKey().ToBase58(),
								Quantity:                currencycreator.ToQuarks(10),
								NativeAmount:            native1,
								UsdMarketValue:          native1,
							},
							{
								DestinationOwnerAccount: receiver2.PublicKey().ToBase58(),
								DestinationTokenAccount: receiver2Vault.PublicKey().ToBase58(),
								Quantity:                currencycreator.ToQuarks(20),
								NativeAmount:            native2,
								UsdMarketValue:          native2,
							},
						},
						Quantity:         currencycreator.ToQuarks(30),
						ExchangeCurrency: currency_lib.USD,
						ExchangeRate:     (native1 + native2) / 30,
						NativeAmount:     native1 + native2,
						UsdMarketValue:   native1 + native2,
					},
					State: intent.StatePending,
				}},
				actions: []*action.Record{
					expectedTransferAction(intentRecord, 0, senderVault, receiver1Vault, currencycreator.ToQuarks(10)),
					expectedTransferAction(intentRecord, 1, senderVault, receiver2Vault, currencycreator.ToQuarks(20)),
				},
				fulfillments: []*fulfillment.Record{
					expectedTransferFulfillment(t, intentRecord, result, 0, senderVault, receiver1Vault),
					expectedTransferFulfillment(t, intentRecord, result, 1, senderVault, receiver2Vault),
				},
				balances: []*balance.Record{
					expectedBalance(senderVault, sender, mint, currencycreator.ToQuarks(970), -(usdCostBasis(native1) + usdCostBasis(native2)), true),
					expectedBalance(receiver1Vault, receiver1, mint, currencycreator.ToQuarks(10), usdCostBasis(native1), true),
					expectedBalance(receiver2Vault, receiver2, mint, currencycreator.ToQuarks(20), usdCostBasis(native2), true),
				},
				history: []*history.Record{
					expectedHistory(intentRecord, history.DirectlySent, sender, nil, currency_lib.USD, native1+native2, currencycreator.ToQuarks(30)),
					expectedHistory(intentRecord, history.DirectlyReceived, receiver1, sender, currency_lib.USD, native1, currencycreator.ToQuarks(10)),
					expectedHistory(intentRecord, history.DirectlyReceived, receiver2, sender, currency_lib.USD, native2, currencycreator.ToQuarks(20)),
				},
			}

			autoOpened := []*common.Account{receiver2}
			if autoOpenBoth {
				autoOpened = append(autoOpened, receiver1)
			}
			for _, receiver := range autoOpened {
				openIntentRecord := env.requireAutoOpenIntent(t, receiver, mint)
				assert.Less(t, openIntentRecord.Id, intentRecord.Id)
				openIntent, openAction, openFulfillment, openAccountInfo, openTimelock := env.expectedAutoOpenChanges(t, openIntentRecord, receiver, mint)
				expected.intents = append(expected.intents, openIntent)
				expected.actions = append(expected.actions, openAction)
				expected.fulfillments = append(expected.fulfillments, openFulfillment)
				expected.accountInfos = append(expected.accountInfos, openAccountInfo)
				expected.timelocks = append(expected.timelocks, openTimelock)
			}

			env.requireChanges(t, before, expected)
			env.requireReservedNonces(t, result)
		})
	}
}

func TestSubmitIntent_SendPublicMultiPayment_Antispam(t *testing.T) {
	env := setupTestEnv(t, withAntispamChecks())

	sender := env.newFundedUser(t, common.ToCoreMintQuarks(100))
	receiver1Vault := env.getVault(t, env.newUser(t), common.CoreMintAccount)
	receiver2Vault := env.getVault(t, env.newUser(t), common.CoreMintAccount)

	newRequest := func() *submitIntentRequest {
		return env.multiPaymentRequest(t, sender, common.CoreMintAccount,
			testMultiPayment{destination: receiver1Vault, quarks: common.ToCoreMintQuarks(1)},
			testMultiPayment{destination: receiver2Vault, quarks: common.ToCoreMintQuarks(1)},
		)
	}

	env.antispamIntegration.setAllow(false)
	req := newRequest()
	before := env.snapshot(t, sender)
	env.submitIntent(t, req).requireDenied(t, ErrTooManyPayments.Error())
	env.requireNoChanges(t, before)

	env.antispamIntegration.setAllow(true)
	req = newRequest()
	env.submitIntent(t, req).requireSuccess(t)

	// Every destination is checked
	calls := env.antispamIntegration.getCalls()
	require.Len(t, calls, 3)
	assert.Equal(t, "send_payment:"+receiver1Vault.PublicKey().ToBase58(), calls[1])
	assert.Equal(t, "send_payment:"+receiver2Vault.PublicKey().ToBase58(), calls[2])
}

func TestSubmitIntent_SendPublicMultiPayment_Aml(t *testing.T) {
	env := setupTestEnv(t, withAmlChecks())

	sender := env.newFundedUser(t, common.ToCoreMintQuarks(10_000))
	receiver1Vault := env.getVault(t, env.newUser(t), common.CoreMintAccount)
	receiver2Vault := env.getVault(t, env.newUser(t), common.CoreMintAccount)

	// Each payment is within the per transaction limit, but the total isn't
	req := env.multiPaymentRequest(t, sender, common.CoreMintAccount,
		testMultiPayment{destination: receiver1Vault, quarks: common.ToCoreMintQuarks(200)},
		testMultiPayment{destination: receiver2Vault, quarks: common.ToCoreMintQuarks(51)},
	)
	before := env.snapshot(t, sender)
	env.submitIntent(t, req).requireDenied(t, ErrTransactionLimitExceeded.Error())
	env.requireNoChanges(t, before)

	// Multi payments count towards the daily limit
	for range 4 {
		req = env.multiPaymentRequest(t, sender, common.CoreMintAccount,
			testMultiPayment{destination: receiver1Vault, quarks: common.ToCoreMintQuarks(200)},
			testMultiPayment{destination: receiver2Vault, quarks: common.ToCoreMintQuarks(50)},
		)
		env.submitIntent(t, req).requireSuccess(t)
	}
	req = env.directPaymentRequest(t, sender, receiver1Vault, common.CoreMintAccount, common.ToCoreMintQuarks(200))
	env.submitIntent(t, req).requireSuccess(t)

	req = env.multiPaymentRequest(t, sender, common.CoreMintAccount,
		testMultiPayment{destination: receiver1Vault, quarks: common.ToCoreMintQuarks(1)},
		testMultiPayment{destination: receiver2Vault, quarks: common.ToCoreMintQuarks(1)},
	)
	env.submitIntent(t, req).requireDenied(t, ErrTransactionLimitExceeded.Error())
}

func TestSubmitIntent_SendPublicMultiPayment_Validation(t *testing.T) {
	type fixture struct {
		env       *testEnv
		sender    *common.Account
		receiver1 *common.Account
		receiver2 *common.Account
	}

	vault := func(t *testing.T, f *fixture, owner *common.Account) *common.Account {
		return f.env.getVault(t, owner, common.CoreMintAccount)
	}

	newRequest := func(t *testing.T, f *fixture, payments ...testMultiPayment) *submitIntentRequest {
		if len(payments) == 0 {
			payments = []testMultiPayment{
				{destination: vault(t, f, f.receiver1), quarks: common.ToCoreMintQuarks(10)},
				{destination: vault(t, f, f.receiver2), quarks: common.ToCoreMintQuarks(15)},
			}
		}
		return f.env.multiPaymentRequest(t, f.sender, common.CoreMintAccount, payments...)
	}

	setupLaunchpadRequest := func(t *testing.T, f *fixture) (*common.Account, *submitIntentRequest) {
		mint := f.env.newLaunchpadMint(t)
		f.env.openAccount(t, f.sender, f.sender, mint, commonpb.AccountType_PRIMARY, 0)
		f.env.credit(t, f.env.getVault(t, f.sender, mint), currencycreator.ToQuarks(100))
		f.env.openAccount(t, f.receiver1, f.receiver1, mint, commonpb.AccountType_PRIMARY, 0)
		f.env.openAccount(t, f.receiver2, f.receiver2, mint, commonpb.AccountType_PRIMARY, 0)
		return mint, f.env.multiPaymentRequest(t, f.sender, mint,
			testMultiPayment{destination: f.env.getVault(t, f.receiver1, mint), quarks: currencycreator.ToQuarks(10)},
			testMultiPayment{destination: f.env.getVault(t, f.receiver2, mint), quarks: currencycreator.ToQuarks(15)},
		)
	}

	for _, tc := range []struct {
		name           string
		build          func(t *testing.T, f *fixture) *submitIntentRequest
		expectedCode   transactionpb.SubmitIntentResponse_Error_Code
		expectedReason string
	}{
		{
			name: "server exchange data",
			build: func(t *testing.T, f *fixture) *submitIntentRequest {
				req := newRequest(t, f)
				req.metadata.GetSendPublicMultiPayment().Payments[1].ExchangeData = &transactionpb.SendPublicMultiPaymentMetadata_Payment_ServerExchangeData{
					ServerExchangeData: &transactionpb.ExchangeData{
						Currency:     "usd",
						ExchangeRate: 1.0,
						NativeAmount: 15.0,
						Quarks:       common.ToCoreMintQuarks(15),
						Mint:         common.CoreMintAccount.ToProto(),
					},
				}
				return req
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_DENIED,
			expectedReason: "client exchange data not provided",
		},
		{
			name: "unknown destination without an owner",
			build: func(t *testing.T, f *fixture) *submitIntentRequest {
				destination := toAta(t, f.env.newAccount(t), common.CoreMintAccount)
				return newRequest(t, f,
					testMultiPayment{destination: vault(t, f, f.receiver1), quarks: common.ToCoreMintQuarks(10)},
					testMultiPayment{destination: destination, quarks: common.ToCoreMintQuarks(15)},
				)
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_INVALID_INTENT,
			expectedReason: "destination owner is required for",
		},
		{
			name: "destination owner doesn't own the destination",
			build: func(t *testing.T, f *fixture) *submitIntentRequest {
				return newRequest(t, f,
					testMultiPayment{destination: vault(t, f, f.receiver1), destinationOwner: f.receiver2, quarks: common.ToCoreMintQuarks(10)},
					testMultiPayment{destination: vault(t, f, f.receiver2), quarks: common.ToCoreMintQuarks(15)},
				)
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_INVALID_INTENT,
			expectedReason: "destination owner for",
		},
		{
			name: "destination owner isn't an ocp user",
			build: func(t *testing.T, f *fixture) *submitIntentRequest {
				destinationOwner := f.env.newAccount(t)
				return newRequest(t, f,
					testMultiPayment{destination: vault(t, f, f.receiver1), quarks: common.ToCoreMintQuarks(10)},
					testMultiPayment{destination: vault(t, f, destinationOwner), destinationOwner: destinationOwner, quarks: common.ToCoreMintQuarks(15)},
				)
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_INVALID_INTENT,
			expectedReason: "must be an ocp primary account",
		},
		{
			name: "destination isn't the owner's derived primary vault",
			build: func(t *testing.T, f *fixture) *submitIntentRequest {
				return newRequest(t, f,
					testMultiPayment{destination: vault(t, f, f.receiver1), quarks: common.ToCoreMintQuarks(10)},
					testMultiPayment{destination: toAta(t, f.receiver2, common.CoreMintAccount), destinationOwner: f.receiver2, quarks: common.ToCoreMintQuarks(15)},
				)
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_INVALID_INTENT,
			expectedReason: "must be an ocp primary account",
		},
		{
			name: "duplicate destinations",
			build: func(t *testing.T, f *fixture) *submitIntentRequest {
				return newRequest(t, f,
					testMultiPayment{destination: vault(t, f, f.receiver1), quarks: common.ToCoreMintQuarks(10)},
					testMultiPayment{destination: vault(t, f, f.receiver1), quarks: common.ToCoreMintQuarks(15)},
				)
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_INVALID_INTENT,
			expectedReason: "duplicate destination account detected",
		},
		{
			name: "duplicate destination owners",
			build: func(t *testing.T, f *fixture) *submitIntentRequest {
				poolAuthority := f.env.newAccount(t)
				f.env.openAccount(t, f.receiver1, poolAuthority, common.CoreMintAccount, commonpb.AccountType_POOL, 0)
				return newRequest(t, f,
					testMultiPayment{destination: vault(t, f, f.receiver1), quarks: common.ToCoreMintQuarks(10)},
					testMultiPayment{destination: vault(t, f, poolAuthority), quarks: common.ToCoreMintQuarks(15)},
				)
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_INVALID_INTENT,
			expectedReason: "duplicate destination owner account detected: ",
		},
		{
			name: "destination is the source",
			build: func(t *testing.T, f *fixture) *submitIntentRequest {
				return newRequest(t, f,
					testMultiPayment{destination: vault(t, f, f.sender), quarks: common.ToCoreMintQuarks(10)},
					testMultiPayment{destination: vault(t, f, f.receiver2), quarks: common.ToCoreMintQuarks(15)},
				)
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_INVALID_INTENT,
			expectedReason: "payment is a no-op",
		},
		{
			name: "destination is owned by the sender",
			build: func(t *testing.T, f *fixture) *submitIntentRequest {
				poolAuthority := f.env.newAccount(t)
				f.env.openAccount(t, f.sender, poolAuthority, common.CoreMintAccount, commonpb.AccountType_POOL, 0)
				return newRequest(t, f,
					testMultiPayment{destination: vault(t, f, f.receiver1), quarks: common.ToCoreMintQuarks(10)},
					testMultiPayment{destination: vault(t, f, poolAuthority), quarks: common.ToCoreMintQuarks(15)},
				)
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_INVALID_INTENT,
			expectedReason: "cannot send a payment to an account owned by the source owner",
		},
		{
			name: "destination isn't a primary account",
			build: func(t *testing.T, f *fixture) *submitIntentRequest {
				poolAuthority := f.env.newAccount(t)
				f.env.openAccount(t, f.receiver2, poolAuthority, common.CoreMintAccount, commonpb.AccountType_POOL, 0)
				return newRequest(t, f,
					testMultiPayment{destination: vault(t, f, f.receiver1), quarks: common.ToCoreMintQuarks(10)},
					testMultiPayment{destination: vault(t, f, poolAuthority), quarks: common.ToCoreMintQuarks(15)},
				)
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_INVALID_INTENT,
			expectedReason: "must be a PRIMARY account",
		},
		{
			name: "destination is a different mint",
			build: func(t *testing.T, f *fixture) *submitIntentRequest {
				mint := f.env.newLaunchpadMint(t)
				f.env.openAccount(t, f.receiver2, f.receiver2, mint, commonpb.AccountType_PRIMARY, 0)
				return newRequest(t, f,
					testMultiPayment{destination: vault(t, f, f.receiver1), quarks: common.ToCoreMintQuarks(10)},
					testMultiPayment{destination: f.env.getVault(t, f.receiver2, mint), quarks: common.ToCoreMintQuarks(15)},
				)
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_INVALID_INTENT,
			expectedReason: "mint is invalid",
		},
		{
			name: "destination isn't managed by code",
			build: func(t *testing.T, f *fixture) *submitIntentRequest {
				f.env.setTimelockState(t, vault(t, f, f.receiver2), timelock_token_v1.StateUnlocked)
				return newRequest(t, f)
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_DENIED,
			expectedReason: ErrDestinationNotManagedByCode.Error(),
		},
		{
			name: "destination is closed",
			build: func(t *testing.T, f *fixture) *submitIntentRequest {
				f.env.setTimelockState(t, vault(t, f, f.receiver2), timelock_token_v1.StateClosed)
				return newRequest(t, f)
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_STALE_STATE,
			expectedReason: "has been closed",
		},
		{
			name: "payments use different exchange rates",
			build: func(t *testing.T, f *fixture) *submitIntentRequest {
				req := newRequest(t, f)
				exchangeRate := f.env.signedExchangeRate(t, currency_lib.USD, 1.0, time.Now().Add(-time.Second))
				req.metadata.GetSendPublicMultiPayment().Payments[1].ExchangeData = &transactionpb.SendPublicMultiPaymentMetadata_Payment_ClientExchangeData{
					ClientExchangeData: f.env.coreMintExchangeData(t, common.ToCoreMintQuarks(15), exchangeRate),
				}
				return req
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_INVALID_INTENT,
			expectedReason: "all payments must use the same core mint fiat exchange rate",
		},
		{
			name: "payments use different currencies",
			build: func(t *testing.T, f *fixture) *submitIntentRequest {
				req := newRequest(t, f)
				exchangeRate := f.env.signedExchangeRate(t, currency_lib.CAD, 1.35, time.Now())
				req.metadata.GetSendPublicMultiPayment().Payments[1].ExchangeData = &transactionpb.SendPublicMultiPaymentMetadata_Payment_ClientExchangeData{
					ClientExchangeData: f.env.coreMintExchangeData(t, common.ToCoreMintQuarks(15), exchangeRate),
				}
				return req
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_INVALID_INTENT,
			expectedReason: "all payments must use the same core mint fiat exchange rate",
		},
		{
			name: "payments use different reserve states",
			build: func(t *testing.T, f *fixture) *submitIntentRequest {
				mint, req := setupLaunchpadRequest(t, f)
				payment := req.metadata.GetSendPublicMultiPayment().Payments[1]
				reserveState := f.env.signedReserveState(t, mint, currencycreator.ToQuarks(defaultLaunchpadSupply+1), time.Now())
				payment.ExchangeData = &transactionpb.SendPublicMultiPaymentMetadata_Payment_ClientExchangeData{
					ClientExchangeData: f.env.launchpadExchangeData(t, mint, payment.GetClientExchangeData().Quarks, payment.GetClientExchangeData().CoreMintFiatExchangeRate, reserveState),
				}
				return req
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_INVALID_INTENT,
			expectedReason: "all payments must use the same launchpad currency reserve state",
		},
		{
			name: "exchange data mint doesn't match intent mint",
			build: func(t *testing.T, f *fixture) *submitIntentRequest {
				req := newRequest(t, f)
				req.metadata.GetSendPublicMultiPayment().Payments[1].ExchangeData = &transactionpb.SendPublicMultiPaymentMetadata_Payment_ClientExchangeData{
					ClientExchangeData: f.env.exchangeData(t, f.env.newLaunchpadMint(t), common.ToCoreMintQuarks(15)),
				}
				return req
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_INVALID_INTENT,
			expectedReason: "expected exchange data mint to be",
		},
		{
			name: "stale exchange rate",
			build: func(t *testing.T, f *fixture) *submitIntentRequest {
				req := newRequest(t, f)
				exchangeRate := f.env.signedExchangeRate(t, currency_lib.USD, 1.0, time.Now().Add(-11*time.Minute))
				for _, payment := range req.metadata.GetSendPublicMultiPayment().Payments {
					payment.GetClientExchangeData().CoreMintFiatExchangeRate = exchangeRate
				}
				return req
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_STALE_STATE,
			expectedReason: "exchange rate is stale",
		},
		{
			name: "native amount doesn't match quarks",
			build: func(t *testing.T, f *fixture) *submitIntentRequest {
				req := newRequest(t, f)
				req.metadata.GetSendPublicMultiPayment().Payments[1].GetClientExchangeData().NativeAmount = 16.0
				return req
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_INVALID_INTENT,
			expectedReason: "native amount and quark value mismatch",
		},
		{
			name: "extra action",
			build: func(t *testing.T, f *fixture) *submitIntentRequest {
				req := newRequest(t, f)
				req.actions = append(req.actions, f.env.transferAction(t, 2, f.sender, vault(t, f, f.receiver1), common.CoreMintAccount, 1))
				return req
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_INVALID_INTENT,
			expectedReason: "expected 1 action per payment",
		},
		{
			name: "payment split across actions",
			build: func(t *testing.T, f *fixture) *submitIntentRequest {
				req := newRequest(t, f)
				req.actions = []*transactionpb.Action{
					f.env.transferAction(t, 0, f.sender, vault(t, f, f.receiver1), common.CoreMintAccount, common.ToCoreMintQuarks(5)),
					f.env.transferAction(t, 1, f.sender, vault(t, f, f.receiver1), common.CoreMintAccount, common.ToCoreMintQuarks(5)),
				}
				return req
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_INVALID_INTENT,
			expectedReason: "in one action",
		},
		{
			name: "payment sent to another account",
			build: func(t *testing.T, f *fixture) *submitIntentRequest {
				req := newRequest(t, f)
				req.actions[1] = f.env.transferAction(t, 1, f.sender, vault(t, f, f.env.newUser(t)), common.CoreMintAccount, common.ToCoreMintQuarks(15))
				return req
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_INVALID_INTENT,
			expectedReason: "must send payment to destination account",
		},
		{
			name: "action amount doesn't match payment",
			build: func(t *testing.T, f *fixture) *submitIntentRequest {
				req := newRequest(t, f)
				req.actions[1].GetNoPrivacyTransfer().Amount = common.ToCoreMintQuarks(14)
				return req
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_INVALID_INTENT,
			expectedReason: fmt.Sprintf("actions[1]: must send %d quarks to destination account", common.ToCoreMintQuarks(15)),
		},
		{
			name: "payment sent from another source",
			build: func(t *testing.T, f *fixture) *submitIntentRequest {
				poolAuthority := f.env.newAccount(t)
				f.env.openAccount(t, f.sender, poolAuthority, common.CoreMintAccount, commonpb.AccountType_POOL, 0)
				f.env.credit(t, vault(t, f, poolAuthority), common.ToCoreMintQuarks(100))
				req := newRequest(t, f)
				req.actions[1] = f.env.transferAction(t, 1, poolAuthority, vault(t, f, f.receiver2), common.CoreMintAccount, common.ToCoreMintQuarks(15))
				return req
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_INVALID_INTENT,
			expectedReason: "must send 2 payments from source account",
		},
		{
			name: "source isn't a primary account",
			build: func(t *testing.T, f *fixture) *submitIntentRequest {
				poolAuthority := f.env.newAccount(t)
				f.env.openAccount(t, f.sender, poolAuthority, common.CoreMintAccount, commonpb.AccountType_POOL, 0)
				f.env.credit(t, vault(t, f, poolAuthority), common.ToCoreMintQuarks(100))
				req := f.env.multiPaymentRequest(t, poolAuthority, common.CoreMintAccount,
					testMultiPayment{destination: vault(t, f, f.receiver1), quarks: common.ToCoreMintQuarks(10)},
					testMultiPayment{destination: vault(t, f, f.receiver2), quarks: common.ToCoreMintQuarks(15)},
				)
				req.owner = f.sender
				return req
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_INVALID_INTENT,
			expectedReason: "source account must be a PRIMARY account",
		},
		{
			name: "insufficient balance",
			build: func(t *testing.T, f *fixture) *submitIntentRequest {
				return newRequest(t, f,
					testMultiPayment{destination: vault(t, f, f.receiver1), quarks: common.ToCoreMintQuarks(50)},
					testMultiPayment{destination: vault(t, f, f.receiver2), quarks: common.ToCoreMintQuarks(51)},
				)
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_INVALID_INTENT,
			expectedReason: "has insufficient balance to perform action",
		},
		{
			name: "source isn't managed by code",
			build: func(t *testing.T, f *fixture) *submitIntentRequest {
				f.env.setTimelockState(t, vault(t, f, f.sender), timelock_token_v1.StateWaitingForTimeout)
				return newRequest(t, f)
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_DENIED,
			expectedReason: ErrSourceNotManagedByCode.Error(),
		},
		{
			name: "fee payment",
			build: func(t *testing.T, f *fixture) *submitIntentRequest {
				req := newRequest(t, f)
				req.actions = append(req.actions, f.env.feePaymentAction(t, 2, f.sender, common.CoreMintAccount, withdrawalFeeQuarks))
				return req
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_INVALID_INTENT,
			expectedReason: "intent doesn't require a fee payment",
		},
		{
			name: "action mint doesn't match intent mint",
			build: func(t *testing.T, f *fixture) *submitIntentRequest {
				_, launchpadReq := setupLaunchpadRequest(t, f)
				req := newRequest(t, f)
				req.actions = launchpadReq.actions
				return req
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_INVALID_INTENT,
			expectedReason: "mint must be " + common.CoreMintAccount.PublicKey().ToBase58(),
		},
		{
			name: "intent ID reserved for a swap",
			build: func(t *testing.T, f *fixture) *submitIntentRequest {
				req := newRequest(t, f)
				f.env.newSwapRecord(t, f.sender, common.CoreMintAccount, f.env.newLaunchpadMint(t), req.intentId, common.ToCoreMintQuarks(25), swap.StateCreated)
				return req
			},
			expectedCode:   transactionpb.SubmitIntentResponse_Error_DENIED,
			expectedReason: "intent id reserved to fund swap",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env := setupTestEnv(t)

			f := &fixture{
				env:       env,
				sender:    env.newFundedUser(t, common.ToCoreMintQuarks(100)),
				receiver1: env.newUser(t),
				receiver2: env.newUser(t),
			}

			req := tc.build(t, f)

			before := env.snapshot(t, f.sender, f.receiver1, f.receiver2)
			result := env.submitIntent(t, req)
			result.requireError(t, tc.expectedCode, tc.expectedReason)
			assert.Empty(t, result.serverParameters)

			env.requireNoChanges(t, before)
			env.requireBalance(t, vault(t, f, f.sender), common.ToCoreMintQuarks(100))
		})
	}
}
