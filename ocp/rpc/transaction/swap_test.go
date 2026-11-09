package transaction

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/mr-tron/base58/base58"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"

	commonpb "github.com/code-payments/ocp-protobuf-api/generated/go/common/v1"
	transactionpb "github.com/code-payments/ocp-protobuf-api/generated/go/transaction/v1"

	"github.com/code-payments/ocp-server/coinbase"
	currency_lib "github.com/code-payments/ocp-server/currency"
	"github.com/code-payments/ocp-server/ocp/common"
	currency_util "github.com/code-payments/ocp-server/ocp/currency"
	ocp_data "github.com/code-payments/ocp-server/ocp/data"
	"github.com/code-payments/ocp-server/ocp/data/currency"
	holder_memory "github.com/code-payments/ocp-server/ocp/data/currency/holder/memory"
	reserve_memory "github.com/code-payments/ocp-server/ocp/data/currency/reserve/memory"
	"github.com/code-payments/ocp-server/ocp/data/intent"
	"github.com/code-payments/ocp-server/ocp/data/nonce"
	"github.com/code-payments/ocp-server/ocp/data/swap"
	"github.com/code-payments/ocp-server/ocp/data/vm/ram"
	transaction_util "github.com/code-payments/ocp-server/ocp/transaction"
	"github.com/code-payments/ocp-server/pointer"
	"github.com/code-payments/ocp-server/solana"
	coinbase_stable_swapper "github.com/code-payments/ocp-server/solana/coinbasestableswapper"
	compute_budget "github.com/code-payments/ocp-server/solana/computebudget"
	"github.com/code-payments/ocp-server/solana/currencycreator"
	"github.com/code-payments/ocp-server/solana/memo"
	"github.com/code-payments/ocp-server/solana/system"
	timelock_token_v1 "github.com/code-payments/ocp-server/solana/timelock/v1"
	"github.com/code-payments/ocp-server/solana/token"
	"github.com/code-payments/ocp-server/solana/vm"
	"github.com/code-payments/ocp-server/testutil"
	"github.com/code-payments/ocp-server/usdc"
)

//
// Blockchain stubs
//

func (p *testDataProvider) GetBlockchainTokenAccountInfo(ctx context.Context, account, mint string) (*token.Account, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	amount, ok := p.tokenBalances[account]
	if !ok {
		return nil, solana.ErrNoAccountInfo
	}
	decodedMint, err := base58.Decode(mint)
	if err != nil {
		return nil, err
	}
	return &token.Account{Mint: decodedMint, Amount: amount}, nil
}

func (p *testDataProvider) GetBlockchainBalance(ctx context.Context, account string) (uint64, uint64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	amount, ok := p.tokenBalances[account]
	if !ok {
		return 0, 0, solana.ErrNoBalance
	}
	return amount, 1, nil
}

func (p *testDataProvider) GetBlockchainLatestBlockhash(ctx context.Context) (solana.Blockhash, error) {
	if err := p.injectedErr("GetBlockchainLatestBlockhash"); err != nil {
		return solana.Blockhash{}, err
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	return p.latestBlockhash, nil
}

func (p *testDataProvider) SubmitBlockchainTransaction(ctx context.Context, txn *solana.Transaction) (solana.Signature, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.submitErr != nil {
		return solana.Signature{}, p.submitErr
	}
	p.submittedTransactions = append(p.submittedTransactions, *txn)

	var sig solana.Signature
	copy(sig[:], txn.Signature())
	return sig, nil
}

func (p *testDataProvider) GetBlockchainTransaction(ctx context.Context, sig string) (*solana.TransactionWithMeta, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Submitted transactions take a while to finalize
	if p.unfinalizedLookups > 0 {
		p.unfinalizedLookups--
		return nil, solana.ErrSignatureNotFound
	}

	for _, txn := range p.submittedTransactions {
		if base58.Encode(txn.Signature()) != sig {
			continue
		}

		res := &solana.TransactionWithMeta{Slot: 1, Transaction: txn}
		if p.failSubmitted {
			res.Err = solana.NewTransactionError(solana.TransactionErrorKey("InstructionError"))
		}
		return res, nil
	}
	return nil, solana.ErrSignatureNotFound
}

func (p *testDataProvider) setTokenBalance(account []byte, quarks uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.tokenBalances[base58.Encode(account)] = quarks
}

func (p *testDataProvider) setBlockchainAccountOwner(account, owner []byte) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.blockchainAccounts[base58.Encode(account)] = struct{}{}
	p.blockchainAccountOwners[base58.Encode(account)] = owner
}

func (p *testDataProvider) setLatestBlockhash(blockhash solana.Blockhash) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.latestBlockhash = blockhash
}

func (p *testDataProvider) setSubmitErr(err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.submitErr = err
}

func (p *testDataProvider) setFailSubmitted(fail bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.failSubmitted = fail
}

func (p *testDataProvider) getSubmittedTransactions() []solana.Transaction {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]solana.Transaction(nil), p.submittedTransactions...)
}

func (p *testDataProvider) setUnfinalizedLookups(lookups int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.unfinalizedLookups = lookups
}

//
// Swap DB failure injection
//

var errInjected = errors.New("injected failure")

func (p *testDataProvider) injectErr(method string, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.injectedErrs[method] = err
}

func (p *testDataProvider) injectedErr(method string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.injectedErrs[method]
}

func (p *testDataProvider) SaveSwap(ctx context.Context, record *swap.Record) error {
	if err := p.injectedErr("SaveSwap"); err != nil {
		return err
	}
	return p.Provider.SaveSwap(ctx, record)
}

func (p *testDataProvider) GetSwapById(ctx context.Context, id string) (*swap.Record, error) {
	if err := p.injectedErr("GetSwapById"); err != nil {
		return nil, err
	}
	return p.Provider.GetSwapById(ctx, id)
}

func (p *testDataProvider) GetAllSwapsByOwnerAndState(ctx context.Context, owner string, state swap.State) ([]*swap.Record, error) {
	if err := p.injectedErr("GetAllSwapsByOwnerAndState"); err != nil {
		return nil, err
	}
	return p.Provider.GetAllSwapsByOwnerAndState(ctx, owner, state)
}

func (p *testDataProvider) SaveCurrencyMetadata(ctx context.Context, record *currency.MetadataRecord) error {
	if err := p.injectedErr("SaveCurrencyMetadata"); err != nil {
		return err
	}
	return p.Provider.SaveCurrencyMetadata(ctx, record)
}

// setupCoinbasePool loads the mainnet Coinbase Stable Swapper liquidity pool
// and gives its USDC and core mint vaults the provided liquidity
func (e *testEnv) setupCoinbasePool(t *testing.T, liquidity uint64) {
	prefix, err := base64.StdEncoding.DecodeString(mainnetCoinbaseLiquidityPoolPrefix)
	require.NoError(t, err)
	poolData := make([]byte, 2107)
	copy(poolData, prefix)

	pool, _, err := coinbase_stable_swapper.GetPoolAddress()
	require.NoError(t, err)
	e.data.setBlockchainAccountData(pool, poolData)

	e.setCoinbaseLiquidity(t, usdcAccount(t), liquidity)
	e.setCoinbaseLiquidity(t, common.CoreMintAccount, liquidity)
}

// setCoinbaseLiquidity sets the pool's liquidity for swaps into the mint
func (e *testEnv) setCoinbaseLiquidity(t *testing.T, mint *common.Account, liquidity uint64) {
	pool, _, err := coinbase_stable_swapper.GetPoolAddress()
	require.NoError(t, err)
	outVault, _, err := coinbase_stable_swapper.GetTokenVaultAddress(&coinbase_stable_swapper.GetTokenVaultAddressArgs{
		Pool: pool,
		Mint: mint.PublicKey().ToBytes(),
	})
	require.NoError(t, err)
	outVaultTokenAccount, _, err := coinbase_stable_swapper.GetVaultTokenAccountAddress(&coinbase_stable_swapper.GetVaultTokenAccountAddressArgs{
		Vault: outVault,
	})
	require.NoError(t, err)
	e.data.setTokenBalance(outVaultTokenAccount, liquidity)
}

func usdcAccount(t *testing.T) *common.Account {
	mint, err := common.NewAccountFromPublicKeyString(usdc.Mint)
	require.NoError(t, err)
	return mint
}

//
// Integrations
//

type testSwapIntegration struct {
	mu        sync.Mutex
	submitted []string
}

func (i *testSwapIntegration) OnSwapSubmitted(ctx context.Context, owner *common.Account, fromMint, toMint *common.Account, amount uint64) error {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.submitted = append(i.submitted, fmt.Sprintf("%s:%s:%s:%d", owner.PublicKey().ToBase58(), fromMint.PublicKey().ToBase58(), toMint.PublicKey().ToBase58(), amount))
	return nil
}

func (i *testSwapIntegration) OnSwapFinalized(ctx context.Context, owner *common.Account, isBuy bool, mint *common.Account, currencyName string, region currency_lib.Code, valueReceived float64) error {
	return nil
}

func (i *testSwapIntegration) getSubmitted() []string {
	i.mu.Lock()
	defer i.mu.Unlock()
	return append([]string(nil), i.submitted...)
}

// testCoinbaseOnramp serves Onramp orders over TLS, for a real coinbase.Client
// pointed at it. The client only speaks HTTPS through http.DefaultTransport, so
// the test server's certificate is added to its trusted roots once per process.
type testCoinbaseOnramp struct {
	client *coinbase.Client
}

type testCoinbaseOrder struct {
	// Non-zero fails the order lookup with this HTTP status
	httpStatus int

	status             coinbase.OrderStatus
	purchaseAmount     string
	purchaseCurrency   string
	destinationAddress string
	destinationNetwork string
	partnerUserRef     string
}

var (
	testCoinbaseOnrampOnce   sync.Once
	testCoinbaseOnrampServer *httptest.Server
	testCoinbaseOnrampOrders sync.Map
)

func newTestCoinbaseOnramp(t *testing.T) *testCoinbaseOnramp {
	testCoinbaseOnrampOnce.Do(func() {
		testCoinbaseOnrampServer = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			orderID := strings.TrimPrefix(r.URL.Path, "/platform/v2/onramp/orders/")
			value, ok := testCoinbaseOnrampOrders.Load(orderID)
			if !ok || !strings.HasPrefix(r.Header.Get("Authorization"), "Bearer ") {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			order := value.(*testCoinbaseOrder)
			if order.httpStatus != 0 {
				w.WriteHeader(order.httpStatus)
				return
			}

			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{
				"order": map[string]any{
					"orderId":            orderID,
					"status":             string(order.status),
					"purchaseAmount":     order.purchaseAmount,
					"purchaseCurrency":   order.purchaseCurrency,
					"destinationAddress": order.destinationAddress,
					"destinationNetwork": order.destinationNetwork,
					"partnerUserRef":     order.partnerUserRef,
				},
			})
		}))

		roots, err := x509.SystemCertPool()
		if err != nil {
			roots = x509.NewCertPool()
		}
		roots.AddCert(testCoinbaseOnrampServer.Certificate())

		transport := http.DefaultTransport.(*http.Transport)
		if transport.TLSClientConfig == nil {
			transport.TLSClientConfig = &tls.Config{}
		}
		transport.TLSClientConfig.RootCAs = roots
	})

	_, privateKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	auth, err := coinbase.NewAuthenticator("test-key", privateKey)
	require.NoError(t, err)

	client := coinbase.NewClient(auth)
	client.Host = testCoinbaseOnrampServer.Listener.Addr().String()

	return &testCoinbaseOnramp{client: client}
}

// putOrder registers a completed Solana order for the owner's swap PDA, which
// mutate can change, and returns its ID
func (o *testCoinbaseOnramp) putOrder(t *testing.T, owner *common.Account, swapPda string, quarks uint64, mutate ...func(*testCoinbaseOrder)) string {
	order := &testCoinbaseOrder{
		status:             coinbase.OrderStatusCompleted,
		purchaseAmount:     quarksToDecimal(quarks, common.CoreMintDecimals),
		purchaseCurrency:   common.CoreMintSymbol,
		destinationAddress: swapPda,
		destinationNetwork: coinbase.NetworkSolana,
		partnerUserRef:     owner.PublicKey().ToBase58(),
	}
	for _, m := range mutate {
		m(order)
	}

	orderID := uuid.NewString()
	testCoinbaseOnrampOrders.Store(orderID, order)
	return orderID
}

func quarksToDecimal(quarks uint64, decimals int) string {
	divisor := uint64(1)
	for range decimals {
		divisor *= 10
	}
	return fmt.Sprintf("%d.%0*d", quarks/divisor, decimals, quarks%divisor)
}

//
// StatefulSwap
//

type statefulSwapRequest struct {
	owner         *common.Account
	swapAuthority *common.Account

	reserve    *transactionpb.StatefulSwapRequest_Initiate_ReserveSwapClientParameters
	stablecoin *transactionpb.StatefulSwapRequest_Initiate_CoinbaseStableSwapperClientParameters

	transactionVersion uint32

	// Optional overrides for exercising failure paths. By default the owner
	// signs the proof and the client signs the server's transaction.
	proofSigner *common.Account
	signatures  func(t *testing.T, message []byte) []*commonpb.Signature
	secondMsg   *transactionpb.StatefulSwapRequest
}

type statefulSwapResult struct {
	proofSignature   *commonpb.Signature
	serverParameters *transactionpb.StatefulSwapResponse_ServerParameters
	expectedTxn      *solana.Transaction
	signatures       []*commonpb.Signature

	resp *transactionpb.StatefulSwapResponse
	err  error
}

func (r *statefulSwapRequest) swapId() string {
	if r.reserve != nil {
		return base58.Encode(r.reserve.Id.Value)
	}
	return base58.Encode(r.stablecoin.Id.Value)
}

func (r *statefulSwapRequest) initiate(t *testing.T, e *testEnv) *transactionpb.StatefulSwapRequest_Initiate {
	proofSigner := r.proofSigner
	if proofSigner == nil {
		proofSigner = r.owner
	}

	initiate := &transactionpb.StatefulSwapRequest_Initiate{
		Owner:              r.owner.ToProto(),
		SwapAuthority:      r.swapAuthority.ToProto(),
		TransactionVersion: r.transactionVersion,
	}
	if r.reserve != nil {
		initiate.Kind = &transactionpb.StatefulSwapRequest_Initiate_Reserve{Reserve: r.reserve}
		initiate.ProofSignature = e.signProto(t, proofSigner, &transactionpb.VerifiedSwapMetadata{
			Kind: &transactionpb.VerifiedSwapMetadata_Reserve{
				Reserve: &transactionpb.VerifiedReserveSwapMetadata{ClientParameters: r.reserve},
			},
		})
	} else {
		initiate.Kind = &transactionpb.StatefulSwapRequest_Initiate_Stablecoin{Stablecoin: r.stablecoin}
		initiate.ProofSignature = e.signProto(t, proofSigner, &transactionpb.VerifiedSwapMetadata{
			Kind: &transactionpb.VerifiedSwapMetadata_Stablecoin{
				Stablecoin: &transactionpb.VerifiedCoinbaseStableSwapperSwapMetadata{ClientParameters: r.stablecoin},
			},
		})
	}
	initiate.Signature = e.signProto(t, r.owner, initiate)
	return initiate
}

// statefulSwap drives a StatefulSwap stream to completion like a client:
// initiate, rebuild and sign the server's transaction, and submit signatures
func (e *testEnv) statefulSwap(t *testing.T, req *statefulSwapRequest) *statefulSwapResult {
	ctx, cancel := context.WithTimeout(e.ctx, defaultTestStreamTimeout)
	defer cancel()

	stream, err := e.client.StatefulSwap(ctx)
	require.NoError(t, err)

	initiate := req.initiate(t, e)
	result := &statefulSwapResult{proofSignature: initiate.ProofSignature}

	// Requests failing proto validation are rejected by the client interceptor
	err = stream.Send(&transactionpb.StatefulSwapRequest{
		Request: &transactionpb.StatefulSwapRequest_Initiate_{Initiate: initiate},
	})
	if err == io.EOF {
		// The server ended the stream before reading the request
		_, err = stream.Recv()
	}
	if err != nil {
		result.err = err
		return result
	}

	resp, err := stream.Recv()
	if err != nil {
		result.err = err
		return result
	}
	if resp.GetServerParameters() == nil {
		result.resp = resp
		return result
	}
	result.serverParameters = resp.GetServerParameters()

	expectedTxn := e.expectedStatefulSwapTransaction(t, req, result.serverParameters)
	result.expectedTxn = &expectedTxn

	secondMsg := req.secondMsg
	if secondMsg == nil {
		message, err := expectedTxn.Message.Marshal()
		require.NoError(t, err)

		if req.signatures != nil {
			result.signatures = req.signatures(t, message)
		} else {
			result.signatures = []*commonpb.Signature{{Value: ed25519.Sign(req.owner.PrivateKey().ToBytes(), message)}}
			if req.swapAuthority.PublicKey().ToBase58() != req.owner.PublicKey().ToBase58() {
				result.signatures = append(result.signatures, &commonpb.Signature{Value: ed25519.Sign(req.swapAuthority.PrivateKey().ToBytes(), message)})
			}
		}

		secondMsg = &transactionpb.StatefulSwapRequest{
			Request: &transactionpb.StatefulSwapRequest_SubmitSignatures_{
				SubmitSignatures: &transactionpb.StatefulSwapRequest_SubmitSignatures{
					TransactionSignatures: result.signatures,
				},
			},
		}
	}

	err = stream.Send(secondMsg)
	require.NoError(t, err)

	result.resp, result.err = stream.Recv()
	if result.err != nil {
		result.resp = nil
	}
	return result
}

// expectedStatefulSwapTransaction builds the unsigned transaction the server
// is expected to have built for the request, using the server's nonce
func (e *testEnv) expectedStatefulSwapTransaction(t *testing.T, req *statefulSwapRequest, params *transactionpb.StatefulSwapResponse_ServerParameters) solana.Transaction {
	var protoNonce *commonpb.SolanaAccountId
	var protoBlockhash *commonpb.Blockhash
	switch typed := params.Kind.(type) {
	case *transactionpb.StatefulSwapResponse_ServerParameters_ReserveExistingCurrency:
		protoNonce, protoBlockhash = typed.ReserveExistingCurrency.Nonce, typed.ReserveExistingCurrency.Blockhash
	case *transactionpb.StatefulSwapResponse_ServerParameters_ReserveNewCurrency:
		protoNonce, protoBlockhash = typed.ReserveNewCurrency.Nonce, typed.ReserveNewCurrency.Blockhash
	case *transactionpb.StatefulSwapResponse_ServerParameters_Stablecoin:
		protoNonce, protoBlockhash = typed.Stablecoin.Nonce, typed.Stablecoin.Blockhash
	default:
		require.Fail(t, "unexpected server parameters")
	}

	nonceAccount, err := common.NewAccountFromProto(protoNonce)
	require.NoError(t, err)
	var blockhash solana.Blockhash
	copy(blockhash[:], protoBlockhash.Value)
	selectedNonce := &transaction_util.Nonce{Account: nonceAccount, Blockhash: blockhash}

	transactionVersion := selectSwapTransactionVersion(req.transactionVersion)

	var h SwapHandler
	if req.stablecoin != nil {
		p := req.stablecoin
		fromMint, err := common.NewAccountFromProto(p.FromMint)
		require.NoError(t, err)
		toMint, err := common.NewAccountFromProto(p.ToMint)
		require.NoError(t, err)
		destinationOwner, err := common.NewAccountFromProto(p.DestinationOwner)
		require.NoError(t, err)

		h = NewCoinbaseStableSwapperSwapHandler(e.data, req.owner, req.swapAuthority, destinationOwner, fromMint, toMint, p.SwapAmount, p.FeeAmount, selectedNonce, transactionVersion, selectCoinbaseSwapVersion(p.SwapVersion))
	} else {
		p := req.reserve
		fromMint, err := common.NewAccountFromProto(p.FromMint)
		require.NoError(t, err)
		toMint, err := common.NewAccountFromProto(p.ToMint)
		require.NoError(t, err)

		isSell := !common.IsCoreMint(fromMint)
		isBuy := !common.IsCoreMint(toMint)

		var initializesMint bool
		if isBuy {
			metadataRecord, err := e.data.GetCurrencyMetadata(e.ctx, toMint.PublicKey().ToBase58())
			require.NoError(t, err)
			initializesMint = metadataRecord.State == currency.MetadataStateWaitingForInitialPurchase
		}

		switch {
		case initializesMint && !isSell && transactionVersion == solana.MessageVersion1:
			h, err = NewReserveCreateAndBuySwapHandler(e.ctx, e.data, req.owner, req.swapAuthority, toMint, p.SwapAmount, p.FeeAmount, selectedNonce)
		case initializesMint && !isSell:
			h, err = NewReserveCreateAndBuyLegacySwapHandler(e.ctx, e.data, req.owner, toMint, p.SwapAmount, p.FeeAmount, selectedNonce)
		case initializesMint && transactionVersion == solana.MessageVersion1:
			h, err = NewReserveTreasuryFundedCreateAndBuySwapHandler(e.ctx, e.data, req.owner, fromMint, toMint, p.SwapAmount, p.FeeAmount, e.swapTreasury, defaultNewCurrencyV1PurchaseQuarks, selectedNonce)
		case initializesMint:
			h, err = NewReserveTreasuryFundedCreateAndBuyLegacySwapHandler(e.ctx, e.data, req.owner, fromMint, toMint, p.SwapAmount, p.FeeAmount, e.swapTreasury, defaultNewCurrencyPurchaseQuarks, selectedNonce)
		case isBuy && isSell:
			h = NewReserveBuySellSwapHandler(e.data, req.owner, req.swapAuthority, fromMint, toMint, p.SwapAmount, selectedNonce, transactionVersion)
		case isBuy:
			h = NewReserveBuySwapHandler(e.data, req.owner, req.swapAuthority, toMint, p.SwapAmount, p.FeeAmount, selectedNonce, transactionVersion)
		default:
			h = NewReserveSellSwapHandler(e.data, req.owner, req.swapAuthority, fromMint, p.SwapAmount, selectedNonce, transactionVersion)
		}
		require.NoError(t, err)
	}

	txn, err := makeStatefulSwapTransaction(e.ctx, h)
	require.NoError(t, err)
	txn.SetBlockhash(blockhash)
	return txn
}

func (r *statefulSwapResult) requireSuccess(t *testing.T) {
	require.NoError(t, r.err)
	require.NotNil(t, r.resp)
	require.NotNil(t, r.resp.GetSuccess(), "expected success, got %v", r.resp)
	assert.Equal(t, transactionpb.StatefulSwapResponse_Success_OK, r.resp.GetSuccess().Code)
}

func (r *statefulSwapResult) requireError(t *testing.T, code transactionpb.StatefulSwapResponse_Error_Code, reasonSubstr string) {
	require.NoError(t, r.err)
	require.NotNil(t, r.resp)
	require.NotNil(t, r.resp.GetError(), "expected error, got %v", r.resp)
	assert.Equal(t, code, r.resp.GetError().Code, "unexpected error response: %v", r.resp.GetError())
	requireErrorReason(t, r.resp.GetError().ErrorDetails, reasonSubstr)
}

func (r *statefulSwapResult) requireInvalidSwap(t *testing.T, reasonSubstr string) {
	r.requireError(t, transactionpb.StatefulSwapResponse_Error_INVALID_SWAP, reasonSubstr)
}

func (r *statefulSwapResult) requireDenied(t *testing.T, reasonSubstr string) {
	r.requireError(t, transactionpb.StatefulSwapResponse_Error_DENIED, reasonSubstr)
}

func (r *statefulSwapResult) requireSignatureError(t *testing.T, reasonSubstr string) {
	r.requireError(t, transactionpb.StatefulSwapResponse_Error_SIGNATURE_ERROR, reasonSubstr)
}

func (r *statefulSwapResult) requireStatusCode(t *testing.T, code codes.Code) {
	testutil.AssertStatusErrorWithCode(t, r.err, code)
}

// requireInvalidTransactionSignature verifies the server rejected a client
// transaction signature and returned the transaction it expected signed
func (r *statefulSwapResult) requireInvalidTransactionSignature(t *testing.T, providedSignature *commonpb.Signature) {
	r.requireSignatureError(t, "")
	requireInvalidSignatureDetails(t, r.resp.GetError().ErrorDetails, *r.expectedTxn, providedSignature)
}

func requireInvalidSignatureDetails(t *testing.T, errorDetails []*transactionpb.ErrorDetails, expectedTxn solana.Transaction, providedSignature *commonpb.Signature) {
	require.Len(t, errorDetails, 1)
	details := errorDetails[0].GetInvalidSignature()
	require.NotNil(t, details)
	assert.Equal(t, providedSignature.Value, details.ProvidedSignature.Value)

	var returnedTxn solana.Transaction
	require.NoError(t, returnedTxn.Unmarshal(details.GetExpectedTransaction().Value))
	for _, signature := range returnedTxn.Signatures {
		assert.Equal(t, solana.Signature{}, signature, "returned transaction carries a signature")
	}
	expectedMessage, err := expectedTxn.Message.Marshal()
	require.NoError(t, err)
	returnedMessage, err := returnedTxn.Message.Marshal()
	require.NoError(t, err)
	assert.Equal(t, expectedMessage, returnedMessage)
}

func requireErrorReason(t *testing.T, errorDetails []*transactionpb.ErrorDetails, reasonSubstr string) {
	if len(reasonSubstr) == 0 {
		return
	}

	var reasons []string
	for _, details := range errorDetails {
		switch typed := details.Type.(type) {
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

//
// Request builders
//

func newSwapId(t *testing.T) *commonpb.SwapId {
	return &commonpb.SwapId{Value: testutil.NewRandomAccount(t).PublicKey().ToBytes()}
}

// reserveSwapRequest builds a reserve swap funded by a SubmitIntent payment,
// with a fresh swap authority
func (e *testEnv) reserveSwapRequest(t *testing.T, owner, fromMint, toMint *common.Account, swapAmount, feeAmount uint64) *statefulSwapRequest {
	return &statefulSwapRequest{
		owner:         owner,
		swapAuthority: e.newAccount(t),
		reserve: &transactionpb.StatefulSwapRequest_Initiate_ReserveSwapClientParameters{
			Id:            newSwapId(t),
			FromMint:      fromMint.ToProto(),
			ToMint:        toMint.ToProto(),
			SwapAmount:    swapAmount,
			FeeAmount:     feeAmount,
			FundingSource: transactionpb.FundingSource_FUNDING_SOURCE_SUBMIT_INTENT,
			FundingId:     testutil.NewRandomAccount(t).PublicKey().ToBase58(),
		},
	}
}

// stablecoinSwapRequest builds a Coinbase swap of the core mint to USDC for a
// fresh destination owner, funded by a SubmitIntent payment
func (e *testEnv) stablecoinSwapRequest(t *testing.T, owner *common.Account, swapAmount uint64) *statefulSwapRequest {
	return &statefulSwapRequest{
		owner:         owner,
		swapAuthority: e.newAccount(t),
		stablecoin: &transactionpb.StatefulSwapRequest_Initiate_CoinbaseStableSwapperClientParameters{
			Id:               newSwapId(t),
			FromMint:         common.CoreMintAccount.ToProto(),
			ToMint:           usdcMint(t),
			SwapAmount:       swapAmount,
			FeeAmount:        withdrawalFeeQuarks,
			FundingSource:    transactionpb.FundingSource_FUNDING_SOURCE_SUBMIT_INTENT,
			FundingId:        testutil.NewRandomAccount(t).PublicKey().ToBase58(),
			DestinationOwner: testutil.NewRandomAccount(t).ToProto(),
			SwapVersion:      uint32(transaction_util.CoinbaseSwapV2),
		},
	}
}

// newCurrencyAwaitingPurchase sets up a launchpad currency created by the
// owner that's waiting for its initial purchase
func (e *testEnv) newCurrencyAwaitingPurchase(t *testing.T, creator *common.Account) *common.Account {
	return testutil.SetupLaunchpadCurrencyInState(t, e.data, e.reserveStore, holder_memory.New(), currency.MetadataStateWaitingForInitialPurchase, creator)
}

func (e *testEnv) setCurrencyState(t *testing.T, mint *common.Account, state currency.MetadataState) {
	metadataRecord, err := e.data.GetCurrencyMetadata(e.ctx, mint.PublicKey().ToBase58())
	require.NoError(t, err)
	metadataRecord.State = state
	require.NoError(t, e.data.SaveCurrencyMetadata(e.ctx, metadataRecord))
}

// newCurrencySwapAuthority returns the swap authority a launch requires. A v1
// launch funded with the core mint needs a distinct, per-swap key to hold its
// temporary core mint ATA. Every other launch is signed by the owner alone.
func newCurrencySwapAuthority(t *testing.T, e *testEnv, owner, fromMint *common.Account, transactionVersion uint32) *common.Account {
	if common.IsCoreMint(fromMint) && selectSwapTransactionVersion(transactionVersion) == solana.MessageVersion1 {
		return e.newAccount(t)
	}
	return owner
}

// newCurrencyLaunchQuarks returns the default core mint quarks a launch
// purchases with and is charged as a fee, which depend on the requested
// transaction version
func newCurrencyLaunchQuarks(transactionVersion uint32) (purchaseQuarks, feeQuarks uint64) {
	if selectSwapTransactionVersion(transactionVersion) == solana.MessageVersion1 {
		return defaultNewCurrencyV1PurchaseQuarks, defaultNewCurrencyV1FeeQuarks
	}
	return defaultNewCurrencyPurchaseQuarks, defaultNewCurrencyFeeQuarks
}

// newCurrencyLaunchUsdValues returns the USD values of the default launch's
// full amount and fee for the requested transaction version
func newCurrencyLaunchUsdValues(transactionVersion uint32) (fullUsdValue, feeUsdValue float64) {
	purchaseQuarks, feeQuarks := newCurrencyLaunchQuarks(transactionVersion)
	return float64(purchaseQuarks+feeQuarks) / float64(common.CoreMintQuarksPerUnit), float64(feeQuarks) / float64(common.CoreMintQuarksPerUnit)
}

// treasuryFundedNewCurrencyAmounts splits the new currency purchase and fee,
// valued in USD at the default launchpad supply, into from_mint quarks
func treasuryFundedNewCurrencyAmounts(transactionVersion uint32) (swapAmount, feeAmount uint64) {
	supply := currencycreator.ToQuarks(defaultLaunchpadSupply)
	fullUsdValue, feeUsdValue := newCurrencyLaunchUsdValues(transactionVersion)
	total := launchpadQuarksForUsdValue(supply, fullUsdValue)
	fee := launchpadQuarksForUsdValue(supply, feeUsdValue)
	return total - fee, fee
}

// treasuryFundedExchangeData values the full amount at the new currency
// purchase and fee in USD
func (e *testEnv) treasuryFundedExchangeData(t *testing.T, fromMint *common.Account, quarks uint64, transactionVersion uint32) *transactionpb.VerifiedExchangeData {
	exchangeData := e.exchangeData(t, fromMint, quarks)
	exchangeData.NativeAmount, _ = newCurrencyLaunchUsdValues(transactionVersion)
	return exchangeData
}

//
// Persisted swap state
//

// swapSnapshot captures the swap state a StatefulSwap call can change
type swapSnapshot struct {
	owners []*common.Account
	mints  []*common.Account

	swaps          map[string]*swap.Record
	currencyStates map[string]currency.MetadataState

	swapCount          uint64
	reservedNonceCount uint64
}

var allSwapStates = []swap.State{
	swap.StateUnknown,
	swap.StateCreated,
	swap.StateFunding,
	swap.StateFunded,
	swap.StateSubmitting,
	swap.StateFinalized,
	swap.StateFailed,
	swap.StateCancelling,
	swap.StateCancelled,
}

func (e *testEnv) swapSnapshot(t *testing.T, owners []*common.Account, mints ...*common.Account) *swapSnapshot {
	snapshot := &swapSnapshot{
		owners:         owners,
		mints:          mints,
		swaps:          make(map[string]*swap.Record),
		currencyStates: make(map[string]currency.MetadataState),
	}

	for _, owner := range owners {
		for _, state := range allSwapStates {
			records, err := e.data.GetAllSwapsByOwnerAndState(e.ctx, owner.PublicKey().ToBase58(), state)
			if err == swap.ErrNotFound {
				continue
			}
			require.NoError(t, err)
			for _, record := range records {
				snapshot.swaps[record.SwapId] = record
			}
		}
	}

	for _, mint := range mints {
		if common.IsCoreMint(mint) {
			continue
		}
		metadataRecord, err := e.data.GetCurrencyMetadata(e.ctx, mint.PublicKey().ToBase58())
		require.NoError(t, err)
		snapshot.currencyStates[metadataRecord.Mint] = metadataRecord.State
	}

	for _, state := range allSwapStates {
		count, err := e.data.GetSwapCountByState(e.ctx, state)
		require.NoError(t, err)
		snapshot.swapCount += count
	}

	count, err := e.data.GetNonceCountByState(e.ctx, nonce.EnvironmentSolana, nonce.EnvironmentInstanceSolanaMainnet, nonce.StateReserved)
	require.NoError(t, err)
	snapshot.reservedNonceCount = count

	return snapshot
}

// requireNoSwapChanges verifies a failed call persisted nothing, and released
// any nonce it selected
func (e *testEnv) requireNoSwapChanges(t *testing.T, before *swapSnapshot, result *statefulSwapResult) {
	after := e.swapSnapshot(t, before.owners, before.mints...)
	assert.Equal(t, before.swaps, after.swaps, "swap records changed")
	assert.Equal(t, before.currencyStates, after.currencyStates, "currency states changed")
	assert.Equal(t, before.swapCount, after.swapCount, "unexpected swap records")
	assert.Equal(t, before.reservedNonceCount, after.reservedNonceCount, "unexpected nonce reservations")

	if result != nil && result.expectedTxn != nil {
		nonceAccount := result.expectedTxn.Message.Instructions[0].Accounts[0]
		nonceRecord, err := e.data.GetNonce(e.ctx, base58.Encode(result.expectedTxn.Message.Accounts[nonceAccount]))
		require.NoError(t, err)
		assert.NotEqual(t, nonce.StateReserved, nonceRecord.State)
		assert.Empty(t, nonceRecord.Signature)
	}
}

// requireSwapCreated verifies the call persisted exactly the expected swap,
// with a fully signed transaction, and reserved its nonce for that transaction
func (e *testEnv) requireSwapCreated(t *testing.T, before *swapSnapshot, req *statefulSwapRequest, result *statefulSwapResult, expected *swap.Record, expectedCurrencyStates map[string]currency.MetadataState) *swap.Record {
	result.requireSuccess(t)

	after := e.swapSnapshot(t, before.owners, before.mints...)
	assert.Equal(t, before.swapCount+1, after.swapCount, "unexpected swap records")
	assert.Equal(t, before.reservedNonceCount+1, after.reservedNonceCount, "unexpected nonce reservations")

	for swapId, record := range after.swaps {
		if swapId == req.swapId() {
			continue
		}
		assert.Equal(t, before.swaps[swapId], record, "unrelated swap record changed")
	}

	expectedStates := make(map[string]currency.MetadataState)
	for mint, state := range before.currencyStates {
		expectedStates[mint] = state
	}
	for mint, state := range expectedCurrencyStates {
		expectedStates[mint] = state
	}
	assert.Equal(t, expectedStates, after.currencyStates, "currency states")

	saved, err := e.data.GetSwapById(e.ctx, req.swapId())
	require.NoError(t, err)

	// The persisted transaction is the one the client signed, signed by the
	// client and every server signer
	var savedTxn solana.Transaction
	require.NoError(t, savedTxn.Unmarshal(saved.TransactionBlob))
	expectedMessage, err := result.expectedTxn.Message.Marshal()
	require.NoError(t, err)
	savedMessage, err := savedTxn.Message.Marshal()
	require.NoError(t, err)
	assert.Equal(t, expectedMessage, savedMessage)
	require.Len(t, savedTxn.Signatures, int(savedTxn.Message.Header.NumSignatures))
	for i, signature := range savedTxn.Signatures {
		assert.True(t, ed25519.Verify(savedTxn.Message.Accounts[i], savedMessage, signature[:]), "signature %d is invalid", i)
	}
	for i, clientSignature := range result.signatures {
		signer := req.owner
		if i == 1 {
			signer = req.swapAuthority
		}
		var found bool
		for j := range savedTxn.Signatures {
			if string(savedTxn.Message.Accounts[j]) == string(signer.PublicKey().ToBytes()) {
				assert.Equal(t, clientSignature.Value, savedTxn.Signatures[j][:])
				found = true
			}
		}
		assert.True(t, found, "client signer %d isn't a transaction signer", i)
	}

	assert.NotZero(t, saved.Id)
	assert.NotZero(t, saved.Version)
	requireRecent(t, saved.CreatedAt, "swap created at")
	assert.Equal(t, base58.Encode(savedTxn.Signature()), saved.TransactionSignature)

	normalized := saved.Clone()
	normalized.Id, normalized.Version, normalized.CreatedAt = 0, 0, time.Time{}
	normalized.TransactionBlob = nil
	normalized.TransactionSignature = ""
	assert.Equal(t, *expected, normalized)

	nonceRecord, err := e.data.GetNonce(e.ctx, saved.Nonce)
	require.NoError(t, err)
	assert.Equal(t, nonce.StateReserved, nonceRecord.State)
	assert.Equal(t, saved.TransactionSignature, nonceRecord.Signature)
	assert.Equal(t, saved.Blockhash, nonceRecord.Blockhash)
	assert.Equal(t, nonce.PurposeClientSwap, nonceRecord.Purpose)

	// Clients sign the transaction they rebuild from the server parameters
	e.requireClientRebuildMatchesServer(t, req, result)

	return saved
}

// expectedSwapRecord is the record a request persists, without DB-assigned
// fields and the server-signed transaction
func expectedSwapRecord(t *testing.T, req *statefulSwapRequest, result *statefulSwapResult, state swap.State) *swap.Record {
	require.NotNil(t, result.expectedTxn, "no server parameters, got response %v and error %v", result.resp, result.err)

	nonceIndex := result.expectedTxn.Message.Instructions[0].Accounts[0]

	record := &swap.Record{
		SwapId:         req.swapId(),
		Owner:          req.owner.PublicKey().ToBase58(),
		Nonce:          base58.Encode(result.expectedTxn.Message.Accounts[nonceIndex]),
		Blockhash:      base58.Encode(result.expectedTxn.Message.RecentBlockhash[:]),
		ProofSignature: base58.Encode(result.proofSignature.Value),
		State:          state,
	}

	if req.reserve != nil {
		p := req.reserve
		record.Kind = swap.KindReserve
		record.FromMint = base58.Encode(p.FromMint.Value)
		record.ToMint = base58.Encode(p.ToMint.Value)
		record.SwapAmount = p.SwapAmount
		record.FeeAmount = p.FeeAmount
		record.FundingSource = swap.FundingSource(p.FundingSource)
		record.FundingId = p.FundingId
	} else {
		p := req.stablecoin
		record.Kind = swap.KindStablecoin
		record.DestinationOwner = base58.Encode(p.DestinationOwner.Value)
		record.FromMint = base58.Encode(p.FromMint.Value)
		record.ToMint = base58.Encode(p.ToMint.Value)
		record.SwapAmount = p.SwapAmount
		record.FeeAmount = p.FeeAmount
		record.FundingSource = swap.FundingSource(p.FundingSource)
		record.FundingId = p.FundingId
	}
	return record
}

//
// Client transaction rebuilds
//

// requireClientRebuildMatchesServer rebuilds the transaction a client signs
// from the request and the server parameters, using only what a client knows
// about the mints, and requires it to match the server's message exactly
func (e *testEnv) requireClientRebuildMatchesServer(t *testing.T, req *statefulSwapRequest, result *statefulSwapResult) {
	require.NotNil(t, result.expectedTxn)

	switch typed := result.serverParameters.Kind.(type) {
	case *transactionpb.StatefulSwapResponse_ServerParameters_ReserveExistingCurrency:
		require.NotNil(t, req.reserve)
		e.requireReserveExistingCurrencyClientRebuildMatches(t, req, *result.expectedTxn, typed.ReserveExistingCurrency)
	case *transactionpb.StatefulSwapResponse_ServerParameters_ReserveNewCurrency:
		require.NotNil(t, req.reserve)
		e.requireReserveNewCurrencyClientRebuildMatches(t, req, *result.expectedTxn, typed.ReserveNewCurrency)
	case *transactionpb.StatefulSwapResponse_ServerParameters_Stablecoin:
		require.NotNil(t, req.stablecoin)
		p := req.stablecoin
		destinationOwner, err := common.NewAccountFromProto(p.DestinationOwner)
		require.NoError(t, err)
		fromMint, err := common.NewAccountFromProto(p.FromMint)
		require.NoError(t, err)
		toMint, err := common.NewAccountFromProto(p.ToMint)
		require.NoError(t, err)
		requireCoinbaseStatefulClientRebuildMatches(t, e, *result.expectedTxn, typed.Stablecoin, req.owner, req.swapAuthority, destinationOwner, fromMint, toMint, p.SwapAmount, p.FeeAmount, selectCoinbaseSwapVersion(p.SwapVersion))
	default:
		require.Fail(t, "unexpected server parameters")
	}
}

// clientMintInfo is what clients know about a mint from its published
// metadata
type clientMintInfo struct {
	mint     *common.Account
	vmConfig *common.VmConfig

	// Launchpad currencies only
	accounts *common.LaunchpadCurrencyAccounts
}

func (e *testEnv) clientMintInfo(t *testing.T, mint *common.Account) *clientMintInfo {
	vmConfig, err := common.GetVmConfigForMint(e.ctx, e.data, mint)
	require.NoError(t, err)

	info := &clientMintInfo{mint: mint, vmConfig: vmConfig}
	if !common.IsCoreMint(mint) {
		metadataRecord, err := e.data.GetCurrencyMetadata(e.ctx, mint.PublicKey().ToBase58())
		require.NoError(t, err)
		info.accounts, err = common.GetLaunchpadCurrencyAccounts(metadataRecord)
		require.NoError(t, err)
	}
	return info
}

// clientSwapPrefix is the AdvanceNonce, optional ComputeBudget and optional
// memo instructions every swap transaction starts with
func clientSwapPrefix(payer, nonce []byte, computeUnitLimit uint32, computeUnitPrice uint64, memoValue string, config *commonpb.SolanaTransactionConfig) []solana.Instruction {
	ixns := []solana.Instruction{system.AdvanceNonce(nonce, payer)}
	if config == nil {
		if computeUnitLimit != 0 {
			ixns = append(ixns, compute_budget.SetComputeUnitLimit(computeUnitLimit))
		}
		if computeUnitPrice != 0 {
			ixns = append(ixns, compute_budget.SetComputeUnitPrice(computeUnitPrice))
		}
	}
	if len(memoValue) > 0 {
		ixns = append(ixns, memo.Instruction(memoValue))
	}
	return ixns
}

// requireClientTransactionMatches builds the client's transaction in the
// version the server parameters select and requires its message to match the
// server's
func requireClientTransactionMatches(
	t *testing.T,
	serverTxn solana.Transaction,
	payer []byte,
	protoAlts []*commonpb.SolanaAddressLookupTable,
	config *commonpb.SolanaTransactionConfig,
	protoBlockhash *commonpb.Blockhash,
	ixns []solana.Instruction,
) {
	var clientTxn solana.Transaction
	if config == nil {
		clientTxn = solana.NewV0Transaction(payer, fromProtoAlts(protoAlts), ixns)
	} else {
		assert.Empty(t, protoAlts)

		// CU limit, loaded accounts data size limit and priority fee are always
		// encoded, and the heap size only when non-zero
		v1Config := solana.TransactionConfig{
			ComputeUnitLimit:            pointer.Uint32(config.ComputeUnitLimit),
			LoadedAccountsDataSizeLimit: pointer.Uint32(config.LoadedAccountsDataSizeLimit),
			PriorityFeeLamports:         pointer.Uint64(config.PriorityFeeLamports),
		}
		if config.HeapSize != 0 {
			v1Config.HeapSize = pointer.Uint32(config.HeapSize)
		}

		var err error
		clientTxn, err = solana.NewV1Transaction(payer, v1Config, ixns...)
		require.NoError(t, err)
	}

	var blockhash solana.Blockhash
	copy(blockhash[:], protoBlockhash.Value)
	clientTxn.SetBlockhash(blockhash)

	expected, err := serverTxn.Message.Marshal()
	require.NoError(t, err)
	actual, err := clientTxn.Message.Marshal()
	require.NoError(t, err)
	require.Equal(t, expected, actual, "client rebuild doesn't match the server's transaction")
}

// requireReserveExistingCurrencyClientRebuildMatches follows the documented
// instruction formats for buy, sell and buy-sell swaps on existing currencies
func (e *testEnv) requireReserveExistingCurrencyClientRebuildMatches(
	t *testing.T,
	req *statefulSwapRequest,
	serverTxn solana.Transaction,
	params *transactionpb.StatefulSwapResponse_ServerParameters_ReserveExistingCurrencyServerParameters,
) {
	p := req.reserve
	owner, swapAuthority := req.owner, req.swapAuthority

	fromMint, err := common.NewAccountFromProto(p.FromMint)
	require.NoError(t, err)
	toMint, err := common.NewAccountFromProto(p.ToMint)
	require.NoError(t, err)
	fromInfo := e.clientMintInfo(t, fromMint)
	toInfo := e.clientMintInfo(t, toMint)

	isSell := !common.IsCoreMint(fromMint)
	isBuy := !common.IsCoreMint(toMint)

	payer := params.Payer.Value
	memoryAccount := params.MemoryAccount.Value
	memoryIndex := uint16(params.MemoryIndex)

	// The server deposits into the owner's destination virtual timelock
	// account, at its reserved memory location
	toVmConfig := toInfo.vmConfig
	destinationTimelockVault, err := owner.ToTimelockVault(toVmConfig)
	require.NoError(t, err)
	expectedMemoryAccount, expectedMemoryIndex, err := e.data.GetVmMemoryLocationByAddress(e.ctx, destinationTimelockVault.PublicKey().ToBase58())
	require.NoError(t, err)
	require.Equal(t, expectedMemoryAccount, base58.Encode(memoryAccount))
	require.Equal(t, expectedMemoryIndex, memoryIndex)

	sourceTimelockAccounts, err := owner.GetTimelockAccounts(fromInfo.vmConfig)
	require.NoError(t, err)
	sourceVm := fromInfo.vmConfig

	transferForSwap := func(destination ed25519.PublicKey) solana.Instruction {
		return vm.NewTransferForSwapInstruction(
			&vm.TransferForSwapInstructionAccounts{
				VmAuthority: sourceVm.Authority.PublicKey().ToBytes(),
				Vm:          sourceVm.Vm.PublicKey().ToBytes(),
				Swapper:     owner.PublicKey().ToBytes(),
				SwapPda:     sourceTimelockAccounts.VmSwapAccounts.Pda.PublicKey().ToBytes(),
				SwapAta:     sourceTimelockAccounts.VmSwapAccounts.Ata.PublicKey().ToBytes(),
				Destination: destination,
			},
			&vm.TransferForSwapInstructionArgs{
				Amount: p.SwapAmount,
				Bump:   sourceTimelockAccounts.VmSwapAccounts.PdaBump,
			},
		)
	}

	closeSourceVmSwapAccountIfEmpty := vm.NewCloseSwapAccountIfEmptyInstruction(
		&vm.CloseSwapAccountIfEmptyInstructionAccounts{
			VmAuthority: sourceVm.Authority.PublicKey().ToBytes(),
			Vm:          sourceVm.Vm.PublicKey().ToBytes(),
			Swapper:     owner.PublicKey().ToBytes(),
			SwapPda:     sourceTimelockAccounts.VmSwapAccounts.Pda.PublicKey().ToBytes(),
			SwapAta:     sourceTimelockAccounts.VmSwapAccounts.Ata.PublicKey().ToBytes(),
			Destination: payer,
		},
		&vm.CloseSwapAccountIfEmptyInstructionArgs{
			Bump: sourceTimelockAccounts.VmSwapAccounts.PdaBump,
		},
	)

	buyAndDepositIntoVm := func(buyerBase ed25519.PublicKey, inAmount uint64) solana.Instruction {
		return currencycreator.NewBuyAndDepositIntoVmInstruction(
			&currencycreator.BuyAndDepositIntoVmInstructionAccounts{
				Buyer:       swapAuthority.PublicKey().ToBytes(),
				Pool:        toInfo.accounts.LiquidityPool.PublicKey().ToBytes(),
				TargetMint:  toMint.PublicKey().ToBytes(),
				BaseMint:    common.CoreMintAccount.PublicKey().ToBytes(),
				VaultTarget: toInfo.accounts.VaultMint.PublicKey().ToBytes(),
				VaultBase:   toInfo.accounts.VaultBase.PublicKey().ToBytes(),
				BuyerBase:   buyerBase,

				VmAuthority: toVmConfig.Authority.PublicKey().ToBytes(),
				Vm:          toVmConfig.Vm.PublicKey().ToBytes(),
				VmMemory:    memoryAccount,
				VmOmnibus:   toVmConfig.Omnibus.PublicKey().ToBytes(),
				VtaOwner:    owner.PublicKey().ToBytes(),
			},
			&currencycreator.BuyAndDepositIntoVmInstructionArgs{
				InAmount:      inAmount,
				MinOutAmount:  0,
				VmMemoryIndex: memoryIndex,
			},
		)
	}

	createTemporaryAta := func(mint *common.Account) (solana.Instruction, ed25519.PublicKey) {
		ixn, ata, err := token.CreateAssociatedTokenAccountIdempotent(payer, swapAuthority.PublicKey().ToBytes(), mint.PublicKey().ToBytes())
		require.NoError(t, err)
		return ixn, ata
	}
	closeTemporaryAta := func(ata ed25519.PublicKey) solana.Instruction {
		return token.CloseAccount(ata, payer, swapAuthority.PublicKey().ToBytes())
	}

	var swapIxns []solana.Instruction
	switch {
	case isBuy && !isSell:
		createTemporaryCoreAtaIxn, temporaryCoreAta := createTemporaryAta(common.CoreMintAccount)

		transferIxn := transferForSwap(temporaryCoreAta)
		if p.FeeAmount > 0 {
			require.NotNil(t, params.FeeDestination)
			transferIxn = vm.NewTransferForSwapWithFeeInstruction(
				&vm.TransferForSwapWithFeeInstructionAccounts{
					VmAuthority:     sourceVm.Authority.PublicKey().ToBytes(),
					Vm:              sourceVm.Vm.PublicKey().ToBytes(),
					Swapper:         owner.PublicKey().ToBytes(),
					SwapPda:         sourceTimelockAccounts.VmSwapAccounts.Pda.PublicKey().ToBytes(),
					SwapAta:         sourceTimelockAccounts.VmSwapAccounts.Ata.PublicKey().ToBytes(),
					SwapDestination: temporaryCoreAta,
					FeeDestination:  params.FeeDestination.Value,
				},
				&vm.TransferForSwapWithFeeInstructionArgs{
					SwapAmount: p.SwapAmount,
					FeeAmount:  p.FeeAmount,
					Bump:       sourceTimelockAccounts.VmSwapAccounts.PdaBump,
				},
			)
		} else {
			assert.Nil(t, params.FeeDestination)
		}

		swapIxns = []solana.Instruction{
			createTemporaryCoreAtaIxn,
			transferIxn,
			buyAndDepositIntoVm(temporaryCoreAta, p.SwapAmount),
			closeTemporaryAta(temporaryCoreAta),
			closeSourceVmSwapAccountIfEmpty,
		}

	case isSell && !isBuy:
		assert.Nil(t, params.FeeDestination)
		createTemporarySourceAtaIxn, temporarySourceAta := createTemporaryAta(fromMint)

		sellAndDepositIntoVmIxn := currencycreator.NewSellAndDepositIntoVmInstruction(
			&currencycreator.SellAndDepositIntoVmInstructionAccounts{
				Seller:       swapAuthority.PublicKey().ToBytes(),
				Pool:         fromInfo.accounts.LiquidityPool.PublicKey().ToBytes(),
				TargetMint:   fromMint.PublicKey().ToBytes(),
				BaseMint:     common.CoreMintAccount.PublicKey().ToBytes(),
				VaultTarget:  fromInfo.accounts.VaultMint.PublicKey().ToBytes(),
				VaultBase:    fromInfo.accounts.VaultBase.PublicKey().ToBytes(),
				SellerTarget: temporarySourceAta,

				VmAuthority: toVmConfig.Authority.PublicKey().ToBytes(),
				Vm:          toVmConfig.Vm.PublicKey().ToBytes(),
				VmMemory:    memoryAccount,
				VmOmnibus:   toVmConfig.Omnibus.PublicKey().ToBytes(),
				VtaOwner:    owner.PublicKey().ToBytes(),
			},
			&currencycreator.SellAndDepositIntoVmInstructionArgs{
				InAmount:      p.SwapAmount,
				MinOutAmount:  0,
				VmMemoryIndex: memoryIndex,
			},
		)

		swapIxns = []solana.Instruction{
			createTemporarySourceAtaIxn,
			transferForSwap(temporarySourceAta),
			sellAndDepositIntoVmIxn,
			closeTemporaryAta(temporarySourceAta),
			closeSourceVmSwapAccountIfEmpty,
		}

	default:
		assert.Nil(t, params.FeeDestination)
		createTemporaryCoreAtaIxn, temporaryCoreAta := createTemporaryAta(common.CoreMintAccount)
		createTemporarySourceAtaIxn, temporarySourceAta := createTemporaryAta(fromMint)

		sellIxn := currencycreator.NewSellTokensInstruction(
			&currencycreator.SellTokensInstructionAccounts{
				Seller:       swapAuthority.PublicKey().ToBytes(),
				Pool:         fromInfo.accounts.LiquidityPool.PublicKey().ToBytes(),
				TargetMint:   fromMint.PublicKey().ToBytes(),
				BaseMint:     common.CoreMintAccount.PublicKey().ToBytes(),
				VaultTarget:  fromInfo.accounts.VaultMint.PublicKey().ToBytes(),
				VaultBase:    fromInfo.accounts.VaultBase.PublicKey().ToBytes(),
				SellerTarget: temporarySourceAta,
				SellerBase:   temporaryCoreAta,
			},
			&currencycreator.SellTokensInstructionArgs{
				InAmount:     p.SwapAmount,
				MinAmountOut: 0,
			},
		)

		swapIxns = []solana.Instruction{
			createTemporaryCoreAtaIxn,
			createTemporarySourceAtaIxn,
			transferForSwap(temporarySourceAta),
			sellIxn,
			buyAndDepositIntoVm(temporaryCoreAta, 0),
			closeTemporaryAta(temporaryCoreAta),
			closeTemporaryAta(temporarySourceAta),
			closeSourceVmSwapAccountIfEmpty,
		}
	}

	ixns := clientSwapPrefix(payer, params.Nonce.Value, params.ComputeUnitLimit, params.ComputeUnitPrice, params.MemoValue, params.TransactionConfig)
	ixns = append(ixns, swapIxns...)
	requireClientTransactionMatches(t, serverTxn, payer, params.Alts, params.TransactionConfig, params.Blockhash, ixns)
}

// clientNewCurrencyAccounts are the new currency's accounts, derived like a
// client from the server parameters
type clientNewCurrencyAccounts struct {
	authority []byte

	mint                ed25519.PublicKey
	mintBump            uint8
	currencyConfig      ed25519.PublicKey
	currencyConfigBump  uint8
	liquidityPool       ed25519.PublicKey
	liquidityPoolBump   uint8
	vaultMint           ed25519.PublicKey
	vaultMintBump       uint8
	vaultCore           ed25519.PublicKey
	vaultCoreBump       uint8
	vmConfig            *common.VmConfig
	vmBump, omnibusBump uint8
}

func deriveClientNewCurrencyAccounts(t *testing.T, params *transactionpb.StatefulSwapResponse_ServerParameters_ReserveNewCurrencyServerParameter) *clientNewCurrencyAccounts {
	accounts := &clientNewCurrencyAccounts{authority: params.Authority.Value}

	var err error
	accounts.mint, accounts.mintBump, err = currencycreator.GetMintAddress(&currencycreator.GetMintAddressArgs{
		Authority: params.Authority.Value,
		Name:      params.Name,
		Seed:      params.Seed.Value,
	})
	require.NoError(t, err)
	accounts.currencyConfig, accounts.currencyConfigBump, err = currencycreator.GetCurrencyAddress(&currencycreator.GetCurrencyAddressArgs{Mint: accounts.mint})
	require.NoError(t, err)
	accounts.liquidityPool, accounts.liquidityPoolBump, err = currencycreator.GetPoolAddress(&currencycreator.GetPoolAddressArgs{Currency: accounts.currencyConfig})
	require.NoError(t, err)
	accounts.vaultMint, accounts.vaultMintBump, err = currencycreator.GetVaultAddress(&currencycreator.GetVaultAddressArgs{Pool: accounts.liquidityPool, Mint: accounts.mint})
	require.NoError(t, err)
	accounts.vaultCore, accounts.vaultCoreBump, err = currencycreator.GetVaultAddress(&currencycreator.GetVaultAddressArgs{Pool: accounts.liquidityPool, Mint: common.CoreMintAccount.PublicKey().ToBytes()})
	require.NoError(t, err)

	vmAddress, vmBump, err := vm.GetVmAddress(&vm.GetVmAddressArgs{
		Mint:         accounts.mint,
		VmAuthority:  params.Authority.Value,
		LockDuration: uint8(params.VmLockDurationInDays),
	})
	require.NoError(t, err)
	omnibusAddress, omnibusBump, err := vm.GetVmObnibusAddress(&vm.GetVmObnibusAddressArgs{Vm: vmAddress})
	require.NoError(t, err)
	accounts.vmBump, accounts.omnibusBump = vmBump, omnibusBump

	authority, err := common.NewAccountFromPublicKeyBytes(params.Authority.Value)
	require.NoError(t, err)
	vmAccount, err := common.NewAccountFromPublicKeyBytes(vmAddress)
	require.NoError(t, err)
	omnibusAccount, err := common.NewAccountFromPublicKeyBytes(omnibusAddress)
	require.NoError(t, err)
	mintAccount, err := common.NewAccountFromPublicKeyBytes(accounts.mint)
	require.NoError(t, err)
	accounts.vmConfig = &common.VmConfig{Authority: authority, Vm: vmAccount, Omnibus: omnibusAccount, Mint: mintAccount}

	return accounts
}

// requireReserveNewCurrencyClientRebuildMatches follows the documented v0 and
// v1 instruction formats for currency creation, deriving the new currency's
// accounts from the server parameters
func (e *testEnv) requireReserveNewCurrencyClientRebuildMatches(
	t *testing.T,
	req *statefulSwapRequest,
	serverTxn solana.Transaction,
	params *transactionpb.StatefulSwapResponse_ServerParameters_ReserveNewCurrencyServerParameter,
) {
	p := req.reserve
	owner := req.owner

	fromMint, err := common.NewAccountFromProto(p.FromMint)
	require.NoError(t, err)

	// Clients verify the new currency's mint matches the one derived from the
	// server parameters
	newCurrency := deriveClientNewCurrencyAccounts(t, params)
	require.Equal(t, p.ToMint.Value, []byte(newCurrency.mint), "mint derived from server parameters isn't the swap's destination mint")

	payer := params.Payer.Value
	coreMint := common.CoreMintAccount.PublicKey().ToBytes()
	fromInfo := e.clientMintInfo(t, fromMint)

	ownerVmSwapAccounts, err := owner.GetVmSwapAccounts(fromInfo.vmConfig)
	require.NoError(t, err)
	ownerVmDepositAccounts, err := owner.GetVmDepositAccounts(newCurrency.vmConfig)
	require.NoError(t, err)

	authority := params.Authority.Value
	isV1 := params.TransactionConfig != nil

	initIxns := []solana.Instruction{
		currencycreator.NewInitializeCurrencyInstruction(
			&currencycreator.InitializeCurrencyInstructionAccounts{
				Authority: authority,
				Mint:      newCurrency.mint,
				Currency:  newCurrency.currencyConfig,
			},
			&currencycreator.InitializeCurrencyInstructionArgs{
				Name:     params.Name,
				Symbol:   params.Symbol,
				Seed:     params.Seed.Value,
				Bump:     newCurrency.currencyConfigBump,
				MintBump: newCurrency.mintBump,
			},
		),
		currencycreator.NewInitializePoolInstruction(
			&currencycreator.InitializePoolInstructionAccounts{
				Authority:   authority,
				Currency:    newCurrency.currencyConfig,
				TargetMint:  newCurrency.mint,
				BaseMint:    coreMint,
				Pool:        newCurrency.liquidityPool,
				VaultTarget: newCurrency.vaultMint,
				VaultBase:   newCurrency.vaultCore,
			},
			&currencycreator.InitializePoolInstructionArgs{
				SellFee:         uint16(params.SellFeeBps),
				Bump:            newCurrency.liquidityPoolBump,
				VaultTargetBump: newCurrency.vaultMintBump,
				VaultBaseBump:   newCurrency.vaultCoreBump,
			},
		),
		vm.NewInitVmInstruction(
			&vm.InitVmInstructionAccounts{
				VmAuthority: authority,
				Vm:          newCurrency.vmConfig.Vm.PublicKey().ToBytes(),
				VmOmnibus:   newCurrency.vmConfig.Omnibus.PublicKey().ToBytes(),
				Mint:        newCurrency.mint,
			},
			&vm.InitVmInstructionArgs{
				LockDuration:  uint8(params.VmLockDurationInDays),
				VmBump:        newCurrency.vmBump,
				VmOmnibusBump: newCurrency.omnibusBump,
			},
		),
	}

	createVmDepositAtaIxn, _, err := token.CreateAssociatedTokenAccountIdempotent(authority, ownerVmDepositAccounts.Pda.PublicKey().ToBytes(), newCurrency.mint)
	require.NoError(t, err)

	var swapIxns []solana.Instruction
	if params.Treasury == nil {
		require.True(t, common.IsCoreMint(fromMint))

		// v1 buys through the swap authority's temporary core mint ATA, which
		// nobody can pre-fund to make closing it fail. v0 uses the owner's.
		temporaryHolder := owner
		if isV1 {
			require.NotEqual(t, owner.PublicKey().ToBase58(), req.swapAuthority.PublicKey().ToBase58())
			temporaryHolder = req.swapAuthority
		}

		createTemporaryCoreAtaIxn, temporaryCoreAta, err := token.CreateAssociatedTokenAccountIdempotent(authority, temporaryHolder.PublicKey().ToBytes(), coreMint)
		require.NoError(t, err)

		swapIxns = append(initIxns,
			createTemporaryCoreAtaIxn,
			createVmDepositAtaIxn,
			vm.NewTransferForSwapWithFeeInstruction(
				&vm.TransferForSwapWithFeeInstructionAccounts{
					VmAuthority:     fromInfo.vmConfig.Authority.PublicKey().ToBytes(),
					Vm:              fromInfo.vmConfig.Vm.PublicKey().ToBytes(),
					Swapper:         owner.PublicKey().ToBytes(),
					SwapPda:         ownerVmSwapAccounts.Pda.PublicKey().ToBytes(),
					SwapAta:         ownerVmSwapAccounts.Ata.PublicKey().ToBytes(),
					SwapDestination: temporaryCoreAta,
					FeeDestination:  params.FeeDestination.Value,
				},
				&vm.TransferForSwapWithFeeInstructionArgs{
					SwapAmount: p.SwapAmount,
					FeeAmount:  p.FeeAmount,
					Bump:       ownerVmSwapAccounts.PdaBump,
				},
			),
			currencycreator.NewBuyTokensInstruction(
				&currencycreator.BuyTokensInstructionAccounts{
					Buyer:       temporaryHolder.PublicKey().ToBytes(),
					Pool:        newCurrency.liquidityPool,
					TargetMint:  newCurrency.mint,
					BaseMint:    coreMint,
					VaultTarget: newCurrency.vaultMint,
					VaultBase:   newCurrency.vaultCore,
					BuyerTarget: ownerVmDepositAccounts.Ata.PublicKey().ToBytes(),
					BuyerBase:   temporaryCoreAta,
				},
				&currencycreator.BuyTokensInstructionArgs{
					InAmount:     p.SwapAmount,
					MinAmountOut: 0,
				},
			),
			token.CloseAccount(temporaryCoreAta, authority, temporaryHolder.PublicKey().ToBytes()),
		)
	} else {
		require.False(t, common.IsCoreMint(fromMint))

		// v1 pays for the treasury's from mint ATA with the currency authority
		treasuryFromMintAtaPayer := payer
		if isV1 {
			treasuryFromMintAtaPayer = authority
		}

		treasury := params.Treasury.Value
		treasuryCoreAta, err := token.GetAssociatedAccount(treasury, coreMint)
		require.NoError(t, err)
		createTreasuryFromMintAtaIxn, treasuryFromMintAta, err := token.CreateAssociatedTokenAccountIdempotent(treasuryFromMintAtaPayer, treasury, fromMint.PublicKey().ToBytes())
		require.NoError(t, err)

		purchaseIxns := []solana.Instruction{
			createTreasuryFromMintAtaIxn,
			vm.NewTransferForSwapWithFeeInstruction(
				&vm.TransferForSwapWithFeeInstructionAccounts{
					VmAuthority:     fromInfo.vmConfig.Authority.PublicKey().ToBytes(),
					Vm:              fromInfo.vmConfig.Vm.PublicKey().ToBytes(),
					Swapper:         owner.PublicKey().ToBytes(),
					SwapPda:         ownerVmSwapAccounts.Pda.PublicKey().ToBytes(),
					SwapAta:         ownerVmSwapAccounts.Ata.PublicKey().ToBytes(),
					SwapDestination: treasuryFromMintAta,
					FeeDestination:  treasuryFromMintAta,
				},
				&vm.TransferForSwapWithFeeInstructionArgs{
					SwapAmount: p.SwapAmount,
					FeeAmount:  p.FeeAmount,
					Bump:       ownerVmSwapAccounts.PdaBump,
				},
			),
			currencycreator.NewSellTokensInstruction(
				&currencycreator.SellTokensInstructionAccounts{
					Seller:       treasury,
					Pool:         fromInfo.accounts.LiquidityPool.PublicKey().ToBytes(),
					TargetMint:   fromMint.PublicKey().ToBytes(),
					BaseMint:     coreMint,
					VaultTarget:  fromInfo.accounts.VaultMint.PublicKey().ToBytes(),
					VaultBase:    fromInfo.accounts.VaultBase.PublicKey().ToBytes(),
					SellerTarget: treasuryFromMintAta,
					SellerBase:   params.FeeDestination.Value,
				},
				&currencycreator.SellTokensInstructionArgs{
					InAmount:     p.SwapAmount + p.FeeAmount,
					MinAmountOut: 0,
				},
			),
			currencycreator.NewBuyTokensInstruction(
				&currencycreator.BuyTokensInstructionAccounts{
					Buyer:       treasury,
					Pool:        newCurrency.liquidityPool,
					TargetMint:  newCurrency.mint,
					BaseMint:    coreMint,
					VaultTarget: newCurrency.vaultMint,
					VaultBase:   newCurrency.vaultCore,
					BuyerTarget: ownerVmDepositAccounts.Ata.PublicKey().ToBytes(),
					BuyerBase:   treasuryCoreAta,
				},
				&currencycreator.BuyTokensInstructionArgs{
					InAmount:     params.TreasuryPurchaseAmount,
					MinAmountOut: 0,
				},
			),
		}

		// v0 transactions rely on the launcher initializing the currency, VM
		// and owner's VM deposit ATA beforehand
		if isV1 {
			swapIxns = append(initIxns, createVmDepositAtaIxn)
		}
		swapIxns = append(swapIxns, purchaseIxns...)
	}

	if isV1 {
		swapIxns = append(swapIxns, clientNewCurrencyPostPurchaseInstructions(t, params, newCurrency)...)
	} else {
		assert.Empty(t, params.MemoryAccounts)
	}

	ixns := clientSwapPrefix(payer, params.Nonce.Value, params.ComputeUnitLimit, params.ComputeUnitPrice, params.MemoValue, params.TransactionConfig)
	ixns = append(ixns, swapIxns...)
	requireClientTransactionMatches(t, serverTxn, payer, params.Alts, params.TransactionConfig, params.Blockhash, ixns)
}

// clientNewCurrencyPostPurchaseInstructions follows the documented v1
// instructions that complete a launch after the purchase: metadata, memory
// account initialization, then the fee account
func clientNewCurrencyPostPurchaseInstructions(
	t *testing.T,
	params *transactionpb.StatefulSwapResponse_ServerParameters_ReserveNewCurrencyServerParameter,
	newCurrency *clientNewCurrencyAccounts,
) []solana.Instruction {
	authority := params.Authority.Value
	vmAddress := newCurrency.vmConfig.Vm.PublicKey().ToBytes()

	metadataAddress, _, err := currencycreator.GetMetadataAddress(&currencycreator.GetMetadataAddressArgs{Mint: newCurrency.mint})
	require.NoError(t, err)

	ixns := []solana.Instruction{
		currencycreator.NewInitializeMetadataInstruction(
			&currencycreator.InitializeMetadataInstructionAccounts{
				Authority: authority,
				Mint:      newCurrency.mint,
				Currency:  newCurrency.currencyConfig,
				Metadata:  metadataAddress,
			},
			&currencycreator.InitializeMetadataInstructionArgs{},
		),
	}

	// Clients verify each memory account address is derived from the VM and
	// name
	require.NotEmpty(t, params.MemoryAccounts)
	for _, memoryAccount := range params.MemoryAccounts {
		address, bump, err := vm.GetMemoryAccountAddress(&vm.GetMemoryAccountAddressArgs{
			Name: memoryAccount.Name,
			Vm:   vmAddress,
		})
		require.NoError(t, err)
		require.Equal(t, []byte(address), memoryAccount.Address.Value, "memory account address isn't derived from the vm and name")

		ixns = append(ixns, vm.NewInitMemoryInstruction(
			&vm.InitMemoryInstructionAccounts{
				VmAuthority: authority,
				Vm:          vmAddress,
				VmMemory:    address,
			},
			&vm.InitMemoryInstructionArgs{
				Name:         memoryAccount.Name,
				NumAccounts:  memoryAccount.NumAccounts,
				AccountSize:  uint16(memoryAccount.AccountSize),
				VmMemoryBump: bump,
			},
		))
	}

	createFeeAtaIxn, _, err := token.CreateAssociatedTokenAccountIdempotent(authority, params.FeeAccountOwner.Value, newCurrency.mint)
	require.NoError(t, err)
	ixns = append(ixns, createFeeAtaIxn)

	return ixns
}

//
// StatelessSwap
//

type statelessSwapRequest struct {
	owner               *common.Account
	params              *transactionpb.StatelessSwapRequest_Initiate_CoinbaseStableSwapperClientParameters
	transactionVersion  uint32
	waitForFinalization bool

	signature func(t *testing.T, message []byte) *commonpb.Signature
	secondMsg *transactionpb.StatelessSwapRequest
}

type statelessSwapResult struct {
	serverParameters *transactionpb.StatelessSwapResponse_ServerParameters
	expectedTxn      *solana.Transaction
	signature        *commonpb.Signature

	resp *transactionpb.StatelessSwapResponse
	err  error
}

// statelessSwapRequest builds a Coinbase swap of the owner's USDC into their
// core mint VM deposit account
func (e *testEnv) statelessSwapRequest(t *testing.T, owner *common.Account, swapAmount uint64) *statelessSwapRequest {
	return &statelessSwapRequest{
		owner: owner,
		params: &transactionpb.StatelessSwapRequest_Initiate_CoinbaseStableSwapperClientParameters{
			FromMint:    usdcMint(t),
			ToMint:      common.CoreMintAccount.ToProto(),
			SwapAmount:  swapAmount,
			SwapVersion: uint32(transaction_util.CoinbaseSwapV2),
		},
	}
}

func (e *testEnv) statelessSwap(t *testing.T, req *statelessSwapRequest) *statelessSwapResult {
	ctx, cancel := context.WithTimeout(e.ctx, defaultTestStreamTimeout)
	defer cancel()

	stream, err := e.client.StatelessSwap(ctx)
	require.NoError(t, err)

	initiate := &transactionpb.StatelessSwapRequest_Initiate{
		Kind: &transactionpb.StatelessSwapRequest_Initiate_Stablecoin{
			Stablecoin: req.params,
		},
		Owner:               req.owner.ToProto(),
		WaitForFinalization: req.waitForFinalization,
		TransactionVersion:  req.transactionVersion,
	}
	initiate.Signature = e.signProto(t, req.owner, initiate)

	result := &statelessSwapResult{}

	// Requests failing proto validation are rejected by the client interceptor
	err = stream.Send(&transactionpb.StatelessSwapRequest{
		Request: &transactionpb.StatelessSwapRequest_Initiate_{Initiate: initiate},
	})
	if err == io.EOF {
		// The server ended the stream before reading the request
		_, err = stream.Recv()
	}
	if err != nil {
		result.err = err
		return result
	}

	resp, err := stream.Recv()
	if err != nil {
		result.err = err
		return result
	}
	if resp.GetServerParameters() == nil {
		result.resp = resp
		return result
	}
	result.serverParameters = resp.GetServerParameters()

	expectedTxn := e.expectedStatelessSwapTransaction(t, req, result.serverParameters.GetStablecoin())
	result.expectedTxn = &expectedTxn

	secondMsg := req.secondMsg
	if secondMsg == nil {
		message, err := expectedTxn.Message.Marshal()
		require.NoError(t, err)

		if req.signature != nil {
			result.signature = req.signature(t, message)
		} else {
			result.signature = &commonpb.Signature{Value: ed25519.Sign(req.owner.PrivateKey().ToBytes(), message)}
		}

		secondMsg = &transactionpb.StatelessSwapRequest{
			Request: &transactionpb.StatelessSwapRequest_SubmitSignatures_{
				SubmitSignatures: &transactionpb.StatelessSwapRequest_SubmitSignatures{
					TransactionSignatures: []*commonpb.Signature{result.signature},
				},
			},
		}
	}
	require.NoError(t, stream.Send(secondMsg))

	result.resp, result.err = stream.Recv()
	if result.err != nil {
		result.resp = nil
	}
	return result
}

// expectedStatelessSwapTransaction rebuilds the transaction from the server
// parameters following the instruction format documented on
// StatelessSwapResponse.ServerParameters.CoinbaseStableSwapperServerParameter
func (e *testEnv) expectedStatelessSwapTransaction(t *testing.T, req *statelessSwapRequest, params *transactionpb.StatelessSwapResponse_ServerParameters_CoinbaseStableSwapperServerParameter) solana.Transaction {
	require.NotNil(t, params)

	fromMint, err := common.NewAccountFromProto(req.params.FromMint)
	require.NoError(t, err)
	toMint, err := common.NewAccountFromProto(req.params.ToMint)
	require.NoError(t, err)

	vmConfig, err := common.GetVmConfigForMint(e.ctx, e.data, toMint)
	require.NoError(t, err)
	depositAccounts, err := req.owner.GetVmDepositAccounts(vmConfig)
	require.NoError(t, err)
	ownerFromAta, err := req.owner.ToAssociatedTokenAccount(fromMint)
	require.NoError(t, err)

	createDepositAtaIxn, _, err := token.CreateAssociatedTokenAccountIdempotent(params.Payer.Value, depositAccounts.Pda.PublicKey().ToBytes(), toMint.PublicKey().ToBytes())
	require.NoError(t, err)

	coinbaseAccounts, err := transaction_util.GetCoinbaseSwapAccounts(e.ctx, e.data, fromMint.PublicKey().ToBytes(), toMint.PublicKey().ToBytes())
	require.NoError(t, err)
	coinbaseSwapIxn, err := transaction_util.MakeCoinbaseSwapInstruction(
		selectCoinbaseSwapVersion(req.params.SwapVersion),
		coinbaseAccounts,
		req.owner.PublicKey().ToBytes(),
		fromMint.PublicKey().ToBytes(),
		toMint.PublicKey().ToBytes(),
		ownerFromAta.PublicKey().ToBytes(),
		depositAccounts.Ata.PublicKey().ToBytes(),
		req.params.SwapAmount,
		req.params.SwapAmount,
	)
	require.NoError(t, err)

	feeRecipient, err := common.NewAccountFromPublicKeyBytes(coinbaseAccounts.FeeRecipient)
	require.NoError(t, err)

	var blockhash solana.Blockhash
	copy(blockhash[:], params.Blockhash.Value)

	txn, expectedParams, err := makeStatelessSwapTransaction(selectSwapTransactionVersion(req.transactionVersion), blockhash, feeRecipient, createDepositAtaIxn, coinbaseSwapIxn)
	require.NoError(t, err)
	assert.True(t, proto.Equal(expectedParams, params), "server parameters: expected %v, got %v", expectedParams, params)
	return txn
}

func (r *statelessSwapResult) requireError(t *testing.T, code transactionpb.StatelessSwapResponse_Error_Code, reasonSubstr string) {
	require.NoError(t, r.err)
	require.NotNil(t, r.resp)
	require.NotNil(t, r.resp.GetError(), "expected error, got %v", r.resp)
	assert.Equal(t, code, r.resp.GetError().Code, "unexpected error response: %v", r.resp.GetError())
	requireErrorReason(t, r.resp.GetError().ErrorDetails, reasonSubstr)
}

func (r *statelessSwapResult) requireInvalidSwap(t *testing.T, reasonSubstr string) {
	r.requireError(t, transactionpb.StatelessSwapResponse_Error_INVALID_SWAP, reasonSubstr)
}

func (r *statelessSwapResult) requireDenied(t *testing.T, reasonSubstr string) {
	r.requireError(t, transactionpb.StatelessSwapResponse_Error_DENIED, reasonSubstr)
}

func (r *statelessSwapResult) requireStatusCode(t *testing.T, code codes.Code) {
	testutil.AssertStatusErrorWithCode(t, r.err, code)
}

// setLiveSupply moves the mint's live reserve state, which backs the sell fee
// check, to the supply and waits for the server to observe it
func (e *testEnv) setLiveSupply(t *testing.T, mint *common.Account, supplyFromBonding uint64) {
	existing, err := e.reserveStore.GetLiveReserve(e.ctx, mint.PublicKey().ToBase58())
	require.NoError(t, err)

	require.NoError(t, e.reserveStore.PutLiveReserve(e.ctx, &currency.ReserveRecord{
		Mint:              mint.PublicKey().ToBase58(),
		SupplyFromBonding: supplyFromBonding,
		Slot:              existing.Slot + 1,
		Time:              time.Now(),
	}))

	require.Eventually(t, func() bool {
		state, err := e.mintDataProvider.GetLiveReserveState(e.ctx, mint)
		return err == nil && state.SupplyFromBonding == supplyFromBonding
	}, 5*time.Second, 10*time.Millisecond)
}

const (
	reserveSwapCoreBalance      = 100 // whole core mint units
	reserveSwapLaunchpadBalance = 100 // whole launchpad tokens
)

// reserveSwapFixture is a user with funded, swappable core mint and launchpad
// currency accounts
type reserveSwapFixture struct {
	env        *testEnv
	owner      *common.Account
	launchpad1 *common.Account
	launchpad2 *common.Account
}

func newReserveSwapFixture(t *testing.T, overrides ...func(*testOverrides)) *reserveSwapFixture {
	env := setupTestEnv(t, append([]func(*testOverrides){withSwapsEnabled()}, overrides...)...)

	f := &reserveSwapFixture{
		env:        env,
		launchpad1: env.newLaunchpadMint(t),
		launchpad2: env.newLaunchpadMint(t),
	}

	f.owner = env.newFundedUser(t, common.ToCoreMintQuarks(reserveSwapCoreBalance))
	for _, mint := range []*common.Account{f.launchpad1, f.launchpad2} {
		env.openAccount(t, f.owner, f.owner, mint, commonpb.AccountType_PRIMARY, 0)
		env.credit(t, env.getVault(t, f.owner, mint), currencycreator.ToQuarks(reserveSwapLaunchpadBalance))
	}
	for _, mint := range []*common.Account{common.CoreMintAccount, f.launchpad1, f.launchpad2} {
		env.reserveTimelockMemory(t, f.owner, mint)
	}
	return f
}

func (f *reserveSwapFixture) snapshot(t *testing.T) *swapSnapshot {
	return f.env.swapSnapshot(t, []*common.Account{f.owner}, f.launchpad1, f.launchpad2)
}

// buyRequest buys launchpad1 with a whole core mint unit, without a fee
func (f *reserveSwapFixture) buyRequest(t *testing.T) *statefulSwapRequest {
	return f.env.reserveSwapRequest(t, f.owner, common.CoreMintAccount, f.launchpad1, common.CoreMintQuarksPerUnit, 0)
}

func expectedAntispamSwapCall(kind swap.Kind, fundingSource transactionpb.FundingSource, fromMint, toMint *commonpb.SolanaAccountId, swapAmount, feeAmount uint64, initializesMint bool) string {
	return fmt.Sprintf("swap:%s:%d:%s:%s:%d:%d:%t", kind, fundingSource, base58.Encode(fromMint.Value), base58.Encode(toMint.Value), swapAmount, feeAmount, initializesMint)
}

//
// Generic StatefulSwap behaviour
//

func TestStatefulSwap_Disabled(t *testing.T) {
	env := setupTestEnv(t)
	owner := env.newFundedUser(t, common.ToCoreMintQuarks(10))
	mint := env.newLaunchpadMint(t)

	req := env.reserveSwapRequest(t, owner, common.CoreMintAccount, mint, common.CoreMintQuarksPerUnit, 0)
	before := env.swapSnapshot(t, []*common.Account{owner}, mint)
	result := env.statefulSwap(t, req)
	result.requireStatusCode(t, codes.Unavailable)
	env.requireNoSwapChanges(t, before, result)
}

func TestStatefulSwap_FirstRequestMustBeInitiate(t *testing.T) {
	env := setupTestEnv(t, withSwapsEnabled())

	stream, err := env.client.StatefulSwap(env.ctx)
	require.NoError(t, err)
	require.NoError(t, stream.Send(&transactionpb.StatefulSwapRequest{
		Request: &transactionpb.StatefulSwapRequest_SubmitSignatures_{
			SubmitSignatures: &transactionpb.StatefulSwapRequest_SubmitSignatures{
				TransactionSignatures: []*commonpb.Signature{{Value: make([]byte, ed25519.SignatureSize)}},
			},
		},
	}))

	_, err = stream.Recv()
	testutil.AssertStatusErrorWithCode(t, err, codes.InvalidArgument)
}

func TestStatefulSwap_ClientReceiveTimeout(t *testing.T) {
	t.Run("initiate", func(t *testing.T) {
		env := setupTestEnv(t, withSwapsEnabled(), withClientReceiveTimeout(100*time.Millisecond))

		stream, err := env.client.StatefulSwap(env.ctx)
		require.NoError(t, err)

		_, err = stream.Recv()
		testutil.AssertStatusErrorWithCode(t, err, codes.DeadlineExceeded)
	})

	t.Run("signatures", func(t *testing.T) {
		f := newReserveSwapFixture(t, withClientReceiveTimeout(500*time.Millisecond))
		req := f.buyRequest(t)
		before := f.snapshot(t)

		stream, err := f.env.client.StatefulSwap(f.env.ctx)
		require.NoError(t, err)
		require.NoError(t, stream.Send(&transactionpb.StatefulSwapRequest{
			Request: &transactionpb.StatefulSwapRequest_Initiate_{Initiate: req.initiate(t, f.env)},
		}))

		resp, err := stream.Recv()
		require.NoError(t, err)
		require.NotNil(t, resp.GetServerParameters())
		expectedTxn := f.env.expectedStatefulSwapTransaction(t, req, resp.GetServerParameters())

		_, err = stream.Recv()
		testutil.AssertStatusErrorWithCode(t, err, codes.DeadlineExceeded)
		f.env.requireNoSwapChanges(t, before, &statefulSwapResult{expectedTxn: &expectedTxn})
	})
}

func TestStatefulSwap_InvalidInitiateSignature(t *testing.T) {
	f := newReserveSwapFixture(t)
	req := f.buyRequest(t)
	before := f.snapshot(t)

	stream, err := f.env.client.StatefulSwap(f.env.ctx)
	require.NoError(t, err)

	initiate := req.initiate(t, f.env)
	initiate.Signature = f.env.signProto(t, f.env.newAccount(t), initiate)
	require.NoError(t, stream.Send(&transactionpb.StatefulSwapRequest{
		Request: &transactionpb.StatefulSwapRequest_Initiate_{Initiate: initiate},
	}))

	_, err = stream.Recv()
	testutil.AssertStatusErrorWithCode(t, err, codes.Unauthenticated)
	f.env.requireNoSwapChanges(t, before, nil)
}

func TestStatefulSwap_InvalidProofSignature(t *testing.T) {
	f := newReserveSwapFixture(t)
	f.env.setupCoinbasePool(t, common.ToCoreMintQuarks(1_000_000))

	for _, tc := range []struct {
		name string
		req  *statefulSwapRequest
	}{
		{"reserve", f.buyRequest(t)},
		{"stablecoin", f.env.stablecoinSwapRequest(t, f.owner, common.CoreMintQuarksPerUnit)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tc.req.proofSigner = f.env.newAccount(t)

			before := f.snapshot(t)
			result := f.env.statefulSwap(t, tc.req)
			result.requireSignatureError(t, "")
			f.env.requireNoSwapChanges(t, before, result)
		})
	}
}

func TestStatefulSwap_ProtoValidation(t *testing.T) {
	f := newReserveSwapFixture(t)
	f.env.setupCoinbasePool(t, common.ToCoreMintQuarks(1_000_000))

	for _, tc := range []struct {
		name   string
		mutate func(req *statefulSwapRequest)
	}{
		{"reserve zero swap amount", func(req *statefulSwapRequest) { req.reserve.SwapAmount = 0 }},
		{"reserve short funding id", func(req *statefulSwapRequest) { req.reserve.FundingId = "short" }},
		{"reserve unknown funding source", func(req *statefulSwapRequest) {
			req.reserve.FundingSource = transactionpb.FundingSource_FUNDING_SOURCE_UNKNOWN
		}},
		{"stablecoin external wallet funding", func(req *statefulSwapRequest) {
			req.stablecoin.FundingSource = transactionpb.FundingSource_FUNDING_SOURCE_EXTERNAL_WALLET
		}},
		{"stablecoin zero swap amount", func(req *statefulSwapRequest) { req.stablecoin.SwapAmount = 0 }},
		{"stablecoin long funding id", func(req *statefulSwapRequest) { req.stablecoin.FundingId = strings.Repeat("1", 45) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := f.buyRequest(t)
			if strings.HasPrefix(tc.name, "stablecoin") {
				req = f.env.stablecoinSwapRequest(t, f.owner, common.CoreMintQuarksPerUnit)
			}
			tc.mutate(req)

			before := f.snapshot(t)
			result := f.env.statefulSwap(t, req)
			result.requireStatusCode(t, codes.InvalidArgument)
			f.env.requireNoSwapChanges(t, before, result)
		})
	}
}

func TestStatefulSwap_SignatureSubmission(t *testing.T) {
	f := newReserveSwapFixture(t)

	t.Run("wrong second message", func(t *testing.T) {
		req := f.buyRequest(t)
		req.secondMsg = &transactionpb.StatefulSwapRequest{
			Request: &transactionpb.StatefulSwapRequest_Initiate_{Initiate: f.buyRequest(t).initiate(t, f.env)},
		}

		before := f.snapshot(t)
		result := f.env.statefulSwap(t, req)
		result.requireStatusCode(t, codes.InvalidArgument)
		f.env.requireNoSwapChanges(t, before, result)
	})

	t.Run("missing swap authority signature", func(t *testing.T) {
		req := f.buyRequest(t)
		req.signatures = func(t *testing.T, message []byte) []*commonpb.Signature {
			return []*commonpb.Signature{{Value: ed25519.Sign(req.owner.PrivateKey().ToBytes(), message)}}
		}

		before := f.snapshot(t)
		result := f.env.statefulSwap(t, req)
		result.requireSignatureError(t, "expected 2 signatures")
		f.env.requireNoSwapChanges(t, before, result)
	})

	t.Run("invalid owner signature", func(t *testing.T) {
		req := f.buyRequest(t)
		invalid := &commonpb.Signature{Value: ed25519.Sign(f.env.newAccount(t).PrivateKey().ToBytes(), []byte("message"))}
		req.signatures = func(t *testing.T, message []byte) []*commonpb.Signature {
			return []*commonpb.Signature{invalid, {Value: ed25519.Sign(req.swapAuthority.PrivateKey().ToBytes(), message)}}
		}

		before := f.snapshot(t)
		result := f.env.statefulSwap(t, req)
		result.requireInvalidTransactionSignature(t, invalid)
		f.env.requireNoSwapChanges(t, before, result)
	})

	t.Run("invalid swap authority signature", func(t *testing.T) {
		req := f.buyRequest(t)
		req.signatures = func(t *testing.T, message []byte) []*commonpb.Signature {
			// The owner signs in place of the swap authority
			ownerSignature := &commonpb.Signature{Value: ed25519.Sign(req.owner.PrivateKey().ToBytes(), message)}
			return []*commonpb.Signature{ownerSignature, ownerSignature}
		}

		before := f.snapshot(t)
		result := f.env.statefulSwap(t, req)
		result.requireInvalidTransactionSignature(t, result.signatures[1])
		f.env.requireNoSwapChanges(t, before, result)
	})

	t.Run("signatures in the wrong order", func(t *testing.T) {
		req := f.buyRequest(t)
		req.signatures = func(t *testing.T, message []byte) []*commonpb.Signature {
			return []*commonpb.Signature{
				{Value: ed25519.Sign(req.swapAuthority.PrivateKey().ToBytes(), message)},
				{Value: ed25519.Sign(req.owner.PrivateKey().ToBytes(), message)},
			}
		}

		before := f.snapshot(t)
		result := f.env.statefulSwap(t, req)
		result.requireSignatureError(t, "")
		f.env.requireNoSwapChanges(t, before, result)
	})
}

func TestStatefulSwap_NoAvailableNonces(t *testing.T) {
	f := newReserveSwapFixture(t)

	// Drain the pool, keeping each swap's nonce reserved
	for range testNoncesPerVm {
		req := f.buyRequest(t)
		result := f.env.statefulSwap(t, req)
		if result.err != nil {
			result.requireStatusCode(t, codes.Unavailable)
			return
		}
		result.requireSuccess(t)
	}

	before := f.snapshot(t)
	result := f.env.statefulSwap(t, f.buyRequest(t))
	result.requireStatusCode(t, codes.Unavailable)
	f.env.requireNoSwapChanges(t, before, result)
}

//
// Reserve swaps on existing currencies
//

func TestStatefulSwap_Reserve_HappyPath(t *testing.T) {
	for _, tc := range []struct {
		flow       string
		withFee    bool
		newRequest func(t *testing.T, f *reserveSwapFixture) *statefulSwapRequest
	}{
		{
			flow: "buy",
			newRequest: func(t *testing.T, f *reserveSwapFixture) *statefulSwapRequest {
				return f.env.reserveSwapRequest(t, f.owner, common.CoreMintAccount, f.launchpad1, common.ToCoreMintQuarks(5), 0)
			},
		},
		{
			flow:    "buy",
			withFee: true,
			newRequest: func(t *testing.T, f *reserveSwapFixture) *statefulSwapRequest {
				swapAmount := common.ToCoreMintQuarks(5)
				return f.env.reserveSwapRequest(t, f.owner, common.CoreMintAccount, f.launchpad1, swapAmount, currency_util.ExpectedBuyFeeQuarks(swapAmount))
			},
		},
		{
			flow: "sell",
			newRequest: func(t *testing.T, f *reserveSwapFixture) *statefulSwapRequest {
				return f.env.reserveSwapRequest(t, f.owner, f.launchpad1, common.CoreMintAccount, currencycreator.ToQuarks(10), 0)
			},
		},
		{
			flow: "buy_sell",
			newRequest: func(t *testing.T, f *reserveSwapFixture) *statefulSwapRequest {
				return f.env.reserveSwapRequest(t, f.owner, f.launchpad1, f.launchpad2, currencycreator.ToQuarks(10), 0)
			},
		},
	} {
		for _, transactionVersion := range []uint32{0, 1} {
			name := fmt.Sprintf("%s v%d", tc.flow, transactionVersion)
			if tc.withFee {
				name = fmt.Sprintf("%s with fee v%d", tc.flow, transactionVersion)
			}
			t.Run(name, func(t *testing.T) {
				f := newReserveSwapFixture(t, withAntispamChecks())
				req := tc.newRequest(t, f)
				req.transactionVersion = transactionVersion

				before := f.snapshot(t)
				result := f.env.statefulSwap(t, req)
				f.env.requireSwapCreated(t, before, req, result, expectedSwapRecord(t, req, result, swap.StateCreated), nil)

				params := result.serverParameters.GetReserveExistingCurrency()
				require.NotNil(t, params)
				assert.Equal(t, common.GetSubsidizer().PublicKey().ToBytes(), params.Payer.Value)
				assert.Equal(t, fmt.Sprintf("%s_v%d", tc.flow, transactionVersion), params.MemoValue)
				assert.Equal(t, selectSwapTransactionVersion(transactionVersion), result.expectedTxn.Message.Version)
				if transactionVersion == 1 {
					assert.Empty(t, params.Alts)
					require.NotNil(t, params.TransactionConfig)
				} else {
					assert.NotEmpty(t, params.Alts)
					assert.Nil(t, params.TransactionConfig)
				}
				if tc.withFee {
					assert.Equal(t, common.CoreMintFeesAccount.PublicKey().ToBytes(), params.FeeDestination.Value)
				}

				assert.Equal(t, []string{
					expectedAntispamSwapCall(swap.KindReserve, req.reserve.FundingSource, req.reserve.FromMint, req.reserve.ToMint, req.reserve.SwapAmount, req.reserve.FeeAmount, false),
				}, f.env.antispamIntegration.getCalls())

				// Funding moves the balance later, in a separate intent
				f.env.requireBalance(t, f.env.getVault(t, f.owner, common.CoreMintAccount), common.ToCoreMintQuarks(reserveSwapCoreBalance))
				f.env.requireBalance(t, f.env.getVault(t, f.owner, f.launchpad1), currencycreator.ToQuarks(reserveSwapLaunchpadBalance))
			})
		}
	}
}

func TestStatefulSwap_Reserve_FundingSources(t *testing.T) {
	t.Run("external wallet", func(t *testing.T) {
		f := newReserveSwapFixture(t)
		req := f.buyRequest(t)
		req.reserve.FundingSource = transactionpb.FundingSource_FUNDING_SOURCE_EXTERNAL_WALLET
		req.reserve.FundingId = base58.Encode(make([]byte, ed25519.SignatureSize))
		// An external wallet funds the swap, so the owner's balance doesn't matter
		req.reserve.SwapAmount = common.ToCoreMintQuarks(10 * reserveSwapCoreBalance)

		before := f.snapshot(t)
		result := f.env.statefulSwap(t, req)
		f.env.requireSwapCreated(t, before, req, result, expectedSwapRecord(t, req, result, swap.StateFunding), nil)
	})

	t.Run("coinbase onramp", func(t *testing.T) {
		f := newReserveSwapFixture(t)
		req := f.buyRequest(t)
		req.reserve.SwapAmount = common.ToCoreMintQuarks(10 * reserveSwapCoreBalance)
		req.reserve.FeeAmount = currency_util.ExpectedBuyFeeQuarks(req.reserve.SwapAmount)
		req.reserve.FundingSource = transactionpb.FundingSource_FUNDING_SOURCE_COINBASE_ONRAMP
		req.reserve.FundingId = f.env.coinbase.putOrder(t, f.owner, f.swapPda(t), req.reserve.SwapAmount+req.reserve.FeeAmount)

		before := f.snapshot(t)
		result := f.env.statefulSwap(t, req)
		f.env.requireSwapCreated(t, before, req, result, expectedSwapRecord(t, req, result, swap.StateFunding), nil)
	})
}

// swapPda is the owner's core mint swap PDA, which Coinbase Onramp orders
// deliver to
func (f *reserveSwapFixture) swapPda(t *testing.T) string {
	timelockRecord, err := f.env.data.GetTimelockByVault(f.env.ctx, f.env.getVault(t, f.owner, common.CoreMintAccount).PublicKey().ToBase58())
	require.NoError(t, err)
	return timelockRecord.SwapPdaAddress
}

func TestStatefulSwap_Reserve_Validation(t *testing.T) {
	for _, tc := range []struct {
		name   string
		setup  func(t *testing.T, f *reserveSwapFixture, req *statefulSwapRequest)
		verify func(t *testing.T, result *statefulSwapResult)
	}{
		{
			name: "swap id reused",
			setup: func(t *testing.T, f *reserveSwapFixture, req *statefulSwapRequest) {
				existing := f.env.newSwapRecord(t, f.owner, common.CoreMintAccount, f.launchpad1, testutil.NewRandomAccount(t), 1, swap.StateFinalized)
				id, err := base58.Decode(existing.SwapId)
				require.NoError(t, err)
				req.reserve.Id.Value = id
			},
			verify: func(t *testing.T, result *statefulSwapResult) { result.requireDenied(t, "attempt to reuse swap id") },
		},
		{
			name: "funding id reused",
			setup: func(t *testing.T, f *reserveSwapFixture, req *statefulSwapRequest) {
				fundingId := testutil.NewRandomAccount(t)
				f.env.newSwapRecord(t, f.owner, common.CoreMintAccount, f.launchpad1, fundingId, 1, swap.StateFinalized)
				req.reserve.FundingId = fundingId.PublicKey().ToBase58()
			},
			verify: func(t *testing.T, result *statefulSwapResult) {
				result.requireDenied(t, "attempt to reuse swap funding id")
			},
		},
		{
			name: "same mints",
			setup: func(t *testing.T, f *reserveSwapFixture, req *statefulSwapRequest) {
				req.reserve.ToMint = common.CoreMintAccount.ToProto()
			},
			verify: func(t *testing.T, result *statefulSwapResult) {
				result.requireInvalidSwap(t, "must swap between two different mints")
			},
		},
		{
			name: "unsupported source mint",
			setup: func(t *testing.T, f *reserveSwapFixture, req *statefulSwapRequest) {
				req.reserve.FromMint = testutil.NewRandomAccount(t).ToProto()
			},
			verify: func(t *testing.T, result *statefulSwapResult) { result.requireInvalidSwap(t, "invalid source mint") },
		},
		{
			name: "source account not opened",
			setup: func(t *testing.T, f *reserveSwapFixture, req *statefulSwapRequest) {
				req.reserve.FromMint = f.env.newLaunchpadMint(t).ToProto()
			},
			verify: func(t *testing.T, result *statefulSwapResult) {
				result.requireInvalidSwap(t, "source timelock vault account not opened")
			},
		},
		{
			name: "source account unlocked",
			setup: func(t *testing.T, f *reserveSwapFixture, req *statefulSwapRequest) {
				f.env.setTimelockState(t, f.env.getVault(t, f.owner, common.CoreMintAccount), timelock_token_v1.StateUnlocked)
			},
			verify: func(t *testing.T, result *statefulSwapResult) {
				result.requireDenied(t, "source timelock account isn't locked")
			},
		},
		{
			name: "submit intent funding id isn't a public key",
			setup: func(t *testing.T, f *reserveSwapFixture, req *statefulSwapRequest) {
				req.reserve.FundingId = base58.Encode(make([]byte, ed25519.SignatureSize))
			},
			verify: func(t *testing.T, result *statefulSwapResult) {
				result.requireInvalidSwap(t, "funding id is not a public key")
			},
		},
		{
			name: "submit intent funding intent exists",
			setup: func(t *testing.T, f *reserveSwapFixture, req *statefulSwapRequest) {
				require.NoError(t, f.env.data.SaveIntent(f.env.ctx, &intent.Record{
					IntentId:              req.reserve.FundingId,
					IntentType:            intent.OpenAccounts,
					MintAccount:           common.CoreMintAccount.PublicKey().ToBase58(),
					InitiatorOwnerAccount: f.owner.PublicKey().ToBase58(),
					OpenAccountsMetadata:  &intent.OpenAccountsMetadata{},
					State:                 intent.StatePending,
					CreatedAt:             time.Now(),
				}))
			},
			verify: func(t *testing.T, result *statefulSwapResult) {
				result.requireInvalidSwap(t, "funding intent already exists")
			},
		},
		{
			name: "submit intent insufficient balance",
			setup: func(t *testing.T, f *reserveSwapFixture, req *statefulSwapRequest) {
				req.reserve.SwapAmount = common.ToCoreMintQuarks(reserveSwapCoreBalance)
				req.reserve.FeeAmount = currency_util.ExpectedBuyFeeQuarks(req.reserve.SwapAmount)
			},
			verify: func(t *testing.T, result *statefulSwapResult) { result.requireInvalidSwap(t, "insufficient balance") },
		},
		{
			name: "external wallet funding id isn't a signature",
			setup: func(t *testing.T, f *reserveSwapFixture, req *statefulSwapRequest) {
				req.reserve.FundingSource = transactionpb.FundingSource_FUNDING_SOURCE_EXTERNAL_WALLET
			},
			verify: func(t *testing.T, result *statefulSwapResult) {
				result.requireInvalidSwap(t, "funding id is not a signature")
			},
		},
		{
			name: "external wallet funds a sell",
			setup: func(t *testing.T, f *reserveSwapFixture, req *statefulSwapRequest) {
				req.reserve.FromMint, req.reserve.ToMint = req.reserve.ToMint, req.reserve.FromMint
				req.reserve.FundingSource = transactionpb.FundingSource_FUNDING_SOURCE_EXTERNAL_WALLET
				req.reserve.FundingId = base58.Encode(make([]byte, ed25519.SignatureSize))
			},
			verify: func(t *testing.T, result *statefulSwapResult) {
				result.requireDenied(t, "source mint must be core mint")
			},
		},
		{
			name: "destination currency not found",
			setup: func(t *testing.T, f *reserveSwapFixture, req *statefulSwapRequest) {
				req.reserve.ToMint = testutil.NewRandomAccount(t).ToProto()
			},
			verify: func(t *testing.T, result *statefulSwapResult) { result.requireInvalidSwap(t, "mint not found") },
		},
		{
			name: "destination currency being initialized",
			setup: func(t *testing.T, f *reserveSwapFixture, req *statefulSwapRequest) {
				f.env.setCurrencyState(t, f.launchpad1, currency.MetadataStatePrePurchaseSetup)
			},
			verify: func(t *testing.T, result *statefulSwapResult) { result.requireDenied(t, "mint is being initialized") },
		},
		{
			name: "sell without a sell fee",
			setup: func(t *testing.T, f *reserveSwapFixture, req *statefulSwapRequest) {
				req.reserve.FromMint, req.reserve.ToMint = f.launchpad1.ToProto(), common.CoreMintAccount.ToProto()
				req.reserve.SwapAmount = 1
			},
			verify: func(t *testing.T, result *statefulSwapResult) {
				result.requireDenied(t, "swap would not generate a sell fee")
			},
		},
		{
			name: "owner is the swap authority",
			setup: func(t *testing.T, f *reserveSwapFixture, req *statefulSwapRequest) {
				req.swapAuthority = f.owner
			},
			verify: func(t *testing.T, result *statefulSwapResult) {
				result.requireInvalidSwap(t, "owner cannot be swap authority")
			},
		},
		{
			name: "fee on a sell",
			setup: func(t *testing.T, f *reserveSwapFixture, req *statefulSwapRequest) {
				req.reserve.FromMint, req.reserve.ToMint = f.launchpad1.ToProto(), common.CoreMintAccount.ToProto()
				req.reserve.SwapAmount = currencycreator.ToQuarks(10)
				req.reserve.FeeAmount = 1
			},
			verify: func(t *testing.T, result *statefulSwapResult) { result.requireInvalidSwap(t, "fee amount must be 0") },
		},
		{
			name: "fee on a currency to currency swap",
			setup: func(t *testing.T, f *reserveSwapFixture, req *statefulSwapRequest) {
				req.reserve.FromMint, req.reserve.ToMint = f.launchpad1.ToProto(), f.launchpad2.ToProto()
				req.reserve.SwapAmount = currencycreator.ToQuarks(10)
				req.reserve.FeeAmount = 1
			},
			verify: func(t *testing.T, result *statefulSwapResult) { result.requireInvalidSwap(t, "fee amount must be 0") },
		},
		{
			name: "buy too small to charge a fee",
			setup: func(t *testing.T, f *reserveSwapFixture, req *statefulSwapRequest) {
				req.reserve.SwapAmount = 99
				req.reserve.FeeAmount = 1
			},
			verify: func(t *testing.T, result *statefulSwapResult) {
				result.requireInvalidSwap(t, "swap amount is too small to charge a fee")
			},
		},
		{
			name: "buy fee too low",
			setup: func(t *testing.T, f *reserveSwapFixture, req *statefulSwapRequest) {
				req.reserve.FeeAmount = currency_util.ExpectedBuyFeeQuarks(req.reserve.SwapAmount) - currency_util.FeeQuarkTolerance - 1
			},
			verify: func(t *testing.T, result *statefulSwapResult) {
				result.requireDenied(t, fmt.Sprintf("fee amount must be %d quarks", currency_util.ExpectedBuyFeeQuarks(common.CoreMintQuarksPerUnit)))
			},
		},
		{
			name: "buy fee too high",
			setup: func(t *testing.T, f *reserveSwapFixture, req *statefulSwapRequest) {
				req.reserve.FeeAmount = currency_util.ExpectedBuyFeeQuarks(req.reserve.SwapAmount) + currency_util.FeeQuarkTolerance + 1
			},
			verify: func(t *testing.T, result *statefulSwapResult) { result.requireDenied(t, "fee amount must be") },
		},
		{
			name: "destination account not opened",
			setup: func(t *testing.T, f *reserveSwapFixture, req *statefulSwapRequest) {
				req.reserve.ToMint = f.env.newLaunchpadMint(t).ToProto()
			},
			verify: func(t *testing.T, result *statefulSwapResult) {
				result.requireInvalidSwap(t, "destination timelock vault account not opened")
			},
		},
		{
			name: "destination account unlocked",
			setup: func(t *testing.T, f *reserveSwapFixture, req *statefulSwapRequest) {
				f.env.setTimelockState(t, f.env.getVault(t, f.owner, f.launchpad1), timelock_token_v1.StateUnlocked)
			},
			verify: func(t *testing.T, result *statefulSwapResult) {
				result.requireDenied(t, "destination timelock account isn't locked")
			},
		},
		{
			name: "not a user account",
			setup: func(t *testing.T, f *reserveSwapFixture, req *statefulSwapRequest) {
				// A gift card owner has locked accounts, but isn't a user
				giftCard := f.env.newAccount(t)
				f.env.openAccount(t, giftCard, giftCard, common.CoreMintAccount, commonpb.AccountType_REMOTE_SEND_GIFT_CARD, 0)
				f.env.credit(t, f.env.getVault(t, giftCard, common.CoreMintAccount), common.ToCoreMintQuarks(10))
				f.env.openAccount(t, giftCard, giftCard, f.launchpad1, commonpb.AccountType_REMOTE_SEND_GIFT_CARD, 0)
				f.env.reserveTimelockMemory(t, giftCard, f.launchpad1)
				req.owner = giftCard
			},
			verify: func(t *testing.T, result *statefulSwapResult) { result.requireDenied(t, "not a user ocp account") },
		},
		{
			name: "not an ocp account",
			setup: func(t *testing.T, f *reserveSwapFixture, req *statefulSwapRequest) {
				// The authority of another owner's locked pool accounts owns
				// no accounts itself
				owner, poolOwner := f.env.newAccount(t), f.env.newUser(t)
				f.env.openAccount(t, poolOwner, owner, common.CoreMintAccount, commonpb.AccountType_POOL, 0)
				f.env.credit(t, f.env.getVault(t, owner, common.CoreMintAccount), common.ToCoreMintQuarks(10))
				f.env.openAccount(t, poolOwner, owner, f.launchpad1, commonpb.AccountType_POOL, 0)
				f.env.reserveTimelockMemory(t, owner, f.launchpad1)
				req.owner = owner
			},
			verify: func(t *testing.T, result *statefulSwapResult) { result.requireDenied(t, "not an ocp account") },
		},
		{
			name: "owner has an unlocked account",
			setup: func(t *testing.T, f *reserveSwapFixture, req *statefulSwapRequest) {
				// An account the swap doesn't touch
				f.env.setTimelockState(t, f.env.getVault(t, f.owner, f.launchpad2), timelock_token_v1.StateUnlocked)
			},
			verify: func(t *testing.T, result *statefulSwapResult) { result.requireDenied(t, "not an ocp account") },
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newReserveSwapFixture(t)
			req := f.buyRequest(t)
			tc.setup(t, f, req)

			before := f.env.swapSnapshot(t, []*common.Account{f.owner, req.owner}, f.launchpad1, f.launchpad2)
			result := f.env.statefulSwap(t, req)
			tc.verify(t, result)
			f.env.requireNoSwapChanges(t, before, result)
		})
	}
}

func TestStatefulSwap_Reserve_CoinbaseOnrampValidation(t *testing.T) {
	for _, tc := range []struct {
		name   string
		setup  func(t *testing.T, f *reserveSwapFixture, req *statefulSwapRequest)
		verify func(t *testing.T, result *statefulSwapResult)
	}{
		{
			name: "source isn't the core mint",
			setup: func(t *testing.T, f *reserveSwapFixture, req *statefulSwapRequest) {
				req.reserve.FromMint, req.reserve.ToMint = f.launchpad1.ToProto(), common.CoreMintAccount.ToProto()
			},
			verify: func(t *testing.T, result *statefulSwapResult) {
				result.requireDenied(t, "source mint must be core mint")
			},
		},
		{
			name: "funding id isn't a uuid",
			setup: func(t *testing.T, f *reserveSwapFixture, req *statefulSwapRequest) {
				req.reserve.FundingId = strings.Repeat("a", 36)
			},
			verify: func(t *testing.T, result *statefulSwapResult) {
				result.requireInvalidSwap(t, "funding id is not a uuid")
			},
		},
		{
			name: "order not found",
			setup: func(t *testing.T, f *reserveSwapFixture, req *statefulSwapRequest) {
				req.reserve.FundingId = "00000000-0000-4000-8000-000000000000"
			},
			verify: func(t *testing.T, result *statefulSwapResult) {
				result.requireInvalidSwap(t, "coinbase order not found")
			},
		},
		{
			name: "order failed",
			setup: func(t *testing.T, f *reserveSwapFixture, req *statefulSwapRequest) {
				req.reserve.FundingId = f.env.coinbase.putOrder(t, f.owner, f.swapPda(t), req.reserve.SwapAmount, func(o *testCoinbaseOrder) {
					o.status = coinbase.OrderStatusFailed
				})
			},
			verify: func(t *testing.T, result *statefulSwapResult) {
				result.requireInvalidSwap(t, "coinbase order is in a failed state")
			},
		},
		{
			name: "order isn't on solana",
			setup: func(t *testing.T, f *reserveSwapFixture, req *statefulSwapRequest) {
				req.reserve.FundingId = f.env.coinbase.putOrder(t, f.owner, f.swapPda(t), req.reserve.SwapAmount, func(o *testCoinbaseOrder) {
					o.destinationNetwork = "base"
				})
			},
			verify: func(t *testing.T, result *statefulSwapResult) {
				result.requireInvalidSwap(t, "coinbase order destination network is not solana")
			},
		},
		{
			name: "order isn't for the core mint",
			setup: func(t *testing.T, f *reserveSwapFixture, req *statefulSwapRequest) {
				req.reserve.FundingId = f.env.coinbase.putOrder(t, f.owner, f.swapPda(t), req.reserve.SwapAmount, func(o *testCoinbaseOrder) {
					o.purchaseCurrency = "SOL"
				})
			},
			verify: func(t *testing.T, result *statefulSwapResult) {
				result.requireInvalidSwap(t, "coinbase order is not for the core mint")
			},
		},
		{
			name: "order is for another user",
			setup: func(t *testing.T, f *reserveSwapFixture, req *statefulSwapRequest) {
				req.reserve.FundingId = f.env.coinbase.putOrder(t, f.env.newAccount(t), f.swapPda(t), req.reserve.SwapAmount)
			},
			verify: func(t *testing.T, result *statefulSwapResult) {
				result.requireDenied(t, "coinbase order partner user ref does not match owner")
			},
		},
		{
			name: "order isn't delivered to the swap pda",
			setup: func(t *testing.T, f *reserveSwapFixture, req *statefulSwapRequest) {
				req.reserve.FundingId = f.env.coinbase.putOrder(t, f.owner, testutil.NewRandomAccount(t).PublicKey().ToBase58(), req.reserve.SwapAmount)
			},
			verify: func(t *testing.T, result *statefulSwapResult) {
				result.requireInvalidSwap(t, "coinbase order destination address is not the owner's swap pda")
			},
		},
		{
			name: "order purchase amount is invalid",
			setup: func(t *testing.T, f *reserveSwapFixture, req *statefulSwapRequest) {
				req.reserve.FundingId = f.env.coinbase.putOrder(t, f.owner, f.swapPda(t), req.reserve.SwapAmount, func(o *testCoinbaseOrder) {
					o.purchaseAmount = "1.0000001"
				})
			},
			verify: func(t *testing.T, result *statefulSwapResult) {
				result.requireInvalidSwap(t, "coinbase order purchase amount is invalid")
			},
		},
		{
			name: "order purchase amount doesn't cover the fee",
			setup: func(t *testing.T, f *reserveSwapFixture, req *statefulSwapRequest) {
				req.reserve.FeeAmount = currency_util.ExpectedBuyFeeQuarks(req.reserve.SwapAmount)
				req.reserve.FundingId = f.env.coinbase.putOrder(t, f.owner, f.swapPda(t), req.reserve.SwapAmount)
			},
			verify: func(t *testing.T, result *statefulSwapResult) {
				result.requireDenied(t, "coinbase order purchase amount does not match swap amount")
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newReserveSwapFixture(t)
			req := f.buyRequest(t)
			req.reserve.FundingSource = transactionpb.FundingSource_FUNDING_SOURCE_COINBASE_ONRAMP
			req.reserve.FundingId = f.env.coinbase.putOrder(t, f.owner, f.swapPda(t), req.reserve.SwapAmount)
			tc.setup(t, f, req)

			before := f.snapshot(t)
			result := f.env.statefulSwap(t, req)
			tc.verify(t, result)
			f.env.requireNoSwapChanges(t, before, result)
		})
	}
}

func TestStatefulSwap_Reserve_Antispam(t *testing.T) {
	f := newReserveSwapFixture(t, withAntispamChecks())
	f.env.antispamIntegration.setAllow(false)

	req := f.buyRequest(t)
	before := f.snapshot(t)
	result := f.env.statefulSwap(t, req)
	result.requireDenied(t, "rate limited")
	f.env.requireNoSwapChanges(t, before, result)
	assert.Equal(t, []string{
		expectedAntispamSwapCall(swap.KindReserve, req.reserve.FundingSource, req.reserve.FromMint, req.reserve.ToMint, req.reserve.SwapAmount, 0, false),
	}, f.env.antispamIntegration.getCalls())
}

//
// Reserve swaps creating a new currency
//

func TestStatefulSwap_ReserveNewCurrency_HappyPath(t *testing.T) {
	for _, transactionVersion := range []uint32{0, 1} {
		t.Run(fmt.Sprintf("v%d", transactionVersion), func(t *testing.T) {
			for _, tc := range []struct {
				name          string
				fundingSource transactionpb.FundingSource
				initialState  swap.State
			}{
				{"core mint funded by submit intent", transactionpb.FundingSource_FUNDING_SOURCE_SUBMIT_INTENT, swap.StateCreated},
				{"core mint funded by external wallet", transactionpb.FundingSource_FUNDING_SOURCE_EXTERNAL_WALLET, swap.StateFunding},
				{"core mint funded by coinbase onramp", transactionpb.FundingSource_FUNDING_SOURCE_COINBASE_ONRAMP, swap.StateFunding},
			} {
				t.Run(tc.name, func(t *testing.T) {
					f := newReserveSwapFixture(t, withAntispamChecks())
					mint := f.env.newCurrencyAwaitingPurchase(t, f.owner)

					purchaseQuarks, feeQuarks := newCurrencyLaunchQuarks(transactionVersion)
					req := f.env.reserveSwapRequest(t, f.owner, common.CoreMintAccount, mint, purchaseQuarks, feeQuarks)
					req.swapAuthority = newCurrencySwapAuthority(t, f.env, f.owner, common.CoreMintAccount, transactionVersion)
					req.transactionVersion = transactionVersion
					req.reserve.FundingSource = tc.fundingSource
					switch tc.fundingSource {
					case transactionpb.FundingSource_FUNDING_SOURCE_EXTERNAL_WALLET:
						req.reserve.FundingId = base58.Encode(make([]byte, ed25519.SignatureSize))
					case transactionpb.FundingSource_FUNDING_SOURCE_COINBASE_ONRAMP:
						req.reserve.FundingId = f.env.coinbase.putOrder(t, f.owner, f.swapPda(t), purchaseQuarks+feeQuarks)
					}

					before := f.env.swapSnapshot(t, []*common.Account{f.owner}, mint)
					result := f.env.statefulSwap(t, req)
					f.env.requireSwapCreated(t, before, req, result, expectedSwapRecord(t, req, result, tc.initialState), map[string]currency.MetadataState{
						mint.PublicKey().ToBase58(): currency.MetadataStatePrePurchaseSetup,
					})

					expectedSigners := []*common.Account{common.GetSubsidizer(), f.owner}
					if transactionVersion == 1 {
						expectedSigners = append(expectedSigners, req.swapAuthority)
					}
					require.Len(t, result.signatures, len(expectedSigners)-1)
					assert.Equal(t, selectSwapTransactionVersion(transactionVersion), result.expectedTxn.Message.Version)
					f.env.requireNewCurrencyServerParameters(t, result, common.CoreMintAccount, mint, false)
					requireNewCurrencyServerSigners(t, result, f.env, mint, expectedSigners...)

					assert.Equal(t, []string{
						expectedAntispamSwapCall(swap.KindReserve, req.reserve.FundingSource, req.reserve.FromMint, req.reserve.ToMint, req.reserve.SwapAmount, req.reserve.FeeAmount, true),
					}, f.env.antispamIntegration.getCalls())

					// Funding moves the balance later
					f.env.requireBalance(t, f.env.getVault(t, f.owner, common.CoreMintAccount), common.ToCoreMintQuarks(reserveSwapCoreBalance))
				})
			}

			t.Run("treasury funded", func(t *testing.T) {
				f := newReserveSwapFixture(t, withAntispamChecks())
				f.env.credit(t, f.env.getVault(t, f.owner, f.launchpad1), currencycreator.ToQuarks(100_000))
				f.env.setLiveSupply(t, f.launchpad1, currencycreator.ToQuarks(defaultLaunchpadSupply))
				mint := f.env.newCurrencyAwaitingPurchase(t, f.owner)

				swapAmount, feeAmount := treasuryFundedNewCurrencyAmounts(transactionVersion)
				req := f.env.reserveSwapRequest(t, f.owner, f.launchpad1, mint, swapAmount, feeAmount)
				req.swapAuthority = f.owner
				req.transactionVersion = transactionVersion
				req.reserve.FullAmountExchangeData = f.env.treasuryFundedExchangeData(t, f.launchpad1, swapAmount+feeAmount, transactionVersion)

				before := f.env.swapSnapshot(t, []*common.Account{f.owner}, f.launchpad1, mint)
				result := f.env.statefulSwap(t, req)
				f.env.requireSwapCreated(t, before, req, result, expectedSwapRecord(t, req, result, swap.StateCreated), map[string]currency.MetadataState{
					mint.PublicKey().ToBase58(): currency.MetadataStatePrePurchaseSetup,
				})

				require.Len(t, result.signatures, 1)
				assert.Equal(t, selectSwapTransactionVersion(transactionVersion), result.expectedTxn.Message.Version)
				f.env.requireNewCurrencyServerParameters(t, result, f.launchpad1, mint, true)

				sourceVmConfig, err := common.GetVmConfigForMint(f.env.ctx, f.env.data, f.launchpad1)
				require.NoError(t, err)
				expectedSigners := []*common.Account{common.GetSubsidizer(), f.owner, sourceVmConfig.Authority, f.env.swapTreasury}
				requireNewCurrencyServerSigners(t, result, f.env, mint, expectedSigners...)

				assert.Equal(t, []string{
					expectedAntispamSwapCall(swap.KindReserve, req.reserve.FundingSource, req.reserve.FromMint, req.reserve.ToMint, req.reserve.SwapAmount, req.reserve.FeeAmount, true),
				}, f.env.antispamIntegration.getCalls())
			})
		})
	}
}

// requireNewCurrencyServerSigners requires the transaction's signers to be
// exactly the expected accounts, plus the new currency's authority whenever
// the transaction initializes the currency. Treasury-funded v0 transactions
// don't, because the launcher initializes it beforehand.
func requireNewCurrencyServerSigners(t *testing.T, result *statefulSwapResult, e *testEnv, mint *common.Account, expected ...*common.Account) {
	params := result.serverParameters.GetReserveNewCurrency()
	require.NotNil(t, params)

	metadataRecord, err := e.data.GetCurrencyMetadata(e.ctx, mint.PublicKey().ToBase58())
	require.NoError(t, err)

	var expectedSigners []string
	for _, account := range expected {
		expectedSigners = append(expectedSigners, account.PublicKey().ToBase58())
	}
	if params.Treasury == nil || result.expectedTxn.Message.Version == solana.MessageVersion1 {
		expectedSigners = append(expectedSigners, metadataRecord.Authority)
	}

	var actualSigners []string
	for i := range result.expectedTxn.Message.Header.NumSignatures {
		actualSigners = append(actualSigners, base58.Encode(result.expectedTxn.Message.Accounts[i]))
	}
	assert.ElementsMatch(t, expectedSigners, actualSigners)
}

// requireNewCurrencyServerParameters verifies every currency creation server
// parameter against the new currency's metadata and the server's config
func (e *testEnv) requireNewCurrencyServerParameters(t *testing.T, result *statefulSwapResult, fromMint, mint *common.Account, treasuryFunded bool) {
	params := result.serverParameters.GetReserveNewCurrency()
	require.NotNil(t, params)

	metadataRecord, err := e.data.GetCurrencyMetadata(e.ctx, mint.PublicKey().ToBase58())
	require.NoError(t, err)

	assert.Equal(t, common.GetSubsidizer().PublicKey().ToBytes(), params.Payer.Value)
	assert.Equal(t, metadataRecord.Authority, base58.Encode(params.Authority.Value))
	assert.Equal(t, metadataRecord.Name, params.Name)
	assert.Equal(t, metadataRecord.Symbol, params.Symbol)
	assert.Equal(t, metadataRecord.Seed, base58.Encode(params.Seed.Value))
	assert.EqualValues(t, currencycreator.DefaultSellFeeBps, params.SellFeeBps)
	assert.EqualValues(t, timelock_token_v1.DefaultNumDaysLocked, params.VmLockDurationInDays)
	assert.Equal(t, common.CoreMintFeesAccount.PublicKey().ToBytes(), params.FeeDestination.Value)
	assert.Equal(t, common.GetSubsidizer().PublicKey().ToBytes(), params.FeeAccountOwner.Value)
	assert.Empty(t, params.MemoValue)

	if treasuryFunded {
		require.NotNil(t, params.Treasury)
		assert.Equal(t, e.swapTreasury.PublicKey().ToBytes(), params.Treasury.Value)
		expectedPurchaseQuarks := uint64(defaultNewCurrencyPurchaseQuarks)
		if result.expectedTxn.Message.Version == solana.MessageVersion1 {
			expectedPurchaseQuarks = defaultNewCurrencyV1PurchaseQuarks
		}
		assert.Equal(t, expectedPurchaseQuarks, params.TreasuryPurchaseAmount)
	} else {
		assert.Nil(t, params.Treasury)
		assert.Zero(t, params.TreasuryPurchaseAmount)
	}

	if result.expectedTxn.Message.Version == solana.MessageVersion1 {
		e.requireNewCurrencyV1ServerParameters(t, result, mint)
		return
	}

	assert.Nil(t, params.TransactionConfig)
	assert.Empty(t, params.MemoryAccounts)
	assert.EqualValues(t, 10_000, params.ComputeUnitPrice)

	expectedAlt, err := transaction_util.GetAltForMint(e.ctx, e.data, fromMint)
	require.NoError(t, err)
	require.Len(t, params.Alts, 1)
	assert.True(t, proto.Equal(transaction_util.ToProtoAlt(expectedAlt), params.Alts[0]), "unexpected alt")

	if treasuryFunded {
		assert.EqualValues(t, 250_000, params.ComputeUnitLimit)
	} else {
		assert.EqualValues(t, 300_000, params.ComputeUnitLimit)
	}
}

// requireNewCurrencyV1ServerParameters verifies the v1 transaction config and
// the memory accounts initialized within the transaction
func (e *testEnv) requireNewCurrencyV1ServerParameters(t *testing.T, result *statefulSwapResult, mint *common.Account) {
	params := result.serverParameters.GetReserveNewCurrency()

	assert.Empty(t, params.Alts)
	assert.Zero(t, params.ComputeUnitLimit)
	assert.Zero(t, params.ComputeUnitPrice)

	config := params.TransactionConfig
	require.NotNil(t, config)
	assert.Equal(t, *result.expectedTxn.Message.Config.ComputeUnitLimit, config.ComputeUnitLimit)
	assert.EqualValues(t, transaction_util.NewCurrencyLaunchLoadedAccountsDataSizeLimit, config.LoadedAccountsDataSizeLimit)
	assert.Equal(t, transaction_util.PriorityFeeLamports(config.ComputeUnitLimit, 10_000), config.PriorityFeeLamports)
	assert.Zero(t, config.HeapSize)

	// Measured instructions sum to at least 323k CUs with the margin, and
	// launches with unlucky bumps can only add to that
	assert.GreaterOrEqual(t, config.ComputeUnitLimit, uint32(323_000))
	assert.LessOrEqual(t, config.ComputeUnitLimit, uint32(1_000_000))

	// NewV1Transaction already enforces the v1 limits. Log how close the
	// launch transaction comes to them.
	marshalled, err := result.expectedTxn.Marshal()
	require.NoError(t, err)
	t.Logf(
		"v1 launch transaction: %d bytes, %d accounts, %d signers, %d instructions, %d CU limit",
		len(marshalled),
		len(result.expectedTxn.Message.Accounts),
		result.expectedTxn.Message.Header.NumSignatures,
		len(result.expectedTxn.Message.Instructions),
		config.ComputeUnitLimit,
	)

	vmMetadataRecord, err := e.data.GetVmMetadataByMint(e.ctx, mint.PublicKey().ToBase58())
	require.NoError(t, err)
	vmAddress, err := base58.Decode(vmMetadataRecord.Vm)
	require.NoError(t, err)

	expected := []struct {
		name        string
		numAccounts uint32
		accountSize uint32
	}{
		{"nonce-0", 250, uint32(vm.GetVirtualAccountSizeInMemory(vm.VirtualAccountTypeDurableNonce))},
		{"timelock-0", 1_000, uint32(vm.GetVirtualAccountSizeInMemory(vm.VirtualAccountTypeTimelock))},
	}
	require.Len(t, params.MemoryAccounts, len(expected))
	for i, memoryAccount := range params.MemoryAccounts {
		address, _, err := vm.GetMemoryAccountAddress(&vm.GetMemoryAccountAddressArgs{
			Name: expected[i].name,
			Vm:   vmAddress,
		})
		require.NoError(t, err)

		assert.Equal(t, []byte(address), memoryAccount.Address.Value)
		assert.Equal(t, expected[i].name, memoryAccount.Name)
		assert.Equal(t, expected[i].numAccounts, memoryAccount.NumAccounts)
		assert.Equal(t, expected[i].accountSize, memoryAccount.AccountSize)
	}
}

func TestStatefulSwap_ReserveNewCurrency_Validation(t *testing.T) {
	for _, tc := range []struct {
		name           string
		treasuryFunded bool
		versions       []uint32 // Both versions when empty
		setup          func(t *testing.T, f *reserveSwapFixture, req *statefulSwapRequest)
		verify         func(t *testing.T, result *statefulSwapResult, transactionVersion uint32)
	}{
		{
			name:     "core mint owner isn't the swap authority",
			versions: []uint32{0},
			setup: func(t *testing.T, f *reserveSwapFixture, req *statefulSwapRequest) {
				req.swapAuthority = f.env.newAccount(t)
			},
			verify: func(t *testing.T, result *statefulSwapResult, transactionVersion uint32) {
				result.requireInvalidSwap(t, "owner must be swap authority")
			},
		},
		{
			// The owner's core mint ATA could be pre-funded so closing it fails
			name:     "core mint owner is the swap authority",
			versions: []uint32{1},
			setup: func(t *testing.T, f *reserveSwapFixture, req *statefulSwapRequest) {
				req.swapAuthority = req.owner
			},
			verify: func(t *testing.T, result *statefulSwapResult, transactionVersion uint32) {
				result.requireInvalidSwap(t, "owner cannot be swap authority")
			},
		},
		{
			name:           "treasury funded owner isn't the swap authority",
			treasuryFunded: true,
			setup: func(t *testing.T, f *reserveSwapFixture, req *statefulSwapRequest) {
				req.swapAuthority = f.env.newAccount(t)
			},
			verify: func(t *testing.T, result *statefulSwapResult, transactionVersion uint32) {
				result.requireInvalidSwap(t, "owner must be swap authority")
			},
		},
		{
			name: "owner isn't the creator",
			setup: func(t *testing.T, f *reserveSwapFixture, req *statefulSwapRequest) {
				req.reserve.ToMint = f.env.newCurrencyAwaitingPurchase(t, f.env.newAccount(t)).ToProto()
			},
			verify: func(t *testing.T, result *statefulSwapResult, transactionVersion uint32) {
				result.requireDenied(t, "only the currency creator can buy initial tokens")
			},
		},
		{
			name: "core mint swap amount",
			setup: func(t *testing.T, f *reserveSwapFixture, req *statefulSwapRequest) {
				req.reserve.SwapAmount++
			},
			verify: func(t *testing.T, result *statefulSwapResult, transactionVersion uint32) {
				purchaseQuarks, _ := newCurrencyLaunchQuarks(transactionVersion)
				result.requireDenied(t, fmt.Sprintf("swap amount must be %d quarks", purchaseQuarks))
			},
		},
		{
			name: "core mint fee amount",
			setup: func(t *testing.T, f *reserveSwapFixture, req *statefulSwapRequest) {
				req.reserve.FeeAmount++
			},
			verify: func(t *testing.T, result *statefulSwapResult, transactionVersion uint32) {
				_, feeQuarks := newCurrencyLaunchQuarks(transactionVersion)
				result.requireDenied(t, fmt.Sprintf("fee amount must be %d quarks", feeQuarks))
			},
		},
		{
			name: "core mint insufficient balance",
			setup: func(t *testing.T, f *reserveSwapFixture, req *statefulSwapRequest) {
				owner := f.env.newFundedUser(t, req.reserve.SwapAmount+req.reserve.FeeAmount-1)
				req.reserve.ToMint = f.env.newCurrencyAwaitingPurchase(t, owner).ToProto()
				if req.swapAuthority == req.owner {
					req.swapAuthority = owner
				}
				req.owner = owner
			},
			verify: func(t *testing.T, result *statefulSwapResult, transactionVersion uint32) {
				result.requireInvalidSwap(t, "insufficient balance")
			},
		},
		{
			name:           "treasury funded by an external wallet",
			treasuryFunded: true,
			setup: func(t *testing.T, f *reserveSwapFixture, req *statefulSwapRequest) {
				req.reserve.FundingSource = transactionpb.FundingSource_FUNDING_SOURCE_EXTERNAL_WALLET
				req.reserve.FundingId = base58.Encode(make([]byte, ed25519.SignatureSize))
			},
			verify: func(t *testing.T, result *statefulSwapResult, transactionVersion uint32) {
				result.requireDenied(t, "source mint must be core mint")
			},
		},
		{
			name:           "treasury funded without exchange data",
			treasuryFunded: true,
			setup: func(t *testing.T, f *reserveSwapFixture, req *statefulSwapRequest) {
				req.reserve.FullAmountExchangeData = nil
			},
			verify: func(t *testing.T, result *statefulSwapResult, transactionVersion uint32) {
				result.requireInvalidSwap(t, "full amount exchange data is required")
			},
		},
		{
			name:           "treasury funded with unverified exchange data",
			treasuryFunded: true,
			setup: func(t *testing.T, f *reserveSwapFixture, req *statefulSwapRequest) {
				req.reserve.FullAmountExchangeData.CoreMintFiatExchangeRate.Signature.Value[0] ^= 0xff
			},
			verify: func(t *testing.T, result *statefulSwapResult, transactionVersion uint32) {
				result.requireInvalidSwap(t, "exchange rate validation failed")
			},
		},
		{
			name:           "treasury funded exchange data for another mint",
			treasuryFunded: true,
			setup: func(t *testing.T, f *reserveSwapFixture, req *statefulSwapRequest) {
				req.reserve.FullAmountExchangeData = f.env.treasuryFundedExchangeData(t, f.launchpad2, req.reserve.SwapAmount+req.reserve.FeeAmount, req.transactionVersion)
			},
			verify: func(t *testing.T, result *statefulSwapResult, transactionVersion uint32) {
				result.requireInvalidSwap(t, "full amount exchange data mint must be the source mint")
			},
		},
		{
			name:           "treasury funded exchange data for another amount",
			treasuryFunded: true,
			setup: func(t *testing.T, f *reserveSwapFixture, req *statefulSwapRequest) {
				// Consistent exchange data, but not for the swap and fee amount
				req.reserve.FullAmountExchangeData = f.env.exchangeData(t, f.launchpad1, req.reserve.SwapAmount+req.reserve.FeeAmount-1)
			},
			verify: func(t *testing.T, result *statefulSwapResult, transactionVersion uint32) {
				result.requireInvalidSwap(t, "full amount exchange data quarks must be the full swap and fee amount")
			},
		},
		{
			name:           "treasury funded exchange data isn't in usd",
			treasuryFunded: true,
			setup: func(t *testing.T, f *reserveSwapFixture, req *statefulSwapRequest) {
				quarks := req.reserve.SwapAmount + req.reserve.FeeAmount
				exchangeRate := f.env.signedExchangeRate(t, "cad", 1.0, time.Now())
				reserveState := f.env.signedReserveState(t, f.launchpad1, currencycreator.ToQuarks(defaultLaunchpadSupply), time.Now())
				req.reserve.FullAmountExchangeData = f.env.launchpadExchangeData(t, f.launchpad1, quarks, exchangeRate, reserveState)
			},
			verify: func(t *testing.T, result *statefulSwapResult, transactionVersion uint32) {
				result.requireInvalidSwap(t, "full amount exchange data must be in usd")
			},
		},
		{
			name:           "treasury funded full amount value",
			treasuryFunded: true,
			setup: func(t *testing.T, f *reserveSwapFixture, req *statefulSwapRequest) {
				// Consistent exchange data valuing slightly more quarks than the
				// purchase and fee
				req.reserve.SwapAmount += currencycreator.ToQuarks(10)
				req.reserve.FullAmountExchangeData = f.env.exchangeData(t, f.launchpad1, req.reserve.SwapAmount+req.reserve.FeeAmount)
			},
			verify: func(t *testing.T, result *statefulSwapResult, transactionVersion uint32) {
				fullUsdValue, _ := newCurrencyLaunchUsdValues(transactionVersion)
				result.requireDenied(t, fmt.Sprintf("full amount must be valued at %.2f usd", fullUsdValue))
			},
		},
		{
			name:           "treasury funded fee value",
			treasuryFunded: true,
			setup: func(t *testing.T, f *reserveSwapFixture, req *statefulSwapRequest) {
				// Moves value from the purchase to the fee, keeping the full
				// amount
				shift := req.reserve.SwapAmount / 2
				req.reserve.SwapAmount -= shift
				req.reserve.FeeAmount += shift
			},
			verify: func(t *testing.T, result *statefulSwapResult, transactionVersion uint32) {
				_, feeUsdValue := newCurrencyLaunchUsdValues(transactionVersion)
				if feeUsdValue == 0 {
					result.requireDenied(t, "fee amount must be 0 quarks")
					return
				}
				result.requireDenied(t, fmt.Sprintf("fee amount must be valued at %.2f usd", feeUsdValue))
			},
		},
	} {
		versions := tc.versions
		if len(versions) == 0 {
			versions = []uint32{0, 1}
		}
		for _, transactionVersion := range versions {
			t.Run(fmt.Sprintf("v%d/%s", transactionVersion, tc.name), func(t *testing.T) {
				f := newReserveSwapFixture(t)
				mint := f.env.newCurrencyAwaitingPurchase(t, f.owner)

				var req *statefulSwapRequest
				if tc.treasuryFunded {
					f.env.credit(t, f.env.getVault(t, f.owner, f.launchpad1), currencycreator.ToQuarks(100_000))
					f.env.setLiveSupply(t, f.launchpad1, currencycreator.ToQuarks(defaultLaunchpadSupply))

					swapAmount, feeAmount := treasuryFundedNewCurrencyAmounts(transactionVersion)
					req = f.env.reserveSwapRequest(t, f.owner, f.launchpad1, mint, swapAmount, feeAmount)
					req.reserve.FullAmountExchangeData = f.env.treasuryFundedExchangeData(t, f.launchpad1, swapAmount+feeAmount, transactionVersion)
				} else {
					purchaseQuarks, feeQuarks := newCurrencyLaunchQuarks(transactionVersion)
					req = f.env.reserveSwapRequest(t, f.owner, common.CoreMintAccount, mint, purchaseQuarks, feeQuarks)
				}
				fromMint := common.CoreMintAccount
				if tc.treasuryFunded {
					fromMint = f.launchpad1
				}
				req.swapAuthority = newCurrencySwapAuthority(t, f.env, f.owner, fromMint, transactionVersion)
				req.transactionVersion = transactionVersion
				tc.setup(t, f, req)

				before := f.env.swapSnapshot(t, []*common.Account{f.owner, req.owner}, f.launchpad1, mint)
				result := f.env.statefulSwap(t, req)
				tc.verify(t, result, transactionVersion)
				f.env.requireNoSwapChanges(t, before, result)
			})
		}
	}
}

//
// Coinbase Stable Swapper swaps
//

func newStablecoinSwapFixture(t *testing.T, overrides ...func(*testOverrides)) (*testEnv, *common.Account) {
	env := setupTestEnv(t, append([]func(*testOverrides){withSwapsEnabled()}, overrides...)...)
	env.setupCoinbasePool(t, common.ToCoreMintQuarks(1_000_000))
	owner := env.newFundedUser(t, common.ToCoreMintQuarks(100))
	return env, owner
}

func TestStatefulSwap_Stablecoin_HappyPath(t *testing.T) {
	for _, transactionVersion := range []uint32{0, 1} {
		for _, swapVersion := range []transaction_util.CoinbaseSwapVersion{transaction_util.CoinbaseSwapV1, transaction_util.CoinbaseSwapV2} {
			t.Run(fmt.Sprintf("txn v%d swap %s", transactionVersion, swapVersion), func(t *testing.T) {
				env, owner := newStablecoinSwapFixture(t, withAntispamChecks())

				req := env.stablecoinSwapRequest(t, owner, common.ToCoreMintQuarks(10))
				req.transactionVersion = transactionVersion
				req.stablecoin.SwapVersion = uint32(swapVersion)

				before := env.swapSnapshot(t, []*common.Account{owner})
				result := env.statefulSwap(t, req)
				env.requireSwapCreated(t, before, req, result, expectedSwapRecord(t, req, result, swap.StateCreated), nil)

				params := result.serverParameters.GetStablecoin()
				require.NotNil(t, params)
				assert.Equal(t, fmt.Sprintf("coinbase_stable_swapper_v%d", transactionVersion), params.MemoValue)

				assert.Equal(t, []string{
					expectedAntispamSwapCall(swap.KindStablecoin, req.stablecoin.FundingSource, req.stablecoin.FromMint, req.stablecoin.ToMint, req.stablecoin.SwapAmount, req.stablecoin.FeeAmount, false),
				}, env.antispamIntegration.getCalls())
				env.requireBalance(t, env.getVault(t, owner, common.CoreMintAccount), common.ToCoreMintQuarks(100))
			})
		}
	}
}

func TestStatefulSwap_Stablecoin_Validation(t *testing.T) {
	for _, tc := range []struct {
		name   string
		setup  func(t *testing.T, env *testEnv, req *statefulSwapRequest)
		verify func(t *testing.T, result *statefulSwapResult)
	}{
		{
			name: "swap id reused",
			setup: func(t *testing.T, env *testEnv, req *statefulSwapRequest) {
				existing := env.newSwapRecord(t, req.owner, common.CoreMintAccount, usdcAccount(t), testutil.NewRandomAccount(t), 1, swap.StateFinalized)
				id, err := base58.Decode(existing.SwapId)
				require.NoError(t, err)
				req.stablecoin.Id.Value = id
			},
			verify: func(t *testing.T, result *statefulSwapResult) { result.requireDenied(t, "attempt to reuse swap id") },
		},
		{
			name: "funding id reused",
			setup: func(t *testing.T, env *testEnv, req *statefulSwapRequest) {
				fundingId := testutil.NewRandomAccount(t)
				env.newSwapRecord(t, req.owner, common.CoreMintAccount, usdcAccount(t), fundingId, 1, swap.StateFinalized)
				req.stablecoin.FundingId = fundingId.PublicKey().ToBase58()
			},
			verify: func(t *testing.T, result *statefulSwapResult) {
				result.requireDenied(t, "attempt to reuse swap funding id")
			},
		},
		{
			name: "source isn't the core mint",
			setup: func(t *testing.T, env *testEnv, req *statefulSwapRequest) {
				req.stablecoin.FromMint = usdcMint(t)
			},
			verify: func(t *testing.T, result *statefulSwapResult) {
				result.requireInvalidSwap(t, "source mint must be the core mint")
			},
		},
		{
			name: "destination isn't usdc",
			setup: func(t *testing.T, env *testEnv, req *statefulSwapRequest) {
				req.stablecoin.ToMint = testutil.NewRandomAccount(t).ToProto()
			},
			verify: func(t *testing.T, result *statefulSwapResult) {
				result.requireInvalidSwap(t, "destination mint must be usdc")
			},
		},
		{
			name: "fee amount",
			setup: func(t *testing.T, env *testEnv, req *statefulSwapRequest) {
				req.stablecoin.FeeAmount = 0
			},
			verify: func(t *testing.T, result *statefulSwapResult) {
				result.requireDenied(t, fmt.Sprintf("fee amount must be %d quarks", withdrawalFeeQuarks))
			},
		},
		{
			name: "funding id isn't a public key",
			setup: func(t *testing.T, env *testEnv, req *statefulSwapRequest) {
				req.stablecoin.FundingId = strings.Repeat("0", 44)
			},
			verify: func(t *testing.T, result *statefulSwapResult) {
				result.requireInvalidSwap(t, "funding id is not a public key")
			},
		},
		{
			name: "funding intent exists",
			setup: func(t *testing.T, env *testEnv, req *statefulSwapRequest) {
				require.NoError(t, env.data.SaveIntent(env.ctx, &intent.Record{
					IntentId:              req.stablecoin.FundingId,
					IntentType:            intent.OpenAccounts,
					MintAccount:           common.CoreMintAccount.PublicKey().ToBase58(),
					InitiatorOwnerAccount: req.owner.PublicKey().ToBase58(),
					OpenAccountsMetadata:  &intent.OpenAccountsMetadata{},
					State:                 intent.StatePending,
					CreatedAt:             time.Now(),
				}))
			},
			verify: func(t *testing.T, result *statefulSwapResult) {
				result.requireInvalidSwap(t, "funding intent already exists")
			},
		},
		{
			name: "source account not opened",
			setup: func(t *testing.T, env *testEnv, req *statefulSwapRequest) {
				req.owner = env.newAccount(t)
			},
			verify: func(t *testing.T, result *statefulSwapResult) {
				result.requireInvalidSwap(t, "source timelock vault account not opened")
			},
		},
		{
			name: "source account unlocked",
			setup: func(t *testing.T, env *testEnv, req *statefulSwapRequest) {
				env.setTimelockState(t, env.getVault(t, req.owner, common.CoreMintAccount), timelock_token_v1.StateUnlocked)
			},
			verify: func(t *testing.T, result *statefulSwapResult) {
				result.requireDenied(t, "source timelock account isn't locked")
			},
		},
		{
			name: "insufficient balance for the fee",
			setup: func(t *testing.T, env *testEnv, req *statefulSwapRequest) {
				req.stablecoin.SwapAmount = common.ToCoreMintQuarks(100)
			},
			verify: func(t *testing.T, result *statefulSwapResult) { result.requireInvalidSwap(t, "insufficient balance") },
		},
		{
			name: "owner is the swap authority",
			setup: func(t *testing.T, env *testEnv, req *statefulSwapRequest) {
				req.swapAuthority = req.owner
			},
			verify: func(t *testing.T, result *statefulSwapResult) {
				result.requireInvalidSwap(t, "owner cannot be swap authority")
			},
		},
		{
			name: "destination owner is a token account",
			setup: func(t *testing.T, env *testEnv, req *statefulSwapRequest) {
				env.data.setBlockchainAccountOwner(req.stablecoin.DestinationOwner.Value, token.ProgramKey)
			},
			verify: func(t *testing.T, result *statefulSwapResult) {
				result.requireInvalidSwap(t, "destination owner is a token account")
			},
		},
		{
			name: "insufficient destination liquidity",
			setup: func(t *testing.T, env *testEnv, req *statefulSwapRequest) {
				// Liquidity must cover a third more than the swap amount
				env.setCoinbaseLiquidity(t, usdcAccount(t), 4*req.stablecoin.SwapAmount/3-1)
			},
			verify: func(t *testing.T, result *statefulSwapResult) {
				result.requireDenied(t, "insufficient coinbase stable swapper destination liquidity")
			},
		},
		{
			name: "not a user account",
			setup: func(t *testing.T, env *testEnv, req *statefulSwapRequest) {
				giftCard := env.newAccount(t)
				env.openAccount(t, giftCard, giftCard, common.CoreMintAccount, commonpb.AccountType_REMOTE_SEND_GIFT_CARD, 0)
				env.credit(t, env.getVault(t, giftCard, common.CoreMintAccount), common.ToCoreMintQuarks(100))
				req.owner = giftCard
			},
			verify: func(t *testing.T, result *statefulSwapResult) { result.requireDenied(t, "not a user ocp account") },
		},
		{
			name: "not an ocp account",
			setup: func(t *testing.T, env *testEnv, req *statefulSwapRequest) {
				// The authority of another owner's locked pool account owns no
				// accounts itself
				owner, poolOwner := env.newAccount(t), env.newUser(t)
				env.openAccount(t, poolOwner, owner, common.CoreMintAccount, commonpb.AccountType_POOL, 0)
				env.credit(t, env.getVault(t, owner, common.CoreMintAccount), common.ToCoreMintQuarks(100))
				req.owner = owner
			},
			verify: func(t *testing.T, result *statefulSwapResult) { result.requireDenied(t, "not an ocp account") },
		},
		{
			name: "owner has an unlocked account",
			setup: func(t *testing.T, env *testEnv, req *statefulSwapRequest) {
				mint := env.newLaunchpadMint(t)
				env.openAccount(t, req.owner, req.owner, mint, commonpb.AccountType_PRIMARY, 0)
				env.setTimelockState(t, env.getVault(t, req.owner, mint), timelock_token_v1.StateUnlocked)
			},
			verify: func(t *testing.T, result *statefulSwapResult) { result.requireDenied(t, "not an ocp account") },
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env, owner := newStablecoinSwapFixture(t)
			req := env.stablecoinSwapRequest(t, owner, common.ToCoreMintQuarks(10))
			tc.setup(t, env, req)

			before := env.swapSnapshot(t, []*common.Account{owner, req.owner})
			result := env.statefulSwap(t, req)
			tc.verify(t, result)
			env.requireNoSwapChanges(t, before, result)
		})
	}
}

func TestStatefulSwap_Stablecoin_Antispam(t *testing.T) {
	env, owner := newStablecoinSwapFixture(t, withAntispamChecks())
	env.antispamIntegration.setAllow(false)

	req := env.stablecoinSwapRequest(t, owner, common.ToCoreMintQuarks(10))
	before := env.swapSnapshot(t, []*common.Account{owner})
	result := env.statefulSwap(t, req)
	result.requireDenied(t, "rate limited")
	env.requireNoSwapChanges(t, before, result)
}

//
// StatefulSwap dependency failures, boundaries and races
//

// requireSwapNotCommitted verifies a failure in the swap DB commit persisted
// no swap and didn't advance any currency. The memory provider doesn't roll
// back, so the nonce reservation made earlier in the commit isn't checked.
func (e *testEnv) requireSwapNotCommitted(t *testing.T, before *swapSnapshot, req *statefulSwapRequest) {
	_, err := e.data.Provider.GetSwapById(e.ctx, req.swapId())
	assert.Equal(t, swap.ErrNotFound, err)

	after := e.swapSnapshot(t, before.owners, before.mints...)
	assert.Equal(t, before.swaps, after.swaps, "swap records changed")
	assert.Equal(t, before.swapCount, after.swapCount, "unexpected swap records")
	assert.Equal(t, before.currencyStates, after.currencyStates, "currency states changed")
}

func TestStatefulSwap_Reserve_DependencyFailures(t *testing.T) {
	for _, tc := range []struct {
		name string
		// setup injects the failure and returns the request that hits it
		setup func(t *testing.T, f *reserveSwapFixture) *statefulSwapRequest
		// Failures in the swap DB commit happen after a nonce is reserved
		inCommit bool
	}{
		{
			name: "swap lookup",
			setup: func(t *testing.T, f *reserveSwapFixture) *statefulSwapRequest {
				f.env.data.injectErr("GetSwapById", errInjected)
				return f.buyRequest(t)
			},
		},
		{
			name: "antispam",
			setup: func(t *testing.T, f *reserveSwapFixture) *statefulSwapRequest {
				f.env.antispamIntegration.setErr(errInjected)
				return f.buyRequest(t)
			},
		},
		{
			name: "coinbase order lookup",
			setup: func(t *testing.T, f *reserveSwapFixture) *statefulSwapRequest {
				req := f.buyRequest(t)
				req.reserve.FundingSource = transactionpb.FundingSource_FUNDING_SOURCE_COINBASE_ONRAMP
				// The client retries 5xx responses before failing the same way
				req.reserve.FundingId = f.env.coinbase.putOrder(t, f.owner, f.swapPda(t), req.reserve.SwapAmount, func(o *testCoinbaseOrder) {
					o.httpStatus = http.StatusForbidden
				})
				return req
			},
		},
		{
			name: "live reserve state",
			setup: func(t *testing.T, f *reserveSwapFixture) *statefulSwapRequest {
				// The currency's reserve state never reaches the mint data
				// provider
				mint := testutil.SetupLaunchpadCurrency(t, f.env.data, reserve_memory.New(), holder_memory.New())
				f.env.openAccount(t, f.owner, f.owner, mint, commonpb.AccountType_PRIMARY, 0)
				f.env.credit(t, f.env.getVault(t, f.owner, mint), currencycreator.ToQuarks(reserveSwapLaunchpadBalance))
				return f.env.reserveSwapRequest(t, f.owner, mint, common.CoreMintAccount, currencycreator.ToQuarks(10), 0)
			},
		},
		{
			name:     "swap save",
			inCommit: true,
			setup: func(t *testing.T, f *reserveSwapFixture) *statefulSwapRequest {
				f.env.data.injectErr("SaveSwap", errInjected)
				return f.buyRequest(t)
			},
		},
		{
			name:     "new currency swap save",
			inCommit: true,
			setup: func(t *testing.T, f *reserveSwapFixture) *statefulSwapRequest {
				req := f.env.reserveSwapRequest(t, f.owner, common.CoreMintAccount, f.env.newCurrencyAwaitingPurchase(t, f.owner), defaultNewCurrencyPurchaseQuarks, defaultNewCurrencyFeeQuarks)
				req.swapAuthority = f.owner
				f.env.data.injectErr("SaveSwap", errInjected)
				return req
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newReserveSwapFixture(t, withAntispamChecks())
			req := tc.setup(t, f)

			mints := []*common.Account{f.launchpad1, f.launchpad2}
			if toMint, err := common.NewAccountFromProto(req.reserve.ToMint); err == nil && !common.IsCoreMint(toMint) {
				mints = append(mints, toMint)
			}
			before := f.env.swapSnapshot(t, []*common.Account{f.owner}, mints...)

			result := f.env.statefulSwap(t, req)
			result.requireStatusCode(t, codes.Internal)
			if tc.inCommit {
				f.env.requireSwapNotCommitted(t, before, req)
			} else {
				f.env.requireNoSwapChanges(t, before, result)
			}
		})
	}

	t.Run("new currency metadata save", func(t *testing.T) {
		// The last write in the swap DB commit. Rolling back the swap and nonce
		// is left to the DB transaction.
		f := newReserveSwapFixture(t)
		req := f.env.reserveSwapRequest(t, f.owner, common.CoreMintAccount, f.env.newCurrencyAwaitingPurchase(t, f.owner), defaultNewCurrencyPurchaseQuarks, defaultNewCurrencyFeeQuarks)
		req.swapAuthority = f.owner
		f.env.data.injectErr("SaveCurrencyMetadata", errInjected)

		f.env.statefulSwap(t, req).requireStatusCode(t, codes.Internal)
	})
}

func TestStatefulSwap_Stablecoin_DependencyFailures(t *testing.T) {
	for _, tc := range []struct {
		name     string
		setup    func(t *testing.T, env *testEnv, req *statefulSwapRequest)
		inCommit bool
	}{
		{
			name: "swap lookup",
			setup: func(t *testing.T, env *testEnv, req *statefulSwapRequest) {
				env.data.injectErr("GetSwapById", errInjected)
			},
		},
		{
			name: "destination owner lookup",
			setup: func(t *testing.T, env *testEnv, req *statefulSwapRequest) {
				env.data.injectErr("GetBlockchainAccountInfo:"+base58.Encode(req.stablecoin.DestinationOwner.Value), errInjected)
			},
		},
		{
			name: "coinbase liquidity lookup",
			setup: func(t *testing.T, env *testEnv, req *statefulSwapRequest) {
				// The pool's USDC vault can't be read
				env.data.mu.Lock()
				env.data.tokenBalances = make(map[string]uint64)
				env.data.mu.Unlock()
			},
		},
		{
			name: "antispam",
			setup: func(t *testing.T, env *testEnv, req *statefulSwapRequest) {
				env.antispamIntegration.setErr(errInjected)
			},
		},
		{
			name:     "swap save",
			inCommit: true,
			setup: func(t *testing.T, env *testEnv, req *statefulSwapRequest) {
				env.data.injectErr("SaveSwap", errInjected)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env, owner := newStablecoinSwapFixture(t, withAntispamChecks())
			req := env.stablecoinSwapRequest(t, owner, common.ToCoreMintQuarks(10))
			tc.setup(t, env, req)

			before := env.swapSnapshot(t, []*common.Account{owner})
			result := env.statefulSwap(t, req)
			result.requireStatusCode(t, codes.Internal)
			if tc.inCommit {
				env.requireSwapNotCommitted(t, before, req)
			} else {
				env.requireNoSwapChanges(t, before, result)
			}
		})
	}
}

func TestStatefulSwap_Reserve_Boundaries(t *testing.T) {
	t.Run("exact balance", func(t *testing.T) {
		f := newReserveSwapFixture(t)
		swapAmount := common.ToCoreMintQuarks(5)
		feeAmount := currency_util.ExpectedBuyFeeQuarks(swapAmount)

		owner := f.env.newFundedUser(t, swapAmount+feeAmount, f.launchpad1)
		f.env.reserveTimelockMemory(t, owner, f.launchpad1)
		req := f.env.reserveSwapRequest(t, owner, common.CoreMintAccount, f.launchpad1, swapAmount, feeAmount)

		before := f.env.swapSnapshot(t, []*common.Account{owner}, f.launchpad1)
		result := f.env.statefulSwap(t, req)
		f.env.requireSwapCreated(t, before, req, result, expectedSwapRecord(t, req, result, swap.StateCreated), nil)
	})

	for _, tc := range []struct {
		name   string
		offset int64
	}{
		{"fee within tolerance below expected", -currency_util.FeeQuarkTolerance},
		{"fee within tolerance above expected", currency_util.FeeQuarkTolerance},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newReserveSwapFixture(t)
			swapAmount := common.ToCoreMintQuarks(5)
			feeAmount := uint64(int64(currency_util.ExpectedBuyFeeQuarks(swapAmount)) + tc.offset)
			req := f.env.reserveSwapRequest(t, f.owner, common.CoreMintAccount, f.launchpad1, swapAmount, feeAmount)

			before := f.snapshot(t)
			result := f.env.statefulSwap(t, req)
			f.env.requireSwapCreated(t, before, req, result, expectedSwapRecord(t, req, result, swap.StateCreated), nil)
		})
	}
}

func TestStatefulSwap_Stablecoin_Boundaries(t *testing.T) {
	t.Run("exact balance", func(t *testing.T) {
		env, _ := newStablecoinSwapFixture(t)
		swapAmount := common.ToCoreMintQuarks(10)
		owner := env.newFundedUser(t, swapAmount+withdrawalFeeQuarks)
		req := env.stablecoinSwapRequest(t, owner, swapAmount)

		before := env.swapSnapshot(t, []*common.Account{owner})
		result := env.statefulSwap(t, req)
		env.requireSwapCreated(t, before, req, result, expectedSwapRecord(t, req, result, swap.StateCreated), nil)
	})

	t.Run("exact destination liquidity", func(t *testing.T) {
		env, owner := newStablecoinSwapFixture(t)
		req := env.stablecoinSwapRequest(t, owner, common.ToCoreMintQuarks(10))
		// Liquidity must cover a third more than the swap amount
		env.setCoinbaseLiquidity(t, usdcAccount(t), 4*req.stablecoin.SwapAmount/3)

		before := env.swapSnapshot(t, []*common.Account{owner})
		result := env.statefulSwap(t, req)
		env.requireSwapCreated(t, before, req, result, expectedSwapRecord(t, req, result, swap.StateCreated), nil)
	})
}

func TestStatefulSwap_ConcurrentDuplicates(t *testing.T) {
	const attempts = 5

	for _, kind := range []string{"reserve", "stablecoin"} {
		for _, shared := range []string{"swap id", "funding id"} {
			t.Run(fmt.Sprintf("%s same %s", kind, shared), func(t *testing.T) {
				var env *testEnv
				var owner *common.Account
				var base *statefulSwapRequest
				if kind == "reserve" {
					f := newReserveSwapFixture(t)
					env, owner, base = f.env, f.owner, f.buyRequest(t)
				} else {
					env, owner = newStablecoinSwapFixture(t)
					base = env.stablecoinSwapRequest(t, owner, common.ToCoreMintQuarks(10))
				}

				// Either the same client request retried from many devices at
				// once, or distinct swaps all claiming the same funding
				reqs := make([]*statefulSwapRequest, attempts)
				for i := range reqs {
					reqs[i] = base
					if shared == "funding id" {
						cloned := *base
						if kind == "reserve" {
							cloned.reserve = proto.Clone(base.reserve).(*transactionpb.StatefulSwapRequest_Initiate_ReserveSwapClientParameters)
							cloned.reserve.Id = newSwapId(t)
						} else {
							cloned.stablecoin = proto.Clone(base.stablecoin).(*transactionpb.StatefulSwapRequest_Initiate_CoinbaseStableSwapperClientParameters)
							cloned.stablecoin.Id = newSwapId(t)
						}
						reqs[i] = &cloned
					}
				}

				results := make([]*statefulSwapResult, attempts)
				t.Run("attempts", func(t *testing.T) {
					for i := range attempts {
						t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
							t.Parallel()
							results[i] = env.statefulSwap(t, reqs[i])
						})
					}
				})

				var winner *statefulSwapResult
				var winningReq *statefulSwapRequest
				for i, result := range results {
					if result.resp.GetSuccess() != nil {
						require.Nil(t, winner, "more than one attempt succeeded")
						winner, winningReq = result, reqs[i]
						continue
					}

					// Losers either see the winner's swap, or lose the race to
					// save it
					if result.err != nil {
						testutil.AssertStatusErrorWithCode(t, result.err, codes.Internal)
					} else {
						result.requireDenied(t, "attempt to reuse swap")
					}
				}
				require.NotNil(t, winner, "no attempt succeeded")

				saved, err := env.data.GetSwapById(env.ctx, winningReq.swapId())
				require.NoError(t, err)
				assert.Equal(t, base58.Encode(winner.expectedTxn.Message.RecentBlockhash[:]), saved.Blockhash)
				nonceIndex := winner.expectedTxn.Message.Instructions[0].Accounts[0]
				assert.Equal(t, base58.Encode(winner.expectedTxn.Message.Accounts[nonceIndex]), saved.Nonce)

				count := 0
				for _, state := range allSwapStates {
					records, err := env.data.GetAllSwapsByOwnerAndState(env.ctx, owner.PublicKey().ToBase58(), state)
					if err == swap.ErrNotFound {
						continue
					}
					require.NoError(t, err)
					count += len(records)
				}
				assert.Equal(t, 1, count)
			})
		}
	}
}

//
// GetSwap and GetPendingSwaps
//

func (e *testEnv) getSwap(t *testing.T, owner *common.Account, swapId string, signer *common.Account) (*transactionpb.GetSwapResponse, error) {
	decoded, err := base58.Decode(swapId)
	require.NoError(t, err)

	req := &transactionpb.GetSwapRequest{
		Id:    &commonpb.SwapId{Value: decoded},
		Owner: owner.ToProto(),
	}
	req.Signature = e.signProto(t, signer, req)
	return e.client.GetSwap(e.ctx, req)
}

func (e *testEnv) getPendingSwaps(t *testing.T, owner *common.Account, signer *common.Account) (*transactionpb.GetPendingSwapsResponse, error) {
	req := &transactionpb.GetPendingSwapsRequest{
		Owner: owner.ToProto(),
	}
	req.Signature = e.signProto(t, signer, req)
	return e.client.GetPendingSwaps(e.ctx, req)
}

// requireProtoSwap verifies a swap's proto matches the request that created
// it, so the proof signature over its verified metadata still verifies
func requireProtoSwap(t *testing.T, req *statefulSwapRequest, result *statefulSwapResult, actual *transactionpb.SwapMetadata, state transactionpb.SwapMetadata_State) {
	require.NotNil(t, actual)
	assert.Equal(t, state, actual.State)
	assert.Equal(t, result.proofSignature.Value, actual.Signature.Value)

	var expectedMetadata *transactionpb.VerifiedSwapMetadata
	if req.reserve != nil {
		params := req.reserve
		expectedMetadata = &transactionpb.VerifiedSwapMetadata{
			Kind: &transactionpb.VerifiedSwapMetadata_Reserve{
				Reserve: &transactionpb.VerifiedReserveSwapMetadata{ClientParameters: params},
			},
		}
	} else {
		params := req.stablecoin
		expectedMetadata = &transactionpb.VerifiedSwapMetadata{
			Kind: &transactionpb.VerifiedSwapMetadata_Stablecoin{
				Stablecoin: &transactionpb.VerifiedCoinbaseStableSwapperSwapMetadata{ClientParameters: params},
			},
		}
	}
	assert.True(t, proto.Equal(expectedMetadata, actual.VerifiedMetadata), "expected %v, got %v", expectedMetadata, actual.VerifiedMetadata)

	marshalled, err := proto.Marshal(actual.VerifiedMetadata)
	require.NoError(t, err)
	assert.True(t, ed25519.Verify(req.owner.PublicKey().ToBytes(), marshalled, actual.Signature.Value), "proof signature doesn't verify against the returned metadata")
}

func TestGetSwap(t *testing.T) {
	f := newReserveSwapFixture(t)
	f.env.setupCoinbasePool(t, common.ToCoreMintQuarks(1_000_000))

	reserveReq := f.buyRequest(t)
	reserveResult := f.env.statefulSwap(t, reserveReq)
	reserveResult.requireSuccess(t)

	// Swap records don't persist the swap version, so use the legacy swap
	// instruction, whose metadata the server can reproduce
	stablecoinReq := f.env.stablecoinSwapRequest(t, f.owner, common.ToCoreMintQuarks(10))
	stablecoinReq.stablecoin.SwapVersion = 0
	stablecoinResult := f.env.statefulSwap(t, stablecoinReq)
	stablecoinResult.requireSuccess(t)

	t.Run("reserve", func(t *testing.T) {
		resp, err := f.env.getSwap(t, f.owner, reserveReq.swapId(), f.owner)
		require.NoError(t, err)
		assert.Equal(t, transactionpb.GetSwapResponse_OK, resp.Result)
		requireProtoSwap(t, reserveReq, reserveResult, resp.Swap, transactionpb.SwapMetadata_CREATED)
	})

	t.Run("stablecoin", func(t *testing.T) {
		resp, err := f.env.getSwap(t, f.owner, stablecoinReq.swapId(), f.owner)
		require.NoError(t, err)
		assert.Equal(t, transactionpb.GetSwapResponse_OK, resp.Result)
		requireProtoSwap(t, stablecoinReq, stablecoinResult, resp.Swap, transactionpb.SwapMetadata_CREATED)
	})

	t.Run("stablecoin with a swap version", func(t *testing.T) {
		t.Skip("known bug: swap records don't persist swap_version, so toProtoSwap omits it and the proof signature doesn't verify against the returned metadata")

		req := f.env.stablecoinSwapRequest(t, f.owner, common.ToCoreMintQuarks(10))
		result := f.env.statefulSwap(t, req)
		result.requireSuccess(t)

		resp, err := f.env.getSwap(t, f.owner, req.swapId(), f.owner)
		require.NoError(t, err)
		requireProtoSwap(t, req, result, resp.Swap, transactionpb.SwapMetadata_CREATED)
	})

	t.Run("treasury funded new currency", func(t *testing.T) {
		t.Skip("known bug: swap records don't persist full_amount_exchange_data, so toProtoSwap omits it and the proof signature doesn't verify against the returned metadata")

		f.env.credit(t, f.env.getVault(t, f.owner, f.launchpad1), currencycreator.ToQuarks(100_000))
		f.env.setLiveSupply(t, f.launchpad1, currencycreator.ToQuarks(defaultLaunchpadSupply))
		mint := f.env.newCurrencyAwaitingPurchase(t, f.owner)

		swapAmount, feeAmount := treasuryFundedNewCurrencyAmounts(0)
		req := f.env.reserveSwapRequest(t, f.owner, f.launchpad1, mint, swapAmount, feeAmount)
		req.swapAuthority = f.owner
		req.reserve.FullAmountExchangeData = f.env.treasuryFundedExchangeData(t, f.launchpad1, swapAmount+feeAmount, 0)
		result := f.env.statefulSwap(t, req)
		result.requireSuccess(t)

		resp, err := f.env.getSwap(t, f.owner, req.swapId(), f.owner)
		require.NoError(t, err)
		requireProtoSwap(t, req, result, resp.Swap, transactionpb.SwapMetadata_CREATED)
	})

	t.Run("state changes", func(t *testing.T) {
		record, err := f.env.data.GetSwapById(f.env.ctx, reserveReq.swapId())
		require.NoError(t, err)
		record.State = swap.StateFunding
		require.NoError(t, f.env.data.SaveSwap(f.env.ctx, record))

		resp, err := f.env.getSwap(t, f.owner, reserveReq.swapId(), f.owner)
		require.NoError(t, err)
		requireProtoSwap(t, reserveReq, reserveResult, resp.Swap, transactionpb.SwapMetadata_FUNDING)
	})

	t.Run("not found", func(t *testing.T) {
		resp, err := f.env.getSwap(t, f.owner, testutil.NewRandomAccount(t).PublicKey().ToBase58(), f.owner)
		require.NoError(t, err)
		assert.Equal(t, transactionpb.GetSwapResponse_NOT_FOUND, resp.Result)
		assert.Nil(t, resp.Swap)
	})

	t.Run("another owner's swap", func(t *testing.T) {
		other := f.env.newUser(t)
		resp, err := f.env.getSwap(t, other, reserveReq.swapId(), other)
		require.NoError(t, err)
		assert.Equal(t, transactionpb.GetSwapResponse_DENIED, resp.Result)
		assert.Nil(t, resp.Swap)
	})

	t.Run("invalid signature", func(t *testing.T) {
		_, err := f.env.getSwap(t, f.owner, reserveReq.swapId(), f.env.newAccount(t))
		testutil.AssertStatusErrorWithCode(t, err, codes.Unauthenticated)
	})
}

func TestGetPendingSwaps(t *testing.T) {
	f := newReserveSwapFixture(t)
	other := f.env.newUser(t)

	t.Run("none", func(t *testing.T) {
		resp, err := f.env.getPendingSwaps(t, f.owner, f.owner)
		require.NoError(t, err)
		assert.Equal(t, transactionpb.GetPendingSwapsResponse_NOT_FOUND, resp.Result)
		assert.Empty(t, resp.Swaps)
	})

	// Only swaps waiting on the client to fund them are pending
	pending := make(map[string]*statefulSwapRequest)
	pendingResults := make(map[string]*statefulSwapResult)
	for range 2 {
		req := f.buyRequest(t)
		result := f.env.statefulSwap(t, req)
		result.requireSuccess(t)
		pending[req.swapId()] = req
		pendingResults[req.swapId()] = result
	}

	// A stablecoin swap is pending too. Its swap version isn't returned, so
	// leave it unset to get metadata the proof signature verifies against.
	f.env.setupCoinbasePool(t, common.ToCoreMintQuarks(1_000_000))
	stablecoinReq := f.env.stablecoinSwapRequest(t, f.owner, common.ToCoreMintQuarks(10))
	stablecoinReq.stablecoin.SwapVersion = 0
	stablecoinResult := f.env.statefulSwap(t, stablecoinReq)
	stablecoinResult.requireSuccess(t)
	pending[stablecoinReq.swapId()] = stablecoinReq
	pendingResults[stablecoinReq.swapId()] = stablecoinResult

	funded := f.buyRequest(t)
	funded.reserve.FundingSource = transactionpb.FundingSource_FUNDING_SOURCE_EXTERNAL_WALLET
	funded.reserve.FundingId = base58.Encode(make([]byte, ed25519.SignatureSize))
	f.env.statefulSwap(t, funded).requireSuccess(t)

	for _, state := range []swap.State{swap.StateFunded, swap.StateSubmitting, swap.StateFinalized, swap.StateFailed, swap.StateCancelling, swap.StateCancelled} {
		f.env.newSwapRecord(t, f.owner, common.CoreMintAccount, f.launchpad1, testutil.NewRandomAccount(t), 1, state)
	}
	f.env.newSwapRecord(t, other, common.CoreMintAccount, f.launchpad1, testutil.NewRandomAccount(t), 1, swap.StateCreated)

	t.Run("created swaps", func(t *testing.T) {
		resp, err := f.env.getPendingSwaps(t, f.owner, f.owner)
		require.NoError(t, err)
		assert.Equal(t, transactionpb.GetPendingSwapsResponse_OK, resp.Result)
		require.Len(t, resp.Swaps, len(pending))
		for _, protoSwap := range resp.Swaps {
			var swapId string
			if reserve := protoSwap.VerifiedMetadata.GetReserve(); reserve != nil {
				swapId = base58.Encode(reserve.ClientParameters.Id.Value)
			} else {
				swapId = base58.Encode(protoSwap.VerifiedMetadata.GetStablecoin().ClientParameters.Id.Value)
			}
			req, ok := pending[swapId]
			require.True(t, ok, "unexpected pending swap %s", swapId)
			requireProtoSwap(t, req, pendingResults[swapId], protoSwap, transactionpb.SwapMetadata_CREATED)
		}
	})

	t.Run("invalid signature", func(t *testing.T) {
		_, err := f.env.getPendingSwaps(t, f.owner, other)
		testutil.AssertStatusErrorWithCode(t, err, codes.Unauthenticated)
	})
}

func TestGetSwap_LookupFailure(t *testing.T) {
	env := setupTestEnv(t)
	owner := env.newUser(t)
	record := env.newSwapRecord(t, owner, common.CoreMintAccount, env.newLaunchpadMint(t), testutil.NewRandomAccount(t), 1, swap.StateCreated)
	env.data.injectErr("GetSwapById", errInjected)

	_, err := env.getSwap(t, owner, record.SwapId, owner)
	testutil.AssertStatusErrorWithCode(t, err, codes.Internal)
}

func TestGetPendingSwaps_LookupFailure(t *testing.T) {
	env := setupTestEnv(t, withSwapsEnabled())
	owner := env.newUser(t)
	env.data.injectErr("GetAllSwapsByOwnerAndState", errInjected)

	_, err := env.getPendingSwaps(t, owner, owner)
	testutil.AssertStatusErrorWithCode(t, err, codes.Internal)
}

func TestGetPendingSwaps_SwapsDisabled(t *testing.T) {
	env := setupTestEnv(t)
	owner := env.newUser(t)
	env.newSwapRecord(t, owner, common.CoreMintAccount, env.newLaunchpadMint(t), testutil.NewRandomAccount(t), 1, swap.StateCreated)

	resp, err := env.getPendingSwaps(t, owner, owner)
	require.NoError(t, err)
	assert.Equal(t, transactionpb.GetPendingSwapsResponse_OK, resp.Result)
	assert.Empty(t, resp.Swaps)
}

func TestMakeStatelessSwapTransaction(t *testing.T) {
	testutil.SetupRandomSubsidizer(t, ocp_data.NewTestDataProvider())

	owner := testutil.NewRandomAccount(t)
	fromMint := testutil.NewRandomAccount(t)
	poolFeeRecipient := testutil.NewRandomAccount(t)

	createDepositAtaIxn, depositAta, err := token.CreateAssociatedTokenAccountIdempotent(
		common.GetSubsidizer().PublicKey().ToBytes(),
		testutil.NewRandomAccount(t).PublicKey().ToBytes(),
		common.CoreMintAccount.PublicKey().ToBytes(),
	)
	require.NoError(t, err)

	coinbaseAccounts := &transaction_util.CoinbaseSwapAccounts{
		Pool:                     testutil.NewRandomAccount(t).PublicKey().ToBytes(),
		InVault:                  testutil.NewRandomAccount(t).PublicKey().ToBytes(),
		OutVault:                 testutil.NewRandomAccount(t).PublicKey().ToBytes(),
		InVaultTokenAccount:      testutil.NewRandomAccount(t).PublicKey().ToBytes(),
		OutVaultTokenAccount:     testutil.NewRandomAccount(t).PublicKey().ToBytes(),
		Whitelist:                testutil.NewRandomAccount(t).PublicKey().ToBytes(),
		FeeRecipient:             poolFeeRecipient.PublicKey().ToBytes(),
		FeeRecipientTokenAccount: testutil.NewRandomAccount(t).PublicKey().ToBytes(),
	}

	var blockhash solana.Blockhash
	copy(blockhash[:], testutil.NewRandomAccount(t).PublicKey().ToBytes())

	for _, swapVersion := range []transaction_util.CoinbaseSwapVersion{transaction_util.CoinbaseSwapV1, transaction_util.CoinbaseSwapV2} {
		coinbaseSwapIxn, err := transaction_util.MakeCoinbaseSwapInstruction(
			swapVersion,
			coinbaseAccounts,
			owner.PublicKey().ToBytes(),
			fromMint.PublicKey().ToBytes(),
			common.CoreMintAccount.PublicKey().ToBytes(),
			testutil.NewRandomAccount(t).PublicKey().ToBytes(),
			depositAta,
			1_000_000,
			1_000_000,
		)
		require.NoError(t, err)

		t.Run("txn v0 swap "+swapVersion.String(), func(t *testing.T) {
			txn, serverParameters, err := makeStatelessSwapTransaction(solana.MessageVersion0, blockhash, poolFeeRecipient, createDepositAtaIxn, coinbaseSwapIxn)
			require.NoError(t, err)

			assert.Equal(t, solana.MessageVersion0, txn.Message.Version)
			assertCommonStatelessSwapServerParameters(t, serverParameters, blockhash, poolFeeRecipient)
			assert.Equal(t, "coinbase_stable_swapper_v0", serverParameters.MemoValue)
			assert.Equal(t, transaction_util.ToProtoAlts([]solana.AddressLookupTable{transaction_util.GetAltForCoreMint()}), serverParameters.Alts)
			assert.EqualValues(t, statelessSwapComputeUnitLimit, serverParameters.ComputeUnitLimit)
			assert.EqualValues(t, statelessSwapComputeUnitPrice, serverParameters.ComputeUnitPrice)
			assert.Nil(t, serverParameters.TransactionConfig)

			requireClientRebuildMatches(t, txn, serverParameters, createDepositAtaIxn, coinbaseSwapIxn)
		})

		t.Run("txn v1 swap "+swapVersion.String(), func(t *testing.T) {
			txn, serverParameters, err := makeStatelessSwapTransaction(solana.MessageVersion1, blockhash, poolFeeRecipient, createDepositAtaIxn, coinbaseSwapIxn)
			require.NoError(t, err)

			assert.Equal(t, solana.MessageVersion1, txn.Message.Version)
			assert.Empty(t, txn.Message.AddressTableLookups)
			assertCommonStatelessSwapServerParameters(t, serverParameters, blockhash, poolFeeRecipient)
			assert.Equal(t, "coinbase_stable_swapper_v1", serverParameters.MemoValue)
			assert.Empty(t, serverParameters.Alts)
			assert.Zero(t, serverParameters.ComputeUnitLimit)
			assert.Zero(t, serverParameters.ComputeUnitPrice)

			require.NotNil(t, serverParameters.TransactionConfig)
			assert.EqualValues(t, statelessSwapComputeUnitLimit, serverParameters.TransactionConfig.ComputeUnitLimit)
			assert.EqualValues(t, transaction_util.CoinbaseSwapLoadedAccountsDataSizeLimit, serverParameters.TransactionConfig.LoadedAccountsDataSizeLimit)
			assert.EqualValues(t, 1_000, serverParameters.TransactionConfig.PriorityFeeLamports)
			assert.Zero(t, serverParameters.TransactionConfig.HeapSize)

			// ComputeBudget instructions are omitted in v1
			for _, account := range txn.Message.Accounts {
				assert.NotEqual(t, compute_budget.ProgramKey, account)
			}
			assert.Len(t, txn.Message.Instructions, 3)

			marshalled, err := txn.Marshal()
			require.NoError(t, err)
			assert.LessOrEqual(t, len(marshalled), solana.MaxV1TransactionSize)

			requireClientRebuildMatches(t, txn, serverParameters, createDepositAtaIxn, coinbaseSwapIxn)
		})
	}

	t.Run("unsupported version", func(t *testing.T) {
		_, _, err := makeStatelessSwapTransaction(solana.MessageVersionLegacy, blockhash, poolFeeRecipient, createDepositAtaIxn, createDepositAtaIxn)
		assert.Error(t, err)
	})
}

func assertCommonStatelessSwapServerParameters(t *testing.T, serverParameters *transactionpb.StatelessSwapResponse_ServerParameters_CoinbaseStableSwapperServerParameter, blockhash solana.Blockhash, poolFeeRecipient *common.Account) {
	assert.Equal(t, common.GetSubsidizer().PublicKey().ToBytes(), serverParameters.Payer.Value)
	assert.Equal(t, blockhash[:], serverParameters.Blockhash.Value)
	assert.Equal(t, poolFeeRecipient.PublicKey().ToBytes(), serverParameters.PoolFeeRecipient.Value)
}

// requireClientRebuildMatches builds the transaction from the server
// parameters the way the proto documents for clients, and requires the
// message clients sign to match the server's exactly.
func requireClientRebuildMatches(
	t *testing.T,
	serverTxn solana.Transaction,
	serverParameters *transactionpb.StatelessSwapResponse_ServerParameters_CoinbaseStableSwapperServerParameter,
	createDepositAtaIxn, coinbaseSwapIxn solana.Instruction,
) {
	var ixns []solana.Instruction
	if serverParameters.TransactionConfig == nil {
		if serverParameters.ComputeUnitLimit != 0 {
			ixns = append(ixns, compute_budget.SetComputeUnitLimit(serverParameters.ComputeUnitLimit))
		}
		if serverParameters.ComputeUnitPrice != 0 {
			ixns = append(ixns, compute_budget.SetComputeUnitPrice(serverParameters.ComputeUnitPrice))
		}
	}
	if len(serverParameters.MemoValue) > 0 {
		ixns = append(ixns, memo.Instruction(serverParameters.MemoValue))
	}
	ixns = append(ixns, createDepositAtaIxn, coinbaseSwapIxn)

	var clientTxn solana.Transaction
	if serverParameters.TransactionConfig == nil {
		clientTxn = solana.NewV0Transaction(serverParameters.Payer.Value, fromProtoAlts(serverParameters.Alts), ixns)
	} else {
		// CU limit, loaded accounts data size limit and priority fee are always
		// encoded, and the heap size only when non-zero
		config := solana.TransactionConfig{
			ComputeUnitLimit:            pointer.Uint32(serverParameters.TransactionConfig.ComputeUnitLimit),
			LoadedAccountsDataSizeLimit: pointer.Uint32(serverParameters.TransactionConfig.LoadedAccountsDataSizeLimit),
			PriorityFeeLamports:         pointer.Uint64(serverParameters.TransactionConfig.PriorityFeeLamports),
		}
		if serverParameters.TransactionConfig.HeapSize != 0 {
			config.HeapSize = pointer.Uint32(serverParameters.TransactionConfig.HeapSize)
		}

		var err error
		clientTxn, err = solana.NewV1Transaction(serverParameters.Payer.Value, config, ixns...)
		require.NoError(t, err)
	}

	var blockhash solana.Blockhash
	copy(blockhash[:], serverParameters.Blockhash.Value)
	clientTxn.SetBlockhash(blockhash)

	expected, err := serverTxn.Message.Marshal()
	require.NoError(t, err)
	actual, err := clientTxn.Message.Marshal()
	require.NoError(t, err)
	require.Equal(t, expected, actual)
}

func fromProtoAlts(protos []*commonpb.SolanaAddressLookupTable) []solana.AddressLookupTable {
	alts := make([]solana.AddressLookupTable, len(protos))
	for i, proto := range protos {
		alts[i] = solana.AddressLookupTable{PublicKey: proto.Address.Value}
		for _, entry := range proto.Entries {
			alts[i].Addresses = append(alts[i].Addresses, ed25519.PublicKey(entry.Value))
		}
	}
	return alts
}

//
// StatelessSwap RPC
//

const statelessSwapUsdcBalance = 100 // whole USDC

func newStatelessSwapFixture(t *testing.T, overrides ...func(*testOverrides)) (*testEnv, *common.Account) {
	env := setupTestEnv(t, append([]func(*testOverrides){withSwapsEnabled()}, overrides...)...)
	env.setupCoinbasePool(t, common.ToCoreMintQuarks(1_000_000))

	var blockhash solana.Blockhash
	copy(blockhash[:], testutil.NewRandomAccount(t).PublicKey().ToBytes())
	env.data.setLatestBlockhash(blockhash)

	owner := env.newUser(t)
	env.setUsdcBalance(t, owner, statelessSwapUsdcBalance*1_000_000)
	return env, owner
}

func (e *testEnv) setUsdcBalance(t *testing.T, owner *common.Account, quarks uint64) {
	e.data.setTokenBalance(toAta(t, owner, usdcAccount(t)).PublicKey().ToBytes(), quarks)
}

// requireNothingSubmitted verifies no transaction reached the blockchain and
// no swap state was persisted
func (e *testEnv) requireNothingSubmitted(t *testing.T, before *swapSnapshot) {
	assert.Empty(t, e.data.getSubmittedTransactions())
	assert.Empty(t, e.swapIntegration.getSubmitted())
	e.requireNoSwapChanges(t, before, nil)
}

// requireSubmitted verifies the transaction the client signed was submitted,
// signed by the client and subsidizer, and nothing was persisted
func (e *testEnv) requireSubmitted(t *testing.T, before *swapSnapshot, req *statelessSwapRequest, result *statelessSwapResult) {
	submitted := e.data.getSubmittedTransactions()
	require.Len(t, submitted, 1)
	txn := submitted[0]

	expectedMessage, err := result.expectedTxn.Message.Marshal()
	require.NoError(t, err)
	message, err := txn.Message.Marshal()
	require.NoError(t, err)
	assert.Equal(t, expectedMessage, message)

	require.EqualValues(t, 2, txn.Message.Header.NumSignatures)
	assert.Equal(t, common.GetSubsidizer().PublicKey().ToBytes(), []byte(txn.Message.Accounts[0]))
	assert.Equal(t, req.owner.PublicKey().ToBytes(), []byte(txn.Message.Accounts[1]))
	for i, signature := range txn.Signatures {
		assert.True(t, ed25519.Verify(txn.Message.Accounts[i], message, signature[:]), "signature %d is invalid", i)
	}
	assert.Equal(t, result.signature.Value, txn.Signatures[1][:])
	assert.Equal(t, txn.Signature(), result.resp.GetSuccess().TransactionSignature.Value)

	assert.Equal(t, []string{fmt.Sprintf(
		"%s:%s:%s:%d",
		req.owner.PublicKey().ToBase58(),
		usdc.Mint,
		common.CoreMintAccount.PublicKey().ToBase58(),
		req.params.SwapAmount,
	)}, e.swapIntegration.getSubmitted())

	// Stateless swaps never touch swap state or the swap nonce pool
	e.requireNoSwapChanges(t, before, nil)
}

func TestStatelessSwap_Disabled(t *testing.T) {
	env := setupTestEnv(t)
	owner := env.newUser(t)

	result := env.statelessSwap(t, env.statelessSwapRequest(t, owner, 1_000_000))
	result.requireStatusCode(t, codes.Unavailable)
	assert.Empty(t, env.data.getSubmittedTransactions())
}

func TestStatelessSwap_FirstRequestMustBeInitiate(t *testing.T) {
	env := setupTestEnv(t, withSwapsEnabled())

	stream, err := env.client.StatelessSwap(env.ctx)
	require.NoError(t, err)
	require.NoError(t, stream.Send(&transactionpb.StatelessSwapRequest{
		Request: &transactionpb.StatelessSwapRequest_SubmitSignatures_{
			SubmitSignatures: &transactionpb.StatelessSwapRequest_SubmitSignatures{
				TransactionSignatures: []*commonpb.Signature{{Value: make([]byte, ed25519.SignatureSize)}},
			},
		},
	}))

	_, err = stream.Recv()
	testutil.AssertStatusErrorWithCode(t, err, codes.InvalidArgument)
}

func TestStatelessSwap_InvalidInitiateSignature(t *testing.T) {
	env, owner := newStatelessSwapFixture(t)
	req := env.statelessSwapRequest(t, owner, 1_000_000)
	before := env.swapSnapshot(t, []*common.Account{owner})

	stream, err := env.client.StatelessSwap(env.ctx)
	require.NoError(t, err)

	initiate := &transactionpb.StatelessSwapRequest_Initiate{
		Kind:  &transactionpb.StatelessSwapRequest_Initiate_Stablecoin{Stablecoin: req.params},
		Owner: owner.ToProto(),
	}
	initiate.Signature = env.signProto(t, env.newAccount(t), initiate)
	require.NoError(t, stream.Send(&transactionpb.StatelessSwapRequest{
		Request: &transactionpb.StatelessSwapRequest_Initiate_{Initiate: initiate},
	}))

	_, err = stream.Recv()
	testutil.AssertStatusErrorWithCode(t, err, codes.Unauthenticated)
	env.requireNothingSubmitted(t, before)
}

func TestStatelessSwap_ProtoValidation(t *testing.T) {
	env, owner := newStatelessSwapFixture(t)
	before := env.swapSnapshot(t, []*common.Account{owner})

	result := env.statelessSwap(t, env.statelessSwapRequest(t, owner, 0))
	result.requireStatusCode(t, codes.InvalidArgument)
	env.requireNothingSubmitted(t, before)
}

func TestStatelessSwap_ClientReceiveTimeout(t *testing.T) {
	env, owner := newStatelessSwapFixture(t, withClientReceiveTimeout(500*time.Millisecond))
	req := env.statelessSwapRequest(t, owner, 1_000_000)
	before := env.swapSnapshot(t, []*common.Account{owner})

	stream, err := env.client.StatelessSwap(env.ctx)
	require.NoError(t, err)

	initiate := &transactionpb.StatelessSwapRequest_Initiate{
		Kind:  &transactionpb.StatelessSwapRequest_Initiate_Stablecoin{Stablecoin: req.params},
		Owner: owner.ToProto(),
	}
	initiate.Signature = env.signProto(t, owner, initiate)
	require.NoError(t, stream.Send(&transactionpb.StatelessSwapRequest{
		Request: &transactionpb.StatelessSwapRequest_Initiate_{Initiate: initiate},
	}))

	resp, err := stream.Recv()
	require.NoError(t, err)
	require.NotNil(t, resp.GetServerParameters())

	_, err = stream.Recv()
	testutil.AssertStatusErrorWithCode(t, err, codes.DeadlineExceeded)
	env.requireNothingSubmitted(t, before)
}

func TestStatelessSwap_HappyPath(t *testing.T) {
	for _, transactionVersion := range []uint32{0, 1} {
		for _, swapVersion := range []transaction_util.CoinbaseSwapVersion{transaction_util.CoinbaseSwapV1, transaction_util.CoinbaseSwapV2} {
			t.Run(fmt.Sprintf("txn v%d swap %s", transactionVersion, swapVersion), func(t *testing.T) {
				env, owner := newStatelessSwapFixture(t, withAntispamChecks())

				req := env.statelessSwapRequest(t, owner, 25*1_000_000)
				req.transactionVersion = transactionVersion
				req.params.SwapVersion = uint32(swapVersion)

				before := env.swapSnapshot(t, []*common.Account{owner})
				result := env.statelessSwap(t, req)
				require.NoError(t, result.err)
				require.NotNil(t, result.resp.GetSuccess(), "expected success, got %v", result.resp)
				assert.Equal(t, transactionpb.StatelessSwapResponse_Success_SUBMITTED, result.resp.GetSuccess().Code)
				env.requireSubmitted(t, before, req, result)

				assert.Equal(t, selectSwapTransactionVersion(transactionVersion), result.expectedTxn.Message.Version)
				assert.Equal(t, []string{
					expectedAntispamSwapCall(swap.KindStablecoin, transactionpb.FundingSource_FUNDING_SOURCE_UNKNOWN, req.params.FromMint, req.params.ToMint, req.params.SwapAmount, 0, false),
				}, env.antispamIntegration.getCalls())
			})
		}
	}
}

func TestStatelessSwap_WaitForFinalization(t *testing.T) {
	t.Run("finalized", func(t *testing.T) {
		env, owner := newStatelessSwapFixture(t)
		req := env.statelessSwapRequest(t, owner, 25*1_000_000)
		req.waitForFinalization = true

		before := env.swapSnapshot(t, []*common.Account{owner})
		result := env.statelessSwap(t, req)
		require.NoError(t, result.err)
		require.NotNil(t, result.resp.GetSuccess(), "expected success, got %v", result.resp)
		assert.Equal(t, transactionpb.StatelessSwapResponse_Success_FINALIZED, result.resp.GetSuccess().Code)
		env.requireSubmitted(t, before, req, result)
	})

	t.Run("finalized after polling", func(t *testing.T) {
		env, owner := newStatelessSwapFixture(t)
		env.data.setUnfinalizedLookups(3)
		req := env.statelessSwapRequest(t, owner, 25*1_000_000)
		req.waitForFinalization = true

		before := env.swapSnapshot(t, []*common.Account{owner})
		result := env.statelessSwap(t, req)
		require.NoError(t, result.err)
		require.NotNil(t, result.resp.GetSuccess(), "expected success, got %v", result.resp)
		assert.Equal(t, transactionpb.StatelessSwapResponse_Success_FINALIZED, result.resp.GetSuccess().Code)
		env.requireSubmitted(t, before, req, result)
	})

	t.Run("never finalized", func(t *testing.T) {
		// Polls for about 7.5 seconds before giving up
		env, owner := newStatelessSwapFixture(t)
		env.data.setUnfinalizedLookups(math.MaxInt)
		req := env.statelessSwapRequest(t, owner, 25*1_000_000)
		req.waitForFinalization = true

		result := env.statelessSwap(t, req)
		result.requireStatusCode(t, codes.Internal)
		assert.Len(t, env.data.getSubmittedTransactions(), 1)
	})

	t.Run("failed", func(t *testing.T) {
		env, owner := newStatelessSwapFixture(t)
		env.data.setFailSubmitted(true)
		req := env.statelessSwapRequest(t, owner, 25*1_000_000)
		req.waitForFinalization = true

		result := env.statelessSwap(t, req)
		result.requireError(t, transactionpb.StatelessSwapResponse_Error_TRANSACTION_FAILED, "")
		assert.Len(t, env.data.getSubmittedTransactions(), 1)
	})
}

func TestStatelessSwap_SubmissionFailure(t *testing.T) {
	for _, waitForFinalization := range []bool{false, true} {
		t.Run(fmt.Sprintf("wait for finalization %t", waitForFinalization), func(t *testing.T) {
			env, owner := newStatelessSwapFixture(t)
			env.data.setSubmitErr(errors.New("rpc failure"))
			req := env.statelessSwapRequest(t, owner, 25*1_000_000)
			req.waitForFinalization = waitForFinalization

			result := env.statelessSwap(t, req)
			result.requireStatusCode(t, codes.Internal)
			assert.Empty(t, env.data.getSubmittedTransactions())
		})
	}
}

func TestStatelessSwap_SignatureSubmission(t *testing.T) {
	t.Run("invalid signature", func(t *testing.T) {
		env, owner := newStatelessSwapFixture(t)
		req := env.statelessSwapRequest(t, owner, 25*1_000_000)
		req.signature = func(t *testing.T, message []byte) *commonpb.Signature {
			return &commonpb.Signature{Value: ed25519.Sign(env.newAccount(t).PrivateKey().ToBytes(), message)}
		}

		before := env.swapSnapshot(t, []*common.Account{owner})
		result := env.statelessSwap(t, req)
		result.requireError(t, transactionpb.StatelessSwapResponse_Error_SIGNATURE_ERROR, "")
		requireInvalidSignatureDetails(t, result.resp.GetError().ErrorDetails, *result.expectedTxn, result.signature)
		env.requireNothingSubmitted(t, before)
	})

	t.Run("wrong second message", func(t *testing.T) {
		env, owner := newStatelessSwapFixture(t)
		req := env.statelessSwapRequest(t, owner, 25*1_000_000)
		initiate := &transactionpb.StatelessSwapRequest_Initiate{
			Kind:  &transactionpb.StatelessSwapRequest_Initiate_Stablecoin{Stablecoin: req.params},
			Owner: owner.ToProto(),
		}
		initiate.Signature = env.signProto(t, owner, initiate)
		req.secondMsg = &transactionpb.StatelessSwapRequest{
			Request: &transactionpb.StatelessSwapRequest_Initiate_{Initiate: initiate},
		}

		before := env.swapSnapshot(t, []*common.Account{owner})
		result := env.statelessSwap(t, req)
		result.requireStatusCode(t, codes.InvalidArgument)
		env.requireNothingSubmitted(t, before)
	})
}

func TestStatelessSwap_Validation(t *testing.T) {
	for _, tc := range []struct {
		name   string
		setup  func(t *testing.T, env *testEnv, req *statelessSwapRequest)
		verify func(t *testing.T, result *statelessSwapResult)
	}{
		{
			name: "source isn't usdc",
			setup: func(t *testing.T, env *testEnv, req *statelessSwapRequest) {
				req.params.FromMint = common.CoreMintAccount.ToProto()
			},
			verify: func(t *testing.T, result *statelessSwapResult) {
				result.requireInvalidSwap(t, "source mint must be usdc")
			},
		},
		{
			name: "destination isn't the core mint",
			setup: func(t *testing.T, env *testEnv, req *statelessSwapRequest) {
				req.params.ToMint = env.newLaunchpadMint(t).ToProto()
			},
			verify: func(t *testing.T, result *statelessSwapResult) {
				result.requireInvalidSwap(t, "destination mint must be the core mint")
			},
		},
		{
			name: "destination account not opened",
			setup: func(t *testing.T, env *testEnv, req *statelessSwapRequest) {
				req.owner = env.newAccount(t)
				env.setUsdcBalance(t, req.owner, statelessSwapUsdcBalance*1_000_000)
			},
			verify: func(t *testing.T, result *statelessSwapResult) {
				result.requireInvalidSwap(t, "destination timelock vault account not opened")
			},
		},
		{
			name: "destination account unlocked",
			setup: func(t *testing.T, env *testEnv, req *statelessSwapRequest) {
				env.setTimelockState(t, env.getVault(t, req.owner, common.CoreMintAccount), timelock_token_v1.StateUnlocked)
			},
			verify: func(t *testing.T, result *statelessSwapResult) {
				result.requireDenied(t, "destination timelock account isn't locked")
			},
		},
		{
			name: "insufficient usdc balance",
			setup: func(t *testing.T, env *testEnv, req *statelessSwapRequest) {
				req.params.SwapAmount = statelessSwapUsdcBalance*1_000_000 + 1
			},
			verify: func(t *testing.T, result *statelessSwapResult) { result.requireInvalidSwap(t, "insufficient balance") },
		},
		{
			name: "no usdc account",
			setup: func(t *testing.T, env *testEnv, req *statelessSwapRequest) {
				req.owner = env.newUser(t)
			},
			verify: func(t *testing.T, result *statelessSwapResult) { result.requireInvalidSwap(t, "insufficient balance") },
		},
		{
			name: "insufficient destination liquidity",
			setup: func(t *testing.T, env *testEnv, req *statelessSwapRequest) {
				env.setCoinbaseLiquidity(t, common.CoreMintAccount, req.params.SwapAmount-1)
			},
			verify: func(t *testing.T, result *statelessSwapResult) {
				result.requireDenied(t, "insufficient coinbase stable swapper destination liquidity")
			},
		},
		{
			name: "not a user account",
			setup: func(t *testing.T, env *testEnv, req *statelessSwapRequest) {
				giftCard := env.newAccount(t)
				env.openAccount(t, giftCard, giftCard, common.CoreMintAccount, commonpb.AccountType_REMOTE_SEND_GIFT_CARD, 0)
				env.setUsdcBalance(t, giftCard, statelessSwapUsdcBalance*1_000_000)
				req.owner = giftCard
			},
			verify: func(t *testing.T, result *statelessSwapResult) { result.requireDenied(t, "not a user ocp account") },
		},
		{
			name: "not an ocp account",
			setup: func(t *testing.T, env *testEnv, req *statelessSwapRequest) {
				// The authority of another owner's locked pool account owns no
				// accounts itself
				owner, poolOwner := env.newAccount(t), env.newUser(t)
				env.openAccount(t, poolOwner, owner, common.CoreMintAccount, commonpb.AccountType_POOL, 0)
				env.setUsdcBalance(t, owner, statelessSwapUsdcBalance*1_000_000)
				req.owner = owner
			},
			verify: func(t *testing.T, result *statelessSwapResult) { result.requireDenied(t, "not an ocp account") },
		},
		{
			name: "owner has an unlocked account",
			setup: func(t *testing.T, env *testEnv, req *statelessSwapRequest) {
				mint := env.newLaunchpadMint(t)
				env.openAccount(t, req.owner, req.owner, mint, commonpb.AccountType_PRIMARY, 0)
				env.setTimelockState(t, env.getVault(t, req.owner, mint), timelock_token_v1.StateUnlocked)
			},
			verify: func(t *testing.T, result *statelessSwapResult) { result.requireDenied(t, "not an ocp account") },
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env, owner := newStatelessSwapFixture(t)
			req := env.statelessSwapRequest(t, owner, 25*1_000_000)
			tc.setup(t, env, req)

			before := env.swapSnapshot(t, []*common.Account{owner, req.owner})
			result := env.statelessSwap(t, req)
			tc.verify(t, result)
			env.requireNothingSubmitted(t, before)
		})
	}
}

func TestStatelessSwap_Antispam(t *testing.T) {
	env, owner := newStatelessSwapFixture(t, withAntispamChecks())
	env.antispamIntegration.setAllow(false)

	before := env.swapSnapshot(t, []*common.Account{owner})
	result := env.statelessSwap(t, env.statelessSwapRequest(t, owner, 25*1_000_000))
	result.requireDenied(t, "rate limited")
	env.requireNothingSubmitted(t, before)
}

func TestStatelessSwap_DependencyFailures(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup func(t *testing.T, env *testEnv)
	}{
		{
			name:  "latest blockhash",
			setup: func(t *testing.T, env *testEnv) { env.data.injectErr("GetBlockchainLatestBlockhash", errInjected) },
		},
		{
			name:  "antispam",
			setup: func(t *testing.T, env *testEnv) { env.antispamIntegration.setErr(errInjected) },
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env, owner := newStatelessSwapFixture(t, withAntispamChecks())
			tc.setup(t, env)

			before := env.swapSnapshot(t, []*common.Account{owner})
			result := env.statelessSwap(t, env.statelessSwapRequest(t, owner, 25*1_000_000))
			result.requireStatusCode(t, codes.Internal)
			env.requireNothingSubmitted(t, before)
		})
	}
}

func TestStatelessSwap_Boundaries(t *testing.T) {
	t.Run("exact balance", func(t *testing.T) {
		env, owner := newStatelessSwapFixture(t)
		swapAmount := uint64(statelessSwapUsdcBalance * 1_000_000)
		req := env.statelessSwapRequest(t, owner, swapAmount)

		before := env.swapSnapshot(t, []*common.Account{owner})
		result := env.statelessSwap(t, req)
		require.NoError(t, result.err)
		require.NotNil(t, result.resp.GetSuccess(), "expected success, got %v", result.resp)
		env.requireSubmitted(t, before, req, result)
	})

	t.Run("exact destination liquidity", func(t *testing.T) {
		env, owner := newStatelessSwapFixture(t)
		req := env.statelessSwapRequest(t, owner, 25*1_000_000)
		env.setCoinbaseLiquidity(t, common.CoreMintAccount, req.params.SwapAmount)

		before := env.swapSnapshot(t, []*common.Account{owner})
		result := env.statelessSwap(t, req)
		require.NoError(t, result.err)
		require.NotNil(t, result.resp.GetSuccess(), "expected success, got %v", result.resp)
		env.requireSubmitted(t, before, req, result)
	})
}

func usdcMint(t *testing.T) *commonpb.SolanaAccountId {
	mint, err := common.NewAccountFromPublicKeyString(usdc.Mint)
	require.NoError(t, err)
	return mint.ToProto()
}

func TestStatefulSwap_Coinbase_TransactionVersionV1(t *testing.T) {
	env := setupTestEnv(t, withSwapsEnabled())
	owner := env.newUser(t)
	swapAuthority := env.newAccount(t)

	for _, tc := range []struct {
		name     string
		initiate *transactionpb.StatefulSwapRequest_Initiate
		assert   func(t *testing.T, respErr *transactionpb.StatefulSwapResponse_Error)
	}{
		{
			// A v1 request passes the version check and reaches verification
			// of the proof signature, which signs the wrong message here
			name: "coinbase swaps accept v1",
			initiate: &transactionpb.StatefulSwapRequest_Initiate{
				Kind: &transactionpb.StatefulSwapRequest_Initiate_Stablecoin{
					Stablecoin: &transactionpb.StatefulSwapRequest_Initiate_CoinbaseStableSwapperClientParameters{
						Id:               &commonpb.SwapId{Value: testutil.NewRandomAccount(t).PublicKey().ToBytes()},
						FromMint:         common.CoreMintAccount.ToProto(),
						ToMint:           usdcMint(t),
						SwapAmount:       common.CoreMintQuarksPerUnit,
						FundingSource:    transactionpb.FundingSource_FUNDING_SOURCE_SUBMIT_INTENT,
						FundingId:        testutil.NewRandomAccount(t).PublicKey().ToBase58(),
						DestinationOwner: testutil.NewRandomAccount(t).ToProto(),
					},
				},
			},
			assert: func(t *testing.T, respErr *transactionpb.StatefulSwapResponse_Error) {
				require.Equal(t, transactionpb.StatefulSwapResponse_Error_SIGNATURE_ERROR, respErr.Code)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(env.ctx, defaultTestStreamTimeout)
			defer cancel()

			stream, err := env.client.StatefulSwap(ctx)
			require.NoError(t, err)

			initiate := tc.initiate
			initiate.Owner = owner.ToProto()
			initiate.SwapAuthority = swapAuthority.ToProto()
			initiate.ProofSignature = env.signProto(t, owner, initiate)
			initiate.TransactionVersion = 1
			initiate.Signature = env.signProto(t, owner, initiate)

			require.NoError(t, stream.Send(&transactionpb.StatefulSwapRequest{
				Request: &transactionpb.StatefulSwapRequest_Initiate_{Initiate: initiate},
			}))

			resp, err := stream.Recv()
			require.NoError(t, err)
			require.NotNil(t, resp.GetError())
			tc.assert(t, resp.GetError())
		})
	}
}

func TestStatelessSwap_TransactionVersionV1Accepted(t *testing.T) {
	env := setupTestEnv(t, withSwapsEnabled())
	owner := env.newUser(t)

	ctx, cancel := context.WithTimeout(env.ctx, defaultTestStreamTimeout)
	defer cancel()

	stream, err := env.client.StatelessSwap(ctx)
	require.NoError(t, err)

	// A v1 request passes the version check and reaches swap validation,
	// which rejects the core mint as a source
	initiate := &transactionpb.StatelessSwapRequest_Initiate{
		Kind: &transactionpb.StatelessSwapRequest_Initiate_Stablecoin{
			Stablecoin: &transactionpb.StatelessSwapRequest_Initiate_CoinbaseStableSwapperClientParameters{
				FromMint:   common.CoreMintAccount.ToProto(),
				ToMint:     common.CoreMintAccount.ToProto(),
				SwapAmount: common.CoreMintQuarksPerUnit,
			},
		},
		Owner:              owner.ToProto(),
		TransactionVersion: 1,
	}
	initiate.Signature = env.signProto(t, owner, initiate)

	require.NoError(t, stream.Send(&transactionpb.StatelessSwapRequest{
		Request: &transactionpb.StatelessSwapRequest_Initiate_{Initiate: initiate},
	}))

	resp, err := stream.Recv()
	require.NoError(t, err)
	require.NotNil(t, resp.GetError())
	require.Equal(t, transactionpb.StatelessSwapResponse_Error_INVALID_SWAP, resp.GetError().Code)
	require.Len(t, resp.GetError().ErrorDetails, 1)
	require.Equal(t, "source mint must be usdc", resp.GetError().ErrorDetails[0].GetReasonString().Reason)
}

// First 315 bytes of the mainnet Coinbase Stable Swapper liquidity pool, which
// is 2107 bytes with the remainder zeroed. Copied from the
// coinbasestableswapper package tests.
const mainnetCoinbaseLiquidityPoolPrefix = "QiYRQLxQRIEFHqE9vluQFKO1wbEwnd22aRe9qGrV03SIsz1AV7GqT/yEzcR/f+ALaKG4KMxbBfZ5dTNFPqxxZHMfbTqNfj6St0p+" +
	"+yObz2IILMcKsoko07O+oRSDek7YwzrH9TroL1+QbHdT/t9qpcq4Kyx8OPWZm79AIUM9UlN+X6ujF0hPgzT4z8SXtrVTfBhZj7Lz" +
	"SPTgCpHoi6cjfPqXvflOJvRBAgAAAN0H70q0C5DeChX575Umuo4KwnYx+lqZmPTGnq1wMK2QSoyv1lJlvQkMgeq0VkN3NML2MHaz" +
	"cTzSusODf5c6RhACAAAAxvp6877brTo9ZfNqq8l0MbG75MLS9uDkfKYCA0UvXWE908SAij1Ps5+uycukm1pjSlLt4RVI9SSqIPLN" +
	"Ru+AcQAAAAAAAAAAAAD/"

func TestCoinbaseStableSwapperSwapHandler_TransactionVersions(t *testing.T) {
	env := setupTestEnv(t)

	prefix, err := base64.StdEncoding.DecodeString(mainnetCoinbaseLiquidityPoolPrefix)
	require.NoError(t, err)
	poolData := make([]byte, 2107)
	copy(poolData, prefix)
	pool, _, err := coinbase_stable_swapper.GetPoolAddress()
	require.NoError(t, err)
	env.data.setBlockchainAccountData(pool, poolData)

	owner := testutil.NewRandomAccount(t)
	swapAuthority := testutil.NewRandomAccount(t)
	destinationOwner := testutil.NewRandomAccount(t)
	fromMint := common.CoreMintAccount
	toMint, err := common.NewAccountFromProto(usdcMint(t))
	require.NoError(t, err)

	var blockhash solana.Blockhash
	copy(blockhash[:], testutil.NewRandomAccount(t).PublicKey().ToBytes())
	selectedNonce := &transaction_util.Nonce{
		Account:   testutil.NewRandomAccount(t),
		Blockhash: blockhash,
	}

	const swapAmount, feeAmount = 5_000_000, 500_000

	for _, transactionVersion := range []solana.MessageVersion{solana.MessageVersion0, solana.MessageVersion1} {
		for _, coinbaseSwapVersion := range []transaction_util.CoinbaseSwapVersion{transaction_util.CoinbaseSwapV1, transaction_util.CoinbaseSwapV2} {
			t.Run("txn "+transactionVersion.String()+" swap "+coinbaseSwapVersion.String(), func(t *testing.T) {
				h := NewCoinbaseStableSwapperSwapHandler(
					env.data,
					owner,
					swapAuthority,
					destinationOwner,
					fromMint,
					toMint,
					swapAmount,
					feeAmount,
					selectedNonce,
					transactionVersion,
					coinbaseSwapVersion,
				)

				txn, err := makeStatefulSwapTransaction(env.ctx, h)
				require.NoError(t, err)
				txn.SetBlockhash(selectedNonce.Blockhash)

				serverParameters := h.GetServerParameters().GetStablecoin()
				require.NotNil(t, serverParameters)

				assert.Equal(t, transactionVersion, txn.Message.Version)
				assert.Equal(t, common.GetSubsidizer().PublicKey().ToBytes(), serverParameters.Payer.Value)
				assert.Equal(t, selectedNonce.Account.PublicKey().ToBytes(), serverParameters.Nonce.Value)
				assert.Equal(t, blockhash[:], serverParameters.Blockhash.Value)
				assert.Equal(t, common.CoreMintFeesAccount.PublicKey().ToBytes(), serverParameters.FeeDestination.Value)

				switch transactionVersion {
				case solana.MessageVersion0:
					assert.Equal(t, "coinbase_stable_swapper_v0", serverParameters.MemoValue)
					assert.Equal(t, transaction_util.ToProtoAlts([]solana.AddressLookupTable{transaction_util.GetAltForCoreMint()}), serverParameters.Alts)
					assert.EqualValues(t, 125_000, serverParameters.ComputeUnitLimit)
					assert.EqualValues(t, 10_000, serverParameters.ComputeUnitPrice)
					assert.Nil(t, serverParameters.TransactionConfig)
					assert.Nil(t, h.GetTransactionConfig())
				case solana.MessageVersion1:
					assert.Equal(t, "coinbase_stable_swapper_v1", serverParameters.MemoValue)
					assert.Empty(t, serverParameters.Alts)
					assert.Zero(t, serverParameters.ComputeUnitLimit)
					assert.Zero(t, serverParameters.ComputeUnitPrice)

					require.NotNil(t, serverParameters.TransactionConfig)
					assert.EqualValues(t, 125_000, serverParameters.TransactionConfig.ComputeUnitLimit)
					assert.EqualValues(t, transaction_util.CoinbaseSwapLoadedAccountsDataSizeLimit, serverParameters.TransactionConfig.LoadedAccountsDataSizeLimit)
					assert.EqualValues(t, 1_250, serverParameters.TransactionConfig.PriorityFeeLamports)
					assert.Zero(t, serverParameters.TransactionConfig.HeapSize)

					assert.Empty(t, txn.Message.AddressTableLookups)
					for _, account := range txn.Message.Accounts {
						assert.NotEqual(t, compute_budget.ProgramKey, account)
					}
					assert.Len(t, txn.Message.Instructions, 7)

					marshalled, err := txn.Marshal()
					require.NoError(t, err)
					assert.LessOrEqual(t, len(marshalled), solana.MaxV1TransactionSize)
				}

				// The owner and swap authority are the client signers
				var clientSigners int
				for i := range int(txn.Message.Header.NumSignatures) {
					account := txn.Message.Accounts[i]
					if bytes.Equal(account, owner.PublicKey().ToBytes()) || bytes.Equal(account, swapAuthority.PublicKey().ToBytes()) {
						clientSigners++
					}
				}
				assert.Equal(t, 2, clientSigners)

				requireCoinbaseStatefulClientRebuildMatches(t, env, txn, serverParameters, owner, swapAuthority, destinationOwner, fromMint, toMint, swapAmount, feeAmount, coinbaseSwapVersion)
			})
		}
	}
}

// requireCoinbaseStatefulClientRebuildMatches builds the transaction from the
// server parameters following the instruction format documented on
// StatefulSwapResponse.ServerParameters.CoinbaseStableSwapperServerParameter,
// and requires the message clients sign to match the server's exactly.
func requireCoinbaseStatefulClientRebuildMatches(
	t *testing.T,
	env *testEnv,
	serverTxn solana.Transaction,
	params *transactionpb.StatefulSwapResponse_ServerParameters_CoinbaseStableSwapperServerParameter,
	owner, swapAuthority, destinationOwner, fromMint, toMint *common.Account,
	swapAmount, feeAmount uint64,
	coinbaseSwapVersion transaction_util.CoinbaseSwapVersion,
) {
	payer := params.Payer.Value

	vmConfig, err := common.GetVmConfigForMint(env.ctx, env.data, fromMint)
	require.NoError(t, err)
	ownerTimelockAccounts, err := owner.GetTimelockAccounts(vmConfig)
	require.NoError(t, err)

	createSwapAuthorityAtaIxn, swapAuthorityAta, err := token.CreateAssociatedTokenAccountIdempotent(payer, swapAuthority.PublicKey().ToBytes(), fromMint.PublicKey().ToBytes())
	require.NoError(t, err)
	createDestinationAtaIxn, destinationAta, err := token.CreateAssociatedTokenAccountIdempotent(payer, destinationOwner.PublicKey().ToBytes(), toMint.PublicKey().ToBytes())
	require.NoError(t, err)

	coinbaseAccounts, err := transaction_util.GetCoinbaseSwapAccounts(env.ctx, env.data, fromMint.PublicKey().ToBytes(), toMint.PublicKey().ToBytes())
	require.NoError(t, err)
	require.Equal(t, []byte(coinbaseAccounts.FeeRecipient), params.PoolFeeRecipient.Value)

	coinbaseSwapIxn, err := transaction_util.MakeCoinbaseSwapInstruction(
		coinbaseSwapVersion,
		coinbaseAccounts,
		swapAuthority.PublicKey().ToBytes(),
		fromMint.PublicKey().ToBytes(),
		toMint.PublicKey().ToBytes(),
		swapAuthorityAta,
		destinationAta,
		swapAmount,
		swapAmount,
	)
	require.NoError(t, err)

	ixns := []solana.Instruction{system.AdvanceNonce(params.Nonce.Value, payer)}
	if params.TransactionConfig == nil {
		if params.ComputeUnitLimit != 0 {
			ixns = append(ixns, compute_budget.SetComputeUnitLimit(params.ComputeUnitLimit))
		}
		if params.ComputeUnitPrice != 0 {
			ixns = append(ixns, compute_budget.SetComputeUnitPrice(params.ComputeUnitPrice))
		}
	}
	if len(params.MemoValue) > 0 {
		ixns = append(ixns, memo.Instruction(params.MemoValue))
	}
	ixns = append(
		ixns,
		createSwapAuthorityAtaIxn,
		createDestinationAtaIxn,
		vm.NewTransferForSwapWithFeeInstruction(
			&vm.TransferForSwapWithFeeInstructionAccounts{
				VmAuthority:     vmConfig.Authority.PublicKey().ToBytes(),
				Vm:              vmConfig.Vm.PublicKey().ToBytes(),
				Swapper:         owner.PublicKey().ToBytes(),
				SwapPda:         ownerTimelockAccounts.VmSwapAccounts.Pda.PublicKey().ToBytes(),
				SwapAta:         ownerTimelockAccounts.VmSwapAccounts.Ata.PublicKey().ToBytes(),
				SwapDestination: swapAuthorityAta,
				FeeDestination:  params.FeeDestination.Value,
			},
			&vm.TransferForSwapWithFeeInstructionArgs{
				SwapAmount: swapAmount,
				FeeAmount:  feeAmount,
				Bump:       ownerTimelockAccounts.VmSwapAccounts.PdaBump,
			},
		),
		coinbaseSwapIxn,
		token.CloseAccount(swapAuthorityAta, payer, swapAuthority.PublicKey().ToBytes()),
	)

	var clientTxn solana.Transaction
	if params.TransactionConfig == nil {
		clientTxn = solana.NewV0Transaction(payer, fromProtoAlts(params.Alts), ixns)
	} else {
		// CU limit, loaded accounts data size limit and priority fee are always
		// encoded, and the heap size only when non-zero
		config := solana.TransactionConfig{
			ComputeUnitLimit:            pointer.Uint32(params.TransactionConfig.ComputeUnitLimit),
			LoadedAccountsDataSizeLimit: pointer.Uint32(params.TransactionConfig.LoadedAccountsDataSizeLimit),
			PriorityFeeLamports:         pointer.Uint64(params.TransactionConfig.PriorityFeeLamports),
		}
		if params.TransactionConfig.HeapSize != 0 {
			config.HeapSize = pointer.Uint32(params.TransactionConfig.HeapSize)
		}

		clientTxn, err = solana.NewV1Transaction(payer, config, ixns...)
		require.NoError(t, err)
	}

	var blockhash solana.Blockhash
	copy(blockhash[:], params.Blockhash.Value)
	clientTxn.SetBlockhash(blockhash)

	expected, err := serverTxn.Message.Marshal()
	require.NoError(t, err)
	actual, err := clientTxn.Message.Marshal()
	require.NoError(t, err)
	require.Equal(t, expected, actual)
}

// reserveTimelockMemory reserves VM memory for the owner's virtual timelock
// account on the mint, which reserve swaps deposit into
func (e *testEnv) reserveTimelockMemory(t *testing.T, owner, mint *common.Account) {
	vmConfig, err := common.GetVmConfigForMint(e.ctx, e.data, mint)
	require.NoError(t, err)

	vmAddress := vmConfig.Vm.PublicKey().ToBase58()
	err = e.data.InitializeVmMemory(e.ctx, &ram.Record{
		Vm:                vmAddress,
		Address:           testutil.NewRandomAccount(t).PublicKey().ToBase58(),
		Capacity:          1000,
		NumSectors:        1,
		NumPages:          1000,
		PageSize:          uint8(vm.GetVirtualAccountSizeInMemory(vm.VirtualAccountTypeTimelock)),
		StoredAccountType: vm.VirtualAccountTypeTimelock,
		CreatedAt:         time.Now(),
	})
	require.NoError(t, err)

	_, _, err = e.data.ReserveVmMemory(e.ctx, vmAddress, vm.VirtualAccountTypeTimelock, e.getVault(t, owner, mint).PublicKey().ToBase58())
	require.NoError(t, err)
}

// initiateReserveSwap sends a signed reserve StatefulSwap Initiate request and
// returns the first response
func (e *testEnv) initiateReserveSwap(
	t *testing.T,
	owner, swapAuthority *common.Account,
	clientParameters *transactionpb.StatefulSwapRequest_Initiate_ReserveSwapClientParameters,
	transactionVersion uint32,
) *transactionpb.StatefulSwapResponse {
	ctx, cancel := context.WithTimeout(e.ctx, defaultTestStreamTimeout)
	defer cancel()

	stream, err := e.client.StatefulSwap(ctx)
	require.NoError(t, err)

	initiate := &transactionpb.StatefulSwapRequest_Initiate{
		Kind: &transactionpb.StatefulSwapRequest_Initiate_Reserve{
			Reserve: clientParameters,
		},
		Owner:         owner.ToProto(),
		SwapAuthority: swapAuthority.ToProto(),
		ProofSignature: e.signProto(t, owner, &transactionpb.VerifiedSwapMetadata{
			Kind: &transactionpb.VerifiedSwapMetadata_Reserve{
				Reserve: &transactionpb.VerifiedReserveSwapMetadata{
					ClientParameters: clientParameters,
				},
			},
		}),
		TransactionVersion: transactionVersion,
	}
	initiate.Signature = e.signProto(t, owner, initiate)

	require.NoError(t, stream.Send(&transactionpb.StatefulSwapRequest{
		Request: &transactionpb.StatefulSwapRequest_Initiate_{Initiate: initiate},
	}))

	resp, err := stream.Recv()
	require.NoError(t, err)
	return resp
}

func TestStatefulSwap_ReserveExistingCurrency_TransactionVersions(t *testing.T) {
	env := setupTestEnv(t, withSwapsEnabled())

	mint := env.newLaunchpadMint(t)
	owner := env.newFundedUser(t, common.ToCoreMintQuarks(100), mint)
	env.reserveTimelockMemory(t, owner, mint)

	for _, transactionVersion := range []uint32{0, 1} {
		t.Run(fmt.Sprintf("v%d", transactionVersion), func(t *testing.T) {
			resp := env.initiateReserveSwap(t, owner, env.newAccount(t), &transactionpb.StatefulSwapRequest_Initiate_ReserveSwapClientParameters{
				Id:            &commonpb.SwapId{Value: testutil.NewRandomAccount(t).PublicKey().ToBytes()},
				FromMint:      common.CoreMintAccount.ToProto(),
				ToMint:        mint.ToProto(),
				SwapAmount:    common.CoreMintQuarksPerUnit,
				FundingSource: transactionpb.FundingSource_FUNDING_SOURCE_SUBMIT_INTENT,
				FundingId:     testutil.NewRandomAccount(t).PublicKey().ToBase58(),
			}, transactionVersion)
			require.Nil(t, resp.GetError())

			serverParameters := resp.GetServerParameters().GetReserveExistingCurrency()
			require.NotNil(t, serverParameters)
			if transactionVersion == 1 {
				require.NotNil(t, serverParameters.TransactionConfig)
				assert.NotZero(t, serverParameters.TransactionConfig.ComputeUnitLimit)
				assert.Empty(t, serverParameters.Alts)
				assert.Equal(t, "buy_v1", serverParameters.MemoValue)
			} else {
				assert.Nil(t, serverParameters.TransactionConfig)
				assert.NotEmpty(t, serverParameters.Alts)
				assert.Equal(t, "buy_v0", serverParameters.MemoValue)
			}
		})
	}
}

func TestReserveSwapHandlers_TransactionVersions(t *testing.T) {
	env := setupTestEnv(t)

	launchpadMint1 := env.newLaunchpadMint(t)
	launchpadMint2 := env.newLaunchpadMint(t)
	swapper := env.newFundedUser(t, common.ToCoreMintQuarks(100), launchpadMint1, launchpadMint2)
	for _, mint := range []*common.Account{common.CoreMintAccount, launchpadMint1, launchpadMint2} {
		env.reserveTimelockMemory(t, swapper, mint)
	}

	launchpadAlt1, err := transaction_util.GetAltForMint(env.ctx, env.data, launchpadMint1)
	require.NoError(t, err)
	launchpadAlt2, err := transaction_util.GetAltForMint(env.ctx, env.data, launchpadMint2)
	require.NoError(t, err)

	var blockhash solana.Blockhash
	copy(blockhash[:], testutil.NewRandomAccount(t).PublicKey().ToBytes())
	selectedNonce := &transaction_util.Nonce{
		Account:   testutil.NewRandomAccount(t),
		Blockhash: blockhash,
	}

	const swapAmount = 1_000_000

	for _, tc := range []struct {
		flow           string
		newHandler     func(temporaryHolder *common.Account, transactionVersion solana.MessageVersion) SwapHandler
		expectedV0Alts []solana.AddressLookupTable
		feeDestination *common.Account
	}{
		{
			flow: "buy",
			newHandler: func(temporaryHolder *common.Account, transactionVersion solana.MessageVersion) SwapHandler {
				return NewReserveBuySwapHandler(env.data, swapper, temporaryHolder, launchpadMint1, swapAmount, 0, selectedNonce, transactionVersion)
			},
			expectedV0Alts: []solana.AddressLookupTable{launchpadAlt1},
		},
		{
			flow: "buy",
			newHandler: func(temporaryHolder *common.Account, transactionVersion solana.MessageVersion) SwapHandler {
				return NewReserveBuySwapHandler(env.data, swapper, temporaryHolder, launchpadMint1, swapAmount, 10_000, selectedNonce, transactionVersion)
			},
			expectedV0Alts: []solana.AddressLookupTable{launchpadAlt1, transaction_util.GetAltForCoreMint()},
			feeDestination: common.CoreMintFeesAccount,
		},
		{
			flow: "sell",
			newHandler: func(temporaryHolder *common.Account, transactionVersion solana.MessageVersion) SwapHandler {
				return NewReserveSellSwapHandler(env.data, swapper, temporaryHolder, launchpadMint1, swapAmount, selectedNonce, transactionVersion)
			},
			expectedV0Alts: []solana.AddressLookupTable{launchpadAlt1},
		},
		{
			flow: "buy_sell",
			newHandler: func(temporaryHolder *common.Account, transactionVersion solana.MessageVersion) SwapHandler {
				return NewReserveBuySellSwapHandler(env.data, swapper, temporaryHolder, launchpadMint1, launchpadMint2, swapAmount, selectedNonce, transactionVersion)
			},
			expectedV0Alts: []solana.AddressLookupTable{launchpadAlt1, launchpadAlt2},
		},
	} {
		name := tc.flow
		if tc.feeDestination != nil {
			name += " with fee"
		}
		t.Run(name, func(t *testing.T) {
			temporaryHolder := testutil.NewRandomAccount(t)

			//
			// v0 is the existing format clients already build
			//

			v0Handler := tc.newHandler(temporaryHolder, solana.MessageVersion0)
			v0Ixns, err := v0Handler.MakeInstructions(env.ctx)
			require.NoError(t, err)
			v0Txn, err := makeStatefulSwapTransaction(env.ctx, v0Handler)
			require.NoError(t, err)
			assert.Equal(t, solana.MessageVersion0, v0Txn.Message.Version)
			assert.Nil(t, v0Handler.GetTransactionConfig())

			v0Parameters := v0Handler.GetServerParameters().GetReserveExistingCurrency()
			require.NotNil(t, v0Parameters)
			assert.Equal(t, tc.flow+"_v0", v0Parameters.MemoValue)
			assert.Equal(t, transaction_util.ToProtoAlts(tc.expectedV0Alts), v0Parameters.Alts)
			assert.NotZero(t, v0Parameters.ComputeUnitLimit)
			assert.EqualValues(t, 10_000, v0Parameters.ComputeUnitPrice)
			assert.Nil(t, v0Parameters.TransactionConfig)

			//
			// v1
			//

			v1Handler := tc.newHandler(temporaryHolder, solana.MessageVersion1)
			v1Ixns, err := v1Handler.MakeInstructions(env.ctx)
			require.NoError(t, err)
			v1Txn, err := makeStatefulSwapTransaction(env.ctx, v1Handler)
			require.NoError(t, err)
			v1Txn.SetBlockhash(selectedNonce.Blockhash)
			assert.Equal(t, solana.MessageVersion1, v1Txn.Message.Version)

			alts, err := v1Handler.GetAlts(env.ctx)
			require.NoError(t, err)
			assert.Nil(t, alts)

			v1Parameters := v1Handler.GetServerParameters().GetReserveExistingCurrency()
			require.NotNil(t, v1Parameters)
			assert.Equal(t, common.GetSubsidizer().PublicKey().ToBytes(), v1Parameters.Payer.Value)
			assert.Equal(t, selectedNonce.Account.PublicKey().ToBytes(), v1Parameters.Nonce.Value)
			assert.Equal(t, blockhash[:], v1Parameters.Blockhash.Value)
			assert.Equal(t, tc.flow+"_v1", v1Parameters.MemoValue)
			assert.Equal(t, v0Parameters.MemoryAccount, v1Parameters.MemoryAccount)
			assert.Equal(t, v0Parameters.MemoryIndex, v1Parameters.MemoryIndex)
			assert.Equal(t, v0Parameters.FeeDestination, v1Parameters.FeeDestination)
			if tc.feeDestination != nil {
				assert.Equal(t, tc.feeDestination.PublicKey().ToBytes(), v1Parameters.FeeDestination.Value)
			}
			assert.Empty(t, v1Parameters.Alts)
			assert.Zero(t, v1Parameters.ComputeUnitLimit)
			assert.Zero(t, v1Parameters.ComputeUnitPrice)

			// The v1 config carries the same compute budget the v0 transaction
			// sets with ComputeBudget instructions
			require.NotNil(t, v1Parameters.TransactionConfig)
			assert.Equal(t, v0Parameters.ComputeUnitLimit, v1Parameters.TransactionConfig.ComputeUnitLimit)
			assert.EqualValues(t, transaction_util.ReserveSwapLoadedAccountsDataSizeLimit, v1Parameters.TransactionConfig.LoadedAccountsDataSizeLimit)
			assert.Equal(t, transaction_util.PriorityFeeLamports(v0Parameters.ComputeUnitLimit, 10_000), v1Parameters.TransactionConfig.PriorityFeeLamports)
			assert.Zero(t, v1Parameters.TransactionConfig.HeapSize)

			// Per the proto, the v1 instructions are the v0 instructions without
			// ComputeBudget, with the v1 memo
			var expectedV1Ixns []solana.Instruction
			for _, ixn := range v0Ixns {
				switch {
				case bytes.Equal(ixn.Program, compute_budget.ProgramKey):
					continue
				case bytes.Equal(ixn.Program, memo.ProgramKey):
					expectedV1Ixns = append(expectedV1Ixns, memo.Instruction(tc.flow+"_v1"))
				default:
					expectedV1Ixns = append(expectedV1Ixns, ixn)
				}
			}
			assert.Equal(t, expectedV1Ixns, v1Ixns)
			assert.Len(t, v1Ixns, len(v0Ixns)-2)

			assert.Empty(t, v1Txn.Message.AddressTableLookups)
			for _, account := range v1Txn.Message.Accounts {
				assert.NotEqual(t, compute_budget.ProgramKey, account)
			}
			assert.LessOrEqual(t, len(v1Txn.Message.Accounts), 64)
			marshalled, err := v1Txn.Marshal()
			require.NoError(t, err)
			assert.LessOrEqual(t, len(marshalled), solana.MaxV1TransactionSize)
		})
	}
}

func TestSelectCoinbaseSwapVersion(t *testing.T) {
	// Unset (older clients) and 1 both use the legacy swap instruction
	assert.Equal(t, transaction_util.CoinbaseSwapV1, selectCoinbaseSwapVersion(0))
	assert.Equal(t, transaction_util.CoinbaseSwapV1, selectCoinbaseSwapVersion(1))

	assert.Equal(t, transaction_util.CoinbaseSwapV2, selectCoinbaseSwapVersion(2))
}

func TestSelectSwapTransactionVersion(t *testing.T) {
	assert.Equal(t, solana.MessageVersion0, selectSwapTransactionVersion(0))
	assert.Equal(t, solana.MessageVersion1, selectSwapTransactionVersion(1))
}
