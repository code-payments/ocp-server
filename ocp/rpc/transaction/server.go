package transaction

import (
	"context"
	"errors"
	"sync"

	"go.uber.org/zap"

	transactionpb "github.com/code-payments/ocp-protobuf-api/generated/go/transaction/v1"

	"github.com/code-payments/ocp-server/coinbase"
	"github.com/code-payments/ocp-server/ocp/aml"
	"github.com/code-payments/ocp-server/ocp/antispam"
	auth_util "github.com/code-payments/ocp-server/ocp/auth"
	"github.com/code-payments/ocp-server/ocp/common"
	currency_util "github.com/code-payments/ocp-server/ocp/currency"
	ocp_data "github.com/code-payments/ocp-server/ocp/data"
	"github.com/code-payments/ocp-server/ocp/data/nonce"
	"github.com/code-payments/ocp-server/ocp/integration"
	"github.com/code-payments/ocp-server/ocp/transaction"
)

type transactionServer struct {
	conf *conf

	log *zap.Logger

	data             ocp_data.Provider
	mintDataProvider *currency_util.MintDataProvider

	auth *auth_util.RPCSignatureVerifier

	submitIntentIntegration integration.SubmitIntent

	antispamGuard *antispam.Guard
	amlGuard      *aml.Guard

	coinbaseClient *coinbase.Client

	nodeID     string
	noncePools []*transaction.LocalNoncePool

	localAccountLocksMu sync.Mutex
	localAccountLocks   map[string]*sync.Mutex

	feeCollector *common.Account

	transactionpb.UnimplementedTransactionServer
}

func NewTransactionServer(
	log *zap.Logger,
	data ocp_data.Provider,
	mintDataProvider *currency_util.MintDataProvider,
	submitIntentIntegration integration.SubmitIntent,
	antispamGuard *antispam.Guard,
	amlGuard *aml.Guard,
	coinbaseClient *coinbase.Client,
	nodeID string,
	noncePools []*transaction.LocalNoncePool,
	configProvider ConfigProvider,
) (transactionpb.TransactionServer, error) {
	ctx := context.Background()

	conf := configProvider()

	if !conf.disableSubmitIntent.Get(ctx) {
		_, err := transaction.SelectNoncePool(
			nonce.EnvironmentVm,
			common.CoreMintVmAccount.PublicKey().ToBase58(),
			nonce.PurposeClientIntent,
			noncePools...,
		)
		if err != nil {
			return nil, errors.New("nonce pool for core mint intent operations is not provided")
		}
	}

	if !conf.disableSwaps.Get(ctx) {
		_, err := transaction.SelectNoncePool(
			nonce.EnvironmentSolana,
			nonce.EnvironmentInstanceSolanaMainnet,
			nonce.PurposeClientSwap,
			noncePools...,
		)
		if err != nil {
			return nil, errors.New("nonce pool for swap operations is not provided")
		}
	}

	s := &transactionServer{
		conf: conf,

		log: log,

		data:             data,
		mintDataProvider: mintDataProvider,

		auth: auth_util.NewRPCSignatureVerifier(log, data),

		submitIntentIntegration: submitIntentIntegration,

		antispamGuard: antispamGuard,
		amlGuard:      amlGuard,

		coinbaseClient: coinbaseClient,

		nodeID:     nodeID,
		noncePools: noncePools,

		localAccountLocks: make(map[string]*sync.Mutex),
	}

	var err error
	s.feeCollector, err = common.NewAccountFromPublicKeyString(s.conf.feeCollectorOwnerPublicKey.Get(ctx))
	if err != nil {
		return nil, err
	}

	return s, nil
}
