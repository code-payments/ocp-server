package transaction_v2

import (
	"context"
	"errors"
	"sync"

	"go.uber.org/zap"

	indexerpb "github.com/code-payments/code-vm-indexer/generated/indexer/v1"
	transactionpb "github.com/code-payments/ocp-protobuf-api/generated/go/transaction/v2"

	"github.com/code-payments/ocp-server/pkg/code/aml"
	"github.com/code-payments/ocp-server/pkg/code/antispam"
	auth_util "github.com/code-payments/ocp-server/pkg/code/auth"
	"github.com/code-payments/ocp-server/pkg/code/common"
	code_data "github.com/code-payments/ocp-server/pkg/code/data"
	"github.com/code-payments/ocp-server/pkg/code/data/nonce"
	"github.com/code-payments/ocp-server/pkg/code/transaction"
)

type transactionServer struct {
	conf *conf

	log *zap.Logger

	data            code_data.Provider
	vmIndexerClient indexerpb.IndexerClient

	auth *auth_util.RPCSignatureVerifier

	submitIntentIntegration SubmitIntentIntegration
	airdropIntegration      AirdropIntegration

	antispamGuard *antispam.Guard
	amlGuard      *aml.Guard

	noncePools []*transaction.LocalNoncePool

	localAccountLocksMu sync.Mutex
	localAccountLocks   map[string]*sync.Mutex

	airdropperLock sync.Mutex
	airdropper     *common.TimelockAccounts

	feeCollector *common.Account

	transactionpb.UnimplementedTransactionServer
}

func NewTransactionServer(
	log *zap.Logger,
	data code_data.Provider,
	vmIndexerClient indexerpb.IndexerClient,
	submitIntentIntegration SubmitIntentIntegration,
	airdropIntegration AirdropIntegration,
	antispamGuard *antispam.Guard,
	amlGuard *aml.Guard,
	noncePools []*transaction.LocalNoncePool,
	configProvider ConfigProvider,
) (transactionpb.TransactionServer, error) {
	ctx := context.Background()

	conf := configProvider()

	if !conf.disableSubmitIntent.Get(ctx) {
		_, err := transaction.SelectNoncePool(
			nonce.EnvironmentCvm,
			common.CodeVmAccount.PublicKey().ToBase58(),
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

		data:            data,
		vmIndexerClient: vmIndexerClient,

		auth: auth_util.NewRPCSignatureVerifier(log, data),

		submitIntentIntegration: submitIntentIntegration,
		airdropIntegration:      airdropIntegration,

		antispamGuard: antispamGuard,
		amlGuard:      amlGuard,

		noncePools: noncePools,

		localAccountLocks: make(map[string]*sync.Mutex),
	}

	var err error
	s.feeCollector, err = common.NewAccountFromPublicKeyString(s.conf.feeCollectorOwnerPublicKey.Get(ctx))
	if err != nil {
		return nil, err
	}

	airdropper := s.conf.airdropperOwnerPublicKey.Get(ctx)
	if len(airdropper) > 0 && airdropper != defaultAirdropperOwnerPublicKey {
		err := s.loadAirdropper(ctx)
		if err != nil {
			return nil, err
		}
	}

	return s, nil
}
