package currency

import (
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/aws/aws-sdk-go-v2/service/s3"

	currencypb "github.com/code-payments/ocp-protobuf-api/generated/go/currency/v1"

	"github.com/code-payments/ocp-server/cache"
	"github.com/code-payments/ocp-server/ocp/antispam"
	auth_util "github.com/code-payments/ocp-server/ocp/auth"
	currency_util "github.com/code-payments/ocp-server/ocp/currency"
	ocp_data "github.com/code-payments/ocp-server/ocp/data"
)

type cachedProtoMint struct {
	mint          *currencypb.Mint
	lastUpdatedAt time.Time
}

type currencyServer struct {
	log *zap.Logger

	conf *conf

	data ocp_data.Provider

	auth *auth_util.RPCSignatureVerifier

	antispamGuard *antispam.Guard

	s3Client *s3.Client

	exchangeRateHistoryCache cache.Cache
	reserveHistoryCache      cache.Cache

	getMintsCacheMu sync.RWMutex
	getMintsCache   map[string]*cachedProtoMint

	mintDataProvider *currency_util.MintDataProvider

	currencypb.UnimplementedCurrencyServer
}

func NewCurrencyServer(
	log *zap.Logger,
	data ocp_data.Provider,
	mintDataProvider *currency_util.MintDataProvider,
	antispamGuard *antispam.Guard,
	s3Client *s3.Client,
	configProvider ConfigProvider,
) currencypb.CurrencyServer {
	conf := configProvider()

	s := &currencyServer{
		log: log,

		conf: conf,

		data:             data,
		mintDataProvider: mintDataProvider,

		auth: auth_util.NewRPCSignatureVerifier(log, data),

		antispamGuard: antispamGuard,

		s3Client: s3Client,

		exchangeRateHistoryCache: cache.NewCache(1_000),
		reserveHistoryCache:      cache.NewCache(1_000),

		getMintsCache: make(map[string]*cachedProtoMint),
	}

	return s
}
