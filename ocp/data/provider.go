package data

import (
	pg "github.com/code-payments/ocp-server/database/postgres"
)

const (
	maxCurrencyHistoryReqSize = 1024
)

type Provider interface {
	BlockchainData
	DatabaseData
	WebData

	GetBlockchainDataProvider() BlockchainData
	GetDatabaseDataProvider() DatabaseData
	GetWebDataProvider() WebData
}

type DataProvider struct {
	*BlockchainProvider
	*DatabaseProvider
	*WebProvider
}

func NewDataProvider(dbConfig *pg.Config, solanaEnv string, configProvider ConfigProvider) (Provider, error) {
	blockchain, err := NewBlockchainProvider(solanaEnv)
	if err != nil {
		return nil, err
	}

	p, err := NewDataProviderWithoutBlockchain(dbConfig, configProvider)
	if err != nil {
		return nil, err
	}

	provider := p.(*DataProvider)
	provider.BlockchainProvider = blockchain.(*BlockchainProvider)

	return provider, nil
}

func NewDataProviderWithoutBlockchain(dbConfig *pg.Config, configProvider ConfigProvider) (Provider, error) {
	db, err := NewDatabaseProvider(dbConfig)
	if err != nil {
		return nil, err
	}

	web, err := NewWebProvider(configProvider)
	if err != nil {
		return nil, err
	}

	provider := &DataProvider{
		DatabaseProvider: db.(*DatabaseProvider),
		WebProvider:      web.(*WebProvider),
	}

	return provider, nil
}

func NewTestDataProvider() Provider {
	// todo: This currently only includes database data and should include the
	//       other provider types.

	blockchain, err := NewBlockchainProvider("https://api.testnet.solana.com")
	if err != nil {
		panic(err)
	}

	return &DataProvider{
		DatabaseProvider:   NewTestDatabaseProvider().(*DatabaseProvider),
		BlockchainProvider: blockchain.(*BlockchainProvider),
	}
}

func (p *DataProvider) GetBlockchainDataProvider() BlockchainData {
	return p.BlockchainProvider
}
func (p *DataProvider) GetWebDataProvider() WebData {
	return p.WebProvider
}
func (p *DataProvider) GetDatabaseDataProvider() DatabaseData {
	return p.DatabaseProvider
}
