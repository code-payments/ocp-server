package tests

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/code-payments/ocp-server/database/query"
	"github.com/code-payments/ocp-server/ocp/data/currency"
	"github.com/code-payments/ocp-server/solana/currencycreator"
)

func RunTests(t *testing.T, s currency.Store, teardown func()) {
	for _, tf := range []func(t *testing.T, s currency.Store){
		testExchangeRateRoundTrip,
		testGetExchangeRatesInRange,
		testMetadataRoundTrip,
		testMetadataSaveWithVersioning,
		testGetAllMints,
		testReserveRoundTrip,
		testGetReservesInRange,
	} {
		tf(t, s)
		teardown()
	}
}

func testExchangeRateRoundTrip(t *testing.T, s currency.Store) {
	now := time.Date(2021, 01, 29, 13, 0, 5, 0, time.UTC)

	record, err := s.GetAllExchangeRates(context.Background(), now)
	assert.Nil(t, record)
	assert.Equal(t, currency.ErrNotFound, err)

	rates := map[string]float64{
		"usd": 0.000055,
		"cad": 0.00007,
	}
	require.NoError(t, s.PutExchangeRates(context.Background(), &currency.MultiRateRecord{
		Time:  now,
		Rates: rates,
	}))

	// Overwrite should fail
	assert.Equal(t, currency.ErrExists, s.PutExchangeRates(context.Background(), &currency.MultiRateRecord{
		Time:  now,
		Rates: rates,
	}))

	// Test GetExchangeRate(), it should return the USD record
	single, err := s.GetExchangeRate(context.Background(), "usd", now)
	require.NoError(t, err)
	assert.Equal(t, now.Unix(), single.Time.Unix())
	assert.EqualValues(t, rates["usd"], single.Rate)

	// Test GetAllExchangeRates(), it should return all recent rates
	record, err = s.GetAllExchangeRates(context.Background(), now)
	require.NoError(t, err)

	assert.Equal(t, now.Unix(), record.Time.Unix())
	assert.EqualValues(t, rates, record.Rates)

	// within same day, should return entry
	record, err = s.GetAllExchangeRates(context.Background(), time.Date(2021, 01, 29, 14, 0, 5, 0, time.UTC))
	require.NoError(t, err)

	assert.Equal(t, now.Unix(), record.Time.Unix())
	assert.EqualValues(t, rates, record.Rates)

	// day after, should be empty
	tomorrow := time.Date(2021, 01, 30, 0, 0, 0, 0, time.UTC)
	record, err = s.GetAllExchangeRates(context.Background(), tomorrow)
	assert.Nil(t, record)
	assert.Equal(t, currency.ErrNotFound, err)
}

func testGetExchangeRatesInRange(t *testing.T, s currency.Store) {
	var rates []currency.MultiRateRecord

	now := time.Now().UTC()

	for i := 0; i < 100; i++ {
		rates = append(rates, currency.MultiRateRecord{
			Time: now.Add(time.Duration(i) * time.Hour),
			Rates: map[string]float64{
				"usd": (0.000058 + float64(i/10000)),
				"cad": (0.00008 + float64(i/10000)),
			},
		})
	}

	record, err := s.GetAllExchangeRates(context.Background(), rates[0].Time)
	assert.Nil(t, record)
	assert.Equal(t, currency.ErrNotFound, err)

	for _, item := range rates {
		require.NoError(t, s.PutExchangeRates(context.Background(), &item))
	}

	result, err := s.GetExchangeRatesInRange(context.Background(), "usd", query.IntervalRaw, rates[0].Time, rates[99].Time, query.Ascending)
	require.NoError(t, err)
	assert.Equal(t, len(result), 100)
	for i, item := range result {
		assert.Equal(t, rates[i].Time.Unix(), item.Time.Unix())
		assert.EqualValues(t, rates[i].Rates["usd"], item.Rate)
	}

	result, err = s.GetExchangeRatesInRange(context.Background(), "usd", query.IntervalRaw, rates[0].Time, rates[49].Time, query.Ascending)
	require.NoError(t, err)
	assert.Equal(t, len(result), 50)
	for i, item := range result {
		assert.Equal(t, rates[i].Time.Unix(), item.Time.Unix())
		assert.EqualValues(t, rates[i].Rates["usd"], item.Rate)
	}

	result, err = s.GetExchangeRatesInRange(context.Background(), "usd", query.IntervalRaw, rates[0].Time, rates[99].Time, query.Descending)
	require.NoError(t, err)
	assert.Equal(t, len(result), 100)
	for i, item := range result {
		assert.Equal(t, rates[99-i].Time.Unix(), item.Time.Unix())
		assert.EqualValues(t, rates[99-i].Rates["usd"], item.Rate)
	}

	_, err = s.GetExchangeRatesInRange(context.Background(), "usd", query.IntervalSecond, rates[0].Time, rates[99].Time, query.Ascending)
	require.NoError(t, err)
	_, err = s.GetExchangeRatesInRange(context.Background(), "usd", query.IntervalMinute, rates[0].Time, rates[99].Time, query.Ascending)
	require.NoError(t, err)
	_, err = s.GetExchangeRatesInRange(context.Background(), "usd", query.IntervalHour, rates[0].Time, rates[99].Time, query.Ascending)
	require.NoError(t, err)
	_, err = s.GetExchangeRatesInRange(context.Background(), "usd", query.IntervalDay, rates[0].Time, rates[99].Time, query.Ascending)
	require.NoError(t, err)
	_, err = s.GetExchangeRatesInRange(context.Background(), "usd", query.IntervalWeek, rates[0].Time, rates[99].Time, query.Ascending)
	require.NoError(t, err)
	_, err = s.GetExchangeRatesInRange(context.Background(), "usd", query.IntervalMonth, rates[0].Time, rates[99].Time, query.Ascending)
	require.NoError(t, err)
}

func testMetadataRoundTrip(t *testing.T, s currency.Store) {
	expected := &currency.MetadataRecord{
		Name:        "Jeffy",
		Symbol:      "JFY",
		Description: "A test currency for Flipcash created by Jeff Yanta so we can eat our own dog food as we build out the platform. Pun intended",
		ImageUrl:    "https://flipcash-currency-assets.s3.us-east-1.amazonaws.com/52MNGpgvydSwCtC2H4qeiZXZ1TxEuRVCRGa8LAfk2kSj/icon.png",
		BillColors:  []string{"#19191A", "#FFFFFF"},
		SocialLinks: []currency.SocialLink{
			{Type: currency.SocialLinkTypeWebsite, Value: "https://flipcash.com"},
			{Type: currency.SocialLinkTypeX, Value: "jeffycurrency"},
		},

		Seed: "H7WNaHtCa5h2k7AwZ8DbdLfM6bU2bi2jmWiUkFqgeBYk",

		Authority: "jfy1btcfsjSn2WCqLVaxiEjp4zgmemGyRsdCPbPwnZV",

		Mint:     "52MNGpgvydSwCtC2H4qeiZXZ1TxEuRVCRGa8LAfk2kSj",
		MintBump: 252,
		Decimals: currencycreator.DefaultMintDecimals,

		CurrencyConfig:     "BDfFyqfasvty3cjSbC2qZx2Dmr4vhhVBt9Ban5XsTcEH",
		CurrencyConfigBump: 251,

		LiquidityPool:     "5cH99GSbr9ECP8gd1vLiAAFPHt1VeCNKzzrPFGmAB61c",
		LiquidityPoolBump: 255,

		VaultMint:     "BFDanLgELhpCCGTtaa7c8WGxTXcTxgwkf9DMQd4qheSK",
		VaultMintBump: 255,

		VaultCore:     "A9NVHVuorNL4y2YFxdwdU3Hqozxw1Y1YJ81ZPxJsRrT4",
		VaultCoreBump: 255,

		SellFeeBps: currencycreator.DefaultSellFeeBps,

		Alt: "EkAeTCceLWbmZrAzVZanDJBtHSnkAWndMFgmTnUnVLRR",

		CreatedBy: "jyyy4RpW3X5ApbW5G6vx9ZVPxhoUKGRLbZ4LxC47LYG",
		CreatedAt: time.Now(),
	}

	_, err := s.GetMetadata(context.Background(), expected.Mint)
	assert.Equal(t, currency.ErrNotFound, err)

	cloned := expected.Clone()
	require.NoError(t, s.SaveMetadata(context.Background(), expected))
	assert.EqualValues(t, 1, expected.Id)
	assert.EqualValues(t, 1, expected.Version)

	actual, err := s.GetMetadata(context.Background(), expected.Mint)
	require.NoError(t, err)
	assertEquivalentMetadataRecords(t, cloned, actual)
	assert.EqualValues(t, currency.MetadataStateUnknown, actual.State)
	assert.EqualValues(t, 1, actual.Version)
}

func testGetAllMints(t *testing.T, s currency.Store) {
	// No mints should exist initially
	mints, err := s.GetAllMints(context.Background())
	assert.Nil(t, mints)
	assert.Equal(t, currency.ErrNotFound, err)

	// Insert two metadata records with different mints
	record1 := &currency.MetadataRecord{
		Name:        "Currency1",
		Symbol:      "C1",
		Description: "First test currency",
		ImageUrl:    "https://example.com/c1.png",
		BillColors:  []string{"#000000"},
		SocialLinks: []currency.SocialLink{{Type: currency.SocialLinkTypeWebsite, Value: "https://example.com"}},

		Seed:      "seed1",
		Authority: "auth1",

		Mint:     "mint1111111111111111111111111111111111111111111",
		MintBump: 255,
		Decimals: currencycreator.DefaultMintDecimals,

		CurrencyConfig:     "config1111111111111111111111111111111111111111",
		CurrencyConfigBump: 255,

		LiquidityPool:     "pool111111111111111111111111111111111111111111",
		LiquidityPoolBump: 255,

		VaultMint:     "vmint11111111111111111111111111111111111111111",
		VaultMintBump: 255,

		VaultCore:     "vcore11111111111111111111111111111111111111111",
		VaultCoreBump: 255,

		SellFeeBps: currencycreator.DefaultSellFeeBps,

		Alt: "alt111111111111111111111111111111111111111111111",

		CreatedBy: "creator1",
		CreatedAt: time.Now(),
	}

	record2 := record1.Clone()
	record2.Name = "Currency2"
	record2.Symbol = "C2"
	record2.Description = "Second test currency"
	record2.Seed = "seed2"
	record2.Mint = "mint2222222222222222222222222222222222222222222"
	record2.CurrencyConfig = "config2222222222222222222222222222222222222222"
	record2.LiquidityPool = "pool222222222222222222222222222222222222222222"
	record2.VaultMint = "vmint22222222222222222222222222222222222222222"
	record2.VaultCore = "vcore22222222222222222222222222222222222222222"
	record2.Alt = "alt222222222222222222222222222222222222222222222"

	require.NoError(t, s.SaveMetadata(context.Background(), record1))
	require.NoError(t, s.SaveMetadata(context.Background(), record2))

	mints, err = s.GetAllMints(context.Background())
	require.NoError(t, err)
	assert.Len(t, mints, 2)
	assert.Contains(t, mints, record1.Mint)
	assert.Contains(t, mints, record2.Mint)
}

func testReserveRoundTrip(t *testing.T, s currency.Store) {
	now := time.Date(2021, 01, 29, 13, 0, 5, 0, time.UTC)

	record, err := s.GetReserveAtTime(context.Background(), "mint", now)
	assert.Nil(t, record)
	assert.Equal(t, currency.ErrNotFound, err)

	expected := &currency.ReserveRecord{
		Mint:              "mint",
		SupplyFromBonding: 1,
		Time:              now,
	}
	require.NoError(t, s.PutReserveRecord(context.Background(), expected))

	assert.Equal(t, currency.ErrExists, s.PutReserveRecord(context.Background(), expected))

	actual, err := s.GetReserveAtTime(context.Background(), "mint", now)
	require.NoError(t, err)
	assert.Equal(t, now.Unix(), actual.Time.Unix())
	assert.Equal(t, actual.SupplyFromBonding, expected.SupplyFromBonding)

	actual, err = s.GetReserveAtTime(context.Background(), "mint", time.Date(2021, 01, 29, 14, 0, 5, 0, time.UTC))
	require.NoError(t, err)

	assert.Equal(t, now.Unix(), actual.Time.Unix())
	assert.Equal(t, actual.SupplyFromBonding, expected.SupplyFromBonding)

	tomorrow := time.Date(2021, 01, 30, 0, 0, 0, 0, time.UTC)
	actual, err = s.GetReserveAtTime(context.Background(), "mint", tomorrow)
	assert.Nil(t, actual)
	assert.Equal(t, currency.ErrNotFound, err)
}

func testGetReservesInRange(t *testing.T, s currency.Store) {
	var reserves []currency.ReserveRecord

	now := time.Now().UTC()
	mint := "test-mint"

	for i := 0; i < 100; i++ {
		reserves = append(reserves, currency.ReserveRecord{
			Mint:              mint,
			SupplyFromBonding: uint64(1000 + i),
			Time:              now.Add(time.Duration(i) * time.Hour),
		})
	}

	record, err := s.GetReserveAtTime(context.Background(), mint, reserves[0].Time)
	assert.Nil(t, record)
	assert.Equal(t, currency.ErrNotFound, err)

	for _, item := range reserves {
		itemCopy := item
		require.NoError(t, s.PutReserveRecord(context.Background(), &itemCopy))
	}

	result, err := s.GetReservesInRange(context.Background(), mint, query.IntervalRaw, reserves[0].Time, reserves[99].Time, query.Ascending)
	require.NoError(t, err)
	assert.Equal(t, len(result), 100)
	for i, item := range result {
		assert.Equal(t, reserves[i].Time.Unix(), item.Time.Unix())
		assert.EqualValues(t, reserves[i].SupplyFromBonding, item.SupplyFromBonding)
	}

	result, err = s.GetReservesInRange(context.Background(), mint, query.IntervalRaw, reserves[0].Time, reserves[49].Time, query.Ascending)
	require.NoError(t, err)
	assert.Equal(t, len(result), 50)
	for i, item := range result {
		assert.Equal(t, reserves[i].Time.Unix(), item.Time.Unix())
		assert.EqualValues(t, reserves[i].SupplyFromBonding, item.SupplyFromBonding)
	}

	result, err = s.GetReservesInRange(context.Background(), mint, query.IntervalRaw, reserves[0].Time, reserves[99].Time, query.Descending)
	require.NoError(t, err)
	assert.Equal(t, len(result), 100)
	for i, item := range result {
		assert.Equal(t, reserves[99-i].Time.Unix(), item.Time.Unix())
		assert.EqualValues(t, reserves[99-i].SupplyFromBonding, item.SupplyFromBonding)
	}

	_, err = s.GetReservesInRange(context.Background(), mint, query.IntervalSecond, reserves[0].Time, reserves[99].Time, query.Ascending)
	require.NoError(t, err)
	_, err = s.GetReservesInRange(context.Background(), mint, query.IntervalMinute, reserves[0].Time, reserves[99].Time, query.Ascending)
	require.NoError(t, err)
	_, err = s.GetReservesInRange(context.Background(), mint, query.IntervalHour, reserves[0].Time, reserves[99].Time, query.Ascending)
	require.NoError(t, err)
	_, err = s.GetReservesInRange(context.Background(), mint, query.IntervalDay, reserves[0].Time, reserves[99].Time, query.Ascending)
	require.NoError(t, err)
	_, err = s.GetReservesInRange(context.Background(), mint, query.IntervalWeek, reserves[0].Time, reserves[99].Time, query.Ascending)
	require.NoError(t, err)
	_, err = s.GetReservesInRange(context.Background(), mint, query.IntervalMonth, reserves[0].Time, reserves[99].Time, query.Ascending)
	require.NoError(t, err)
}

func testMetadataSaveWithVersioning(t *testing.T, s currency.Store) {
	record := &currency.MetadataRecord{
		Name:        "Versioned",
		Symbol:      "VER",
		Description: "A versioned test currency",
		ImageUrl:    "https://example.com/ver.png",
		BillColors:  []string{"#000000"},
		SocialLinks: []currency.SocialLink{{Type: currency.SocialLinkTypeWebsite, Value: "https://example.com"}},

		Seed:      "verseed1",
		Authority: "verauth1",

		Mint:     "vermint1111111111111111111111111111111111111111",
		MintBump: 255,
		Decimals: currencycreator.DefaultMintDecimals,

		CurrencyConfig:     "verconfig111111111111111111111111111111111111",
		CurrencyConfigBump: 255,

		LiquidityPool:     "verpool1111111111111111111111111111111111111111",
		LiquidityPoolBump: 255,

		VaultMint:     "vervmint111111111111111111111111111111111111111",
		VaultMintBump: 255,

		VaultCore:     "vervcore111111111111111111111111111111111111111",
		VaultCoreBump: 255,

		SellFeeBps: currencycreator.DefaultSellFeeBps,

		Alt: "veralt11111111111111111111111111111111111111111",

		CreatedBy: "vercreator1",
		CreatedAt: time.Now(),
	}

	// First save — insert
	require.NoError(t, s.SaveMetadata(context.Background(), record))
	assert.EqualValues(t, 1, record.Version)
	assert.EqualValues(t, currency.MetadataStateUnknown, record.State)

	// Update state and save again with correct version
	record.State = currency.MetadataStateAvailable
	require.NoError(t, s.SaveMetadata(context.Background(), record))
	assert.EqualValues(t, 2, record.Version)
	assert.EqualValues(t, currency.MetadataStateAvailable, record.State)

	// Verify via get
	actual, err := s.GetMetadata(context.Background(), record.Mint)
	require.NoError(t, err)
	assert.EqualValues(t, 2, actual.Version)
	assert.EqualValues(t, currency.MetadataStateAvailable, actual.State)

	// Attempt save with stale version
	record.State = currency.MetadataStateUnknown
	record.Version = 1
	assert.Equal(t, currency.ErrStaleMetadataVersion, s.SaveMetadata(context.Background(), record))

	// Verify via get
	actual, err = s.GetMetadata(context.Background(), record.Mint)
	require.NoError(t, err)
	assert.EqualValues(t, 2, actual.Version)
	assert.EqualValues(t, currency.MetadataStateAvailable, actual.State)
}

func assertEquivalentMetadataRecords(t *testing.T, obj1, obj2 *currency.MetadataRecord) {
	assert.Equal(t, obj1.Name, obj2.Name)
	assert.Equal(t, obj1.Symbol, obj2.Symbol)
	assert.Equal(t, obj1.Description, obj2.Description)
	assert.Equal(t, obj1.ImageUrl, obj2.ImageUrl)
	assert.Equal(t, obj1.BillColors, obj2.BillColors)
	assert.Equal(t, obj1.SocialLinks, obj2.SocialLinks)
	assert.Equal(t, obj1.Seed, obj2.Seed)
	assert.Equal(t, obj1.Authority, obj2.Authority)
	assert.Equal(t, obj1.Mint, obj2.Mint)
	assert.Equal(t, obj1.MintBump, obj2.MintBump)
	assert.Equal(t, obj1.Decimals, obj2.Decimals)
	assert.Equal(t, obj1.CurrencyConfig, obj2.CurrencyConfig)
	assert.Equal(t, obj1.CurrencyConfigBump, obj2.CurrencyConfigBump)
	assert.Equal(t, obj1.LiquidityPool, obj2.LiquidityPool)
	assert.Equal(t, obj1.LiquidityPoolBump, obj2.LiquidityPoolBump)
	assert.Equal(t, obj1.VaultMint, obj2.VaultMint)
	assert.Equal(t, obj1.VaultMintBump, obj2.VaultMintBump)
	assert.Equal(t, obj1.VaultCore, obj2.VaultCore)
	assert.Equal(t, obj1.VaultCoreBump, obj2.VaultCoreBump)
	assert.Equal(t, obj1.SellFeeBps, obj2.SellFeeBps)
	assert.Equal(t, obj1.Alt, obj2.Alt)
	assert.Equal(t, obj1.State, obj2.State)
	assert.Equal(t, obj1.CreatedBy, obj2.CreatedBy)
	assert.Equal(t, obj1.CreatedAt.Unix(), obj2.CreatedAt.Unix())
}
