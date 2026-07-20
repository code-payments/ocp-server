package tests

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/code-payments/ocp-server/database/query"
	"github.com/code-payments/ocp-server/ocp/data/currency"
	"github.com/code-payments/ocp-server/ocp/data/currency/metadata"
	"github.com/code-payments/ocp-server/solana/currencycreator"
)

func RunTests(t *testing.T, s metadata.Store, teardown func()) {
	for _, tf := range []func(t *testing.T, s metadata.Store){
		testMetadataRoundTrip,
		testMetadataSaveWithVersioning,
		testMetadataUniqueNameConstraint,
		testIsNameAvailable,
		testAbandonedCurrencyNameReuse,
		testGetAllMetadataByState,
		testGetAllMints,
		testCountMints,
		testCountMetadataByState,
	} {
		tf(t, s)
		teardown()
	}
}

func testMetadataRoundTrip(t *testing.T, s metadata.Store) {
	expected := &currency.MetadataRecord{
		Name:        "Jeffy",
		Symbol:      "JFY",
		Description: "A test currency for Flipcash created by Jeff Yanta so we can eat our own dog food as we build out the platform. Pun intended",
		ImageUrl:    "https://flipcash-currency-assets.s3.us-east-1.amazonaws.com/52MNGpgvydSwCtC2H4qeiZXZ1TxEuRVCRGa8LAfk2kSj/icon.png",
		BillColors:  []string{"#19191A", "#FFFFFF"},
		SocialLinks: []currency.SocialLink{
			{Type: currency.SocialLinkTypeWebsite, Value: "https://flipcash.com"},
			{Type: currency.SocialLinkTypeX, Value: "jeffycurrency"},
			{Type: currency.SocialLinkTypeTelegram, Value: "jeffycurrency"},
			{Type: currency.SocialLinkTypeDiscord, Value: "abc123"},
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

		IsDiscoverable: true,

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
	assertEquivalentRecords(t, cloned, actual)
	assert.True(t, actual.IsDiscoverable)
	assert.EqualValues(t, currency.MetadataStateUnknown, actual.State)
	assert.EqualValues(t, 1, actual.Version)
}

func testMetadataUniqueNameConstraint(t *testing.T, s metadata.Store) {
	record1 := &currency.MetadataRecord{
		Name:        "UniqueName",
		Symbol:      "UN1",
		Description: "First currency",
		ImageUrl:    "https://example.com/un1.png",
		BillColors:  []string{"#000000"},
		SocialLinks: []currency.SocialLink{{Type: currency.SocialLinkTypeWebsite, Value: "https://example.com"}},

		Seed:      "uniqueseed1",
		Authority: "uniqueauth1",

		Mint:     "uniquemint1111111111111111111111111111111111111",
		MintBump: 255,
		Decimals: currencycreator.DefaultMintDecimals,

		CurrencyConfig:     "uniqueconfig11111111111111111111111111111111",
		CurrencyConfigBump: 255,

		LiquidityPool:     "uniquepool1111111111111111111111111111111111",
		LiquidityPoolBump: 255,

		VaultMint:     "uniquevmint111111111111111111111111111111111",
		VaultMintBump: 255,

		VaultCore:     "uniquevcore111111111111111111111111111111111",
		VaultCoreBump: 255,

		SellFeeBps: currencycreator.DefaultSellFeeBps,

		Alt: "uniquealt11111111111111111111111111111111111111",

		CreatedBy: "uniquecreator1",
		CreatedAt: time.Now(),
	}

	require.NoError(t, s.SaveMetadata(context.Background(), record1))

	// Second record with the same name (different case) but different everything else
	record2 := &currency.MetadataRecord{
		Name:        "uniquename",
		Symbol:      "UN2",
		Description: "Second currency",
		ImageUrl:    "https://example.com/un2.png",
		BillColors:  []string{"#FFFFFF"},
		SocialLinks: []currency.SocialLink{{Type: currency.SocialLinkTypeWebsite, Value: "https://example2.com"}},

		Seed:      "uniqueseed2",
		Authority: "uniqueauth2",

		Mint:     "uniquemint2222222222222222222222222222222222222",
		MintBump: 255,
		Decimals: currencycreator.DefaultMintDecimals,

		CurrencyConfig:     "uniqueconfig22222222222222222222222222222222",
		CurrencyConfigBump: 255,

		LiquidityPool:     "uniquepool2222222222222222222222222222222222",
		LiquidityPoolBump: 255,

		VaultMint:     "uniquevmint222222222222222222222222222222222",
		VaultMintBump: 255,

		VaultCore:     "uniquevcore222222222222222222222222222222222",
		VaultCoreBump: 255,

		SellFeeBps: currencycreator.DefaultSellFeeBps,

		Alt: "uniquealt22222222222222222222222222222222222222",

		CreatedBy: "uniquecreator2",
		CreatedAt: time.Now(),
	}

	assert.Equal(t, currency.ErrDuplicateCurrency, s.SaveMetadata(context.Background(), record2))
}

func testIsNameAvailable(t *testing.T, s metadata.Store) {
	ctx := context.Background()

	// Name should be available when no records exist
	available, err := s.IsNameAvailable(ctx, "TestCurrency")
	require.NoError(t, err)
	assert.True(t, available)

	// Save a metadata record
	record := &currency.MetadataRecord{
		Name:        "TestCurrency",
		Symbol:      "TC",
		Description: "A test currency",
		ImageUrl:    "https://example.com/tc.png",
		BillColors:  []string{"#000000"},
		SocialLinks: []currency.SocialLink{{Type: currency.SocialLinkTypeWebsite, Value: "https://example.com"}},

		Seed:      "nameseed1",
		Authority: "nameauth1",

		Mint:     "namemint11111111111111111111111111111111111111",
		MintBump: 255,
		Decimals: currencycreator.DefaultMintDecimals,

		CurrencyConfig:     "nameconfig111111111111111111111111111111111",
		CurrencyConfigBump: 255,

		LiquidityPool:     "namepool11111111111111111111111111111111111",
		LiquidityPoolBump: 255,

		VaultMint:     "namevmint1111111111111111111111111111111111",
		VaultMintBump: 255,

		VaultCore:     "namevcore1111111111111111111111111111111111",
		VaultCoreBump: 255,

		SellFeeBps: currencycreator.DefaultSellFeeBps,

		Alt: "namealt111111111111111111111111111111111111111",

		CreatedBy: "namecreator1",
		CreatedAt: time.Now(),
	}

	require.NoError(t, s.SaveMetadata(ctx, record))

	// Exact name should not be available
	available, err = s.IsNameAvailable(ctx, "TestCurrency")
	require.NoError(t, err)
	assert.False(t, available)

	// Case-insensitive match should not be available
	available, err = s.IsNameAvailable(ctx, "testcurrency")
	require.NoError(t, err)
	assert.False(t, available)

	available, err = s.IsNameAvailable(ctx, "TESTCURRENCY")
	require.NoError(t, err)
	assert.False(t, available)

	// Different name should be available
	available, err = s.IsNameAvailable(ctx, "OtherCurrency")
	require.NoError(t, err)
	assert.True(t, available)
}

func testAbandonedCurrencyNameReuse(t *testing.T, s metadata.Store) {
	ctx := context.Background()

	record := &currency.MetadataRecord{
		Name:        "AbandonedCurrency",
		Symbol:      "AB1",
		Description: "A currency that will be abandoned",
		ImageUrl:    "https://example.com/ab1.png",
		BillColors:  []string{"#000000"},
		SocialLinks: []currency.SocialLink{{Type: currency.SocialLinkTypeWebsite, Value: "https://example.com"}},

		Seed:      "abandonedseed1",
		Authority: "abandonedauth1",

		Mint:     "abandonedmint111111111111111111111111111111111",
		MintBump: 255,
		Decimals: currencycreator.DefaultMintDecimals,

		CurrencyConfig:     "abandonedconfig1111111111111111111111111111",
		CurrencyConfigBump: 255,

		LiquidityPool:     "abandonedpool11111111111111111111111111111111",
		LiquidityPoolBump: 255,

		VaultMint:     "abandonedvmint1111111111111111111111111111111",
		VaultMintBump: 255,

		VaultCore:     "abandonedvcore1111111111111111111111111111111",
		VaultCoreBump: 255,

		SellFeeBps: currencycreator.DefaultSellFeeBps,

		Alt: "abandonedalt1111111111111111111111111111111111",

		CreatedBy: "abandonedcreator1",
		CreatedAt: time.Now(),
	}

	require.NoError(t, s.SaveMetadata(ctx, record))

	// Name should not be available while active
	available, err := s.IsNameAvailable(ctx, "AbandonedCurrency")
	require.NoError(t, err)
	assert.False(t, available)

	// Case-insensitive should also not be available
	available, err = s.IsNameAvailable(ctx, "abandonedcurrency")
	require.NoError(t, err)
	assert.False(t, available)

	// Transition to abandoned state
	record.State = currency.MetadataStateAbandoned
	require.NoError(t, s.SaveMetadata(ctx, record))

	// Name should now be available
	available, err = s.IsNameAvailable(ctx, "AbandonedCurrency")
	require.NoError(t, err)
	assert.True(t, available)

	// Case-insensitive should also be available
	available, err = s.IsNameAvailable(ctx, "abandonedcurrency")
	require.NoError(t, err)
	assert.True(t, available)

	// Should be able to create a new currency with the same name
	record2 := &currency.MetadataRecord{
		Name:        "AbandonedCurrency",
		Symbol:      "AB2",
		Description: "Reusing the abandoned name",
		ImageUrl:    "https://example.com/ab2.png",
		BillColors:  []string{"#FFFFFF"},
		SocialLinks: []currency.SocialLink{{Type: currency.SocialLinkTypeWebsite, Value: "https://example2.com"}},

		Seed:      "abandonedseed2",
		Authority: "abandonedauth2",

		Mint:     "abandonedmint222222222222222222222222222222222",
		MintBump: 255,
		Decimals: currencycreator.DefaultMintDecimals,

		CurrencyConfig:     "abandonedconfig2222222222222222222222222222",
		CurrencyConfigBump: 255,

		LiquidityPool:     "abandonedpool22222222222222222222222222222222",
		LiquidityPoolBump: 255,

		VaultMint:     "abandonedvmint2222222222222222222222222222222",
		VaultMintBump: 255,

		VaultCore:     "abandonedvcore2222222222222222222222222222222",
		VaultCoreBump: 255,

		SellFeeBps: currencycreator.DefaultSellFeeBps,

		Alt: "abandonedalt2222222222222222222222222222222222",

		CreatedBy: "abandonedcreator2",
		CreatedAt: time.Now(),
	}

	require.NoError(t, s.SaveMetadata(ctx, record2))

	// New currency's name should no longer be available
	available, err = s.IsNameAvailable(ctx, "AbandonedCurrency")
	require.NoError(t, err)
	assert.False(t, available)
}

func testGetAllMetadataByState(t *testing.T, s metadata.Store) {
	t.Run("testGetAllMetadataByState", func(t *testing.T) {
		ctx := context.Background()

		// No records should exist initially
		_, err := s.GetAllMetadataByState(ctx, currency.MetadataStateUnknown, query.EmptyCursor, 100, query.Ascending)
		assert.Equal(t, currency.ErrNotFound, err)

		// Create records
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
		record2.Seed = "seed2"
		record2.Mint = "mint2222222222222222222222222222222222222222222"
		record2.CurrencyConfig = "config2222222222222222222222222222222222222222"
		record2.LiquidityPool = "pool222222222222222222222222222222222222222222"
		record2.VaultMint = "vmint22222222222222222222222222222222222222222"
		record2.VaultCore = "vcore22222222222222222222222222222222222222222"
		record2.Alt = "alt222222222222222222222222222222222222222222222"

		require.NoError(t, s.SaveMetadata(ctx, record1))
		require.NoError(t, s.SaveMetadata(ctx, record2))

		// Both should be in unknown state
		items, err := s.GetAllMetadataByState(ctx, currency.MetadataStateUnknown, query.EmptyCursor, 100, query.Ascending)
		require.NoError(t, err)
		assert.Len(t, items, 2)

		// No records in available state
		_, err = s.GetAllMetadataByState(ctx, currency.MetadataStateAvailable, query.EmptyCursor, 100, query.Ascending)
		assert.Equal(t, currency.ErrNotFound, err)

		// Move record1 to available
		record1.State = currency.MetadataStateAvailable
		require.NoError(t, s.SaveMetadata(ctx, record1))

		// Now only record2 should be unknown
		items, err = s.GetAllMetadataByState(ctx, currency.MetadataStateUnknown, query.EmptyCursor, 100, query.Ascending)
		require.NoError(t, err)
		assert.Len(t, items, 1)
		assert.Equal(t, record2.Mint, items[0].Mint)

		// Record1 should be available
		items, err = s.GetAllMetadataByState(ctx, currency.MetadataStateAvailable, query.EmptyCursor, 100, query.Ascending)
		require.NoError(t, err)
		assert.Len(t, items, 1)
		assert.Equal(t, record1.Mint, items[0].Mint)

		// Test limit
		record2.State = currency.MetadataStateAvailable
		require.NoError(t, s.SaveMetadata(ctx, record2))

		items, err = s.GetAllMetadataByState(ctx, currency.MetadataStateAvailable, query.EmptyCursor, 1, query.Ascending)
		require.NoError(t, err)
		assert.Len(t, items, 1)

		// Test cursor pagination ascending
		items, err = s.GetAllMetadataByState(ctx, currency.MetadataStateAvailable, query.ToCursor(items[0].Id), 10, query.Ascending)
		require.NoError(t, err)
		assert.Len(t, items, 1)
		assert.Equal(t, record2.Mint, items[0].Mint)

		// Test descending order returns highest ID first
		items, err = s.GetAllMetadataByState(ctx, currency.MetadataStateAvailable, query.EmptyCursor, 10, query.Descending)
		require.NoError(t, err)
		assert.Len(t, items, 2)
		assert.Equal(t, record2.Mint, items[0].Mint)
		assert.Equal(t, record1.Mint, items[1].Mint)

		// Test cursor pagination descending
		items, err = s.GetAllMetadataByState(ctx, currency.MetadataStateAvailable, query.ToCursor(items[0].Id), 10, query.Descending)
		require.NoError(t, err)
		assert.Len(t, items, 1)
		assert.Equal(t, record1.Mint, items[0].Mint)
	})
}

func testGetAllMints(t *testing.T, s metadata.Store) {
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

func testCountMints(t *testing.T, s metadata.Store) {
	// No mints should exist initially
	count, err := s.CountMints(context.Background())
	require.NoError(t, err)
	assert.EqualValues(t, 0, count)

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

	count, err = s.CountMints(context.Background())
	require.NoError(t, err)
	assert.EqualValues(t, 1, count)

	require.NoError(t, s.SaveMetadata(context.Background(), record2))

	count, err = s.CountMints(context.Background())
	require.NoError(t, err)
	assert.EqualValues(t, 2, count)

	// Abandoned mints must not be counted
	record2.State = currency.MetadataStateAbandoned
	require.NoError(t, s.SaveMetadata(context.Background(), record2))

	count, err = s.CountMints(context.Background())
	require.NoError(t, err)
	assert.EqualValues(t, 1, count)
}

func testCountMetadataByState(t *testing.T, s metadata.Store) {
	ctx := context.Background()

	// No records should exist initially
	for _, state := range []currency.MetadataState{
		currency.MetadataStateUnknown,
		currency.MetadataStateAvailable,
		currency.MetadataStateWaitingForInitialPurchase,
		currency.MetadataStatePrePurchaseSetup,
		currency.MetadataStateExecutingInitialPurchase,
		currency.MetadataStateCompletingInitialization,
		currency.MetadataStateFinalValidation,
	} {
		count, err := s.CountMetadataByState(ctx, state)
		require.NoError(t, err)
		assert.EqualValues(t, 0, count)
	}

	// Insert two metadata records (both default to Unknown state)
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
	record2.Seed = "seed2"
	record2.Mint = "mint2222222222222222222222222222222222222222222"
	record2.CurrencyConfig = "config2222222222222222222222222222222222222222"
	record2.LiquidityPool = "pool222222222222222222222222222222222222222222"
	record2.VaultMint = "vmint22222222222222222222222222222222222222222"
	record2.VaultCore = "vcore22222222222222222222222222222222222222222"
	record2.Alt = "alt222222222222222222222222222222222222222222222"

	require.NoError(t, s.SaveMetadata(ctx, record1))
	require.NoError(t, s.SaveMetadata(ctx, record2))

	// Both should be in unknown state
	count, err := s.CountMetadataByState(ctx, currency.MetadataStateUnknown)
	require.NoError(t, err)
	assert.EqualValues(t, 2, count)

	count, err = s.CountMetadataByState(ctx, currency.MetadataStateAvailable)
	require.NoError(t, err)
	assert.EqualValues(t, 0, count)

	// Move record1 to available
	record1.State = currency.MetadataStateAvailable
	require.NoError(t, s.SaveMetadata(ctx, record1))

	count, err = s.CountMetadataByState(ctx, currency.MetadataStateUnknown)
	require.NoError(t, err)
	assert.EqualValues(t, 1, count)

	count, err = s.CountMetadataByState(ctx, currency.MetadataStateAvailable)
	require.NoError(t, err)
	assert.EqualValues(t, 1, count)

	// Move record2 to completing initializing
	record2.State = currency.MetadataStateCompletingInitialization
	require.NoError(t, s.SaveMetadata(ctx, record2))

	count, err = s.CountMetadataByState(ctx, currency.MetadataStateUnknown)
	require.NoError(t, err)
	assert.EqualValues(t, 0, count)

	count, err = s.CountMetadataByState(ctx, currency.MetadataStateAvailable)
	require.NoError(t, err)
	assert.EqualValues(t, 1, count)

	count, err = s.CountMetadataByState(ctx, currency.MetadataStateCompletingInitialization)
	require.NoError(t, err)
	assert.EqualValues(t, 1, count)
}

func testMetadataSaveWithVersioning(t *testing.T, s metadata.Store) {
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

		IsDiscoverable: true,

		CreatedBy: "vercreator1",
		CreatedAt: time.Now(),
	}

	// First save — insert
	require.NoError(t, s.SaveMetadata(context.Background(), record))
	assert.EqualValues(t, 1, record.Version)
	assert.EqualValues(t, currency.MetadataStateUnknown, record.State)
	assert.True(t, record.IsDiscoverable)

	// Update mutable fields and save again with correct version
	record.State = currency.MetadataStateAvailable
	record.Description = "Updated description"
	record.ImageUrl = "https://example.com/updated.png"
	record.BillColors = []string{"#FF0000", "#00FF00", "#0000FF"}
	record.SocialLinks = []currency.SocialLink{
		{Type: currency.SocialLinkTypeWebsite, Value: "https://updated.example.com"},
		{Type: currency.SocialLinkTypeX, Value: "updatedhandle"},
		{Type: currency.SocialLinkTypeTelegram, Value: "updatedtelegram"},
		{Type: currency.SocialLinkTypeDiscord, Value: "updateddiscord"},
	}
	record.Alt = "updatedalt1111111111111111111111111111111111111"
	record.IsDiscoverable = false
	require.NoError(t, s.SaveMetadata(context.Background(), record))
	assert.EqualValues(t, 2, record.Version)
	assert.EqualValues(t, currency.MetadataStateAvailable, record.State)

	// Verify mutable fields were updated
	actual, err := s.GetMetadata(context.Background(), record.Mint)
	require.NoError(t, err)
	assert.EqualValues(t, 2, actual.Version)
	assert.EqualValues(t, currency.MetadataStateAvailable, actual.State)
	assert.Equal(t, "Updated description", actual.Description)
	assert.Equal(t, "https://example.com/updated.png", actual.ImageUrl)
	assert.Equal(t, []string{"#FF0000", "#00FF00", "#0000FF"}, actual.BillColors)
	assert.Equal(t, []currency.SocialLink{
		{Type: currency.SocialLinkTypeWebsite, Value: "https://updated.example.com"},
		{Type: currency.SocialLinkTypeX, Value: "updatedhandle"},
		{Type: currency.SocialLinkTypeTelegram, Value: "updatedtelegram"},
		{Type: currency.SocialLinkTypeDiscord, Value: "updateddiscord"},
	}, actual.SocialLinks)
	assert.Equal(t, "updatedalt1111111111111111111111111111111111111", actual.Alt)
	assert.False(t, actual.IsDiscoverable)

	// Verify immutable fields were preserved
	assert.Equal(t, "Versioned", actual.Name)
	assert.Equal(t, "VER", actual.Symbol)

	// Attempt save with stale version
	record.Description = "Updated description 2"
	record.ImageUrl = "https://example.com/updated2.png"
	record.BillColors = []string{"#FFFFFF", "#FFFFFF", "#FFFFFF"}
	record.SocialLinks = []currency.SocialLink{
		{Type: currency.SocialLinkTypeWebsite, Value: "https://updated2.example.com"},
		{Type: currency.SocialLinkTypeX, Value: "updatedhandle2"},
		{Type: currency.SocialLinkTypeTelegram, Value: "staletelegram"},
		{Type: currency.SocialLinkTypeDiscord, Value: "stalediscord"},
	}
	record.Alt = "stalealt1111111111111111111111111111111111111111"
	record.State = currency.MetadataStateUnknown
	record.Version = 1
	assert.Equal(t, currency.ErrStaleMetadataVersion, s.SaveMetadata(context.Background(), record))

	// Verify via get that nothing changed
	actual, err = s.GetMetadata(context.Background(), record.Mint)
	require.NoError(t, err)
	assert.EqualValues(t, 2, actual.Version)
	assert.EqualValues(t, currency.MetadataStateAvailable, actual.State)
	assert.Equal(t, "Updated description", actual.Description)
	assert.Equal(t, "https://example.com/updated.png", actual.ImageUrl)
	assert.Equal(t, []string{"#FF0000", "#00FF00", "#0000FF"}, actual.BillColors)
	assert.Equal(t, []currency.SocialLink{
		{Type: currency.SocialLinkTypeWebsite, Value: "https://updated.example.com"},
		{Type: currency.SocialLinkTypeX, Value: "updatedhandle"},
		{Type: currency.SocialLinkTypeTelegram, Value: "updatedtelegram"},
		{Type: currency.SocialLinkTypeDiscord, Value: "updateddiscord"},
	}, actual.SocialLinks)
	assert.Equal(t, "updatedalt1111111111111111111111111111111111111", actual.Alt)
}

func assertEquivalentRecords(t *testing.T, obj1, obj2 *currency.MetadataRecord) {
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
	assert.Equal(t, obj1.IsDiscoverable, obj2.IsDiscoverable)
	assert.Equal(t, obj1.State, obj2.State)
	assert.Equal(t, obj1.CreatedBy, obj2.CreatedBy)
	assert.Equal(t, obj1.CreatedAt.Unix(), obj2.CreatedAt.Unix())
}
