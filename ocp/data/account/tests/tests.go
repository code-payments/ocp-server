package tests

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	commonpb "github.com/code-payments/ocp-protobuf-api/generated/go/common/v1"

	"github.com/code-payments/ocp-server/ocp/data/account"
	"github.com/code-payments/ocp-server/pointer"
)

func RunTests(t *testing.T, s account.Store, teardown func()) {
	for _, tf := range []func(t *testing.T, s account.Store){
		testRoundTrip,
		testPutMultipleRecords,
		testPutErrors,
		testGetLatestByOwner,
		testBatchedMethods,
		testRemoteSendEdgeCases,
		testSwapAccountEdgeCases,
		testGetByMintAndType,
		testDepositSyncMethods,
		testAutoReturnCheckMethods,
		testBalanceLifecycle,
		testBalanceConcurrentDeltas,
		testBalanceInsertGuard,
	} {
		tf(t, s)
		teardown()
	}
}

func testRoundTrip(t *testing.T, s account.Store) {
	t.Run("testRoundTrip", func(t *testing.T) {
		ctx := context.Background()

		start := time.Now()

		expected := &account.Record{
			OwnerAccount:         "owner",
			AuthorityAccount:     "authority",
			TokenAccount:         "token",
			MintAccount:          "mint",
			AccountType:          commonpb.AccountType_POOL,
			Index:                123,
			RequiresDepositSync:  true,
			DepositsLastSyncedAt: time.Now().Add(-time.Hour),
		}
		cloned := expected.Clone()

		_, err := s.GetByTokenAddress(ctx, cloned.TokenAccount)
		assert.Equal(t, account.ErrAccountInfoNotFound, err)

		err = s.Update(ctx, expected)
		assert.Equal(t, account.ErrAccountInfoNotFound, err)

		require.NoError(t, s.Put(ctx, expected))

		assert.True(t, expected.Id > 0)
		assert.True(t, expected.CreatedAt.After(start))

		actual, err := s.GetByTokenAddress(ctx, cloned.TokenAccount)
		require.NoError(t, err)
		assertEquivalentRecords(t, &cloned, actual)

		actualByMint, err := s.GetByAuthorityAddress(ctx, cloned.AuthorityAccount)
		require.NoError(t, err)
		require.Len(t, actualByMint, 1)
		actual = actualByMint[cloned.MintAccount]
		assertEquivalentRecords(t, &cloned, actual)

		expected.RequiresDepositSync = false
		expected.DepositsLastSyncedAt = time.Now()
		cloned = expected.Clone()
		require.NoError(t, s.Update(ctx, expected))

		actual, err = s.GetByTokenAddress(ctx, cloned.TokenAccount)
		require.NoError(t, err)
		assertEquivalentRecords(t, &cloned, actual)

		actualByMint, err = s.GetByAuthorityAddress(ctx, cloned.AuthorityAccount)
		require.NoError(t, err)
		require.Len(t, actualByMint, 1)
		actual = actualByMint[cloned.MintAccount]
		assertEquivalentRecords(t, &cloned, actual)
	})
}

func testPutMultipleRecords(t *testing.T, s account.Store) {
	t.Run("testPutMultipleRecords", func(t *testing.T) {
		ctx := context.Background()

		var records []*account.Record

		// Accounts within the same type case
		for i := 0; i < 5; i++ {
			record := &account.Record{
				OwnerAccount:     "owner_part1",
				AuthorityAccount: fmt.Sprintf("authority_part1_%d", i),
				TokenAccount:     fmt.Sprintf("token_part1_%d", i),
				MintAccount:      "mint",
				AccountType:      commonpb.AccountType_POOL,
				Index:            uint64(i),
			}
			cloned := record.Clone()

			require.NoError(t, s.Put(ctx, record))

			records = append(records, &cloned)
		}

		// Accounts across different type case
		for i, accountType := range []commonpb.AccountType{
			commonpb.AccountType_PRIMARY,
			commonpb.AccountType_SWAP,
		} {
			record := &account.Record{
				OwnerAccount:     "owner_part2",
				AuthorityAccount: fmt.Sprintf("authority_part2_%d", i),
				TokenAccount:     fmt.Sprintf("token_part2_%d", i),
				MintAccount:      "mint",
				AccountType:      accountType,
				Index:            0,
			}
			if accountType == commonpb.AccountType_PRIMARY {
				record.AuthorityAccount = record.OwnerAccount
			}
			cloned := record.Clone()

			require.NoError(t, s.Put(ctx, record))

			records = append(records, &cloned)
		}

		// Accounts across different mints
		for i := 0; i < 5; i++ {
			record := &account.Record{
				OwnerAccount:     "owner_part3",
				AuthorityAccount: "owner_part3",
				TokenAccount:     fmt.Sprintf("token_part3_%d", i),
				MintAccount:      fmt.Sprintf("mint%d", i),
				AccountType:      commonpb.AccountType_PRIMARY,
				Index:            0,
			}
			cloned := record.Clone()

			require.NoError(t, s.Put(ctx, record))

			records = append(records, &cloned)
		}

		for _, expected := range records {
			actual, err := s.GetByTokenAddress(ctx, expected.TokenAccount)
			require.NoError(t, err)
			assertEquivalentRecords(t, expected, actual)
		}
	})
}

func testPutErrors(t *testing.T, s account.Store) {
	t.Run("testPutErrors", func(t *testing.T) {
		ctx := context.Background()

		record := &account.Record{
			OwnerAccount:     "owner",
			AuthorityAccount: "authority",
			TokenAccount:     "token",
			MintAccount:      "mint",
			AccountType:      commonpb.AccountType_POOL,
			Index:            0,
		}
		original := record.Clone()

		require.NoError(t, s.Put(ctx, record))

		assert.Equal(t, account.ErrAccountInfoExists, s.Put(ctx, record))

		// Cannot change any 1 field

		cloned := original.Clone()
		cloned.OwnerAccount = "new_owner"
		assert.Equal(t, account.ErrInvalidAccountInfo, s.Put(ctx, &cloned))

		cloned = original.Clone()
		cloned.AuthorityAccount = "new_authority"
		assert.Equal(t, account.ErrInvalidAccountInfo, s.Put(ctx, &cloned))

		cloned = original.Clone()
		cloned.TokenAccount = "new_token"
		assert.Equal(t, account.ErrInvalidAccountInfo, s.Put(ctx, &cloned))

		cloned = original.Clone()
		cloned.Index = cloned.Index + 1
		assert.Equal(t, account.ErrInvalidAccountInfo, s.Put(ctx, &cloned))

		cloned = original.Clone()
		cloned.AccountType = commonpb.AccountType_SWAP
		assert.Equal(t, account.ErrInvalidAccountInfo, s.Put(ctx, &cloned))

		// Changing multiple fields with owner changed

		cloned = original.Clone()
		cloned.OwnerAccount = "new_owner"
		cloned.AuthorityAccount = "new_authority"
		assert.Equal(t, account.ErrInvalidAccountInfo, s.Put(ctx, &cloned))

		cloned = original.Clone()
		cloned.OwnerAccount = "new_owner"
		cloned.AuthorityAccount = "new_authority"
		cloned.Index = cloned.Index + 1
		assert.Equal(t, account.ErrInvalidAccountInfo, s.Put(ctx, &cloned))

		cloned = original.Clone()
		cloned.OwnerAccount = "new_owner"
		cloned.TokenAccount = "new_token"
		assert.Equal(t, account.ErrInvalidAccountInfo, s.Put(ctx, &cloned))

		cloned = original.Clone()
		cloned.OwnerAccount = "new_owner"
		cloned.TokenAccount = "new_token"
		cloned.Index = cloned.Index + 1
		assert.Equal(t, account.ErrInvalidAccountInfo, s.Put(ctx, &cloned))

		cloned = original.Clone()
		cloned.OwnerAccount = "new_owner"
		cloned.TokenAccount = "new_token"
		cloned.AccountType = commonpb.AccountType_SWAP
		assert.Equal(t, account.ErrInvalidAccountInfo, s.Put(ctx, &cloned))

		// todo: this case isn't possible with current account structures
		/*cloned = original.Clone()
		cloned.OwnerAccount = "new_owner"
		cloned.TokenAccount = "new_token"
		cloned.AccountType = ?
		cloned.Index = cloned.Index + 1
		assert.Equal(t, account.ErrInvalidAccountInfo, s.Put(ctx, &cloned))*/

		// Changing multiple fields with token changed

		cloned = original.Clone()
		cloned.TokenAccount = "new_token"
		cloned.AuthorityAccount = "new_authority"
		assert.Equal(t, account.ErrInvalidAccountInfo, s.Put(ctx, &cloned))

		cloned = original.Clone()
		cloned.TokenAccount = "new_token"
		cloned.AccountType = commonpb.AccountType_SWAP
		assert.Equal(t, account.ErrInvalidAccountInfo, s.Put(ctx, &cloned))

		// todo: this case isn't possible with current account structures
		/*cloned = original.Clone()
		cloned.TokenAccount = "new_token"
		cloned.AccountType = ?
		cloned.Index = cloned.Index + 1
		assert.Equal(t, account.ErrInvalidAccountInfo, s.Put(ctx, &cloned))*/

		// Changing multiple fields with authority changed

		cloned = original.Clone()
		cloned.AuthorityAccount = "new_authority"
		cloned.Index = cloned.Index + 1
		assert.Equal(t, account.ErrInvalidAccountInfo, s.Put(ctx, &cloned))

		// todo: this case isn't possible with current account structures
		/*cloned = original.Clone()
		cloned.AuthorityAccount = "new_authority"
		cloned.AccountType = ?
		cloned.Index = cloned.Index + 1
		assert.Equal(t, account.ErrInvalidAccountInfo, s.Put(ctx, &cloned))*/

		// Changing multiple fields with account type changed

		// todo: this case isn't possible with current account structures
		/*cloned = original.Clone()
		cloned.AccountType = ?
		cloned.Index = cloned.Index + 1
		assert.Equal(t, account.ErrInvalidAccountInfo, s.Put(ctx, &cloned))*/

		// Ensure we didn't overwrite the original record
		actual, err := s.GetByTokenAddress(ctx, original.TokenAccount)
		require.NoError(t, err)
		assertEquivalentRecords(t, &original, actual)
	})
}

func testGetLatestByOwner(t *testing.T, s account.Store) {
	t.Run("testGetLatestByOwner", func(t *testing.T) {
		ctx := context.Background()

		_, err := s.GetLatestByOwnerAddress(ctx, "owner")
		assert.Equal(t, account.ErrAccountInfoNotFound, err)

		_, err = s.GetLatestByOwnerAddressAndType(ctx, "owner", commonpb.AccountType_POOL)
		assert.Equal(t, account.ErrAccountInfoNotFound, err)

		for _, mint := range []string{"mint1", "mint2"} {
			for _, accountType := range []commonpb.AccountType{
				commonpb.AccountType_PRIMARY,
				commonpb.AccountType_SWAP,
			} {
				record := &account.Record{
					OwnerAccount:     "owner",
					AuthorityAccount: fmt.Sprintf("authority_%s", accountType.String()),
					TokenAccount:     fmt.Sprintf("token_%s_%s", accountType.String(), mint),
					MintAccount:      mint,
					AccountType:      accountType,
					Index:            0,
				}
				if accountType == commonpb.AccountType_PRIMARY {
					record.AuthorityAccount = record.OwnerAccount
				}

				require.NoError(t, s.Put(ctx, record))
			}
		}

		for _, mint := range []string{"mint1", "mint2"} {
			for i, accountType := range []commonpb.AccountType{
				commonpb.AccountType_POOL,
			} {
				for j := 0; j < 5; j++ {
					record := &account.Record{
						OwnerAccount:     "owner",
						AuthorityAccount: fmt.Sprintf("authority_%s_%d%d", accountType.String(), i, j),
						TokenAccount:     fmt.Sprintf("token_%s_%s_%d%d", accountType.String(), mint, i, j),
						MintAccount:      mint,
						AccountType:      accountType,
						Index:            uint64(j),
					}
					require.NoError(t, s.Put(ctx, record))
				}
			}
		}

		actualByMintAndType, err := s.GetLatestByOwnerAddress(ctx, "owner")
		require.NoError(t, err)
		require.Len(t, actualByMintAndType, 2)
		for _, mint := range []string{"mint1", "mint2"} {
			actualByType, ok := actualByMintAndType[mint]
			require.True(t, ok)

			allActual, ok := actualByType[commonpb.AccountType_PRIMARY]
			require.True(t, ok)
			require.Len(t, allActual, 1)
			assert.Equal(t, fmt.Sprintf("token_PRIMARY_%s", mint), allActual[0].TokenAccount)

			allActual, ok = actualByType[commonpb.AccountType_SWAP]
			require.True(t, ok)
			require.Len(t, allActual, 1)
			assert.Equal(t, fmt.Sprintf("token_SWAP_%s", mint), allActual[0].TokenAccount)

			allActual, ok = actualByType[commonpb.AccountType_POOL]
			require.True(t, ok)
			require.Len(t, allActual, 5)
			for i, actual := range allActual {
				assert.Equal(t, fmt.Sprintf("token_POOL_%s_0%d", mint, i), actual.TokenAccount)
			}
		}

		actualByMint, err := s.GetLatestByOwnerAddressAndType(ctx, "owner", commonpb.AccountType_POOL)
		require.NoError(t, err)
		require.Len(t, actualByMint, 2)
		for _, mint := range []string{"mint1", "mint2"} {
			actual, ok := actualByMint[mint]
			require.True(t, ok)
			assert.Equal(t, fmt.Sprintf("token_POOL_%s_04", mint), actual.TokenAccount)
		}
	})
}

func testBatchedMethods(t *testing.T, s account.Store) {
	t.Run("testBatchedMethods", func(t *testing.T) {
		ctx := context.Background()

		var records []*account.Record
		for i := 0; i < 100; i++ {
			record := &account.Record{
				OwnerAccount:     fmt.Sprintf("owner%d", i),
				AuthorityAccount: fmt.Sprintf("authority%d", i),
				TokenAccount:     fmt.Sprintf("token%d", i),
				MintAccount:      fmt.Sprintf("mint%d", i),
				AccountType:      commonpb.AccountType_POOL,
				Index:            uint64(i),
			}

			require.NoError(t, s.Put(ctx, record))

			records = append(records, record)
		}

		actual, err := s.GetByTokenAddressBatch(ctx, "token0", "token1")
		require.NoError(t, err)
		require.Len(t, actual, 2)
		assertEquivalentRecords(t, records[0], actual[records[0].TokenAccount])
		assertEquivalentRecords(t, records[1], actual[records[1].TokenAccount])

		actual, err = s.GetByTokenAddressBatch(ctx, "token0", "token1", "token2", "token3", "token4")
		require.NoError(t, err)
		require.Len(t, actual, 5)
		assertEquivalentRecords(t, records[0], actual[records[0].TokenAccount])
		assertEquivalentRecords(t, records[1], actual[records[1].TokenAccount])
		assertEquivalentRecords(t, records[2], actual[records[2].TokenAccount])
		assertEquivalentRecords(t, records[3], actual[records[3].TokenAccount])
		assertEquivalentRecords(t, records[4], actual[records[4].TokenAccount])

		_, err = s.GetByTokenAddressBatch(ctx, "not-found")
		assert.Equal(t, account.ErrAccountInfoNotFound, err)

		_, err = s.GetByTokenAddressBatch(ctx, "token0", "not-found")
		assert.Equal(t, account.ErrAccountInfoNotFound, err)
	})
}

func testRemoteSendEdgeCases(t *testing.T, s account.Store) {
	t.Run("testRemoteSendEdgeCases", func(t *testing.T) {
		ctx := context.Background()

		remoteSendRecord := &account.Record{
			OwnerAccount:            "owner",
			AuthorityAccount:        "owner",
			TokenAccount:            "token",
			MintAccount:             "mint",
			AccountType:             commonpb.AccountType_REMOTE_SEND_GIFT_CARD,
			Index:                   uint64(0),
			RequiresAutoReturnCheck: true,
		}
		cloned := remoteSendRecord.Clone()

		primaryRecord := remoteSendRecord.Clone()
		primaryRecord.AccountType = commonpb.AccountType_PRIMARY

		require.NoError(t, s.Put(ctx, remoteSendRecord))
		assert.Error(t, s.Put(ctx, &primaryRecord))

		actual, err := s.GetByTokenAddress(ctx, "token")
		require.NoError(t, err)
		assert.Equal(t, commonpb.AccountType_REMOTE_SEND_GIFT_CARD, actual.AccountType)
		assertEquivalentRecords(t, &cloned, actual)

		actualByMint, err := s.GetByAuthorityAddress(ctx, cloned.AuthorityAccount)
		require.NoError(t, err)
		require.Len(t, actualByMint, 1)
		actual = actualByMint[cloned.MintAccount]
		assert.Equal(t, commonpb.AccountType_REMOTE_SEND_GIFT_CARD, actual.AccountType)
		assertEquivalentRecords(t, &cloned, actual)

		actualByMint, err = s.GetLatestByOwnerAddressAndType(ctx, "owner", commonpb.AccountType_REMOTE_SEND_GIFT_CARD)
		require.NoError(t, err)
		require.Len(t, actualByMint, 1)
		actual = actualByMint[cloned.MintAccount]
		assert.Equal(t, commonpb.AccountType_REMOTE_SEND_GIFT_CARD, actual.AccountType)
		assertEquivalentRecords(t, &cloned, actual)

		latestByMintAndType, err := s.GetLatestByOwnerAddress(ctx, "owner")
		require.NoError(t, err)
		require.Len(t, latestByMintAndType, 1)
		require.Len(t, latestByMintAndType[cloned.MintAccount], 1)
		records, ok := latestByMintAndType[cloned.MintAccount][commonpb.AccountType_REMOTE_SEND_GIFT_CARD]
		require.True(t, ok)
		require.Len(t, records, 1)
		actual = records[0]
		assert.Equal(t, commonpb.AccountType_REMOTE_SEND_GIFT_CARD, actual.AccountType)
		assertEquivalentRecords(t, &cloned, actual)

		remoteSendRecord.RequiresAutoReturnCheck = false
		cloned = remoteSendRecord.Clone()

		require.NoError(t, s.Update(ctx, remoteSendRecord))

		actual, err = s.GetByTokenAddress(ctx, "token")
		require.NoError(t, err)
		assert.False(t, actual.RequiresAutoReturnCheck)
		assertEquivalentRecords(t, &cloned, actual)
	})
}

func testSwapAccountEdgeCases(t *testing.T, s account.Store) {
	t.Run("testSwapAccountEdgeCases", func(t *testing.T) {
		ctx := context.Background()

		swapRecord := &account.Record{
			OwnerAccount:     "owner",
			AuthorityAccount: "authority",
			TokenAccount:     "token1",
			MintAccount:      "mint1",
			AccountType:      commonpb.AccountType_SWAP,
			Index:            uint64(0),
		}
		cloned := swapRecord.Clone()

		require.NoError(t, s.Put(ctx, swapRecord))
		assert.Equal(t, account.ErrAccountInfoExists, s.Put(ctx, swapRecord))

		actual, err := s.GetByTokenAddress(ctx, cloned.TokenAccount)
		require.NoError(t, err)
		assertEquivalentRecords(t, &cloned, actual)

		actualByMint, err := s.GetByAuthorityAddress(ctx, cloned.AuthorityAccount)
		require.NoError(t, err)
		require.Len(t, actualByMint, 1)
		actual = actualByMint[cloned.MintAccount]
		assertEquivalentRecords(t, &cloned, actual)

		actualByMint, err = s.GetLatestByOwnerAddressAndType(ctx, cloned.OwnerAccount, commonpb.AccountType_SWAP)
		require.NoError(t, err)
		require.Len(t, actualByMint, 1)
		actual = actualByMint[cloned.MintAccount]
		assertEquivalentRecords(t, &cloned, actual)

		actualByMintAndType, err := s.GetLatestByOwnerAddress(ctx, cloned.OwnerAccount)
		require.NoError(t, err)
		require.Len(t, actualByMintAndType, 1)
		require.Len(t, actualByMintAndType[actual.MintAccount], 1)
		require.Len(t, actualByMintAndType[actual.MintAccount][commonpb.AccountType_SWAP], 1)
		actual = actualByMintAndType[actual.MintAccount][commonpb.AccountType_SWAP][0]
		assertEquivalentRecords(t, &cloned, actual)
	})
}

func testGetByMintAndType(t *testing.T, s account.Store) {
	t.Run("testGetByMintAndType", func(t *testing.T) {
		ctx := context.Background()

		_, err := s.GetByMintAndType(ctx, "mint", commonpb.AccountType_POOL)
		assert.Equal(t, account.ErrAccountInfoNotFound, err)

		// Create multiple POOL accounts for mint1
		for i := 0; i < 3; i++ {
			record := &account.Record{
				OwnerAccount:     fmt.Sprintf("owner_pool_%d", i),
				AuthorityAccount: fmt.Sprintf("authority_pool_%d", i),
				TokenAccount:     fmt.Sprintf("token_pool_mint1_%d", i),
				MintAccount:      "mint1",
				AccountType:      commonpb.AccountType_POOL,
				Index:            uint64(i),
			}
			require.NoError(t, s.Put(ctx, record))
		}

		// Create a PRIMARY account for mint1
		primaryRecord := &account.Record{
			OwnerAccount:     "owner_primary",
			AuthorityAccount: "owner_primary",
			TokenAccount:     "token_primary_mint1",
			MintAccount:      "mint1",
			AccountType:      commonpb.AccountType_PRIMARY,
			Index:            0,
		}
		require.NoError(t, s.Put(ctx, primaryRecord))

		// Create a POOL account for mint2
		mint2Record := &account.Record{
			OwnerAccount:     "owner_pool_mint2",
			AuthorityAccount: "authority_pool_mint2",
			TokenAccount:     "token_pool_mint2",
			MintAccount:      "mint2",
			AccountType:      commonpb.AccountType_POOL,
			Index:            0,
		}
		require.NoError(t, s.Put(ctx, mint2Record))

		// Query POOL accounts for mint1
		results, err := s.GetByMintAndType(ctx, "mint1", commonpb.AccountType_POOL)
		require.NoError(t, err)
		require.Len(t, results, 3)
		for i, actual := range results {
			assert.Equal(t, "mint1", actual.MintAccount)
			assert.Equal(t, commonpb.AccountType_POOL, actual.AccountType)
			assert.Equal(t, uint64(i), actual.Index)
		}

		// Query PRIMARY accounts for mint1
		results, err = s.GetByMintAndType(ctx, "mint1", commonpb.AccountType_PRIMARY)
		require.NoError(t, err)
		require.Len(t, results, 1)
		assertEquivalentRecords(t, primaryRecord, results[0])

		// Query POOL accounts for mint2
		results, err = s.GetByMintAndType(ctx, "mint2", commonpb.AccountType_POOL)
		require.NoError(t, err)
		require.Len(t, results, 1)
		assertEquivalentRecords(t, mint2Record, results[0])

		// Query for a type with no results
		_, err = s.GetByMintAndType(ctx, "mint1", commonpb.AccountType_SWAP)
		assert.Equal(t, account.ErrAccountInfoNotFound, err)

		// Query for a mint with no results
		_, err = s.GetByMintAndType(ctx, "mint3", commonpb.AccountType_POOL)
		assert.Equal(t, account.ErrAccountInfoNotFound, err)
	})
}

func testDepositSyncMethods(t *testing.T, s account.Store) {
	t.Run("testDepositSyncMethods", func(t *testing.T) {
		ctx := context.Background()

		_, err := s.GetPrioritizedRequiringDepositSync(ctx, 10)
		assert.Equal(t, account.ErrAccountInfoNotFound, err)

		count, err := s.CountRequiringDepositSync(ctx)
		require.NoError(t, err)
		assert.EqualValues(t, 0, count)

		var records []*account.Record
		for i := 0; i < 10; i++ {
			record := &account.Record{
				OwnerAccount:         fmt.Sprintf("owner%d", i),
				AuthorityAccount:     fmt.Sprintf("owner%d", i),
				TokenAccount:         fmt.Sprintf("token%d", i),
				MintAccount:          "mint",
				AccountType:          commonpb.AccountType_PRIMARY,
				Index:                uint64(0),
				DepositsLastSyncedAt: time.Now().Add(time.Duration(-i) * time.Hour),
			}

			if i < 7 {
				record.RequiresDepositSync = true
			}

			require.NoError(t, s.Put(ctx, record))
			records = append(records, record)
		}

		count, err = s.CountRequiringDepositSync(ctx)
		require.NoError(t, err)
		assert.EqualValues(t, 7, count)

		result, err := s.GetPrioritizedRequiringDepositSync(ctx, 10)
		require.NoError(t, err)
		require.Len(t, result, 7)

		for i, actual := range result {
			assertEquivalentRecords(t, records[6-i], actual)
		}

		result, err = s.GetPrioritizedRequiringDepositSync(ctx, 3)
		require.NoError(t, err)
		require.Len(t, result, 3)

		for i, actual := range result {
			assertEquivalentRecords(t, records[6-i], actual)
		}
	})
}

func testAutoReturnCheckMethods(t *testing.T, s account.Store) {
	t.Run("testAutoReturnCheckMethods", func(t *testing.T) {
		ctx := context.Background()

		_, err := s.GetPrioritizedRequiringAutoReturnCheck(ctx, time.Duration(0), 10)
		assert.Equal(t, account.ErrAccountInfoNotFound, err)

		count, err := s.CountRequiringAutoReturnCheck(ctx)
		require.NoError(t, err)
		assert.EqualValues(t, 0, count)

		var records []*account.Record
		for i := 0; i < 10; i++ {
			record := &account.Record{
				OwnerAccount:     fmt.Sprintf("owner%d", i),
				AuthorityAccount: fmt.Sprintf("owner%d", i),
				TokenAccount:     fmt.Sprintf("token%d", i),
				MintAccount:      "mint",
				AccountType:      commonpb.AccountType_REMOTE_SEND_GIFT_CARD,
				Index:            uint64(0),
				CreatedAt:        time.Now().Add(time.Duration(-i) * time.Hour),
			}

			if i < 7 {
				record.RequiresAutoReturnCheck = true
			}

			require.NoError(t, s.Put(ctx, record))
			records = append(records, record)
		}

		count, err = s.CountRequiringAutoReturnCheck(ctx)
		require.NoError(t, err)
		assert.EqualValues(t, 7, count)

		result, err := s.GetPrioritizedRequiringAutoReturnCheck(ctx, time.Duration(0), 10)
		require.NoError(t, err)
		require.Len(t, result, 7)

		for i, actual := range result {
			assertEquivalentRecords(t, records[6-i], actual)
		}

		result, err = s.GetPrioritizedRequiringAutoReturnCheck(ctx, time.Duration(0), 3)
		require.NoError(t, err)
		require.Len(t, result, 3)

		for i, actual := range result {
			assertEquivalentRecords(t, records[6-i], actual)
		}

		result, err = s.GetPrioritizedRequiringAutoReturnCheck(ctx, 2*time.Hour+time.Second, 10)
		require.NoError(t, err)
		require.Len(t, result, 4)

		for i, actual := range result {
			assertEquivalentRecords(t, records[6-i], actual)
		}
	})
}

func testBalanceLifecycle(t *testing.T, s account.Store) {
	t.Run("testBalanceLifecycle", func(t *testing.T) {
		ctx := context.Background()

		// Balance operations on an account that doesn't exist.
		_, err := s.GetBalanceForUpdate(ctx, "token")
		assert.Equal(t, account.ErrAccountInfoNotFound, err)

		record := &account.Record{
			OwnerAccount:     "owner",
			AuthorityAccount: "owner",
			TokenAccount:     "token",
			MintAccount:      "mint",
			AccountType:      commonpb.AccountType_PRIMARY,
		}
		require.NoError(t, s.Put(ctx, record))

		// A newly created account starts with an initialized zero balance.
		balance, err := s.GetBalanceForUpdate(ctx, "token")
		require.NoError(t, err)
		require.NotNil(t, balance)
		assert.EqualValues(t, 0, *balance)

		// It is already initialized, so it cannot be initialized again.
		assert.Equal(t, account.ErrBalanceAlreadyInitialized, s.InitializeBalance(ctx, "token", 500))

		// Positive and negative deltas are applied atomically.
		require.NoError(t, s.ApplyBalanceDelta(ctx, "token", 500))
		require.NoError(t, s.ApplyBalanceDelta(ctx, "token", 250))
		require.NoError(t, s.ApplyBalanceDelta(ctx, "token", -300))

		balance, err = s.GetBalanceForUpdate(ctx, "token")
		require.NoError(t, err)
		require.NotNil(t, balance)
		assert.EqualValues(t, 450, *balance)

		// A delta that would drive the balance below zero is rejected, and the
		// balance is left unchanged.
		assert.Equal(t, account.ErrNegativeBalance, s.ApplyBalanceDelta(ctx, "token", -451))

		balance, err = s.GetBalanceForUpdate(ctx, "token")
		require.NoError(t, err)
		require.NotNil(t, balance)
		assert.EqualValues(t, 450, *balance)

		// A delta bringing the balance to exactly zero is allowed.
		require.NoError(t, s.ApplyBalanceDelta(ctx, "token", -450))

		// The stored balance is visible through the regular getter.
		actual, err := s.GetByTokenAddress(ctx, "token")
		require.NoError(t, err)
		require.NotNil(t, actual.Balance)
		assert.EqualValues(t, 0, *actual.Balance)

		// Per the store contract a caller checks GetBalanceForUpdate first, so
		// these are unreachable in practice, but a missing account still
		// resolves to the same 0-row result as an un/already-initialized one.
		assert.Equal(t, account.ErrBalanceNotInitialized, s.ApplyBalanceDelta(ctx, "missing", 1))
		assert.Equal(t, account.ErrBalanceAlreadyInitialized, s.InitializeBalance(ctx, "missing", 1))
	})
}

func testBalanceConcurrentDeltas(t *testing.T, s account.Store) {
	t.Run("testBalanceConcurrentDeltas", func(t *testing.T) {
		ctx := context.Background()

		record := &account.Record{
			OwnerAccount:     "owner",
			AuthorityAccount: "owner",
			TokenAccount:     "token",
			MintAccount:      "mint",
			AccountType:      commonpb.AccountType_PRIMARY,
		}
		require.NoError(t, s.Put(ctx, record))

		// The account starts at a zero balance. Concurrent deltas must not lose
		// updates: each is serialized by the account's row lock (postgres) or
		// the store mutex (memory).
		const workers = 20
		const perWorker = 25

		var wg sync.WaitGroup
		wg.Add(workers)
		for i := 0; i < workers; i++ {
			go func() {
				defer wg.Done()
				for j := 0; j < perWorker; j++ {
					assert.NoError(t, s.ApplyBalanceDelta(ctx, "token", 1))
				}
			}()
		}
		wg.Wait()

		balance, err := s.GetBalanceForUpdate(ctx, "token")
		require.NoError(t, err)
		require.NotNil(t, balance)
		assert.EqualValues(t, workers*perWorker, *balance)
	})
}

func testBalanceInsertGuard(t *testing.T, s account.Store) {
	t.Run("testBalanceInsertGuard", func(t *testing.T) {
		ctx := context.Background()

		// A new account cannot be created with a pre-existing non-zero balance.
		nonZero := &account.Record{
			OwnerAccount:     "owner_nonzero",
			AuthorityAccount: "owner_nonzero",
			TokenAccount:     "token_nonzero",
			MintAccount:      "mint",
			AccountType:      commonpb.AccountType_PRIMARY,
			Balance:          pointer.Uint64(100),
		}
		assert.Equal(t, account.ErrInvalidAccountInfo, s.Put(ctx, nonZero))

		_, err := s.GetByTokenAddress(ctx, "token_nonzero")
		assert.Equal(t, account.ErrAccountInfoNotFound, err)

		// An explicit zero balance on insert is allowed.
		zero := &account.Record{
			OwnerAccount:     "owner_zero",
			AuthorityAccount: "owner_zero",
			TokenAccount:     "token_zero",
			MintAccount:      "mint",
			AccountType:      commonpb.AccountType_PRIMARY,
			Balance:          pointer.Uint64(0),
		}
		require.NoError(t, s.Put(ctx, zero))

		actual, err := s.GetByTokenAddress(ctx, "token_zero")
		require.NoError(t, err)
		require.NotNil(t, actual.Balance)
		assert.EqualValues(t, 0, *actual.Balance)

		// An unset balance on insert is defaulted to zero — a new account has
		// no history.
		unset := &account.Record{
			OwnerAccount:     "owner_unset",
			AuthorityAccount: "owner_unset",
			TokenAccount:     "token_unset",
			MintAccount:      "mint",
			AccountType:      commonpb.AccountType_PRIMARY,
		}
		require.NoError(t, s.Put(ctx, unset))
		require.NotNil(t, unset.Balance)
		assert.EqualValues(t, 0, *unset.Balance)

		actual, err = s.GetByTokenAddress(ctx, "token_unset")
		require.NoError(t, err)
		require.NotNil(t, actual.Balance)
		assert.EqualValues(t, 0, *actual.Balance)
	})
}

// RunUninitializedBalanceTests verifies the balance methods against an account
// whose stored balance is NULL — a legacy, pre-migration row. The Store no
// longer produces that state (new accounts are created with a zero balance),
// so the caller must clear tokenAccount's balance by implementation-specific
// means before calling this.
func RunUninitializedBalanceTests(t *testing.T, s account.Store, tokenAccount string) {
	ctx := context.Background()

	// An uninitialized balance reads back as nil.
	balance, err := s.GetBalanceForUpdate(ctx, tokenAccount)
	require.NoError(t, err)
	assert.Nil(t, balance)

	// A delta cannot be applied before the balance is initialized.
	assert.Equal(t, account.ErrBalanceNotInitialized, s.ApplyBalanceDelta(ctx, tokenAccount, 100))

	// Initialize the balance.
	require.NoError(t, s.InitializeBalance(ctx, tokenAccount, 777))

	balance, err = s.GetBalanceForUpdate(ctx, tokenAccount)
	require.NoError(t, err)
	require.NotNil(t, balance)
	assert.EqualValues(t, 777, *balance)

	// The balance cannot be initialized twice.
	assert.Equal(t, account.ErrBalanceAlreadyInitialized, s.InitializeBalance(ctx, tokenAccount, 999))

	// Once initialized, deltas apply normally.
	require.NoError(t, s.ApplyBalanceDelta(ctx, tokenAccount, -77))

	balance, err = s.GetBalanceForUpdate(ctx, tokenAccount)
	require.NoError(t, err)
	require.NotNil(t, balance)
	assert.EqualValues(t, 700, *balance)
}

func assertEquivalentRecords(t *testing.T, obj1, obj2 *account.Record) {
	assert.Equal(t, obj1.OwnerAccount, obj2.OwnerAccount)
	assert.Equal(t, obj1.AuthorityAccount, obj2.AuthorityAccount)
	assert.Equal(t, obj1.TokenAccount, obj2.TokenAccount)
	assert.Equal(t, obj1.MintAccount, obj2.MintAccount)
	assert.Equal(t, obj1.AccountType, obj2.AccountType)
	assert.Equal(t, obj1.Index, obj2.Index)
	assert.Equal(t, obj1.RequiresDepositSync, obj2.RequiresDepositSync)
	assert.Equal(t, obj1.DepositsLastSyncedAt.Unix(), obj2.DepositsLastSyncedAt.Unix())
	assert.Equal(t, obj1.RequiresAutoReturnCheck, obj2.RequiresAutoReturnCheck)
}
