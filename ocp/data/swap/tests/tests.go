package tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/code-payments/ocp-server/database/query"
	"github.com/code-payments/ocp-server/ocp/data/swap"
)

func RunTests(t *testing.T, s swap.Store, teardown func()) {
	for _, tf := range []func(t *testing.T, s swap.Store){
		testRoundTrip,
		testUpdateHappyPath,
		testUpdateStaleRecord,
		testGetAllByOwnerAndState,
		testGetAllByOwnerMintAndState,
		testGetAllByState,
	} {
		tf(t, s)
		teardown()
	}
}

func testRoundTrip(t *testing.T, s swap.Store) {
	t.Run("testRoundTrip", func(t *testing.T) {
		ctx := context.Background()

		actual, err := s.GetById(ctx, "test_swap_id")
		require.Error(t, err)
		assert.Equal(t, swap.ErrNotFound, err)
		assert.Nil(t, actual)

		actual, err = s.GetByFundingId(ctx, "test_funding_id")
		require.Error(t, err)
		assert.Equal(t, swap.ErrNotFound, err)
		assert.Nil(t, actual)

		expected := &swap.Record{
			SwapId: "test_swap_id",

			Owner: "test_owner",

			FromMint: "test_from_mint",
			ToMint:   "test_to_mint",
			SwapAmount:   12345,

			FundingId:     "test_funding_id",
			FundingSource: swap.FundingSourceSubmitIntent,

			Nonce:     "test_nonce",
			Blockhash: "test_blockhash",

			ProofSignature: "test_proof_signature",

			TransactionSignature: "test_transaction_signature",
			TransactionBlob:      []byte("test_transaction_blob"),

			State: swap.StateFinalized,

			CreatedAt: time.Now(),
		}
		cloned := expected.Clone()
		err = s.Save(ctx, expected)
		require.NoError(t, err)
		assert.EqualValues(t, 1, expected.Id)
		assert.EqualValues(t, 1, expected.Version)

		actual, err = s.GetById(ctx, "test_swap_id")
		require.NoError(t, err)
		assertEquivalentRecords(t, &cloned, actual)

		actual, err = s.GetByFundingId(ctx, "test_funding_id")
		require.NoError(t, err)
		assertEquivalentRecords(t, &cloned, actual)
	})
}

func testUpdateHappyPath(t *testing.T, s swap.Store) {
	t.Run("testUpdateHappyPath", func(t *testing.T) {
		ctx := context.Background()

		actual, err := s.GetById(ctx, "test_swap_id")
		require.Error(t, err)
		assert.Equal(t, swap.ErrNotFound, err)
		assert.Nil(t, actual)

		expected := &swap.Record{
			SwapId: "test_swap_id",

			Owner: "test_owner",

			FromMint: "test_from_mint",
			ToMint:   "test_to_mint",
			SwapAmount:   12345,

			FundingId:     "test_funding_id",
			FundingSource: swap.FundingSourceSubmitIntent,

			Nonce:     "test_nonce",
			Blockhash: "test_blockhash",

			ProofSignature: "test_proof_signature",

			TransactionSignature: "test_transaction_signature",
			TransactionBlob:      nil,

			State: swap.StateCreated,

			CreatedAt: time.Now(),
		}
		err = s.Save(ctx, expected)
		require.NoError(t, err)
		assert.EqualValues(t, 1, expected.Id)
		assert.EqualValues(t, 1, expected.Version)

		expected.TransactionBlob = []byte("transaction_blob")
		expected.State = swap.StateFinalized

		err = s.Save(ctx, expected)
		require.NoError(t, err)
		assert.EqualValues(t, 1, expected.Id)
		assert.EqualValues(t, 2, expected.Version)

		actual, err = s.GetById(ctx, "test_swap_id")
		require.NoError(t, err)
		assertEquivalentRecords(t, expected, actual)
	})
}

func testUpdateStaleRecord(t *testing.T, s swap.Store) {
	t.Run("testUpdateStaleRecord", func(t *testing.T) {
		ctx := context.Background()

		expected := &swap.Record{
			SwapId: "test_swap_id",

			Owner: "test_owner",

			FromMint: "test_from_mint",
			ToMint:   "test_to_mint",
			SwapAmount:   12345,

			FundingId:     "test_funding_id",
			FundingSource: swap.FundingSourceSubmitIntent,

			Nonce:     "test_nonce",
			Blockhash: "test_blockhash",

			ProofSignature: "test_proof_signature",

			TransactionSignature: "test_transaction_signature",
			TransactionBlob:      []byte("test_transaction_blob"),

			State: swap.StateFinalized,

			CreatedAt: time.Now(),
		}
		err := s.Save(ctx, expected)
		require.NoError(t, err)
		assert.EqualValues(t, 1, expected.Id)
		assert.EqualValues(t, 1, expected.Version)

		stale := expected.Clone()
		expected.State = swap.StateUnknown
		expected.TransactionBlob = nil
		stale.Version -= 1

		err = s.Save(ctx, &stale)
		assert.Equal(t, swap.ErrStaleVersion, err)
		assert.EqualValues(t, 1, stale.Id)
		assert.EqualValues(t, 0, stale.Version)

		actual, err := s.GetById(ctx, "test_swap_id")
		require.NoError(t, err)
		assert.Equal(t, swap.StateFinalized, actual.State)
		assert.NotNil(t, actual.TransactionSignature)
		assert.NotEmpty(t, actual.TransactionBlob)
		assert.EqualValues(t, 1, actual.Id)
		assert.EqualValues(t, 1, actual.Version)
	})
}

func testGetAllByOwnerAndState(t *testing.T, s swap.Store) {
	t.Run("testGetAllByOwnerAndState", func(t *testing.T) {
		ctx := context.Background()

		_, err := s.GetAllByOwnerAndState(ctx, "test_owner_0", swap.StateFinalized)
		assert.Equal(t, swap.ErrNotFound, err)

		var records []*swap.Record
		for i := range 100 {
			record := &swap.Record{
				SwapId: fmt.Sprintf("test_swap_id_%d", i),

				Owner: fmt.Sprintf("test_owner_%d", i%2),

				FromMint: fmt.Sprintf("test_from_mint_%d", i),
				ToMint:   fmt.Sprintf("test_to_mint_%d", i),
				SwapAmount:   uint64(i + 1),

				FundingId:     fmt.Sprintf("test_funding_id_%d", i),
				FundingSource: swap.FundingSourceSubmitIntent,

				Nonce:     fmt.Sprintf("test_nonce_%d", i),
				Blockhash: fmt.Sprintf("test_blockhash_%d", i),

				ProofSignature: fmt.Sprintf("test_proof_signature_%d", i),

				TransactionSignature: fmt.Sprintf("test_transaction_signature_%d", i),
				TransactionBlob:      []byte(fmt.Sprintf("test_transaction_blob_%d", i)),

				State: swap.State(i % int(swap.StateCancelled+1)),

				CreatedAt: time.Now(),
			}
			require.NoError(t, s.Save(ctx, record))

			records = append(records, record)
		}

		for _, owner := range []string{"test_owner_0", "test_owner_1"} {
			for state := range swap.StateCancelled + 1 {
				allActual, err := s.GetAllByOwnerAndState(ctx, owner, state)
				require.NoError(t, err)
				require.NotEmpty(t, allActual)

				for _, record := range records {
					if record.Owner == owner && record.State == state {
						var found bool
						for _, actual := range allActual {
							if actual.SwapId == record.SwapId {
								found = true
								assertEquivalentRecords(t, record, actual)
								break
							}
						}
						assert.True(t, found)
					}
				}
			}
		}
	})
}

func testGetAllByOwnerMintAndState(t *testing.T, s swap.Store) {
	t.Run("testGetAllByOwnerAndMint", func(t *testing.T) {
		ctx := context.Background()

		_, err := s.GetAllByOwnerMintAndState(ctx, "test_owner", "test_mint", swap.StateFinalized)
		assert.Equal(t, swap.ErrNotFound, err)

		// Create swaps with different owners, mints, and states
		records := []*swap.Record{
			{ // owner_a buying mint_x
				SwapId: "swap_0", Owner: "owner_a",
				FromMint: "core_mint", ToMint: "mint_x", SwapAmount: 100,
				FundingId: "fund_0", FundingSource: swap.FundingSourceSubmitIntent,
				Nonce: "nonce_0", Blockhash: "bh_0", ProofSignature: "proof_0",
				TransactionSignature: "sig_0", State: swap.StateFinalized, CreatedAt: time.Now(),
			},
			{ // owner_a selling mint_x
				SwapId: "swap_1", Owner: "owner_a",
				FromMint: "mint_x", ToMint: "core_mint", SwapAmount: 50,
				FundingId: "fund_1", FundingSource: swap.FundingSourceSubmitIntent,
				Nonce: "nonce_1", Blockhash: "bh_1", ProofSignature: "proof_1",
				TransactionSignature: "sig_1", State: swap.StateFinalized, CreatedAt: time.Now(),
			},
			{ // owner_a buying mint_y (different mint)
				SwapId: "swap_2", Owner: "owner_a",
				FromMint: "core_mint", ToMint: "mint_y", SwapAmount: 200,
				FundingId: "fund_2", FundingSource: swap.FundingSourceSubmitIntent,
				Nonce: "nonce_2", Blockhash: "bh_2", ProofSignature: "proof_2",
				TransactionSignature: "sig_2", State: swap.StateFinalized, CreatedAt: time.Now(),
			},
			{ // owner_b buying mint_x (different owner)
				SwapId: "swap_3", Owner: "owner_b",
				FromMint: "core_mint", ToMint: "mint_x", SwapAmount: 300,
				FundingId: "fund_3", FundingSource: swap.FundingSourceSubmitIntent,
				Nonce: "nonce_3", Blockhash: "bh_3", ProofSignature: "proof_3",
				TransactionSignature: "sig_3", State: swap.StateFinalized, CreatedAt: time.Now(),
			},
			{ // owner_a buying mint_x but not finalized
				SwapId: "swap_4", Owner: "owner_a",
				FromMint: "core_mint", ToMint: "mint_x", SwapAmount: 400,
				FundingId: "fund_4", FundingSource: swap.FundingSourceSubmitIntent,
				Nonce: "nonce_4", Blockhash: "bh_4", ProofSignature: "proof_4",
				TransactionSignature: "sig_4", State: swap.StateCreated, CreatedAt: time.Now(),
			},
		}

		for _, record := range records {
			require.NoError(t, s.Save(ctx, record))
		}

		// owner_a + mint_x + finalized: should get swap_0 (buy) and swap_1 (sell)
		results, err := s.GetAllByOwnerMintAndState(ctx, "owner_a", "mint_x", swap.StateFinalized)
		require.NoError(t, err)
		require.Len(t, results, 2)
		assert.Equal(t, "swap_0", results[0].SwapId)
		assert.Equal(t, "swap_1", results[1].SwapId)

		// owner_a + mint_y + finalized: should get swap_2 only
		results, err = s.GetAllByOwnerMintAndState(ctx, "owner_a", "mint_y", swap.StateFinalized)
		require.NoError(t, err)
		require.Len(t, results, 1)
		assert.Equal(t, "swap_2", results[0].SwapId)

		// owner_b + mint_x + finalized: should get swap_3 only
		results, err = s.GetAllByOwnerMintAndState(ctx, "owner_b", "mint_x", swap.StateFinalized)
		require.NoError(t, err)
		require.Len(t, results, 1)
		assert.Equal(t, "swap_3", results[0].SwapId)

		// owner_a + mint_x + created: should get swap_4 only
		results, err = s.GetAllByOwnerMintAndState(ctx, "owner_a", "mint_x", swap.StateCreated)
		require.NoError(t, err)
		require.Len(t, results, 1)
		assert.Equal(t, "swap_4", results[0].SwapId)

		// no matching records
		_, err = s.GetAllByOwnerMintAndState(ctx, "owner_b", "mint_y", swap.StateFinalized)
		assert.Equal(t, swap.ErrNotFound, err)
	})
}

func testGetAllByState(t *testing.T, s swap.Store) {
	t.Run("testGetAllByState", func(t *testing.T) {
		ctx := context.Background()

		_, err := s.GetAllByState(ctx, swap.StateFinalized, query.EmptyCursor, 1, query.Ascending)
		assert.Equal(t, swap.ErrNotFound, err)

		var records []*swap.Record
		for i := range 100 {
			state := swap.StateFinalized
			if i >= 50 {
				state = swap.StateCreated
			}

			record := &swap.Record{
				SwapId: fmt.Sprintf("test_swap_id_%d", i),

				Owner: fmt.Sprintf("test_owner_%d", i%3),

				FromMint: "test_from_mint",
				ToMint:   "test_to_mint",
				SwapAmount:   uint64(i + 1),

				FundingId:     fmt.Sprintf("test_funding_id_%d", i),
				FundingSource: swap.FundingSourceSubmitIntent,

				Nonce:     fmt.Sprintf("test_nonce_%d", i),
				Blockhash: fmt.Sprintf("test_blockhash_%d", i),

				ProofSignature: fmt.Sprintf("test_proof_signature_%d", i),

				TransactionSignature: fmt.Sprintf("test_transaction_signature_%d", i),
				TransactionBlob:      []byte(fmt.Sprintf("test_transaction_blob_%d", i)),

				State: state,

				CreatedAt: time.Now(),
			}
			require.NoError(t, s.Save(ctx, record))

			records = append(records, record)
		}

		allActual, err := s.GetAllByState(ctx, swap.StateFinalized, query.EmptyCursor, 100, query.Ascending)
		require.NoError(t, err)
		require.Len(t, allActual, 50)
		for i, actual := range allActual {
			assertEquivalentRecords(t, records[i], actual)
		}

		allActual, err = s.GetAllByState(ctx, swap.StateFinalized, query.EmptyCursor, 10, query.Ascending)
		require.NoError(t, err)
		require.Len(t, allActual, 10)
		for i, actual := range allActual {
			assertEquivalentRecords(t, records[i], actual)
		}

		allActual, err = s.GetAllByState(ctx, swap.StateFinalized, query.EmptyCursor, 10, query.Descending)
		require.NoError(t, err)
		require.Len(t, allActual, 10)
		for i, actual := range allActual {
			assertEquivalentRecords(t, records[50-i-1], actual)
		}

		allActual, err = s.GetAllByState(ctx, swap.StateFinalized, query.ToCursor(records[23].Id), 10, query.Ascending)
		require.NoError(t, err)
		require.Len(t, allActual, 10)
		for i, actual := range allActual {
			assertEquivalentRecords(t, records[23+i+1], actual)
		}

		allActual, err = s.GetAllByState(ctx, swap.StateFinalized, query.ToCursor(records[23].Id), 10, query.Descending)
		require.NoError(t, err)
		require.Len(t, allActual, 10)
		for i, actual := range allActual {
			assertEquivalentRecords(t, records[23-i-1], actual)
		}

		_, err = s.GetAllByState(ctx, swap.StateFinalized, query.ToCursor(records[50].Id), 10, query.Ascending)
		assert.Equal(t, swap.ErrNotFound, err)
	})
}

func assertEquivalentRecords(t *testing.T, obj1, obj2 *swap.Record) {
	assert.Equal(t, obj1.SwapId, obj2.SwapId)

	assert.Equal(t, obj1.Owner, obj2.Owner)

	assert.Equal(t, obj1.FromMint, obj2.FromMint)
	assert.Equal(t, obj1.ToMint, obj2.ToMint)
	assert.Equal(t, obj1.SwapAmount, obj2.SwapAmount)
	assert.Equal(t, obj1.FeeAmount, obj2.FeeAmount)

	assert.Equal(t, obj1.FundingId, obj2.FundingId)
	assert.Equal(t, obj1.FundingSource, obj2.FundingSource)

	assert.Equal(t, obj1.Nonce, obj2.Nonce)
	assert.Equal(t, obj1.Blockhash, obj2.Blockhash)

	assert.Equal(t, obj1.ProofSignature, obj2.ProofSignature)

	assert.EqualValues(t, obj1.TransactionSignature, obj2.TransactionSignature)
	assert.Equal(t, obj1.TransactionBlob, obj2.TransactionBlob)

	assert.Equal(t, obj1.State, obj2.State)
}
