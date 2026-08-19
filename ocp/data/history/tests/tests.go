package tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/code-payments/ocp-server/currency"
	"github.com/code-payments/ocp-server/database/query"
	"github.com/code-payments/ocp-server/ocp/data/history"
	"github.com/code-payments/ocp-server/pointer"
)

func RunTests(t *testing.T, s history.Store, teardown func()) {
	for _, tf := range []func(t *testing.T, s history.Store){
		testRoundTrip,
		testSaveDuplicateReference,
		testUpdateHappyPath,
		testUpdateAppliesOnlyMutableFields,
		testUpdateStaleRecord,
		testGetAllByOwner,
		testGetAllByOwnerOrdersByEventTime,
		testGetAllByOwnerMint,
		testGetAllByIds,
		testGetAllByReference,
		testGetAllByGiftCardVault,
	} {
		tf(t, s)
		teardown()
	}
}

func testRoundTrip(t *testing.T, s history.Store) {
	t.Run("testRoundTrip", func(t *testing.T) {
		ctx := context.Background()

		actual, err := s.GetAllByReference(ctx, history.IntentReference, "test_reference")
		require.Error(t, err)
		assert.Equal(t, history.ErrNotFound, err)
		assert.Nil(t, actual)

		expected := newRecord("test_owner", "test_reference")
		require.NoError(t, s.Save(ctx, expected))

		assert.True(t, expected.Id > 0)
		assert.EqualValues(t, 1, expected.Version)
		assert.Equal(t, expected.CreatedAt, expected.UpdatedAt)

		actual, err = s.GetAllByReference(ctx, history.IntentReference, "test_reference")
		require.NoError(t, err)
		require.Len(t, actual, 1)
		assertEquivalentRecords(t, expected, actual[0])
	})
}

func testSaveDuplicateReference(t *testing.T, s history.Store) {
	t.Run("testSaveDuplicateReference", func(t *testing.T) {
		ctx := context.Background()

		require.NoError(t, s.Save(ctx, newRecord("test_owner", "test_reference")))

		// The same owner cannot hold two records for one reference, which is what
		// makes a retried write a no-op instead of a double entry.
		err := s.Save(ctx, newRecord("test_owner", "test_reference"))
		require.Error(t, err)
		assert.Equal(t, history.ErrExists, err)

		// A different owner's view of the same event is a separate record.
		require.NoError(t, s.Save(ctx, newRecord("test_other_owner", "test_reference")))

		actual, err := s.GetAllByReference(ctx, history.IntentReference, "test_reference")
		require.NoError(t, err)
		assert.Len(t, actual, 2)

		// An ID is only unique within its own kind. Intent IDs and swap IDs are
		// both client supplied public keys, so the same owner can hold a record
		// for each without either write landing on the other.
		sameIdOtherKind := newRecord("test_owner", "test_reference")
		sameIdOtherKind.ReferenceType = history.SwapReference
		require.NoError(t, s.Save(ctx, sameIdOtherKind))

		actual, err = s.GetAllByReference(ctx, history.SwapReference, "test_reference")
		require.NoError(t, err)
		require.Len(t, actual, 1)
		assert.Equal(t, sameIdOtherKind.Id, actual[0].Id)
		assert.Equal(t, "test_owner", actual[0].OwnerAccount)

		// And a lookup of one kind never returns the other's.
		actual, err = s.GetAllByReference(ctx, history.IntentReference, "test_reference")
		require.NoError(t, err)
		assert.Len(t, actual, 2)
		for _, record := range actual {
			assert.Equal(t, history.IntentReference, record.ReferenceType)
		}
	})
}

func testUpdateHappyPath(t *testing.T, s history.Store) {
	t.Run("testUpdateHappyPath", func(t *testing.T) {
		ctx := context.Background()

		record := newSwapRecord("test_owner", "test_reference")
		record.State = history.StatePending
		require.NoError(t, s.Save(ctx, record))
		require.Nil(t, record.DestinationQuantity)
		writtenAt := record.UpdatedAt

		// A swap finalizing is what fills in the destination leg and the fee taken
		// out of the trade, neither of which is known at submission.
		record.State = history.StateCompleted
		record.DestinationQuantity = pointer.Uint64(999)
		record.Fees = []history.Fee{{
			Type:         history.ReserveSellFee,
			NativeAmount: 0.25,
		}}
		require.NoError(t, s.Save(ctx, record))
		assert.EqualValues(t, 2, record.Version)

		actual, err := s.GetAllByReference(ctx, history.IntentReference, "test_reference")
		require.NoError(t, err)
		require.Len(t, actual, 1)

		assert.Equal(t, history.StateCompleted, actual[0].State)
		require.NotNil(t, actual[0].DestinationQuantity)
		assert.EqualValues(t, 999, *actual[0].DestinationQuantity)
		require.Len(t, actual[0].Fees, 1)
		assert.Equal(t, history.ReserveSellFee, actual[0].Fees[0].Type)
		assert.EqualValues(t, 0.25, actual[0].Fees[0].NativeAmount)
		assert.EqualValues(t, 2, actual[0].Version)

		// An update is a new write, so it moves updated_at off the time of the
		// write before it, and leaves the event time alone.
		assert.True(t, actual[0].UpdatedAt.After(writtenAt), "an update must advance updated_at")
		assert.True(t, record.UpdatedAt.After(writtenAt), "an update must advance the caller's updated_at")
		assert.True(t, actual[0].CreatedAt.Equal(record.CreatedAt), "an update must not move created_at")
	})
}

func testUpdateAppliesOnlyMutableFields(t *testing.T, s history.Store) {
	t.Run("testUpdateAppliesOnlyMutableFields", func(t *testing.T) {
		ctx := context.Background()

		record := newRecord("test_owner", "test_reference")
		require.NoError(t, s.Save(ctx, record))

		// A caller editing what an update doesn't carry neither persists the
		// edit nor keeps it: the record comes back as stored.
		record.State = history.StateFailed
		record.MintAccount = "edited_mint"
		record.Quantity = 999

		require.NoError(t, s.Save(ctx, record))

		assert.Equal(t, history.StateFailed, record.State)
		assert.Equal(t, "test_mint", record.MintAccount)
		assert.EqualValues(t, 12345, record.Quantity)

		actual, err := s.GetAllByReference(ctx, history.IntentReference, "test_reference")
		require.NoError(t, err)
		require.Len(t, actual, 1)
		assert.Equal(t, history.StateFailed, actual[0].State)
		assert.Equal(t, "test_mint", actual[0].MintAccount)
		assert.EqualValues(t, 12345, actual[0].Quantity)
	})
}

func testUpdateStaleRecord(t *testing.T, s history.Store) {
	t.Run("testUpdateStaleRecord", func(t *testing.T) {
		ctx := context.Background()

		record := newRecord("test_owner", "test_reference")
		require.NoError(t, s.Save(ctx, record))

		stale := record.Clone()
		require.NoError(t, s.Save(ctx, record))

		stale.State = history.StateCompleted
		err := s.Save(ctx, &stale)
		require.Error(t, err)
		assert.Equal(t, history.ErrStaleVersion, err)
	})
}

func testGetAllByOwner(t *testing.T, s history.Store) {
	t.Run("testGetAllByOwner", func(t *testing.T) {
		ctx := context.Background()

		_, err := s.GetAllByOwner(ctx, "test_owner", query.EmptyCursor, 10, query.Ascending)
		assert.Equal(t, history.ErrNotFound, err)

		var saved []*history.Record
		for i := 0; i < 5; i++ {
			record := newRecord("test_owner", fmt.Sprintf("test_reference_%d", i))
			require.NoError(t, s.Save(ctx, record))
			saved = append(saved, record)
		}
		require.NoError(t, s.Save(ctx, newRecord("test_other_owner", "test_other_reference")))

		// An owner sees only their own records, oldest first.
		actual, err := s.GetAllByOwner(ctx, "test_owner", query.EmptyCursor, 10, query.Ascending)
		require.NoError(t, err)
		require.Len(t, actual, 5)
		for i, record := range actual {
			assert.Equal(t, saved[i].Id, record.Id)
			assert.Equal(t, "test_owner", record.OwnerAccount)
		}

		// Descending is the feed order, newest first.
		actual, err = s.GetAllByOwner(ctx, "test_owner", query.EmptyCursor, 10, query.Descending)
		require.NoError(t, err)
		require.Len(t, actual, 5)
		for i, record := range actual {
			assert.Equal(t, saved[len(saved)-1-i].Id, record.Id)
		}

		// A limit bounds the page.
		actual, err = s.GetAllByOwner(ctx, "test_owner", query.EmptyCursor, 2, query.Ascending)
		require.NoError(t, err)
		require.Len(t, actual, 2)
		assert.Equal(t, saved[0].Id, actual[0].Id)
		assert.Equal(t, saved[1].Id, actual[1].Id)

		// A cursor resumes strictly after the record it names.
		cursor := history.ToCursor(saved[1].CreatedAt, saved[1].Id)

		actual, err = s.GetAllByOwner(ctx, "test_owner", cursor, 10, query.Ascending)
		require.NoError(t, err)
		require.Len(t, actual, 3)
		assert.Equal(t, saved[2].Id, actual[0].Id)

		actual, err = s.GetAllByOwner(ctx, "test_owner", cursor, 10, query.Descending)
		require.NoError(t, err)
		require.Len(t, actual, 1)
		assert.Equal(t, saved[0].Id, actual[0].Id)

		// Paging past the end is empty rather than an error-free short page.
		_, err = s.GetAllByOwner(ctx, "test_owner", history.ToCursor(saved[4].CreatedAt, saved[4].Id), 10, query.Ascending)
		assert.Equal(t, history.ErrNotFound, err)

		// A cursor this package did not produce is rejected rather than treated
		// as a position.
		_, err = s.GetAllByOwner(ctx, "test_owner", query.ToCursor(saved[1].Id), 10, query.Ascending)
		assert.Equal(t, history.ErrInvalidCursor, err)
	})
}

func testGetAllByOwnerOrdersByEventTime(t *testing.T, s history.Store) {
	t.Run("testGetAllByOwnerOrdersByEventTime", func(t *testing.T) {
		ctx := context.Background()

		recent := newRecord("test_owner", "test_reference_recent")
		recent.CreatedAt = baseTime.Add(time.Hour)
		require.NoError(t, s.Save(ctx, recent))

		// Written second but happened first, as a backfill or a deposit noticed
		// after the fact would be. A history is read in the order events
		// happened, so this belongs before the record already stored, even
		// though it was written after it and carries a higher ID.
		backdated := newRecord("test_owner", "test_reference_backdated")
		backdated.CreatedAt = baseTime
		require.NoError(t, s.Save(ctx, backdated))
		require.True(t, backdated.Id > recent.Id)

		actual, err := s.GetAllByOwner(ctx, "test_owner", query.EmptyCursor, 10, query.Ascending)
		require.NoError(t, err)
		require.Len(t, actual, 2)
		assert.Equal(t, backdated.Id, actual[0].Id)
		assert.Equal(t, recent.Id, actual[1].Id)

		actual, err = s.GetAllByOwner(ctx, "test_owner", query.EmptyCursor, 10, query.Descending)
		require.NoError(t, err)
		require.Len(t, actual, 2)
		assert.Equal(t, recent.Id, actual[0].Id)
		assert.Equal(t, backdated.Id, actual[1].Id)

		// The tiebreaker is the ID, so records sharing an event time still have a
		// total order and a cursor cannot skip or repeat one.
		tied := newRecord("test_owner", "test_reference_tied")
		tied.CreatedAt = recent.CreatedAt
		require.NoError(t, s.Save(ctx, tied))

		actual, err = s.GetAllByOwner(ctx, "test_owner", history.ToCursor(recent.CreatedAt, recent.Id), 10, query.Ascending)
		require.NoError(t, err)
		require.Len(t, actual, 1)
		assert.Equal(t, tied.Id, actual[0].Id)
	})
}

func testGetAllByOwnerMint(t *testing.T, s history.Store) {
	t.Run("testGetAllByOwnerMint", func(t *testing.T) {
		ctx := context.Background()

		sent := newRecord("test_owner", "test_reference_sent")
		sent.MintAccount = "mint_a"
		require.NoError(t, s.Save(ctx, sent))

		// A trade out of mint_a and a trade into it both belong to mint_a's
		// history, so a mint matches on either leg.
		sell := newSwapRecord("test_owner", "test_reference_sell")
		sell.MintAccount = "mint_a"
		sell.DestinationMintAccount = pointer.String("mint_b")
		require.NoError(t, s.Save(ctx, sell))

		buy := newSwapRecord("test_owner", "test_reference_buy")
		buy.MintAccount = "mint_b"
		buy.DestinationMintAccount = pointer.String("mint_a")
		require.NoError(t, s.Save(ctx, buy))

		unrelated := newRecord("test_owner", "test_reference_unrelated")
		unrelated.MintAccount = "mint_c"
		require.NoError(t, s.Save(ctx, unrelated))

		actual, err := s.GetAllByOwnerMint(ctx, "test_owner", "mint_a", query.EmptyCursor, 10, query.Ascending)
		require.NoError(t, err)
		require.Len(t, actual, 3)
		assert.Equal(t, sent.Id, actual[0].Id)
		assert.Equal(t, sell.Id, actual[1].Id)
		assert.Equal(t, buy.Id, actual[2].Id)

		actual, err = s.GetAllByOwnerMint(ctx, "test_owner", "mint_c", query.EmptyCursor, 10, query.Ascending)
		require.NoError(t, err)
		require.Len(t, actual, 1)
		assert.Equal(t, unrelated.Id, actual[0].Id)

		_, err = s.GetAllByOwnerMint(ctx, "test_owner", "mint_unknown", query.EmptyCursor, 10, query.Ascending)
		assert.Equal(t, history.ErrNotFound, err)

		_, err = s.GetAllByOwnerMint(ctx, "test_other_owner", "mint_a", query.EmptyCursor, 10, query.Ascending)
		assert.Equal(t, history.ErrNotFound, err)
	})
}

func testGetAllByIds(t *testing.T, s history.Store) {
	t.Run("testGetAllByIds", func(t *testing.T) {
		ctx := context.Background()

		first := newRecord("test_owner", "test_reference_1")
		require.NoError(t, s.Save(ctx, first))
		second := newRecord("test_owner", "test_reference_2")
		require.NoError(t, s.Save(ctx, second))
		other := newRecord("test_other_owner", "test_reference_3")
		require.NoError(t, s.Save(ctx, other))

		// Results come back ordered by ID whatever order they were asked for, so
		// a caller cannot rely on them lining up with the IDs it passed.
		actual, err := s.GetAllByIds(ctx, []uint64{second.Id, first.Id})
		require.NoError(t, err)
		require.Len(t, actual, 2)
		assert.Equal(t, first.Id, actual[0].Id)
		assert.Equal(t, second.Id, actual[1].Id)

		// An ID with no record is omitted rather than reported, so a short result
		// is normal.
		actual, err = s.GetAllByIds(ctx, []uint64{first.Id, 999999})
		require.NoError(t, err)
		require.Len(t, actual, 1)
		assert.Equal(t, first.Id, actual[0].Id)

		// The lookup is not scoped to an owner: a record belonging to someone
		// else comes back, and it is the caller's job to reject it.
		actual, err = s.GetAllByIds(ctx, []uint64{other.Id})
		require.NoError(t, err)
		require.Len(t, actual, 1)
		assert.Equal(t, "test_other_owner", actual[0].OwnerAccount)

		_, err = s.GetAllByIds(ctx, []uint64{999999})
		assert.Equal(t, history.ErrNotFound, err)

		_, err = s.GetAllByIds(ctx, nil)
		assert.Equal(t, history.ErrNotFound, err)
	})
}

func testGetAllByReference(t *testing.T, s history.Store) {
	t.Run("testGetAllByReference", func(t *testing.T) {
		ctx := context.Background()

		// Both sides of one payment, which is how an outcome naming only the
		// intent or swap finds every record it has to transition.
		sender := newRecord("test_sender", "test_reference")
		sender.Type = history.DirectlySent
		sender.CounterpartyOwnerAccount = pointer.String("test_receiver")
		require.NoError(t, s.Save(ctx, sender))

		receiver := newRecord("test_receiver", "test_reference")
		receiver.Type = history.DirectlyReceived
		receiver.CounterpartyOwnerAccount = pointer.String("test_sender")
		require.NoError(t, s.Save(ctx, receiver))

		require.NoError(t, s.Save(ctx, newRecord("test_sender", "test_other_reference")))

		actual, err := s.GetAllByReference(ctx, history.IntentReference, "test_reference")
		require.NoError(t, err)
		require.Len(t, actual, 2)

		owners := map[string]history.Type{}
		for _, record := range actual {
			owners[record.OwnerAccount] = record.Type
		}
		assert.Equal(t, history.DirectlySent, owners["test_sender"])
		assert.Equal(t, history.DirectlyReceived, owners["test_receiver"])

		_, err = s.GetAllByReference(ctx, history.IntentReference, "test_reference_unknown")
		assert.Equal(t, history.ErrNotFound, err)
	})
}

func testGetAllByGiftCardVault(t *testing.T, s history.Store) {
	t.Run("testGetAllByGiftCardVault", func(t *testing.T) {
		ctx := context.Background()

		_, err := s.GetAllByGiftCardVault(ctx, "test_vault")
		assert.Equal(t, history.ErrNotFound, err)

		// The issuer's record and the claimant's record share a vault but not a
		// reference, since claiming is its own event.
		issued := newRecord("test_issuer", "test_reference_issued")
		issued.Type = history.IndirectlySent
		issued.State = history.StatePending
		issued.GiftCardVault = pointer.String("test_vault")
		require.NoError(t, s.Save(ctx, issued))

		claimed := newRecord("test_claimant", "test_reference_claimed")
		claimed.Type = history.IndirectlyReceived
		claimed.GiftCardVault = pointer.String("test_vault")
		require.NoError(t, s.Save(ctx, claimed))

		require.NoError(t, s.Save(ctx, newRecord("test_issuer", "test_reference_unrelated")))

		actual, err := s.GetAllByGiftCardVault(ctx, "test_vault")
		require.NoError(t, err)
		require.Len(t, actual, 2)

		types := map[string]history.Type{}
		for _, record := range actual {
			types[record.OwnerAccount] = record.Type
		}
		assert.Equal(t, history.IndirectlySent, types["test_issuer"])
		assert.Equal(t, history.IndirectlyReceived, types["test_claimant"])

		_, err = s.GetAllByGiftCardVault(ctx, "test_vault_unknown")
		assert.Equal(t, history.ErrNotFound, err)
	})
}

// baseTime anchors record timestamps so a suite can rely on the order it
// creates records in. Timestamps are staggered rather than taken from the clock
// because a history is ordered by event time, and records created in the same
// microsecond would leave that order down to the ID tiebreaker instead.
var baseTime = time.Date(2026, time.August, 14, 12, 0, 0, 0, time.UTC)

var recordCount int

func newRecord(owner, referenceId string) *history.Record {
	recordCount++

	return &history.Record{
		ReferenceId:   referenceId,
		ReferenceType: history.IntentReference,

		Type: history.DirectlySent,

		OwnerAccount: owner,

		ExchangeCurrency: currency.USD,
		NativeAmount:     1.23,

		MintAccount: "test_mint",
		Quantity:    12345,

		State: history.StateCompleted,

		CreatedAt: baseTime.Add(time.Duration(recordCount) * time.Minute),
	}
}

func newSwapRecord(owner, referenceId string) *history.Record {
	record := newRecord(owner, referenceId)
	record.Type = history.Swap
	record.DestinationMintAccount = pointer.String("test_destination_mint")
	return record
}

func assertEquivalentRecords(t *testing.T, obj1, obj2 *history.Record) {
	assert.Equal(t, obj1.Id, obj2.Id)
	assert.Equal(t, obj1.ReferenceId, obj2.ReferenceId)
	assert.Equal(t, obj1.ReferenceType, obj2.ReferenceType)
	assert.Equal(t, obj1.Type, obj2.Type)
	assert.Equal(t, obj1.OwnerAccount, obj2.OwnerAccount)
	assert.Equal(t, obj1.CounterpartyOwnerAccount, obj2.CounterpartyOwnerAccount)
	assert.Equal(t, obj1.ExchangeCurrency, obj2.ExchangeCurrency)
	assert.Equal(t, obj1.NativeAmount, obj2.NativeAmount)
	assert.Equal(t, obj1.Fees, obj2.Fees)
	assert.Equal(t, obj1.MintAccount, obj2.MintAccount)
	assert.Equal(t, obj1.Quantity, obj2.Quantity)
	assert.Equal(t, obj1.DestinationMintAccount, obj2.DestinationMintAccount)
	assert.Equal(t, obj1.DestinationQuantity, obj2.DestinationQuantity)
	assert.Equal(t, obj1.GiftCardVault, obj2.GiftCardVault)
	assert.Equal(t, obj1.AppMetadata, obj2.AppMetadata)
	assert.Equal(t, obj1.Version, obj2.Version)
	assert.Equal(t, obj1.State, obj2.State)
	assert.Equal(t, obj1.CreatedAt.Unix(), obj2.CreatedAt.Unix())
	assert.Equal(t, obj1.UpdatedAt.Unix(), obj2.UpdatedAt.Unix())
}
