package tests

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/code-payments/ocp-server/ocp/data/vm/metadata"
)

func RunTests(t *testing.T, s metadata.Store, teardown func()) {
	for _, tf := range []func(t *testing.T, s metadata.Store){
		testHappyPath,
		testGetAllVms,
	} {
		tf(t, s)
		teardown()
	}
}

func testHappyPath(t *testing.T, s metadata.Store) {
	t.Run("testHappyPath", func(t *testing.T) {
		ctx := context.Background()

		record := &metadata.Record{
			Mint:        "mint1",
			Authority:   "authority1",
			Vm:          "vm1",
			VmBump:      255,
			Omnibus:     "omnibus1",
			OmnibusBump: 254,
			DaysLocked:  21,
			State:       metadata.StateUnknown,
		}

		// Get on non-existent record returns ErrNotFound
		_, err := s.GetByMint(ctx, "mint1")
		assert.Equal(t, metadata.ErrNotFound, err)

		// Save and verify fields are populated
		cloned := record.Clone()
		start := time.Now()
		require.NoError(t, s.Save(ctx, record))
		assert.True(t, record.Id > 0)
		assert.EqualValues(t, 1, record.Version)
		assert.Equal(t, metadata.StateUnknown, record.State)
		assert.True(t, record.CreatedAt.After(start) || record.CreatedAt.Equal(start))

		// Get by mint and verify all fields
		actual, err := s.GetByMint(ctx, "mint1")
		require.NoError(t, err)
		assert.Equal(t, record.Id, actual.Id)
		assert.EqualValues(t, 1, actual.Version)
		assertEquivalentRecords(t, &cloned, actual)

		// Update state with correct version
		record.State = metadata.StateAvailable
		require.NoError(t, s.Save(ctx, record))
		assert.EqualValues(t, 2, record.Version)
		assert.Equal(t, metadata.StateAvailable, record.State)

		// Verify update persisted
		actual, err = s.GetByMint(ctx, "mint1")
		require.NoError(t, err)
		assert.EqualValues(t, 2, actual.Version)
		assert.Equal(t, metadata.StateAvailable, actual.State)

		// Stale version returns ErrStaleVersion
		record.State = metadata.StateUnknown
		record.Version = 1
		assert.Equal(t, metadata.ErrStaleVersion, s.Save(ctx, record))

		// Verify update didn't persisted
		actual, err = s.GetByMint(ctx, "mint1")
		require.NoError(t, err)
		assert.EqualValues(t, 2, actual.Version)
		assert.Equal(t, metadata.StateAvailable, actual.State)

		// Get on non-existent mint still returns ErrNotFound
		_, err = s.GetByMint(ctx, "mint2")
		assert.Equal(t, metadata.ErrNotFound, err)
	})
}

func testGetAllVms(t *testing.T, s metadata.Store) {
	t.Run("testGetAllVms", func(t *testing.T) {
		ctx := context.Background()

		// No records returns ErrNotFound
		_, err := s.GetAllVms(ctx)
		assert.Equal(t, metadata.ErrNotFound, err)

		// Save a record and verify GetAllVms returns its VM
		require.NoError(t, s.Save(ctx, &metadata.Record{
			Mint:      "mint1",
			Authority: "authority1",
			Vm:        "vm1",
			Omnibus:   "omnibus1",
		}))

		vms, err := s.GetAllVms(ctx)
		require.NoError(t, err)
		assert.Len(t, vms, 1)
		assert.Contains(t, vms, "vm1")

		// Save another record with a different VM
		require.NoError(t, s.Save(ctx, &metadata.Record{
			Mint:      "mint2",
			Authority: "authority2",
			Vm:        "vm2",
			Omnibus:   "omnibus2",
		}))

		vms, err = s.GetAllVms(ctx)
		require.NoError(t, err)
		assert.Len(t, vms, 2)
		assert.Contains(t, vms, "vm1")
		assert.Contains(t, vms, "vm2")

		// Save another record with the same VM, should not duplicate
		require.NoError(t, s.Save(ctx, &metadata.Record{
			Mint:      "mint3",
			Authority: "authority3",
			Vm:        "vm1",
			Omnibus:   "omnibus3",
		}))

		vms, err = s.GetAllVms(ctx)
		require.NoError(t, err)
		assert.Len(t, vms, 2)
		assert.Contains(t, vms, "vm1")
		assert.Contains(t, vms, "vm2")
	})
}

func assertEquivalentRecords(t *testing.T, obj1, obj2 *metadata.Record) {
	assert.Equal(t, obj1.Mint, obj2.Mint)
	assert.Equal(t, obj1.Authority, obj2.Authority)
	assert.Equal(t, obj1.Vm, obj2.Vm)
	assert.Equal(t, obj1.VmBump, obj2.VmBump)
	assert.Equal(t, obj1.Omnibus, obj2.Omnibus)
	assert.Equal(t, obj1.OmnibusBump, obj2.OmnibusBump)
	assert.Equal(t, obj1.DaysLocked, obj2.DaysLocked)
	assert.Equal(t, obj1.State, obj2.State)
}
