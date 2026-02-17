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
		}

		// Get on non-existent record returns ErrNotFound
		_, err := s.GetByMint(ctx, "mint1")
		assert.Equal(t, metadata.ErrNotFound, err)

		// Put and verify fields are populated
		cloned := record.Clone()
		start := time.Now()
		require.NoError(t, s.Put(ctx, record))
		assert.True(t, record.Id > 0)
		assert.True(t, record.CreatedAt.After(start) || record.CreatedAt.Equal(start))

		// Get by mint and verify all fields
		actual, err := s.GetByMint(ctx, "mint1")
		require.NoError(t, err)
		assertEquivalentRecords(t, &cloned, actual)
		assert.Equal(t, record.Id, actual.Id)

		// Duplicate put returns ErrAlreadyExists
		assert.Equal(t, metadata.ErrAlreadyExists, s.Put(ctx, record))

		// Get on non-existent mint still returns ErrNotFound
		_, err = s.GetByMint(ctx, "mint2")
		assert.Equal(t, metadata.ErrNotFound, err)
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
}
