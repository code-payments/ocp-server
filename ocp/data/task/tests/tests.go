package tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/code-payments/ocp-server/database/query"
	"github.com/code-payments/ocp-server/ocp/data/task"
	"github.com/code-payments/ocp-server/pointer"
)

func RunTests(t *testing.T, s task.Store, teardown func()) {
	for _, tf := range []func(t *testing.T, s task.Store){
		testRoundTrip,
		testPutAllBatch,
		testPutAllDuplicate,
		testUpdateHappyPath,
		testUpdateStaleRecord,
		testGetAllReadyByState,
		testCountByState,
	} {
		tf(t, s)
		teardown()
	}
}

func testRoundTrip(t *testing.T, s task.Store) {
	t.Run("testRoundTrip", func(t *testing.T) {
		ctx := context.Background()

		actual, err := s.GetByTaskId(ctx, "test_task_id")
		require.Error(t, err)
		assert.Equal(t, task.ErrNotFound, err)
		assert.Nil(t, actual)

		invalid := &task.Record{
			Type: 1,
			Data: []byte("test_data"),
		}
		require.Error(t, s.PutAll(ctx, invalid))

		invalid = &task.Record{
			TaskId: "test_task_id",
			Data:   []byte("test_data"),
		}
		require.Error(t, s.PutAll(ctx, invalid))

		expected := &task.Record{
			TaskId: "test_task_id",

			Type: 1,
			Data: []byte("test_data"),

			ReferenceId: pointer.String("test_reference_id"),

			State: task.StatePending,

			NextAttemptAt: time.Now(),

			CreatedAt: time.Now(),
		}
		cloned := expected.Clone()
		err = s.PutAll(ctx, expected)
		require.NoError(t, err)
		assert.EqualValues(t, 1, expected.Id)
		assert.EqualValues(t, 1, expected.Version)

		actual, err = s.GetByTaskId(ctx, "test_task_id")
		require.NoError(t, err)
		assertEquivalentRecords(t, &cloned, actual)
		assert.EqualValues(t, 1, actual.Id)
		assert.EqualValues(t, 1, actual.Version)
	})
}

func testPutAllBatch(t *testing.T, s task.Store) {
	t.Run("testPutAllBatch", func(t *testing.T) {
		ctx := context.Background()

		require.Error(t, s.PutAll(ctx))

		var expected []*task.Record
		for i := range 10 {
			expected = append(expected, &task.Record{
				TaskId: fmt.Sprintf("test_task_id_%d", i),

				Type: uint32(i + 1),
				Data: []byte(fmt.Sprintf("test_data_%d", i)),

				State: task.StatePending,

				NextAttemptAt: time.Now(),

				CreatedAt: time.Now(),
			})
		}

		var cloned []task.Record
		for _, record := range expected {
			cloned = append(cloned, record.Clone())
		}

		require.NoError(t, s.PutAll(ctx, expected...))

		for i, record := range expected {
			assert.EqualValues(t, i+1, record.Id)
			assert.EqualValues(t, 1, record.Version)

			actual, err := s.GetByTaskId(ctx, record.TaskId)
			require.NoError(t, err)
			assertEquivalentRecords(t, &cloned[i], actual)
		}
	})
}

func testPutAllDuplicate(t *testing.T, s task.Store) {
	t.Run("testPutAllDuplicate", func(t *testing.T) {
		ctx := context.Background()

		record := &task.Record{
			TaskId: "test_task_id",

			Type: 1,
			Data: []byte("test_data"),

			State: task.StatePending,

			NextAttemptAt: time.Now(),

			CreatedAt: time.Now(),
		}
		require.NoError(t, s.PutAll(ctx, record))

		duplicate := &task.Record{
			TaskId: "test_task_id",

			Type: 2,
			Data: []byte("other_data"),

			State: task.StatePending,

			NextAttemptAt: time.Now(),

			CreatedAt: time.Now(),
		}
		err := s.PutAll(ctx, duplicate)
		assert.Equal(t, task.ErrExists, err)

		// The batch is all-or-nothing
		other := &task.Record{
			TaskId: "test_other_task_id",

			Type: 1,
			Data: []byte("test_data"),

			State: task.StatePending,

			NextAttemptAt: time.Now(),

			CreatedAt: time.Now(),
		}
		duplicate.Id = 0
		err = s.PutAll(ctx, other, duplicate)
		assert.Equal(t, task.ErrExists, err)

		_, err = s.GetByTaskId(ctx, "test_other_task_id")
		assert.Equal(t, task.ErrNotFound, err)

		actual, err := s.GetByTaskId(ctx, "test_task_id")
		require.NoError(t, err)
		assert.EqualValues(t, 1, actual.Type)
	})
}

func testUpdateHappyPath(t *testing.T, s task.Store) {
	t.Run("testUpdateHappyPath", func(t *testing.T) {
		ctx := context.Background()

		expected := &task.Record{
			TaskId: "test_task_id",

			Type: 1,
			Data: []byte("test_data"),

			ReferenceId: pointer.String("test_reference_id"),

			State: task.StatePending,

			NextAttemptAt: time.Now(),

			CreatedAt: time.Now(),
		}
		require.NoError(t, s.PutAll(ctx, expected))
		assert.EqualValues(t, 1, expected.Id)
		assert.EqualValues(t, 1, expected.Version)

		expected.State = task.StatePending
		expected.Attempts = 3
		expected.NextAttemptAt = time.Now().Add(time.Minute)

		err := s.Update(ctx, expected)
		require.NoError(t, err)
		assert.EqualValues(t, 1, expected.Id)
		assert.EqualValues(t, 2, expected.Version)

		actual, err := s.GetByTaskId(ctx, "test_task_id")
		require.NoError(t, err)
		assertEquivalentRecords(t, expected, actual)
		assert.EqualValues(t, 3, actual.Attempts)

		expected.State = task.StateConfirmed
		mutatedData := []byte("mutated_data_should_be_ignored")
		expected.Data = mutatedData

		err = s.Update(ctx, expected)
		require.NoError(t, err)
		assert.EqualValues(t, 1, expected.Id)
		assert.EqualValues(t, 3, expected.Version)

		actual, err = s.GetByTaskId(ctx, "test_task_id")
		require.NoError(t, err)
		assert.Equal(t, task.StateConfirmed, actual.State)
		assert.Equal(t, []byte("test_data"), actual.Data)
		assert.NotEqual(t, mutatedData, actual.Data)

		// The stored value is copied back into the updated record
		assert.Equal(t, []byte("test_data"), expected.Data)
	})
}

func testUpdateStaleRecord(t *testing.T, s task.Store) {
	t.Run("testUpdateStaleRecord", func(t *testing.T) {
		ctx := context.Background()

		missing := &task.Record{
			TaskId: "test_missing_task_id",

			Type: 1,
			Data: []byte("test_data"),

			State: task.StatePending,

			NextAttemptAt: time.Now(),

			CreatedAt: time.Now(),
		}
		err := s.Update(ctx, missing)
		assert.Equal(t, task.ErrStaleVersion, err)

		expected := &task.Record{
			TaskId: "test_task_id",

			Type: 1,
			Data: []byte("test_data"),

			State: task.StatePending,

			NextAttemptAt: time.Now(),

			CreatedAt: time.Now(),
		}
		require.NoError(t, s.PutAll(ctx, expected))
		assert.EqualValues(t, 1, expected.Version)

		stale := expected.Clone()
		stale.Version -= 1
		stale.State = task.StateFailed

		err = s.Update(ctx, &stale)
		assert.Equal(t, task.ErrStaleVersion, err)

		actual, err := s.GetByTaskId(ctx, "test_task_id")
		require.NoError(t, err)
		assert.Equal(t, task.StatePending, actual.State)
		assert.EqualValues(t, 1, actual.Version)
	})
}

func testGetAllReadyByState(t *testing.T, s task.Store) {
	t.Run("testGetAllReadyByState", func(t *testing.T) {
		ctx := context.Background()

		now := time.Now()

		_, err := s.GetAllReadyByState(ctx, task.StatePending, now, query.EmptyCursor, 10, query.Ascending)
		assert.Equal(t, task.ErrNotFound, err)

		var records []*task.Record
		for i := range 100 {
			state := task.StatePending
			if i >= 50 {
				state = task.StateConfirmed
			}

			// Even pending tasks are ready, odd pending tasks are scheduled
			// for the future
			nextAttemptAt := now
			if i%2 == 1 {
				nextAttemptAt = now.Add(time.Hour)
			}

			record := &task.Record{
				TaskId: fmt.Sprintf("test_task_id_%d", i),

				Type: uint32(i + 1),
				Data: []byte(fmt.Sprintf("test_data_%d", i)),

				State: state,

				NextAttemptAt: nextAttemptAt,

				CreatedAt: now,
			}
			records = append(records, record)
		}
		require.NoError(t, s.PutAll(ctx, records...))

		// Only the 25 ready pending tasks are returned
		allActual, err := s.GetAllReadyByState(ctx, task.StatePending, now, query.EmptyCursor, 100, query.Ascending)
		require.NoError(t, err)
		require.Len(t, allActual, 25)
		for i, actual := range allActual {
			assertEquivalentRecords(t, records[2*i], actual)
		}

		// All 50 pending tasks are ready in an hour
		allActual, err = s.GetAllReadyByState(ctx, task.StatePending, now.Add(time.Hour), query.EmptyCursor, 100, query.Ascending)
		require.NoError(t, err)
		require.Len(t, allActual, 50)

		allActual, err = s.GetAllReadyByState(ctx, task.StatePending, now, query.EmptyCursor, 10, query.Ascending)
		require.NoError(t, err)
		require.Len(t, allActual, 10)
		for i, actual := range allActual {
			assertEquivalentRecords(t, records[2*i], actual)
		}

		allActual, err = s.GetAllReadyByState(ctx, task.StatePending, now, query.EmptyCursor, 10, query.Descending)
		require.NoError(t, err)
		require.Len(t, allActual, 10)
		for i, actual := range allActual {
			assertEquivalentRecords(t, records[48-2*i], actual)
		}

		allActual, err = s.GetAllReadyByState(ctx, task.StatePending, now, query.ToCursor(records[24].Id), 10, query.Ascending)
		require.NoError(t, err)
		require.Len(t, allActual, 10)
		for i, actual := range allActual {
			assertEquivalentRecords(t, records[26+2*i], actual)
		}

		allActual, err = s.GetAllReadyByState(ctx, task.StatePending, now, query.ToCursor(records[24].Id), 10, query.Descending)
		require.NoError(t, err)
		require.Len(t, allActual, 10)
		for i, actual := range allActual {
			assertEquivalentRecords(t, records[22-2*i], actual)
		}

		_, err = s.GetAllReadyByState(ctx, task.StatePending, now, query.ToCursor(records[98].Id), 10, query.Ascending)
		assert.Equal(t, task.ErrNotFound, err)

		_, err = s.GetAllReadyByState(ctx, task.StateFailed, now, query.EmptyCursor, 10, query.Ascending)
		assert.Equal(t, task.ErrNotFound, err)
	})
}

func testCountByState(t *testing.T, s task.Store) {
	t.Run("testCountByState", func(t *testing.T) {
		ctx := context.Background()

		count, err := s.CountByState(ctx, task.StatePending)
		require.NoError(t, err)
		assert.EqualValues(t, 0, count)

		var records []*task.Record
		for i := range 10 {
			state := task.StatePending
			if i >= 6 {
				state = task.StateConfirmed
			}
			if i >= 9 {
				state = task.StateFailed
			}

			records = append(records, &task.Record{
				TaskId: fmt.Sprintf("test_task_id_%d", i),

				Type: uint32(i + 1),
				Data: []byte(fmt.Sprintf("test_data_%d", i)),

				State: state,

				NextAttemptAt: time.Now(),

				CreatedAt: time.Now(),
			})
		}
		require.NoError(t, s.PutAll(ctx, records...))

		count, err = s.CountByState(ctx, task.StatePending)
		require.NoError(t, err)
		assert.EqualValues(t, 6, count)

		count, err = s.CountByState(ctx, task.StateConfirmed)
		require.NoError(t, err)
		assert.EqualValues(t, 3, count)

		count, err = s.CountByState(ctx, task.StateFailed)
		require.NoError(t, err)
		assert.EqualValues(t, 1, count)
	})
}

func assertEquivalentRecords(t *testing.T, obj1, obj2 *task.Record) {
	assert.Equal(t, obj1.TaskId, obj2.TaskId)

	assert.Equal(t, obj1.Type, obj2.Type)
	assert.Equal(t, obj1.Data, obj2.Data)

	assert.Equal(t, obj1.ReferenceId, obj2.ReferenceId)

	assert.Equal(t, obj1.State, obj2.State)

	assert.Equal(t, obj1.Attempts, obj2.Attempts)
	assert.Equal(t, obj1.NextAttemptAt.UTC().Truncate(time.Microsecond), obj2.NextAttemptAt.UTC().Truncate(time.Microsecond))
}
