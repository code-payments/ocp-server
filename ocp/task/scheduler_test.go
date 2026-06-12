package task

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	ocp_data "github.com/code-payments/ocp-server/ocp/data"
	task_data "github.com/code-payments/ocp-server/ocp/data/task"
	"github.com/code-payments/ocp-server/retry/backoff"
)

type mockExecutor struct {
	mu        sync.Mutex
	executed  []string
	executeFn func(record *task_data.Record) error
}

func (e *mockExecutor) Execute(ctx context.Context, record *task_data.Record) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.executed = append(e.executed, record.TaskId)

	if e.executeFn != nil {
		return e.executeFn(record)
	}
	return nil
}

func (e *mockExecutor) executionCount() int {
	e.mu.Lock()
	defer e.mu.Unlock()

	return len(e.executed)
}

func newTestScheduler(executor *mockExecutor, opts ...Option) (*Scheduler, ocp_data.Provider) {
	data := ocp_data.NewTestDataProvider()
	return NewScheduler(zap.Must(zap.NewDevelopment()), data, executor, opts...), data
}

func newTestTask(taskId string) *task_data.Record {
	return &task_data.Record{
		TaskId: taskId,
		Type:   1,
		Data:   []byte("test_data"),
	}
}

func TestScheduler_EnqueueHappyPath(t *testing.T) {
	ctx := context.Background()

	scheduler, data := newTestScheduler(&mockExecutor{})

	require.NoError(t, scheduler.Enqueue(ctx))

	record := newTestTask("test_task_id")
	require.NoError(t, scheduler.Enqueue(ctx, record))
	assert.Equal(t, task_data.StatePending, record.State)

	actual, err := data.GetTaskById(ctx, "test_task_id")
	require.NoError(t, err)
	assert.Equal(t, task_data.StatePending, actual.State)
	assert.False(t, actual.NextAttemptAt.After(time.Now()))
}

func TestScheduler_EnqueueValidation(t *testing.T) {
	ctx := context.Background()

	scheduler, data := newTestScheduler(&mockExecutor{})

	invalid := newTestTask("test_task_id")
	invalid.State = task_data.StateConfirmed
	require.Error(t, scheduler.Enqueue(ctx, invalid))

	invalid = newTestTask("test_task_id")
	invalid.Type = 0
	require.Error(t, scheduler.Enqueue(ctx, invalid))

	_, err := data.GetTaskById(ctx, "test_task_id")
	assert.Equal(t, task_data.ErrNotFound, err)
}

func TestScheduler_TryExecuteNowHappyPath(t *testing.T) {
	ctx := context.Background()

	executor := &mockExecutor{}
	scheduler, data := newTestScheduler(executor)

	records := []*task_data.Record{
		newTestTask("test_task_id_1"),
		newTestTask("test_task_id_2"),
	}
	require.NoError(t, scheduler.Enqueue(ctx, records...))

	scheduler.TryExecuteNow(ctx, records...)
	assert.Equal(t, 2, executor.executionCount())

	for _, record := range records {
		actual, err := data.GetTaskById(ctx, record.TaskId)
		require.NoError(t, err)
		assert.Equal(t, task_data.StateConfirmed, actual.State)
		assert.EqualValues(t, 0, actual.FailedAttempts)
	}
}

func TestScheduler_ExecuteAndAdvanceRetryWithBackoff(t *testing.T) {
	ctx := context.Background()

	executor := &mockExecutor{
		executeFn: func(record *task_data.Record) error {
			return errors.New("transient failure")
		},
	}
	scheduler, data := newTestScheduler(executor, WithBackoff(backoff.Constant(time.Minute), time.Hour))

	record := newTestTask("test_task_id")
	require.NoError(t, scheduler.Enqueue(ctx, record))

	err := scheduler.ExecuteAndAdvance(ctx, record)
	require.Error(t, err)

	actual, err := data.GetTaskById(ctx, "test_task_id")
	require.NoError(t, err)
	assert.Equal(t, task_data.StatePending, actual.State)
	assert.EqualValues(t, 1, actual.FailedAttempts)
	assert.True(t, actual.NextAttemptAt.After(time.Now().Add(30*time.Second)))

	// The task is no longer ready for execution until the backoff elapses
	_, err = data.GetAllReadyTasksByState(ctx, task_data.StatePending, time.Now())
	assert.Equal(t, task_data.ErrNotFound, err)
}

func TestScheduler_ExecuteAndAdvanceDeadLetter(t *testing.T) {
	ctx := context.Background()

	executor := &mockExecutor{
		executeFn: func(record *task_data.Record) error {
			return errors.New("permanent failure")
		},
	}
	scheduler, data := newTestScheduler(executor, WithMaxFailedAttempts(3), WithBackoff(backoff.Constant(0), 0))

	record := newTestTask("test_task_id")
	require.NoError(t, scheduler.Enqueue(ctx, record))

	for range 3 {
		require.Error(t, scheduler.ExecuteAndAdvance(ctx, record))
	}

	actual, err := data.GetTaskById(ctx, "test_task_id")
	require.NoError(t, err)
	assert.Equal(t, task_data.StateFailed, actual.State)
	assert.EqualValues(t, 3, actual.FailedAttempts)

	// Failed tasks are no longer executed
	require.NoError(t, scheduler.ExecuteAndAdvance(ctx, actual))
	assert.Equal(t, 3, executor.executionCount())
}

func TestScheduler_ExecuteAndAdvanceStaleRace(t *testing.T) {
	ctx := context.Background()

	executor := &mockExecutor{}
	scheduler, data := newTestScheduler(executor)

	record := newTestTask("test_task_id")
	require.NoError(t, scheduler.Enqueue(ctx, record))

	// Simulate another process executing the same task concurrently
	racingCopy := record.Clone()
	require.NoError(t, scheduler.ExecuteAndAdvance(ctx, &racingCopy))

	// The loser of the race treats the stale update as a no-op success
	require.NoError(t, scheduler.ExecuteAndAdvance(ctx, record))

	actual, err := data.GetTaskById(ctx, "test_task_id")
	require.NoError(t, err)
	assert.Equal(t, task_data.StateConfirmed, actual.State)
	assert.EqualValues(t, 2, actual.Version)
}
