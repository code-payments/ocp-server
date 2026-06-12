package task

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/code-payments/ocp-server/metrics"
	"github.com/code-payments/ocp-server/metrics/noop"
	ocp_data "github.com/code-payments/ocp-server/ocp/data"
	task_data "github.com/code-payments/ocp-server/ocp/data/task"
	ocp_task "github.com/code-payments/ocp-server/ocp/task"
	"github.com/code-payments/ocp-server/retry/backoff"
	"github.com/code-payments/ocp-server/testutil"
)

type mockExecutor struct {
	executions int32
	executeFn  func(record *task_data.Record) error
}

func (e *mockExecutor) Execute(ctx context.Context, record *task_data.Record) error {
	atomic.AddInt32(&e.executions, 1)

	if e.executeFn != nil {
		return e.executeFn(record)
	}
	return nil
}

func TestWorker_ProcessesPendingTasks(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, metrics.ProviderContextKey, noop.NewProvider())

	log := zap.Must(zap.NewDevelopment())
	data := ocp_data.NewTestDataProvider()

	// Fail the flaky task once, then succeed
	var flakyFailures int32
	executor := &mockExecutor{
		executeFn: func(record *task_data.Record) error {
			if record.TaskId == "flaky_task" && atomic.CompareAndSwapInt32(&flakyFailures, 0, 1) {
				return errors.New("transient failure")
			}
			return nil
		},
	}

	scheduler := ocp_task.NewScheduler(log, data, executor, ocp_task.WithBackoff(backoff.Constant(0), 0))

	records := []*task_data.Record{
		{TaskId: "happy_task", Type: 1, Data: []byte("data"), State: task_data.StatePending},
		{TaskId: "flaky_task", Type: 1, Data: []byte("data"), State: task_data.StatePending},
	}
	require.NoError(t, scheduler.Enqueue(ctx, records...))

	worker := New(log, data, scheduler, WithEnvConfigs())
	go func() {
		worker.Start(ctx, time.Millisecond)
	}()

	require.NoError(t, testutil.WaitFor(5*time.Second, 10*time.Millisecond, func() bool {
		count, err := data.GetTaskCountByState(ctx, task_data.StateConfirmed)
		return err == nil && count == 2
	}))

	flaky, err := data.GetTaskById(ctx, "flaky_task")
	require.NoError(t, err)
	assert.Equal(t, task_data.StateConfirmed, flaky.State)
	assert.EqualValues(t, 1, flaky.FailedAttempts)

	happy, err := data.GetTaskById(ctx, "happy_task")
	require.NoError(t, err)
	assert.Equal(t, task_data.StateConfirmed, happy.State)
	assert.EqualValues(t, 0, happy.FailedAttempts)

	assert.GreaterOrEqual(t, atomic.LoadInt32(&executor.executions), int32(3))
}
