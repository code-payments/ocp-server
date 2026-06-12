package task

import (
	"context"
	"sync"
	"time"

	"go.uber.org/zap"

	ocp_data "github.com/code-payments/ocp-server/ocp/data"
	task_data "github.com/code-payments/ocp-server/ocp/data/task"
	"github.com/code-payments/ocp-server/ocp/integration"
	"github.com/code-payments/ocp-server/retry/backoff"
)

const (
	defaultMaxFailedAttempts = 10
	defaultBackoffBase       = time.Second
	defaultMaxBackoffDelay   = 5 * time.Minute
)

// Scheduler is the entry point into the guaranteed task execution system.
// Tasks are durably enqueued within the caller's DB transaction and executed
// at least once via the app's TaskExecutor, either through the best-effort
// fast path or the background worker.
type Scheduler struct {
	log               *zap.Logger
	data              ocp_data.Provider
	executor          integration.TaskExecutor
	maxFailedAttempts uint32
	backoffStrategy   backoff.Strategy
	maxBackoffDelay   time.Duration
}

type Option func(*Scheduler)

// WithMaxFailedAttempts overrides the number of failed execution attempts
// before a task is marked as failed
func WithMaxFailedAttempts(maxFailedAttempts uint32) Option {
	return func(s *Scheduler) {
		s.maxFailedAttempts = maxFailedAttempts
	}
}

// WithBackoff overrides the retry backoff strategy and maximum delay between
// execution attempts
func WithBackoff(strategy backoff.Strategy, maxDelay time.Duration) Option {
	return func(s *Scheduler) {
		s.backoffStrategy = strategy
		s.maxBackoffDelay = maxDelay
	}
}

func NewScheduler(log *zap.Logger, data ocp_data.Provider, executor integration.TaskExecutor, opts ...Option) *Scheduler {
	s := &Scheduler{
		log:               log,
		data:              data,
		executor:          executor,
		maxFailedAttempts: defaultMaxFailedAttempts,
		backoffStrategy:   backoff.BinaryExponential(defaultBackoffBase),
		maxBackoffDelay:   defaultMaxBackoffDelay,
	}

	for _, opt := range opts {
		opt(s)
	}

	return s
}

// Enqueue durably persists tasks for guaranteed execution. When called within
// a DB transaction passed along ctx (eg. the one committing an intent), the
// tasks are persisted atomically with that transaction.
func (s *Scheduler) Enqueue(ctx context.Context, records ...*task_data.Record) error {
	if len(records) == 0 {
		return nil
	}

	for _, record := range records {
		if record.State == task_data.StateUnknown {
			record.State = task_data.StatePending
		}

		if record.State != task_data.StatePending {
			return task_data.ErrExists
		}

		if err := record.Validate(); err != nil {
			return err
		}
	}

	return s.data.PutAllTasks(ctx, records...)
}

// TryExecuteNow makes a best-effort immediate execution attempt for the
// provided tasks. Failures are left for the background worker to retry.
//
// This must only be called after the tasks have been committed to the DB.
func (s *Scheduler) TryExecuteNow(ctx context.Context, records ...*task_data.Record) {
	var wg sync.WaitGroup
	for _, record := range records {
		wg.Add(1)

		go func(record *task_data.Record) {
			defer wg.Done()

			err := s.ExecuteAndAdvance(ctx, record)
			if err != nil {
				s.log.With(
					zap.Error(err),
					zap.String("task_id", record.TaskId),
				).Warn("failure executing task on fast path")
			}
		}(record)
	}
	wg.Wait()
}

// ExecuteAndAdvance executes a single pending task and advances its state
// based on the outcome. On success the task is confirmed. On failure the
// task is scheduled for a retry with backoff, or marked as failed once the
// maximum attempt count is reached.
//
// Both the fast path and the background worker funnel through this method,
// so the two cannot disagree on semantics. Concurrent executions of the same
// task are resolved by the optimistic concurrency check when the task record
// is updated.
func (s *Scheduler) ExecuteAndAdvance(ctx context.Context, record *task_data.Record) error {
	if record.State != task_data.StatePending {
		return nil
	}

	executionErr := s.executor.Execute(ctx, record)
	if executionErr == nil {
		record.State = task_data.StateConfirmed
	} else {
		record.FailedAttempts++
		if record.FailedAttempts >= s.maxFailedAttempts {
			record.State = task_data.StateFailed
		} else {
			delay := s.backoffStrategy(uint(record.FailedAttempts))
			if delay > s.maxBackoffDelay {
				delay = s.maxBackoffDelay
			}
			record.NextAttemptAt = time.Now().Add(delay)
		}
	}

	err := s.data.UpdateTask(ctx, record)
	if err == task_data.ErrStaleVersion {
		// Another process has advanced the task. Idempotency of the executor
		// makes this a safe no-op.
		return nil
	} else if err != nil {
		return err
	}

	if record.State == task_data.StateFailed {
		s.log.With(
			zap.NamedError("execution_error", executionErr),
			zap.String("task_id", record.TaskId),
			zap.Uint32("failed_attempts", record.FailedAttempts),
		).Error("task exhausted max failed attempts and is marked as failed")
	}

	return executionErr
}
