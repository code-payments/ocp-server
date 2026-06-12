package integration

import (
	"context"
	"errors"

	"github.com/code-payments/ocp-server/ocp/data/task"
)

// TaskExecutor executes app-defined tasks whose execution is guaranteed by
// the base system.
//
// Implementations must be idempotent. Tasks are delivered at-least-once and
// may be executed concurrently, in any order, by any number of processes.
// The task ID is a natural deduplication key.
//
// A returned error means the task will be retried at a later time, until a
// maximum attempt count is reached.
type TaskExecutor interface {
	Execute(ctx context.Context, record *task.Record) error
}

type defaultTaskExecutor struct {
}

// NewDefaultTaskExecutor returns a TaskExecutor that fails every task, so
// orphaned tasks are surfaced loudly instead of being silently confirmed.
func NewDefaultTaskExecutor() TaskExecutor {
	return &defaultTaskExecutor{}
}

func (e *defaultTaskExecutor) Execute(ctx context.Context, record *task.Record) error {
	return errors.New("no task executor registered")
}
