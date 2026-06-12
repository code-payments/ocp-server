package task

import (
	"context"
	"errors"
	"time"

	"github.com/code-payments/ocp-server/database/query"
)

var (
	ErrNotFound     = errors.New("task not found")
	ErrExists       = errors.New("task already exists")
	ErrStaleVersion = errors.New("task version is stale")
)

type Store interface {
	// PutAll creates all tasks in a single operation. This method supports
	// being executed within an existing DB transaction passed along ctx.
	PutAll(ctx context.Context, records ...*Record) error

	// Update updates an existing task with an optimistic concurrency check
	// on the version. Only mutable fields (state, failed attempts, next
	// attempt timestamp) are updated.
	Update(ctx context.Context, record *Record) error

	// GetByTaskId gets a task by its task ID
	GetByTaskId(ctx context.Context, taskId string) (*Record, error)

	// GetAllReadyByState gets all tasks in the provided state whose next
	// attempt timestamp is at or before asOf
	GetAllReadyByState(ctx context.Context, state State, asOf time.Time, cursor query.Cursor, limit uint64, direction query.Ordering) ([]*Record, error)

	// CountByState returns the count of tasks in the requested state
	CountByState(ctx context.Context, state State) (uint64, error)
}
