package memory

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/code-payments/ocp-server/database/query"
	"github.com/code-payments/ocp-server/ocp/data/task"
)

type ById []*task.Record

func (a ById) Len() int           { return len(a) }
func (a ById) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ById) Less(i, j int) bool { return a[i].Id < a[j].Id }

type store struct {
	mu      sync.RWMutex
	records []*task.Record
	last    uint64
}

func New() task.Store {
	return &store{}
}

func (s *store) PutAll(ctx context.Context, records ...*task.Record) error {
	if len(records) == 0 {
		return errors.New("empty task set")
	}

	for _, data := range records {
		if data.Id > 0 {
			return task.ErrExists
		}

		if err := data.Validate(); err != nil {
			return err
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	seen := make(map[string]struct{})
	for _, data := range records {
		if _, ok := seen[data.TaskId]; ok {
			return task.ErrExists
		}
		seen[data.TaskId] = struct{}{}

		if item := s.findByTaskId(data.TaskId); item != nil {
			return task.ErrExists
		}
	}

	for _, data := range records {
		s.last++
		data.Id = s.last
		if data.CreatedAt.IsZero() {
			data.CreatedAt = time.Now()
		}
		if data.NextAttemptAt.IsZero() {
			data.NextAttemptAt = data.CreatedAt
		}
		data.Version++

		c := data.Clone()
		s.records = append(s.records, &c)
	}

	return nil
}

func (s *store) Update(ctx context.Context, data *task.Record) error {
	if err := data.Validate(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	item := s.findByTaskId(data.TaskId)
	if item == nil || item.Version != data.Version {
		return task.ErrStaleVersion
	}

	data.Version++

	item.State = data.State
	item.FailedAttempts = data.FailedAttempts
	item.NextAttemptAt = data.NextAttemptAt
	item.Version = data.Version

	item.CopyTo(data)

	return nil
}

func (s *store) GetByTaskId(ctx context.Context, taskId string) (*task.Record, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	item := s.findByTaskId(taskId)
	if item == nil {
		return nil, task.ErrNotFound
	}

	cloned := item.Clone()
	return &cloned, nil
}

func (s *store) GetAllReadyByState(ctx context.Context, state task.State, asOf time.Time, cursor query.Cursor, limit uint64, direction query.Ordering) ([]*task.Record, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	items := s.findByState(state)
	items = s.filterReady(items, asOf)

	if items = s.filter(items, cursor, limit, direction); len(items) > 0 {
		return cloneRecords(items), nil
	}

	return nil, task.ErrNotFound
}

func (s *store) CountByState(ctx context.Context, state task.State) (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	items := s.findByState(state)
	return uint64(len(items)), nil
}

func (s *store) findByTaskId(taskId string) *task.Record {
	for _, item := range s.records {
		if item.TaskId == taskId {
			return item
		}
	}
	return nil
}

func (s *store) findByState(state task.State) []*task.Record {
	var res []*task.Record
	for _, item := range s.records {
		if item.State == state {
			res = append(res, item)
		}
	}
	return res
}

func (s *store) filterReady(items []*task.Record, asOf time.Time) []*task.Record {
	var res []*task.Record
	for _, item := range items {
		if !item.NextAttemptAt.After(asOf) {
			res = append(res, item)
		}
	}
	return res
}

func (s *store) filter(items []*task.Record, cursor query.Cursor, limit uint64, direction query.Ordering) []*task.Record {
	var start uint64

	start = 0
	if direction == query.Descending {
		start = s.last + 1
	}
	if len(cursor) > 0 {
		start = cursor.ToUint64()
	}

	var res []*task.Record
	for _, item := range items {
		if item.Id > start && direction == query.Ascending {
			res = append(res, item)
		}
		if item.Id < start && direction == query.Descending {
			res = append(res, item)
		}
	}

	if direction == query.Descending {
		sort.Sort(sort.Reverse(ById(res)))
	}

	if len(res) >= int(limit) {
		return res[:limit]
	}

	return res
}

func cloneRecords(items []*task.Record) []*task.Record {
	var res []*task.Record
	for _, item := range items {
		cloned := item.Clone()
		res = append(res, &cloned)
	}
	return res
}

func (s *store) reset() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.records = nil
	s.last = 0
}
