package memory

import (
	"context"
	"sync"
	"time"

	"github.com/code-payments/ocp-server/ocp/data/vm/metadata"
)

type store struct {
	mu      sync.Mutex
	last    uint64
	records []*metadata.Record
}

// New returns a new in memory vm.metadata.Store
func New() metadata.Store {
	return &store{}
}

// Put implements vm.metadata.Store.Put
func (s *store) Put(_ context.Context, record *metadata.Record) error {
	if err := record.Validate(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if item := s.find(record); item != nil {
		return metadata.ErrAlreadyExists
	}

	s.last++
	record.Id = s.last
	if record.CreatedAt.IsZero() {
		record.CreatedAt = time.Now()
	}

	cloned := record.Clone()
	s.records = append(s.records, &cloned)

	return nil
}

// GetByMint implements vm.metadata.Store.GetByMint
func (s *store) GetByMint(_ context.Context, mint string) (*metadata.Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, item := range s.records {
		if item.Mint == mint {
			cloned := item.Clone()
			return &cloned, nil
		}
	}

	return nil, metadata.ErrNotFound
}

func (s *store) find(data *metadata.Record) *metadata.Record {
	for _, item := range s.records {
		if item.Id == data.Id && data.Id != 0 {
			return item
		}

		if item.Mint == data.Mint {
			return item
		}
	}

	return nil
}

func (s *store) reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.last = 0
	s.records = nil
}
