package memory

import (
	"context"
	"sort"
	"strings"
	"sync"

	"github.com/code-payments/ocp-server/database/query"
	"github.com/code-payments/ocp-server/ocp/data/currency"
	"github.com/code-payments/ocp-server/ocp/data/currency/metadata"
)

type store struct {
	mu                sync.Mutex
	metadataRecords   []*currency.MetadataRecord
	lastMetadataIndex uint64
}

func New() metadata.Store {
	return &store{
		lastMetadataIndex: 1,
	}
}

func (s *store) reset() {
	s.mu.Lock()
	s.metadataRecords = make([]*currency.MetadataRecord, 0)
	s.lastMetadataIndex = 1
	s.mu.Unlock()
}

func (s *store) SaveMetadata(ctx context.Context, data *currency.MetadataRecord) error {
	if err := data.Validate(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	for i, item := range s.metadataRecords {
		if item.Mint == data.Mint {
			if item.Version != data.Version {
				return currency.ErrStaleMetadataVersion
			}

			cloned := item.Clone()
			cloned.Description = data.Description
			cloned.ImageUrl = data.ImageUrl
			cloned.BillColors = append([]string(nil), data.BillColors...)
			cloned.SocialLinks = append([]currency.SocialLink(nil), data.SocialLinks...)
			cloned.Alt = data.Alt
			cloned.IsDiscoverable = data.IsDiscoverable
			cloned.State = data.State
			cloned.Version = item.Version + 1

			s.metadataRecords[i] = cloned
			cloned.CopyTo(data)
			return nil
		}
	}

	for _, item := range s.metadataRecords {
		if strings.EqualFold(item.Name, data.Name) && item.State != currency.MetadataStateAbandoned {
			return currency.ErrDuplicateCurrency
		}
	}

	data.Version = 1
	data.Id = s.lastMetadataIndex
	s.metadataRecords = append(s.metadataRecords, data.Clone())
	s.lastMetadataIndex = s.lastMetadataIndex + 1

	return nil
}

func (s *store) GetMetadata(ctx context.Context, mint string) (*currency.MetadataRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, item := range s.metadataRecords {
		if item.Mint == mint {
			return item.Clone(), nil
		}
	}

	return nil, currency.ErrNotFound
}

type metadataById []*currency.MetadataRecord

func (a metadataById) Len() int           { return len(a) }
func (a metadataById) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a metadataById) Less(i, j int) bool { return a[i].Id < a[j].Id }

func (s *store) GetAllMetadataByState(_ context.Context, state currency.MetadataState, cursor query.Cursor, limit uint64, direction query.Ordering) ([]*currency.MetadataRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var items []*currency.MetadataRecord
	for _, item := range s.metadataRecords {
		if item.State == state {
			items = append(items, item)
		}
	}

	if len(items) == 0 {
		return nil, currency.ErrNotFound
	}

	var start uint64
	start = 0
	if direction == query.Descending {
		start = s.lastMetadataIndex + 1
	}
	if len(cursor) > 0 {
		start = cursor.ToUint64()
	}

	var res []*currency.MetadataRecord
	for _, item := range items {
		if item.Id > start && direction == query.Ascending {
			res = append(res, item.Clone())
		}
		if item.Id < start && direction == query.Descending {
			res = append(res, item.Clone())
		}
	}

	if len(res) == 0 {
		return nil, currency.ErrNotFound
	}

	if direction == query.Descending {
		sort.Sort(sort.Reverse(metadataById(res)))
	}

	if uint64(len(res)) > limit {
		res = res[:limit]
	}

	return res, nil
}

func (s *store) GetAllMints(ctx context.Context) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.metadataRecords) == 0 {
		return nil, currency.ErrNotFound
	}

	var mints []string
	for _, item := range s.metadataRecords {
		mints = append(mints, item.Mint)
	}

	return mints, nil
}

func (s *store) CountMetadataByState(_ context.Context, state currency.MetadataState) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var count uint64
	for _, item := range s.metadataRecords {
		if item.State == state {
			count++
		}
	}
	return count, nil
}

func (s *store) CountMints(ctx context.Context) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var count uint64
	for _, item := range s.metadataRecords {
		if item.State == currency.MetadataStateAbandoned {
			continue
		}
		count++
	}
	return count, nil
}

func (s *store) IsNameAvailable(_ context.Context, name string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, item := range s.metadataRecords {
		if strings.EqualFold(item.Name, name) && item.State != currency.MetadataStateAbandoned {
			return false, nil
		}
	}
	return true, nil
}
