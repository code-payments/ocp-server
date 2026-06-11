package task

import (
	"errors"
	"time"
)

type State uint8

const (
	StateUnknown State = iota
	StatePending
	StateConfirmed
	StateFailed
)

type Record struct {
	Id uint64

	TaskId string

	// Type and Data are opaque to the base system. The implementing app owns
	// the namespace and serialization.
	Type uint32
	Data []byte

	// ReferenceId is an optional correlation ID (eg. an intent ID) used purely
	// for observability. The base system never reads it.
	ReferenceId *string

	State State

	Attempts      uint32
	NextAttemptAt time.Time

	Version uint64

	CreatedAt time.Time
}

func (r *Record) Clone() Record {
	var referenceIdCopy *string
	if r.ReferenceId != nil {
		value := *r.ReferenceId
		referenceIdCopy = &value
	}

	var dataCopy []byte
	if r.Data != nil {
		dataCopy = make([]byte, len(r.Data))
		copy(dataCopy, r.Data)
	}

	return Record{
		Id: r.Id,

		TaskId: r.TaskId,

		Type: r.Type,
		Data: dataCopy,

		ReferenceId: referenceIdCopy,

		State: r.State,

		Attempts:      r.Attempts,
		NextAttemptAt: r.NextAttemptAt,

		Version: r.Version,

		CreatedAt: r.CreatedAt,
	}
}

func (r *Record) CopyTo(dst *Record) {
	cloned := r.Clone()

	dst.Id = cloned.Id

	dst.TaskId = cloned.TaskId

	dst.Type = cloned.Type
	dst.Data = cloned.Data

	dst.ReferenceId = cloned.ReferenceId

	dst.State = cloned.State

	dst.Attempts = cloned.Attempts
	dst.NextAttemptAt = cloned.NextAttemptAt

	dst.Version = cloned.Version

	dst.CreatedAt = cloned.CreatedAt
}

func (r *Record) Validate() error {
	if len(r.TaskId) == 0 {
		return errors.New("task id is required")
	}

	if r.Type == 0 {
		return errors.New("type is required")
	}

	if r.ReferenceId != nil && len(*r.ReferenceId) == 0 {
		return errors.New("reference id is empty when set")
	}

	return nil
}

func (s State) String() string {
	switch s {
	case StatePending:
		return "pending"
	case StateConfirmed:
		return "confirmed"
	case StateFailed:
		return "failed"
	}
	return "unknown"
}
