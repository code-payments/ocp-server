package watch

import (
	"hash/fnv"

	"github.com/google/uuid"
)

const (
	// ScheduleShards is how many ways the evaluation queue is split. A worker
	// pages each shard for the owners due in it (Store.GetSchedulesDue), so
	// the count bounds how many reads a full sweep of the queue costs and how
	// wide the queue's write load spreads. Owners are assigned by
	// ScheduleShard.
	ScheduleShards = 64

	// EventShards is how many ways each subscriber's event queue is split. A
	// subscriber's poll reads shards in turn until it has its fill
	// (Store.LeaseEvents), so the count bounds the reads an empty poll costs.
	// Events are assigned by owner (EventShardForOwner), so one owner's events
	// share a shard and keep their order.
	//
	// It is at most 64 because the shard is carried in six bits of the event
	// ID (see NewEventID).
	EventShards = 16
)

// ScheduleShard is the evaluation queue shard an owner's schedule is kept in.
func ScheduleShard(owner string) int {
	return int(hash(owner) % ScheduleShards)
}

// EventShardForOwner is the queue shard events about an owner are kept in,
// for every subscriber.
func EventShardForOwner(owner string) int {
	return int(hash(owner) % EventShards)
}

// NewEventID issues the ID for an event about owner. IDs are UUIDv7, so they
// order by time of issue — to the millisecond by the clock and within a
// millisecond by a per-process sequence — and the store sorts a queue by them.
// The event's shard is written into the six low bits of byte 8 (the byte that
// carries the RFC 4122 variant in its two high bits), so that an ack, which
// carries nothing but the ID, can find the item: see EventShard.
func NewEventID(owner string) (uuid.UUID, error) {
	id, err := uuid.NewV7()
	if err != nil {
		return uuid.Nil, err
	}
	id[8] = (id[8] & 0xC0) | byte(EventShardForOwner(owner))
	return id, nil
}

// EventShard recovers the queue shard from an event ID issued by NewEventID.
func EventShard(id uuid.UUID) int {
	return int(id[8] & 0x3F)
}

func hash(s string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(s))
	return h.Sum32()
}
