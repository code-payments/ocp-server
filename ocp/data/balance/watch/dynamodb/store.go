// Package dynamodb implements the watch.Store interface on top of DynamoDB.
//
// Two tables. The watches table holds one partition per owner, so that an
// evaluation reads an owner's schedule and every watch on the owner in one
// strongly consistent Query:
//
//	pk = "<owner>"   sk = "#meta"                              next_at, due_shard, version
//	pk = "<owner>"   sk = "watch#<subscriber>#<key hex>"       subscriber, key, threshold…, state…, generation, version
//
// The "#meta" item is the owner's schedule. It sorts ahead of every watch, is
// created by the first put on the owner and never deleted. Two sparse global
// secondary indexes serve the other access paths: by_subscriber (subscriber,
// "<owner>#<key hex>") lists a subscriber's watches for ListWatches, and
// by_due (due_shard, next_at) is the evaluation queue for GetSchedulesDue.
// Each index is populated only by the item kind that carries its keys, so a
// new item kind written into an owner's partition must omit both sets of
// attributes or it leaks into an index.
//
// Every write is conditional on the version it read (or on the item's
// absence), and a put's schedule pull-forward rides in the same transaction
// as the watch, so a watch never exists without its owner being scheduled.
// CommitTransitions is one transaction of up to MaxTransitionsPerCommit watch
// puts and the event that announces them; nothing spans more than that (see
// the watch package comment for why).
//
// The events table holds each subscriber's queue, sharded by owner so that one
// owner's events sit in one partition in order:
//
//	pk = "<subscriber>#<shard>"   sk = "<event id>"   owner, ts, transitions, leased_until?, expires_at
//
// Event IDs are UUIDv7 (watch.NewEventID), so the sort key is issue order and
// the shard is recoverable from the ID alone, which is what lets an ack — a
// bare list of IDs — find its items. A lease is a conditional update of
// leased_until on an item nobody holds; an ack is a delete. Items expire by
// TTL after eventRetention whether or not they were acked, which bounds what a
// subscriber that stopped polling leaves behind.
package dynamodb

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/google/uuid"

	"github.com/code-payments/ocp-server/currency"
	"github.com/code-payments/ocp-server/database/query"
	"github.com/code-payments/ocp-server/ocp/data/balance/watch"
)

const (
	attrPK = "pk"
	attrSK = "sk"

	// Watch items
	attrSubscriber    = "subscriber"
	attrSubscriberSK  = "sub_sk"
	attrKey           = "key"
	attrThresholdKind = "threshold_kind"
	attrQuarks        = "quarks"
	attrCurrency      = "currency"
	attrNativeAmount  = "native_amount"
	attrMints         = "mints"
	attrGrace         = "grace"
	attrState         = "state"
	attrStateSince    = "state_since"
	attrGeneration    = "generation"
	attrCoreMintValue = "core_mint_value"
	attrFiatValue     = "fiat_value"
	attrEvaluatedAt   = "evaluated_at"
	attrVersion       = "version"

	// Schedule items
	attrDueShard = "due_shard"
	attrNextAt   = "next_at"

	// Event items
	attrOwner         = "owner"
	attrTS            = "ts"
	attrTransitions   = "transitions"
	attrPreviousState = "prev_state"
	attrLeasedUntil   = "leased_until"
	attrExpiresAt     = "expires_at"

	skMeta        = "#meta"
	skWatchPrefix = "watch#"

	// codeConditionalCheckFailed is the DynamoDB cancellation reason code for a
	// transaction item whose ConditionExpression evaluated false.
	codeConditionalCheckFailed = "ConditionalCheckFailed"

	// eventRetention is how long an event item lives, acked or not.
	eventRetention = 30 * 24 * time.Hour
)

type store struct {
	client       *dynamodb.Client
	watchesTable string
	eventsTable  string

	// leaseCursor rotates the shard a lease starts from, so polls that never
	// fill their limit do not always drain the same shards first.
	leaseCursor atomic.Uint32
}

// New returns a watch.Store backed by the given DynamoDB tables. Use
// CreateTables to provision them.
func New(client *dynamodb.Client, watchesTable, eventsTable string) watch.Store {
	return &store{
		client:       client,
		watchesTable: watchesTable,
		eventsTable:  eventsTable,
	}
}

// GetWatch implements watch.Store.GetWatch.
func (s *store) GetWatch(ctx context.Context, subscriber, owner string, key []byte) (*watch.Record, error) {
	out, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName:      aws.String(s.watchesTable),
		Key:            watchKey(subscriber, owner, key),
		ConsistentRead: aws.Bool(true),
	})
	if err != nil {
		return nil, err
	}
	if len(out.Item) == 0 {
		return nil, watch.ErrWatchNotFound
	}
	return recordFromItem(out.Item)
}

// GetOwner implements watch.Store.GetOwner.
func (s *store) GetOwner(ctx context.Context, owner string) (*watch.Schedule, []*watch.Record, error) {
	items, err := s.queryAll(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(s.watchesTable),
		KeyConditionExpression: aws.String("#pk = :pk"),
		ExpressionAttributeNames: map[string]string{
			"#pk": attrPK,
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk": avS(owner),
		},
		ConsistentRead: aws.Bool(true),
	})
	if err != nil {
		return nil, nil, err
	}

	var schedule *watch.Schedule
	records := make([]*watch.Record, 0, len(items))
	for _, item := range items {
		if asS(item[attrSK]) == skMeta {
			schedule, err = scheduleFromItem(item)
			if err != nil {
				return nil, nil, err
			}
			continue
		}
		record, err := recordFromItem(item)
		if err != nil {
			return nil, nil, err
		}
		records = append(records, record)
	}
	if schedule == nil {
		return nil, nil, watch.ErrOwnerNotFound
	}
	return schedule, records, nil
}

// GetWatchesBySubscriber implements watch.Store.GetWatchesBySubscriber as a
// range read of the owner's partition: a subscriber's watches are the run of
// sort keys under "watch#<subscriber>#", in key order. The cursor is the hex
// of the last key returned, from which the start position's sort key is
// rebuilt; no index is involved, so the read is strongly consistent.
func (s *store) GetWatchesBySubscriber(ctx context.Context, subscriber, owner string, cursor query.Cursor, limit uint64) ([]*watch.Record, query.Cursor, error) {
	if limit == 0 {
		return nil, query.EmptyCursor, nil
	}

	input := &dynamodb.QueryInput{
		TableName:              aws.String(s.watchesTable),
		KeyConditionExpression: aws.String("#pk = :pk AND begins_with(#sk, :prefix)"),
		ExpressionAttributeNames: map[string]string{
			"#pk": attrPK,
			"#sk": attrSK,
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk":     avS(owner),
			":prefix": avS(watchSKPrefix(subscriber)),
		},
		ConsistentRead: aws.Bool(true),
		// One past the page tells whether there is another without a second
		// query.
		Limit: aws.Int32(int32(min(limit+1, 1<<30))),
	}
	if len(cursor) > 0 {
		key, err := hex.DecodeString(string(cursor))
		if err != nil || len(key) == 0 {
			return nil, nil, watch.ErrInvalidCursor
		}
		input.ExclusiveStartKey = watchKey(subscriber, owner, key)
	}

	var records []*watch.Record
	for uint64(len(records)) <= limit {
		out, err := s.client.Query(ctx, input)
		if err != nil {
			return nil, nil, err
		}
		for _, item := range out.Items {
			record, err := recordFromItem(item)
			if err != nil {
				return nil, nil, err
			}
			records = append(records, record)
		}
		if len(out.LastEvaluatedKey) == 0 {
			break
		}
		input.ExclusiveStartKey = out.LastEvaluatedKey
	}

	next := query.EmptyCursor
	if uint64(len(records)) > limit {
		records = records[:limit]
		next = query.Cursor(hex.EncodeToString(records[len(records)-1].Key))
	}
	return records, next, nil
}

// ListWatches implements watch.Store.ListWatches over the by_subscriber index,
// in (owner, key) order. The cursor is the index sort key of the last watch
// returned, from which every key attribute of the start position is derived.
func (s *store) ListWatches(ctx context.Context, subscriber string, cursor query.Cursor, limit uint64) ([]*watch.Record, query.Cursor, error) {
	if limit == 0 {
		return nil, query.EmptyCursor, nil
	}

	input := &dynamodb.QueryInput{
		TableName:              aws.String(s.watchesTable),
		IndexName:              aws.String(indexBySubscriber),
		KeyConditionExpression: aws.String("#subscriber = :subscriber"),
		ExpressionAttributeNames: map[string]string{
			"#subscriber": attrSubscriber,
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":subscriber": avS(subscriber),
		},
		// One past the page tells whether there is another without a second
		// query.
		Limit: aws.Int32(int32(min(limit+1, 1<<30))),
	}
	if len(cursor) > 0 {
		owner, key, err := parseListCursor(string(cursor))
		if err != nil {
			return nil, nil, watch.ErrInvalidCursor
		}
		input.ExclusiveStartKey = map[string]types.AttributeValue{
			attrPK:           avS(owner),
			attrSK:           avS(watchSK(subscriber, key)),
			attrSubscriber:   avS(subscriber),
			attrSubscriberSK: avS(listPosition(owner, key)),
		}
	}

	var records []*watch.Record
	for uint64(len(records)) <= limit {
		out, err := s.client.Query(ctx, input)
		if err != nil {
			return nil, nil, err
		}
		for _, item := range out.Items {
			record, err := recordFromItem(item)
			if err != nil {
				return nil, nil, err
			}
			records = append(records, record)
		}
		if len(out.LastEvaluatedKey) == 0 {
			break
		}
		input.ExclusiveStartKey = out.LastEvaluatedKey
	}

	next := query.EmptyCursor
	if uint64(len(records)) > limit {
		records = records[:limit]
		last := records[len(records)-1]
		next = query.Cursor(listPosition(last.Owner, last.Key))
	}
	return records, next, nil
}

// PutWatch implements watch.Store.PutWatch as one transaction: the watch item,
// conditioned on absence or on the version read, the owner's schedule pulled
// to now with its version moved, and the event if any.
func (s *store) PutWatch(ctx context.Context, record *watch.Record, event *watch.Event) error {
	if err := record.Validate(); err != nil {
		return err
	}
	if event != nil {
		if err := event.Validate(); err != nil {
			return err
		}
	}

	put := &types.Put{
		TableName: aws.String(s.watchesTable),
		Item:      watchItem(record, record.Version+1),
	}
	if record.Version == 0 {
		put.ConditionExpression = aws.String("attribute_not_exists(#pk)")
		put.ExpressionAttributeNames = map[string]string{"#pk": attrPK}
	} else {
		put.ConditionExpression = aws.String("#version = :version")
		put.ExpressionAttributeNames = map[string]string{"#version": attrVersion}
		put.ExpressionAttributeValues = map[string]types.AttributeValue{
			":version": avNU(record.Version),
		}
	}

	now := time.Now()
	transactItems := []types.TransactWriteItem{
		{Put: put},
		{Update: &types.Update{
			TableName: aws.String(s.watchesTable),
			Key: map[string]types.AttributeValue{
				attrPK: avS(record.Owner),
				attrSK: avS(skMeta),
			},
			UpdateExpression: aws.String("SET #next = :now, #shard = :shard, #version = if_not_exists(#version, :zero) + :one"),
			ExpressionAttributeNames: map[string]string{
				"#next":    attrNextAt,
				"#shard":   attrDueShard,
				"#version": attrVersion,
			},
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":now":   avTime(now),
				":shard": avN(int64(watch.ScheduleShard(record.Owner))),
				":zero":  avN(0),
				":one":   avN(1),
			},
		}},
	}
	if event != nil {
		transactItems = append(transactItems, types.TransactWriteItem{Put: s.eventPut(event)})
	}

	_, err := s.client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
		TransactItems: transactItems,
	})
	if err != nil {
		if isConditionalCheckFailed(err) {
			if record.Version == 0 {
				return watch.ErrWatchExists
			}
			return watch.ErrStaleVersion
		}
		return err
	}
	record.Version++
	return nil
}

// DeleteWatch implements watch.Store.DeleteWatch. The condition passes for an
// item that is absent, so the delete is idempotent, and for one at the given
// generation; an item at any other generation fails it.
func (s *store) DeleteWatch(ctx context.Context, subscriber, owner string, key []byte, generation uint64) error {
	_, err := s.client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName:           aws.String(s.watchesTable),
		Key:                 watchKey(subscriber, owner, key),
		ConditionExpression: aws.String("attribute_not_exists(#pk) OR #generation = :generation"),
		ExpressionAttributeNames: map[string]string{
			"#pk":         attrPK,
			"#generation": attrGeneration,
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":generation": avNU(generation),
		},
	})
	if err != nil {
		var ccf *types.ConditionalCheckFailedException
		if errors.As(err, &ccf) {
			return watch.ErrStaleGeneration
		}
		return err
	}
	return nil
}

// CommitTransitions implements watch.Store.CommitTransitions as one
// transaction: every record put at its next version, conditioned on the
// version read, and the event.
func (s *store) CommitTransitions(ctx context.Context, records []*watch.Record, event *watch.Event) error {
	if err := watch.ValidateCommit(records, event); err != nil {
		return err
	}

	transactItems := make([]types.TransactWriteItem, 0, len(records)+1)
	for _, record := range records {
		transactItems = append(transactItems, types.TransactWriteItem{Put: &types.Put{
			TableName:           aws.String(s.watchesTable),
			Item:                watchItem(record, record.Version+1),
			ConditionExpression: aws.String("#version = :version"),
			ExpressionAttributeNames: map[string]string{
				"#version": attrVersion,
			},
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":version": avNU(record.Version),
			},
		}})
	}
	transactItems = append(transactItems, types.TransactWriteItem{Put: s.eventPut(event)})

	_, err := s.client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
		TransactItems: transactItems,
	})
	if err != nil {
		if isConditionalCheckFailed(err) {
			return watch.ErrStaleVersion
		}
		return err
	}
	for _, record := range records {
		record.Version++
	}
	return nil
}

// GetSchedulesDue implements watch.Store.GetSchedulesDue over the by_due index.
func (s *store) GetSchedulesDue(ctx context.Context, shard int, asOf time.Time, limit int) ([]*watch.Schedule, error) {
	if limit <= 0 {
		return nil, nil
	}

	input := &dynamodb.QueryInput{
		TableName:              aws.String(s.watchesTable),
		IndexName:              aws.String(indexByDue),
		KeyConditionExpression: aws.String("#shard = :shard AND #next <= :asOf"),
		ExpressionAttributeNames: map[string]string{
			"#shard": attrDueShard,
			"#next":  attrNextAt,
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":shard": avN(int64(shard)),
			":asOf":  avTime(asOf),
		},
		ScanIndexForward: aws.Bool(true),
		Limit:            aws.Int32(int32(limit)),
	}

	schedules := make([]*watch.Schedule, 0, limit)
	for len(schedules) < limit {
		out, err := s.client.Query(ctx, input)
		if err != nil {
			return nil, err
		}
		for _, item := range out.Items {
			schedule, err := scheduleFromItem(item)
			if err != nil {
				return nil, err
			}
			schedules = append(schedules, schedule)
			if len(schedules) == limit {
				break
			}
		}
		if len(out.LastEvaluatedKey) == 0 {
			break
		}
		input.ExclusiveStartKey = out.LastEvaluatedKey
	}
	return schedules, nil
}

// ClaimSchedule implements watch.Store.ClaimSchedule.
func (s *store) ClaimSchedule(ctx context.Context, schedule *watch.Schedule, until time.Time) error {
	if err := schedule.Validate(); err != nil {
		return err
	}
	if err := s.updateSchedule(ctx, schedule, until); err != nil {
		return err
	}
	schedule.NextEvaluationAt = until
	return nil
}

// SetSchedule implements watch.Store.SetSchedule.
func (s *store) SetSchedule(ctx context.Context, schedule *watch.Schedule) error {
	if err := schedule.Validate(); err != nil {
		return err
	}
	return s.updateSchedule(ctx, schedule, schedule.NextEvaluationAt)
}

// updateSchedule moves the schedule's next evaluation time at the version the
// caller holds, and advances the caller's copy to the new version. A schedule
// that does not exist has no version attribute, so the condition fails for it
// too.
func (s *store) updateSchedule(ctx context.Context, schedule *watch.Schedule, nextAt time.Time) error {
	_, err := s.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(s.watchesTable),
		Key: map[string]types.AttributeValue{
			attrPK: avS(schedule.Owner),
			attrSK: avS(skMeta),
		},
		UpdateExpression:    aws.String("SET #next = :next, #version = :nextVersion"),
		ConditionExpression: aws.String("#version = :version"),
		ExpressionAttributeNames: map[string]string{
			"#next":    attrNextAt,
			"#version": attrVersion,
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":next":        avTime(nextAt),
			":version":     avNU(schedule.Version),
			":nextVersion": avNU(schedule.Version + 1),
		},
	})
	if err != nil {
		var ccf *types.ConditionalCheckFailedException
		if errors.As(err, &ccf) {
			return watch.ErrStaleVersion
		}
		return err
	}
	schedule.Version++
	return nil
}

// LeaseEvents implements watch.Store.LeaseEvents. It walks the subscriber's
// shards from a rotating start, and in each takes the unleased events in
// order, one conditional update apiece, until it has its fill. A lease lost
// to a concurrent poll is skipped, never retried: the other poll has it.
func (s *store) LeaseEvents(ctx context.Context, subscriber string, asOf, until time.Time, limit int) ([]*watch.Event, error) {
	if limit <= 0 {
		return nil, nil
	}

	leased := make([]*watch.Event, 0)
	start := int(s.leaseCursor.Add(1) % watch.EventShards)
	for i := 0; i < watch.EventShards && len(leased) < limit; i++ {
		shard := (start + i) % watch.EventShards

		input := &dynamodb.QueryInput{
			TableName:              aws.String(s.eventsTable),
			KeyConditionExpression: aws.String("#pk = :pk"),
			FilterExpression:       aws.String("attribute_not_exists(#leased) OR #leased <= :asOf"),
			ExpressionAttributeNames: map[string]string{
				"#pk":     attrPK,
				"#leased": attrLeasedUntil,
			},
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":pk":   avS(eventPK(subscriber, shard)),
				":asOf": avTime(asOf),
			},
			ConsistentRead:   aws.Bool(true),
			ScanIndexForward: aws.Bool(true),
		}
		for len(leased) < limit {
			out, err := s.client.Query(ctx, input)
			if err != nil {
				return nil, err
			}
			for _, item := range out.Items {
				if len(leased) >= limit {
					break
				}
				event, err := s.leaseEvent(ctx, item, asOf, until)
				if err != nil {
					return nil, err
				}
				if event != nil {
					leased = append(leased, event)
				}
			}
			if len(out.LastEvaluatedKey) == 0 {
				break
			}
			input.ExclusiveStartKey = out.LastEvaluatedKey
		}
	}
	return leased, nil
}

// leaseEvent takes the lease on one queued item, returning nil if another
// poll took it first.
func (s *store) leaseEvent(ctx context.Context, item map[string]types.AttributeValue, asOf, until time.Time) (*watch.Event, error) {
	out, err := s.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(s.eventsTable),
		Key: map[string]types.AttributeValue{
			attrPK: item[attrPK],
			attrSK: item[attrSK],
		},
		UpdateExpression:    aws.String("SET #leased = :until"),
		ConditionExpression: aws.String("attribute_exists(#pk) AND (attribute_not_exists(#leased) OR #leased <= :asOf)"),
		ExpressionAttributeNames: map[string]string{
			"#pk":     attrPK,
			"#leased": attrLeasedUntil,
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":until": avTime(until),
			":asOf":  avTime(asOf),
		},
		ReturnValues: types.ReturnValueAllNew,
	})
	if err != nil {
		var ccf *types.ConditionalCheckFailedException
		if errors.As(err, &ccf) {
			return nil, nil
		}
		return nil, err
	}
	return eventFromItem(out.Attributes)
}

// AckEvents implements watch.Store.AckEvents. Each ID names its shard, so the
// acks are plain key deletes, batched.
func (s *store) AckEvents(ctx context.Context, subscriber string, ids []uuid.UUID) error {
	requests := make([]types.WriteRequest, 0, len(ids))
	seen := make(map[uuid.UUID]struct{}, len(ids))
	for _, id := range ids {
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		requests = append(requests, types.WriteRequest{
			DeleteRequest: &types.DeleteRequest{Key: map[string]types.AttributeValue{
				attrPK: avS(eventPK(subscriber, watch.EventShard(id))),
				attrSK: avS(id.String()),
			}},
		})
	}

	for start := 0; start < len(requests); start += maxBatchWriteItems {
		end := min(start+maxBatchWriteItems, len(requests))
		batch := map[string][]types.WriteRequest{s.eventsTable: requests[start:end]}
		// Drain UnprocessedItems (DynamoDB may return a partial batch under load).
		for len(batch[s.eventsTable]) > 0 {
			out, err := s.client.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{RequestItems: batch})
			if err != nil {
				return err
			}
			batch = out.UnprocessedItems
		}
	}
	return nil
}

// reset deletes every item from both tables, for tests.
func (s *store) reset() {
	for _, table := range []string{s.watchesTable, s.eventsTable} {
		if err := clearTable(context.Background(), s.client, table, []string{attrPK, attrSK}); err != nil {
			panic(err)
		}
	}
}

// queryAll runs the query, following LastEvaluatedKey until the result set is
// drained, and returns every matched item.
func (s *store) queryAll(ctx context.Context, input *dynamodb.QueryInput) ([]map[string]types.AttributeValue, error) {
	var items []map[string]types.AttributeValue
	for {
		out, err := s.client.Query(ctx, input)
		if err != nil {
			return nil, err
		}
		items = append(items, out.Items...)
		if len(out.LastEvaluatedKey) == 0 {
			break
		}
		input.ExclusiveStartKey = out.LastEvaluatedKey
	}
	return items, nil
}

// eventPut is the write of an event into its subscriber's queue. The item
// carries no lease; expires_at bounds its life whether or not it is acked.
func (s *store) eventPut(event *watch.Event) *types.Put {
	transitions := make([]types.AttributeValue, 0, len(event.Transitions))
	for _, transition := range event.Transitions {
		m := watchAttributes(transition.Watch, transition.Watch.Version)
		m[attrPreviousState] = avN(int64(transition.PreviousState))
		transitions = append(transitions, &types.AttributeValueMemberM{Value: m})
	}
	return &types.Put{
		TableName: aws.String(s.eventsTable),
		Item: map[string]types.AttributeValue{
			attrPK:          avS(eventPK(event.Subscriber, watch.EventShard(event.ID))),
			attrSK:          avS(event.ID.String()),
			attrOwner:       avS(event.Owner),
			attrTS:          avTime(event.Timestamp),
			attrTransitions: &types.AttributeValueMemberL{Value: transitions},
			attrExpiresAt:   avN(event.Timestamp.Add(eventRetention).Unix()),
		},
	}
}

func eventFromItem(item map[string]types.AttributeValue) (*watch.Event, error) {
	id, err := uuid.Parse(asS(item[attrSK]))
	if err != nil {
		return nil, err
	}
	subscriber, _, ok := strings.Cut(asS(item[attrPK]), "#")
	if !ok {
		return nil, fmt.Errorf("unexpected event pk %q", asS(item[attrPK]))
	}
	ts, err := parseTime(item[attrTS])
	if err != nil {
		return nil, err
	}

	event := &watch.Event{
		ID:         id,
		Subscriber: subscriber,
		Owner:      asS(item[attrOwner]),
		Timestamp:  ts,
	}
	if av, ok := item[attrLeasedUntil]; ok {
		if event.LeasedUntil, err = parseTime(av); err != nil {
			return nil, err
		}
	}

	list, ok := item[attrTransitions].(*types.AttributeValueMemberL)
	if !ok {
		return nil, fmt.Errorf("expected list attribute for transitions, got %T", item[attrTransitions])
	}
	event.Transitions = make([]watch.Transition, 0, len(list.Value))
	for _, av := range list.Value {
		m, ok := av.(*types.AttributeValueMemberM)
		if !ok {
			return nil, fmt.Errorf("expected map attribute for transition, got %T", av)
		}
		record, err := recordFromAttributes(m.Value)
		if err != nil {
			return nil, err
		}
		previous, err := parseN(m.Value[attrPreviousState])
		if err != nil {
			return nil, err
		}
		event.Transitions = append(event.Transitions, watch.Transition{
			Watch:         record,
			PreviousState: watch.State(previous),
		})
	}
	return event, nil
}

// watchItem is a watch's table item at the given version: its attributes plus
// the keys of the base table and the by_subscriber index.
func watchItem(record *watch.Record, version uint64) map[string]types.AttributeValue {
	item := watchAttributes(record, version)
	item[attrPK] = avS(record.Owner)
	item[attrSK] = avS(watchSK(record.Subscriber, record.Key))
	item[attrSubscriberSK] = avS(listPosition(record.Owner, record.Key))
	return item
}

// watchAttributes is a watch's data as attributes, without any key: what an
// event embeds for each transition.
func watchAttributes(record *watch.Record, version uint64) map[string]types.AttributeValue {
	mints := make([]types.AttributeValue, 0, len(record.Mints))
	for _, mint := range record.Mints {
		mints = append(mints, avS(mint))
	}

	item := map[string]types.AttributeValue{
		attrSubscriber:    avS(record.Subscriber),
		attrOwner:         avS(record.Owner),
		attrKey:           avB(record.Key),
		attrThresholdKind: avN(int64(record.Threshold.Kind)),
		attrMints:         &types.AttributeValueMemberL{Value: mints},
		attrGrace:         avN(int64(record.Grace)),
		attrState:         avN(int64(record.State)),
		attrStateSince:    avTime(record.StateSince),
		attrGeneration:    avNU(record.Generation),
		attrCoreMintValue: avNU(record.LastValuation.CoreMintValue),
		attrEvaluatedAt:   avTime(record.LastValuation.EvaluatedAt),
		attrVersion:       avNU(version),
	}
	switch record.Threshold.Kind {
	case watch.ThresholdKindCoreMintQuarks:
		item[attrQuarks] = avNU(record.Threshold.Quarks)
	case watch.ThresholdKindFiat:
		item[attrCurrency] = avS(string(record.Threshold.Currency))
		item[attrNativeAmount] = avF(record.Threshold.NativeAmount)
	}
	if record.LastValuation.FiatValue != nil {
		item[attrFiatValue] = avF(*record.LastValuation.FiatValue)
	}
	return item
}

func recordFromItem(item map[string]types.AttributeValue) (*watch.Record, error) {
	return recordFromAttributes(item)
}

func recordFromAttributes(item map[string]types.AttributeValue) (*watch.Record, error) {
	record := &watch.Record{
		Subscriber: asS(item[attrSubscriber]),
		Owner:      asS(item[attrOwner]),
		Key:        asB(item[attrKey]),
	}

	var err error
	kind, err := parseN(item[attrThresholdKind])
	if err != nil {
		return nil, err
	}
	record.Threshold.Kind = watch.ThresholdKind(kind)
	switch record.Threshold.Kind {
	case watch.ThresholdKindCoreMintQuarks:
		if record.Threshold.Quarks, err = parseNU(item[attrQuarks]); err != nil {
			return nil, err
		}
	case watch.ThresholdKindFiat:
		record.Threshold.Currency = currency.Code(asS(item[attrCurrency]))
		if record.Threshold.NativeAmount, err = parseF(item[attrNativeAmount]); err != nil {
			return nil, err
		}
	}

	if list, ok := item[attrMints].(*types.AttributeValueMemberL); ok && len(list.Value) > 0 {
		record.Mints = make([]string, 0, len(list.Value))
		for _, av := range list.Value {
			record.Mints = append(record.Mints, asS(av))
		}
	}

	grace, err := parseN(item[attrGrace])
	if err != nil {
		return nil, err
	}
	record.Grace = time.Duration(grace)

	state, err := parseN(item[attrState])
	if err != nil {
		return nil, err
	}
	record.State = watch.State(state)
	if record.StateSince, err = parseTime(item[attrStateSince]); err != nil {
		return nil, err
	}
	if record.Generation, err = parseNU(item[attrGeneration]); err != nil {
		return nil, err
	}

	if record.LastValuation.CoreMintValue, err = parseNU(item[attrCoreMintValue]); err != nil {
		return nil, err
	}
	if av, ok := item[attrFiatValue]; ok {
		value, err := parseF(av)
		if err != nil {
			return nil, err
		}
		record.LastValuation.FiatValue = &value
	}
	if record.LastValuation.EvaluatedAt, err = parseTime(item[attrEvaluatedAt]); err != nil {
		return nil, err
	}

	if record.Version, err = parseNU(item[attrVersion]); err != nil {
		return nil, err
	}
	return record, nil
}

func scheduleFromItem(item map[string]types.AttributeValue) (*watch.Schedule, error) {
	nextAt, err := parseTime(item[attrNextAt])
	if err != nil {
		return nil, err
	}
	version, err := parseNU(item[attrVersion])
	if err != nil {
		return nil, err
	}
	return &watch.Schedule{
		Owner:            asS(item[attrPK]),
		NextEvaluationAt: nextAt,
		Version:          version,
	}, nil
}

func watchKey(subscriber, owner string, key []byte) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{
		attrPK: avS(owner),
		attrSK: avS(watchSK(subscriber, key)),
	}
}

// watchSK is a watch's sort key. The key is hex so that arbitrary bytes are
// safe in a string key; subscribers are base58 and never contain '#'.
func watchSK(subscriber string, key []byte) string {
	return watchSKPrefix(subscriber) + hex.EncodeToString(key)
}

func watchSKPrefix(subscriber string) string {
	return skWatchPrefix + subscriber + "#"
}

// listPosition is a watch's sort key in the by_subscriber index, and the
// ListWatches cursor.
func listPosition(owner string, key []byte) string {
	return owner + "#" + hex.EncodeToString(key)
}

func parseListCursor(cursor string) (owner string, key []byte, err error) {
	owner, keyHex, ok := strings.Cut(cursor, "#")
	if !ok || len(owner) == 0 {
		return "", nil, errors.New("malformed cursor")
	}
	key, err = hex.DecodeString(keyHex)
	if err != nil || len(key) == 0 {
		return "", nil, errors.New("malformed cursor")
	}
	return owner, key, nil
}

func eventPK(subscriber string, shard int) string {
	return subscriber + "#" + strconv.Itoa(shard)
}

// isConditionalCheckFailed reports whether a transaction was cancelled because
// one of its conditions evaluated false, as opposed to a conflict or a
// throttle, which the caller should surface as-is.
func isConditionalCheckFailed(err error) bool {
	var tce *types.TransactionCanceledException
	if !errors.As(err, &tce) {
		return false
	}
	for _, reason := range tce.CancellationReasons {
		if aws.ToString(reason.Code) == codeConditionalCheckFailed {
			return true
		}
	}
	return false
}

func avS(v string) types.AttributeValue { return &types.AttributeValueMemberS{Value: v} }
func avB(v []byte) types.AttributeValue { return &types.AttributeValueMemberB{Value: v} }
func avN(v int64) types.AttributeValue {
	return &types.AttributeValueMemberN{Value: strconv.FormatInt(v, 10)}
}
func avNU(v uint64) types.AttributeValue {
	return &types.AttributeValueMemberN{Value: strconv.FormatUint(v, 10)}
}
func avF(v float64) types.AttributeValue {
	return &types.AttributeValueMemberN{Value: strconv.FormatFloat(v, 'g', -1, 64)}
}
func avTime(t time.Time) types.AttributeValue { return avN(t.UTC().UnixNano()) }

func asS(av types.AttributeValue) string {
	if s, ok := av.(*types.AttributeValueMemberS); ok {
		return s.Value
	}
	return ""
}

func asB(av types.AttributeValue) []byte {
	if b, ok := av.(*types.AttributeValueMemberB); ok {
		return b.Value
	}
	return nil
}

func parseN(av types.AttributeValue) (int64, error) {
	n, ok := av.(*types.AttributeValueMemberN)
	if !ok {
		return 0, fmt.Errorf("expected number attribute, got %T", av)
	}
	return strconv.ParseInt(n.Value, 10, 64)
}

func parseNU(av types.AttributeValue) (uint64, error) {
	n, ok := av.(*types.AttributeValueMemberN)
	if !ok {
		return 0, fmt.Errorf("expected number attribute, got %T", av)
	}
	return strconv.ParseUint(n.Value, 10, 64)
}

func parseF(av types.AttributeValue) (float64, error) {
	n, ok := av.(*types.AttributeValueMemberN)
	if !ok {
		return 0, fmt.Errorf("expected number attribute, got %T", av)
	}
	return strconv.ParseFloat(n.Value, 64)
}

func parseTime(av types.AttributeValue) (time.Time, error) {
	nanos, err := parseN(av)
	if err != nil {
		return time.Time{}, err
	}
	return time.Unix(0, nanos).UTC(), nil
}
