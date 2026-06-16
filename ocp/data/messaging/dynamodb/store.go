// Package dynamodb implements the messaging.Store interface on top of DynamoDB.
//
// Each account is a bin (partition); each message is one item within it:
//
//	pk = "<account>"   sk = "<message-id>"   message = <bytes>
//
// Insert is a conditional Put (so a duplicate message ID in the bin fails),
// Delete is an idempotent DeleteItem, and Get is a single Query over the bin's
// partition.
package dynamodb

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/google/uuid"

	"github.com/code-payments/ocp-server/ocp/data/messaging"
)

const (
	attrPK        = "pk"
	attrSK        = "sk"
	attrMessage   = "message"
	attrExpiresAt = "expires_at"
)

type store struct {
	client *dynamodb.Client
	table  string
}

// New returns a messaging.Store backed by the given DynamoDB table.
// Use CreateTables to provision it.
func New(client *dynamodb.Client, table string) messaging.Store {
	return &store{
		client: client,
		table:  table,
	}
}

// Insert implements messaging.Store.Insert.
func (s *store) Insert(ctx context.Context, record *messaging.Record) error {
	if err := record.Validate(); err != nil {
		return err
	}

	item := map[string]types.AttributeValue{
		attrPK:      avS(record.Account),
		attrSK:      avS(record.MessageID.String()),
		attrMessage: avB(record.Message),
	}
	// DynamoDB TTL expects epoch seconds; a zero ExpiresAt means no expiry.
	if !record.ExpiresAt.IsZero() {
		item[attrExpiresAt] = avN(record.ExpiresAt.Unix())
	}

	_, err := s.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName:           aws.String(s.table),
		Item:                item,
		ConditionExpression: aws.String(fmt.Sprintf("attribute_not_exists(%s)", attrPK)),
	})
	if err != nil {
		var ccf *types.ConditionalCheckFailedException
		if errors.As(err, &ccf) {
			return messaging.ErrDuplicateMessageID
		}
		return err
	}
	return nil
}

// Delete implements messaging.Store.Delete. It is idempotent: deleting a message
// that does not exist is a no-op.
func (s *store) Delete(ctx context.Context, account string, messageID uuid.UUID) error {
	_, err := s.client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName: aws.String(s.table),
		Key: map[string]types.AttributeValue{
			attrPK: avS(account),
			attrSK: avS(messageID.String()),
		},
	})
	return err
}

// Get implements messaging.Store.Get.
func (s *store) Get(ctx context.Context, account string) ([]*messaging.Record, error) {
	now := time.Now()
	var records []*messaging.Record
	input := &dynamodb.QueryInput{
		TableName:              aws.String(s.table),
		KeyConditionExpression: aws.String(fmt.Sprintf("%s = :pk", attrPK)),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk": avS(account),
		},
	}
	for {
		out, err := s.client.Query(ctx, input)
		if err != nil {
			return nil, err
		}
		for _, item := range out.Items {
			record, err := recordFromItem(account, item)
			if err != nil {
				return nil, err
			}
			// DynamoDB TTL deletion is lazy, so drop already-expired messages here.
			if !record.ExpiresAt.IsZero() && !record.ExpiresAt.After(now) {
				continue
			}
			records = append(records, record)
		}
		if len(out.LastEvaluatedKey) == 0 {
			break
		}
		input.ExclusiveStartKey = out.LastEvaluatedKey
	}
	return records, nil
}

// reset deletes every item from the table, for tests.
func (s *store) reset() {
	if err := clearTable(context.Background(), s.client, s.table); err != nil {
		panic(err)
	}
}

// recordFromItem rebuilds a record from a bin item. The account is the query
// parameter (it is the partition key, so it is not re-parsed from the item).
func recordFromItem(account string, item map[string]types.AttributeValue) (*messaging.Record, error) {
	id, err := uuid.Parse(asS(item[attrSK]))
	if err != nil {
		return nil, err
	}
	record := &messaging.Record{
		Account:   account,
		MessageID: id,
		Message:   asB(item[attrMessage]),
	}
	if av, ok := item[attrExpiresAt].(*types.AttributeValueMemberN); ok {
		epoch, err := strconv.ParseInt(av.Value, 10, 64)
		if err != nil {
			return nil, err
		}
		record.ExpiresAt = time.Unix(epoch, 0).UTC()
	}
	return record, nil
}

func avS(v string) types.AttributeValue { return &types.AttributeValueMemberS{Value: v} }
func avB(v []byte) types.AttributeValue { return &types.AttributeValueMemberB{Value: v} }
func avN(v int64) types.AttributeValue {
	return &types.AttributeValueMemberN{Value: strconv.FormatInt(v, 10)}
}

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
