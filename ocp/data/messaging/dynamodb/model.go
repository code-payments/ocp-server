package dynamodb

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/google/uuid"
	"github.com/pkg/errors"

	dynamodbutil "github.com/code-payments/ocp-server/database/dynamodb"

	"github.com/code-payments/ocp-server/ocp/data/messaging"
)

type model struct {
	Account   string
	MessageID string
	Message   []byte
	CreatedAt time.Time
}

func toModel(record *messaging.Record) (*model, error) {
	if err := record.Validate(); err != nil {
		return nil, err
	}

	return &model{
		Account:   record.Account,
		MessageID: record.MessageID.String(),
		Message:   record.Message,
		// The only time we call toModel is on create, so it's fine to default
		// to UTC now.
		CreatedAt: time.Now().UTC(),
	}, nil
}

func fromModel(m *model) (*messaging.Record, error) {
	parsedMessageID, err := uuid.Parse(m.MessageID)
	if err != nil {
		return nil, errors.Wrap(err, "failure parsing message id")
	}

	return &messaging.Record{
		Account:   m.Account,
		MessageID: parsedMessageID,
		Message:   m.Message,
	}, nil
}

func (m *model) dbPut(ctx context.Context, client *dynamodb.Client, tableName string) error {
	req := client.PutItemRequest(&dynamodb.PutItemInput{
		TableName: aws.String(tableName),
		Item: map[string]dynamodb.AttributeValue{
			"account":    {S: aws.String(m.Account)},
			"message_id": {S: aws.String(m.MessageID)},
			"message":    {B: m.Message},
			"created_at": {N: aws.String(fmt.Sprintf("%d", m.CreatedAt.Unix()))},
		},
		ConditionExpression: aws.String("attribute_not_exists(account) AND attribute_not_exists(message_id)"),
	})

	_, err := req.Send(ctx)
	if err != nil {
		return dynamodbutil.CheckConditionalCheckFailed(err, messaging.ErrDuplicateMessageID)
	}

	return nil
}

func dbGetAllForAccount(ctx context.Context, client *dynamodb.Client, tableName string, account string) ([]*model, error) {
	req := client.QueryRequest(&dynamodb.QueryInput{
		TableName:              aws.String(tableName),
		KeyConditionExpression: aws.String("account = :account"),
		ExpressionAttributeValues: map[string]dynamodb.AttributeValue{
			":account": {S: aws.String(account)},
		},
	})

	resp, err := req.Send(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "failed to query messages")
	}

	models := make([]*model, len(resp.Items))
	for i, item := range resp.Items {
		m := &model{}
		if v, ok := item["account"]; ok && v.S != nil {
			m.Account = *v.S
		}
		if v, ok := item["message_id"]; ok && v.S != nil {
			m.MessageID = *v.S
		}
		if v, ok := item["message"]; ok {
			m.Message = v.B
		}
		if v, ok := item["created_at"]; ok && v.N != nil {
			seconds, err := strconv.ParseInt(*v.N, 10, 64)
			if err != nil {
				return nil, errors.Wrap(err, "failed to parse created_at")
			}
			m.CreatedAt = time.Unix(seconds, 0).UTC()
		}
		models[i] = m
	}

	return models, nil
}

func dbDelete(ctx context.Context, client *dynamodb.Client, tableName string, account, messageID string) error {
	req := client.DeleteItemRequest(&dynamodb.DeleteItemInput{
		TableName: aws.String(tableName),
		Key: map[string]dynamodb.AttributeValue{
			"account":    {S: aws.String(account)},
			"message_id": {S: aws.String(messageID)},
		},
	})

	_, err := req.Send(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to delete message")
	}

	return nil
}
