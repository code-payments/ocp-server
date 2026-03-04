package dynamodb

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/google/uuid"

	"github.com/code-payments/ocp-server/ocp/data/messaging"
)

type store struct {
	client    *dynamodb.Client
	tableName string
}

// New returns a DynamoDB backed messaging.Store.
func New(client *dynamodb.Client, tableName string) messaging.Store {
	return &store{
		client:    client,
		tableName: tableName,
	}
}

// Insert implements messaging.Store.Insert.
func (s *store) Insert(ctx context.Context, record *messaging.Record) error {
	model, err := toModel(record)
	if err != nil {
		return err
	}

	return model.dbPut(ctx, s.client, s.tableName)
}

// Delete implements messaging.Store.Delete.
func (s *store) Delete(ctx context.Context, account string, messageID uuid.UUID) error {
	return dbDelete(ctx, s.client, s.tableName, account, messageID.String())
}

// Get implements messaging.Store.Get.
func (s *store) Get(ctx context.Context, account string) ([]*messaging.Record, error) {
	models, err := dbGetAllForAccount(ctx, s.client, s.tableName, account)
	if err != nil {
		return nil, err
	}

	records := make([]*messaging.Record, len(models))
	for i, m := range models {
		record, err := fromModel(m)
		if err != nil {
			return nil, err
		}
		records[i] = record
	}

	return records, nil
}
