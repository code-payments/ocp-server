package dynamodb

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

const (
	// indexBySubscriber lists a subscriber's watches: pk = subscriber,
	// sk = "<owner>#<key hex>". Sparse: only watch items carry `subscriber`,
	// so schedule items never appear in it.
	indexBySubscriber = "by_subscriber"

	// indexByDue is the evaluation queue: pk = due_shard, sk = next_at. Sparse:
	// only schedule items carry those attributes, so watch items never appear
	// in it.
	indexByDue = "by_due"

	// maxBatchWriteItems is DynamoDB's per-call BatchWriteItem limit.
	maxBatchWriteItems = 25
)

// CreateTables provisions the watches and events tables with on-demand
// billing. The watches table is keyed by (pk, sk) with the by_subscriber and
// by_due indexes; the events table is keyed by (pk, sk) with a TTL on
// expires_at. It is idempotent and blocks until both tables are ACTIVE.
func CreateTables(ctx context.Context, client *dynamodb.Client, watchesTable, eventsTable string) error {
	inputs := []*dynamodb.CreateTableInput{
		{
			TableName:   aws.String(watchesTable),
			BillingMode: types.BillingModePayPerRequest,
			AttributeDefinitions: []types.AttributeDefinition{
				{AttributeName: aws.String(attrPK), AttributeType: types.ScalarAttributeTypeS},
				{AttributeName: aws.String(attrSK), AttributeType: types.ScalarAttributeTypeS},
				{AttributeName: aws.String(attrSubscriber), AttributeType: types.ScalarAttributeTypeS},
				{AttributeName: aws.String(attrSubscriberSK), AttributeType: types.ScalarAttributeTypeS},
				{AttributeName: aws.String(attrDueShard), AttributeType: types.ScalarAttributeTypeN},
				{AttributeName: aws.String(attrNextAt), AttributeType: types.ScalarAttributeTypeN},
			},
			KeySchema: []types.KeySchemaElement{
				{AttributeName: aws.String(attrPK), KeyType: types.KeyTypeHash},
				{AttributeName: aws.String(attrSK), KeyType: types.KeyTypeRange},
			},
			GlobalSecondaryIndexes: []types.GlobalSecondaryIndex{
				{
					IndexName: aws.String(indexBySubscriber),
					KeySchema: []types.KeySchemaElement{
						{AttributeName: aws.String(attrSubscriber), KeyType: types.KeyTypeHash},
						{AttributeName: aws.String(attrSubscriberSK), KeyType: types.KeyTypeRange},
					},
					Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
				},
				{
					IndexName: aws.String(indexByDue),
					KeySchema: []types.KeySchemaElement{
						{AttributeName: aws.String(attrDueShard), KeyType: types.KeyTypeHash},
						{AttributeName: aws.String(attrNextAt), KeyType: types.KeyTypeRange},
					},
					Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
				},
			},
		},
		{
			TableName:   aws.String(eventsTable),
			BillingMode: types.BillingModePayPerRequest,
			AttributeDefinitions: []types.AttributeDefinition{
				{AttributeName: aws.String(attrPK), AttributeType: types.ScalarAttributeTypeS},
				{AttributeName: aws.String(attrSK), AttributeType: types.ScalarAttributeTypeS},
			},
			KeySchema: []types.KeySchemaElement{
				{AttributeName: aws.String(attrPK), KeyType: types.KeyTypeHash},
				{AttributeName: aws.String(attrSK), KeyType: types.KeyTypeRange},
			},
		},
	}

	for _, input := range inputs {
		if _, err := client.CreateTable(ctx, input); err != nil {
			var inUse *types.ResourceInUseException
			if !errors.As(err, &inUse) {
				return err
			}
			// Already exists; still ensure it is ACTIVE before returning.
		}
		if err := dynamodb.NewTableExistsWaiter(client).Wait(ctx, &dynamodb.DescribeTableInput{
			TableName: input.TableName,
		}, 2*time.Minute); err != nil {
			return err
		}
	}

	return ensureTTL(ctx, client, eventsTable, attrExpiresAt)
}

// ensureTTL idempotently enables DynamoDB TTL on the table's attr. Enabling when
// already enabled (or enabling) is a no-op, so re-running CreateTables is safe.
func ensureTTL(ctx context.Context, client *dynamodb.Client, table, attr string) error {
	desc, err := client.DescribeTimeToLive(ctx, &dynamodb.DescribeTimeToLiveInput{
		TableName: aws.String(table),
	})
	if err != nil {
		return err
	}
	if d := desc.TimeToLiveDescription; d != nil {
		switch d.TimeToLiveStatus {
		case types.TimeToLiveStatusEnabled, types.TimeToLiveStatusEnabling:
			return nil
		}
	}

	_, err = client.UpdateTimeToLive(ctx, &dynamodb.UpdateTimeToLiveInput{
		TableName: aws.String(table),
		TimeToLiveSpecification: &types.TimeToLiveSpecification{
			Enabled:       aws.Bool(true),
			AttributeName: aws.String(attr),
		},
	})
	return err
}

// clearTable deletes every item from the table, for tests. keyAttrs are the
// table's key attribute names.
func clearTable(ctx context.Context, client *dynamodb.Client, table string, keyAttrs []string) error {
	var startKey map[string]types.AttributeValue
	for {
		out, err := client.Scan(ctx, &dynamodb.ScanInput{
			TableName:            aws.String(table),
			ProjectionExpression: aws.String(strings.Join(keyAttrs, ", ")),
			ExclusiveStartKey:    startKey,
		})
		if err != nil {
			return err
		}

		for start := 0; start < len(out.Items); start += maxBatchWriteItems {
			end := start + maxBatchWriteItems
			if end > len(out.Items) {
				end = len(out.Items)
			}
			requests := make([]types.WriteRequest, 0, end-start)
			for _, item := range out.Items[start:end] {
				key := make(map[string]types.AttributeValue, len(keyAttrs))
				for _, attr := range keyAttrs {
					key[attr] = item[attr]
				}
				requests = append(requests, types.WriteRequest{
					DeleteRequest: &types.DeleteRequest{Key: key},
				})
			}
			if _, err := client.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{
				RequestItems: map[string][]types.WriteRequest{table: requests},
			}); err != nil {
				return err
			}
		}

		if len(out.LastEvaluatedKey) == 0 {
			break
		}
		startKey = out.LastEvaluatedKey
	}
	return nil
}
