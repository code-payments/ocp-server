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

// maxBatchWriteItems is DynamoDB's per-call BatchWriteItem limit.
const maxBatchWriteItems = 25

// CreateTables provisions the history and live holder-count tables with
// on-demand billing. The history table is keyed by (pk, sk); the live table is
// keyed by pk only (one item per mint) so it can be scanned cheaply for all
// mints. It is idempotent and blocks until both tables are ACTIVE.
func CreateTables(ctx context.Context, client *dynamodb.Client, historyTable, liveTable string) error {
	inputs := []*dynamodb.CreateTableInput{
		{
			TableName:   aws.String(historyTable),
			BillingMode: types.BillingModePayPerRequest,
			AttributeDefinitions: []types.AttributeDefinition{
				{AttributeName: aws.String(attrPK), AttributeType: types.ScalarAttributeTypeS},
				{AttributeName: aws.String(attrSK), AttributeType: types.ScalarAttributeTypeN},
			},
			KeySchema: []types.KeySchemaElement{
				{AttributeName: aws.String(attrPK), KeyType: types.KeyTypeHash},
				{AttributeName: aws.String(attrSK), KeyType: types.KeyTypeRange},
			},
		},
		{
			TableName:   aws.String(liveTable),
			BillingMode: types.BillingModePayPerRequest,
			AttributeDefinitions: []types.AttributeDefinition{
				{AttributeName: aws.String(attrPK), AttributeType: types.ScalarAttributeTypeS},
			},
			KeySchema: []types.KeySchemaElement{
				{AttributeName: aws.String(attrPK), KeyType: types.KeyTypeHash},
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
	return nil
}

// clearTable deletes every item from the table, for tests. keyAttrs are the
// table's key attribute names (one for a hash-only table, two for composite).
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
				for _, a := range keyAttrs {
					key[a] = item[a]
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
