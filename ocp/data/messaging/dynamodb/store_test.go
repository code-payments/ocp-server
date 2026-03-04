package dynamodb

import (
	"context"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/ory/dockertest/v3"
	"go.uber.org/zap"

	"github.com/code-payments/ocp-server/ocp/data/messaging"
	"github.com/code-payments/ocp-server/ocp/data/messaging/tests"

	dynamodbtest "github.com/code-payments/ocp-server/database/dynamodb/test"
)

const (
	testTableName = "test-messaging"
)

var (
	testStore messaging.Store
	teardown  func()
)

func TestMain(m *testing.M) {
	log := zap.Must(zap.NewDevelopment())

	testPool, err := dockertest.NewPool("")
	if err != nil {
		log.With(zap.Error(err)).Error("Error creating docker pool")
		os.Exit(1)
	}

	client, cleanUpFunc, err := dynamodbtest.StartDynamoDB(testPool)
	if err != nil {
		log.With(zap.Error(err)).Error("Error starting dynamodb-local image")
		os.Exit(1)
	}

	if err := createTestTable(client); err != nil {
		log.With(zap.Error(err)).Error("Error creating test table")
		cleanUpFunc()
		os.Exit(1)
	}

	testStore = New(client, testTableName)
	teardown = func() {
		if pc := recover(); pc != nil {
			cleanUpFunc()
			panic(pc)
		}

		if err := resetTestTable(client); err != nil {
			log.With(zap.Error(err)).Error("Error resetting test table")
			cleanUpFunc()
			os.Exit(1)
		}
	}

	code := m.Run()
	cleanUpFunc()
	os.Exit(code)
}

func TestMessagingDynamoDBStore(t *testing.T) {
	tests.RunTests(t, testStore, teardown)
}

func createTestTable(client *dynamodb.Client) error {
	req := client.CreateTableRequest(&dynamodb.CreateTableInput{
		TableName: aws.String(testTableName),
		AttributeDefinitions: []dynamodb.AttributeDefinition{
			{
				AttributeName: aws.String("account"),
				AttributeType: dynamodb.ScalarAttributeTypeS,
			},
			{
				AttributeName: aws.String("message_id"),
				AttributeType: dynamodb.ScalarAttributeTypeS,
			},
		},
		KeySchema: []dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String("account"),
				KeyType:       dynamodb.KeyTypeHash,
			},
			{
				AttributeName: aws.String("message_id"),
				KeyType:       dynamodb.KeyTypeRange,
			},
		},
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(5),
			WriteCapacityUnits: aws.Int64(5),
		},
	})

	_, err := req.Send(context.Background())
	return err
}

func resetTestTable(client *dynamodb.Client) error {
	deleteReq := client.DeleteTableRequest(&dynamodb.DeleteTableInput{
		TableName: aws.String(testTableName),
	})
	_, err := deleteReq.Send(context.Background())
	if err != nil {
		return err
	}

	return createTestTable(client)
}
