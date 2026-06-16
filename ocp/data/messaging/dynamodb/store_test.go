package dynamodb

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	dynamotest "github.com/code-payments/ocp-server/database/dynamodb/test"
	"github.com/code-payments/ocp-server/ocp/data/messaging/tests"
)

const messagingTable = "messaging_test"

var testEnv *dynamotest.TestEnv

func TestMain(m *testing.M) {
	log := zap.Must(zap.NewDevelopment())

	env, err := dynamotest.NewTestEnv()
	if err != nil {
		log.With(zap.Error(err)).Error("Error creating dynamodb test environment")
		os.Exit(1)
	}

	testEnv = env

	os.Exit(m.Run())
}

func TestMessaging_DynamoDBStore(t *testing.T) {
	require.NoError(t, CreateTables(context.Background(), testEnv.Client, messagingTable))

	testStore := New(testEnv.Client, messagingTable).(*store)
	teardown := func() {
		testStore.reset()
	}
	tests.RunTests(t, testStore, teardown)
}
