package test

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/defaults"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/ory/dockertest/v3"
	"github.com/pkg/errors"

	"github.com/code-payments/ocp-server/retry"
	"github.com/code-payments/ocp-server/retry/backoff"
)

const (
	containerRepository = "amazon/dynamodb-local"
	containerTag        = "latest"
	containerAutoKill   = 120 // seconds

	port = 8000
)

// StartDynamoDB starts a Docker container using the amazon/dynamodb-local image and returns a DynamoDB client for testing purposes.
func StartDynamoDB(pool *dockertest.Pool) (client *dynamodb.Client, closeFunc func(), err error) {
	closeFunc = func() {}

	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: containerRepository,
		Tag:        containerTag,
		Cmd:        []string{"-jar", "DynamoDBLocal.jar", "-inMemory"},
	})
	if err != nil {
		return nil, closeFunc, errors.Wrap(err, "failed to start resource")
	}

	resource.Expire(containerAutoKill)

	endpoint := fmt.Sprintf("http://localhost:%s", resource.GetPort(fmt.Sprintf("%d/tcp", port)))

	cfg := defaults.Config()
	cfg.Region = "us-east-1"
	cfg.EndpointResolver = aws.ResolveWithEndpointURL(endpoint)
	cfg.Credentials = aws.NewStaticCredentialsProvider("dummy", "dummy", "")

	client = dynamodb.New(cfg)

	// Wait for the container to be ready by issuing a ListTables request.
	_, err = retry.Retry(
		func() error {
			_, listErr := client.ListTablesRequest(&dynamodb.ListTablesInput{}).Send(context.Background())
			return listErr
		},
		retry.Limit(50),
		retry.Backoff(backoff.Constant(500*time.Millisecond), 500*time.Second),
	)
	if err != nil {
		resource.Close()
		return nil, closeFunc, errors.Wrap(err, "timed out waiting for dynamodb container to become available")
	}

	return client, func() { resource.Close() }, nil
}
