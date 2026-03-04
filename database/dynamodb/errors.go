package dynamodb

import (
	"github.com/aws/aws-sdk-go-v2/aws/awserr"
	awsdynamodb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

// CheckConditionalCheckFailed maps a DynamoDB ConditionalCheckFailedException
// to the provided error. This is the DynamoDB equivalent of checking for a
// unique constraint violation.
func CheckConditionalCheckFailed(inErr, outErr error) error {
	if inErr != nil {
		if awsErr, ok := inErr.(awserr.Error); ok {
			if awsErr.Code() == awsdynamodb.ErrCodeConditionalCheckFailedException {
				return outErr
			}
		}
	}
	return inErr
}

// IsConditionalCheckFailed returns true if the error is a DynamoDB
// ConditionalCheckFailedException.
func IsConditionalCheckFailed(err error) bool {
	if err == nil {
		return false
	}

	if awsErr, ok := err.(awserr.Error); ok {
		if awsErr.Code() == awsdynamodb.ErrCodeConditionalCheckFailedException {
			return true
		}
	}

	return false
}
