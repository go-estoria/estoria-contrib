package eventstore

import (
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func NewDefaultS3Client(awsConfig aws.Config) (*s3.Client, error) {
	// Create an S3 client with the default configuration.
	return s3.NewFromConfig(awsConfig), nil
}
