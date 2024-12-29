package snapshotstore

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func NewDefaultS3Client(ctx context.Context, awsConfig aws.Config) (*s3.Client, error) {
	client := s3.NewFromConfig(awsConfig,
		func(o *s3.Options) {
			o.BaseEndpoint = aws.String("http://localhost:9000")
			o.UsePathStyle = true
		},
	)

	return client, nil
}
