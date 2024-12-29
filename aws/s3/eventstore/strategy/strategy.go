package strategy

import (
	"context"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/go-estoria/estoria/eventstore"
)

type S3Client interface {
	GetObject(ctx context.Context, input *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	PutObject(ctx context.Context, input *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error)
}

type BucketCursor struct {
	s3             *s3.Client
	bucket         string
	prefix         string
	direction      int
	currentVersion int64
	maxVersion     int64
}

func (c *BucketCursor) HasNext(ctx context.Context) bool {
	return c.currentVersion < c.maxVersion
}

func (c *BucketCursor) Next(ctx context.Context) (io.ReadCloser, error) {
	key := fmt.Sprintf("%s/%d", c.prefix, c.currentVersion)
	result, err := c.s3.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &c.bucket,
		Key:    &key,
	})
	if err != nil {
		return nil, fmt.Errorf("getting object: %w", err)
	}

	c.currentVersion += int64(c.direction)
	return result.Body, nil
}

type InsertStreamEventsResult struct {
	InsertedEvents []*eventstore.Event
}
