package strategy

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/go-estoria/estoria/eventstore"
)

type ObjectGetter interface {
	GetObject(ctx context.Context, input *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
}

type InsertStreamEventsResult struct {
	InsertedEvents []*eventstore.Event
}
