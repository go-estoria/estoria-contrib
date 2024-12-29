package eventstore

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/go-estoria/estoria-contrib/aws/s3/eventstore/strategy"
	"github.com/go-estoria/estoria/eventstore"
	"github.com/go-estoria/estoria/typeid"
	"go.mongodb.org/mongo-driver/mongo"
)

type EventStore struct {
	s3Client *s3.Client
	strategy Strategy
	log      *slog.Logger
}

var _ eventstore.StreamReader = (*EventStore)(nil)
var _ eventstore.StreamWriter = (*EventStore)(nil)

type TransactionHook interface {
	HandleEvents(sessCtx mongo.SessionContext, events []*eventstore.Event) error
}

type Strategy interface {
	GetStreamIterator(
		ctx context.Context,
		streamID typeid.UUID,
		opts eventstore.ReadStreamOptions,
	) (eventstore.StreamIterator, error)
	InsertStreamEvents(
		ctx context.Context,
		streamID typeid.UUID,
		events []*eventstore.WritableEvent,
		opts eventstore.AppendStreamOptions,
	) (*strategy.InsertStreamEventsResult, error)
}

// New creates a new event store using the given S3 client.
func New(s3Client *s3.Client, opts ...EventStoreOption) (*EventStore, error) {
	eventStore := &EventStore{
		s3Client: s3Client,
		log:      slog.Default().WithGroup("s3eventstore"),
	}

	for _, opt := range opts {
		if err := opt(eventStore); err != nil {
			return nil, fmt.Errorf("applying option: %w", err)
		}
	}

	if eventStore.strategy == nil {
		bucket := "events"
		strat, err := strategy.NewSingleBucketStrategy(s3Client, bucket)
		if err != nil {
			return nil, fmt.Errorf("creating default strategy: %w", err)
		}

		eventStore.strategy = strat
	}

	return eventStore, nil
}

// ReadStream returns an iterator for reading events from the specified stream.
func (s *EventStore) ReadStream(ctx context.Context, streamID typeid.UUID, opts eventstore.ReadStreamOptions) (eventstore.StreamIterator, error) {
	s.log.Debug("reading events from S3 stream", "stream_id", streamID.String())

	iter, err := s.strategy.GetStreamIterator(ctx, streamID, opts)
	if err != nil {
		return nil, fmt.Errorf("getting stream cursor: %w", err)
	}

	return iter, nil
}

// AppendStream appends events to the specified stream.
func (s *EventStore) AppendStream(ctx context.Context, streamID typeid.UUID, events []*eventstore.WritableEvent, opts eventstore.AppendStreamOptions) error {
	s.log.Debug("appending events to S3 stream", "stream_id", streamID.String(), "events", len(events))

	if _, err := s.strategy.InsertStreamEvents(ctx, streamID, events, opts); err != nil {
		slog.Debug("inserting events failed", "err", err)
		return fmt.Errorf("inserting events: %w", err)
	}

	return nil
}
