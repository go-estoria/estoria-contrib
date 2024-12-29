package strategy

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/go-estoria/estoria/eventstore"
	"github.com/go-estoria/estoria/typeid"
)

type BucketPathResolver func(aggregateID typeid.UUID, events []*eventstore.WritableEvent) string

type SingleBucketStrategy struct {
	s3        *s3.Client
	bucket    string
	marshaler ObjectMarshaler
	log       *slog.Logger
}

func NewSingleBucketStrategy(client *s3.Client, bucket string, opts ...SingleBucketStrategyOption) (*SingleBucketStrategy, error) {
	if client == nil {
		return nil, fmt.Errorf("client is required")
	} else if bucket == "" {
		return nil, fmt.Errorf("bucket is required")
	}

	strategy := &SingleBucketStrategy{
		s3:     client,
		bucket: bucket,
		log:    slog.Default().WithGroup("eventstore"),
	}

	for _, opt := range opts {
		if err := opt(strategy); err != nil {
			return nil, fmt.Errorf("applying option: %w", err)
		}
	}

	if strategy.marshaler == nil {
		if err := WithObjectMarshaler(JSONObjectMarshaler{})(strategy); err != nil {
			return nil, fmt.Errorf("setting default document marshaler: %w", err)
		}
	}

	return strategy, nil
}

func (s *SingleBucketStrategy) GetStreamIterator(
	ctx context.Context,
	streamID typeid.UUID,
	opts eventstore.ReadStreamOptions,
) (eventstore.StreamIterator, error) {
	offset := opts.Offset
	count := opts.Count
	direction := 1
	if opts.Direction == eventstore.Reverse {
		direction = -1
	}

	cursor := BucketCursor{
		direction:      direction,
		currentVersion: offset,
		maxVersion:     offset + count,
	}

	return &streamIterator{
		streamID:  streamID,
		cursor:    cursor,
		marshaler: s.marshaler,
	}, nil
}

func (s *SingleBucketStrategy) InsertStreamEvents(
	ctx context.Context,
	streamID typeid.UUID,
	events []*eventstore.WritableEvent,
	opts eventstore.AppendStreamOptions,
) (*InsertStreamEventsResult, error) {
	latestVersion, err := s.getLatestVersion(ctx, streamID)
	if err != nil {
		return nil, fmt.Errorf("getting latest version: %w", err)
	}

	if opts.ExpectVersion > 0 && latestVersion != opts.ExpectVersion {
		return nil, fmt.Errorf("expected version %d, but stream has version %d", opts.ExpectVersion, latestVersion)
	}

	now := time.Now()

	fullEvents := make([]*eventstore.Event, len(events))
	for i, we := range events {
		if we.Timestamp.IsZero() {
			we.Timestamp = now
		}

		fullEvents[i] = &eventstore.Event{
			ID:            we.ID,
			StreamID:      streamID,
			StreamVersion: latestVersion + int64(i) + 1,
			Timestamp:     now,
			Data:          we.Data,
		}

		reader, err := s.marshaler.MarshalObject(fullEvents[i])
		if err != nil {
			return nil, fmt.Errorf("marshaling event: %w", err)
		}

		defer reader.Close()

		if _, err := s.s3.PutObject(ctx, &s3.PutObjectInput{
			Bucket: &s.bucket,
			Key:    aws.String(fmt.Sprintf("%s/%s", streamID.TypeName(), streamID.Value())),
			Body:   reader,
		}); err != nil {
			return nil, fmt.Errorf("putting object: %w", err)
		}
	}

	return &InsertStreamEventsResult{
		InsertedEvents: fullEvents,
	}, nil
}

func (s *SingleBucketStrategy) getLatestVersion(ctx context.Context, streamID typeid.UUID) (int64, error) {
	// enumerate all objects in the bucket directory for the stream
	// and return the highest version number
	results, err := s.s3.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: &s.bucket,
		Prefix: aws.String(fmt.Sprintf("%s/%s", streamID.TypeName(), streamID.Value())),
	})
	if err != nil {
		return 0, fmt.Errorf("listing objects: %w", err)
	} else if len(results.Contents) == 0 {
		return 0, nil
	}

	var latestVersion int64
	for _, obj := range results.Contents {
		version, err := strconv.Atoi(*obj.Key)
		if err != nil {
			return 0, fmt.Errorf("parsing version number: %w", err)
		}

		if int64(version) > latestVersion {
			latestVersion = int64(version)
		}
	}

	return latestVersion, nil
}

type DefaultObjectMarshaler struct{}

type SingleBucketStrategyOption func(*SingleBucketStrategy) error

func WithObjectMarshaler(marshaler ObjectMarshaler) SingleBucketStrategyOption {
	return func(s *SingleBucketStrategy) error {
		s.marshaler = marshaler
		return nil
	}
}
