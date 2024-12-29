package strategy

import (
	"bytes"
	"context"
	"fmt"
	"path"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/go-estoria/estoria"
	"github.com/go-estoria/estoria/eventstore"
	"github.com/go-estoria/estoria/typeid"
)

type BucketKeyResolver func(aggregateID typeid.UUID, version int64) string

type BucketKeyResolverFunc func(aggregateID typeid.UUID, version int64) string

func (f BucketKeyResolverFunc) ResolveKey(aggregateID typeid.UUID, version int64) string {
	return f(aggregateID, version)
}

func DefaultBuckeyKeyResolver(aggregateID typeid.UUID, version int64) string {
	return fmt.Sprintf("%s/%s/%d.json", aggregateID.TypeName(), aggregateID.Value(), version)
}

type S3 interface {
	ObjectGetter
	ListObjectsV2(ctx context.Context, input *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
	PutObject(ctx context.Context, input *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error)
}

type SingleBucketStrategy struct {
	s3         S3
	bucket     string
	resolveKey BucketKeyResolver
	marshaler  ObjectMarshaler
	log        estoria.Logger
}

func NewSingleBucketStrategy(client *s3.Client, bucket string, opts ...SingleBucketStrategyOption) (*SingleBucketStrategy, error) {
	if client == nil {
		return nil, fmt.Errorf("client is required")
	} else if bucket == "" {
		return nil, fmt.Errorf("bucket is required")
	}

	strategy := &SingleBucketStrategy{
		s3:         client,
		bucket:     bucket,
		resolveKey: DefaultBuckeyKeyResolver,
		marshaler:  JSONObjectMarshaler{},
		log:        estoria.GetLogger().WithGroup("s3eventstore"),
	}

	for _, opt := range opts {
		if err := opt(strategy); err != nil {
			return nil, fmt.Errorf("applying option: %w", err)
		}
	}

	return strategy, nil
}

func (s *SingleBucketStrategy) GetStreamIterator(
	ctx context.Context,
	streamID typeid.UUID,
	opts eventstore.ReadStreamOptions,
) (eventstore.StreamIterator, error) {
	dir, _ := path.Split(s.resolveKey(streamID, 0))
	paginator := s3.NewListObjectsV2Paginator(s.s3, &s3.ListObjectsV2Input{
		Bucket:    &s.bucket,
		Prefix:    &dir,
		Delimiter: aws.String("/"),
	})

	toVersion := int64(0)
	if opts.Count > 0 {
		toVersion = opts.Offset + opts.Count
	}

	return &streamIterator{
		streamID:       streamID,
		bucket:         s.bucket,
		paginator:      paginator,
		s3:             s.s3,
		fromVersion:    opts.Offset,
		currentVersion: opts.Offset,
		toVersion:      toVersion,
		marshaler:      s.marshaler,
		log:            s.log,
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

		data, err := s.marshaler.MarshalObject(fullEvents[i])
		if err != nil {
			return nil, fmt.Errorf("marshaling event: %w", err)
		}

		key := s.resolveKey(streamID, fullEvents[i].StreamVersion)
		if _, err := s.s3.PutObject(ctx, &s3.PutObjectInput{
			Bucket: &s.bucket,
			Key:    &key,
			Body:   bytes.NewReader(data),
		}); err != nil {
			return nil, fmt.Errorf("putting object: %w", err)
		}
	}

	return &InsertStreamEventsResult{
		InsertedEvents: fullEvents,
	}, nil
}

func (s *SingleBucketStrategy) getLatestVersion(ctx context.Context, streamID typeid.UUID) (int64, error) {
	dir, _ := path.Split(s.resolveKey(streamID, 0))
	results, err := s.s3.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket:    &s.bucket,
		Prefix:    &dir,
		Delimiter: aws.String("/"),
	})
	if err != nil {
		if _, ok := err.(*types.NotFound); ok {
			s.log.Debug("not found", "bucket", s.bucket)
			return 0, nil
		} else if _, ok := err.(*types.NoSuchKey); ok {
			s.log.Debug("key not found", "bucket", s.bucket)
			return 0, nil
		} else if _, ok := err.(*types.NoSuchBucket); ok {
			s.log.Error("bucket not found", "bucket", s.bucket)
			return 0, nil
		}

		return 0, fmt.Errorf("listing objects: %w", err)
	} else if len(results.Contents) == 0 {
		s.log.Debug("no objects found", "bucket", s.bucket, "dir", dir)
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

	s.log.Debug("fonud latest version for stream", "stream_id", streamID, "version", latestVersion)
	return latestVersion, nil
}

type SingleBucketStrategyOption func(*SingleBucketStrategy) error

func WithObjectMarshaler(marshaler ObjectMarshaler) SingleBucketStrategyOption {
	return func(s *SingleBucketStrategy) error {
		s.marshaler = marshaler
		return nil
	}
}

func WithBucketKeyResolver(resolver BucketKeyResolver) SingleBucketStrategyOption {
	return func(s *SingleBucketStrategy) error {
		s.resolveKey = resolver
		return nil
	}
}
