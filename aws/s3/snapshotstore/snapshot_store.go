package snapshotstore

import (
	"bytes"
	"context"
	"fmt"
	"path"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/go-estoria/estoria"
	"github.com/go-estoria/estoria/snapshotstore"
	"github.com/go-estoria/estoria/typeid"
)

type ObjectGetter interface {
	GetObject(ctx context.Context, input *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
}

type S3 interface {
	ObjectGetter
	ListObjectsV2(ctx context.Context, input *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
	PutObject(ctx context.Context, input *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error)
}

type BucketResolver func(aggregateID typeid.UUID) string

func DefaultBucketResolver(typeid.UUID) string {
	return "snapshots"
}

type BucketKeyResolver func(aggregateID typeid.UUID, version int64) string

func DefaultBucketKeyResolver(aggregateID typeid.UUID, version int64) string {
	return fmt.Sprintf("%s/%s/%d.json", aggregateID.TypeName(), aggregateID.Value(), version)
}

type SnapshotStore struct {
	s3            S3
	resolveBucket BucketResolver
	resolveKey    BucketKeyResolver
	marshaler     ObjectMarshaler
	log           estoria.Logger
}

func New(s3Client S3) *SnapshotStore {
	return &SnapshotStore{
		s3:            s3Client,
		resolveBucket: DefaultBucketResolver,
		resolveKey:    DefaultBucketKeyResolver,
		marshaler:     JSONObjectMarshaler{},
		log:           estoria.GetLogger().WithGroup("s3snapshotstore"),
	}
}

func (s *SnapshotStore) ReadSnapshot(ctx context.Context, aggregateID typeid.UUID, opts snapshotstore.ReadSnapshotOptions) (*snapshotstore.AggregateSnapshot, error) {
	estoria.GetLogger().Debug("finding snapshot", "aggregate_id", aggregateID)

	bucket := s.resolveBucket(aggregateID)
	dir, _ := path.Split(s.resolveKey(aggregateID, 0))
	paginator := s3.NewListObjectsV2Paginator(s.s3, &s3.ListObjectsV2Input{
		Bucket:    &bucket,
		Prefix:    &dir,
		Delimiter: aws.String("/"),
	})

	maxVersionFound := int64(0)
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("getting next page: %w", err)
		}

		for _, o := range page.Contents {
			version, err := versionFromS3Object(o)
			if err != nil {
				return nil, fmt.Errorf("parsing version number: %w", err)
			}

			if version > maxVersionFound {
				maxVersionFound = version
			}

			if opts.MaxVersion > 0 && version == opts.MaxVersion {
				break
			}
		}
	}

	if maxVersionFound == 0 {
		return nil, snapshotstore.ErrSnapshotNotFound
	}

	key := s.resolveKey(aggregateID, maxVersionFound)
	obj, err := s.s3.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &bucket,
		Key:    &key,
	})
	if err != nil {
		return nil, fmt.Errorf("getting object: %w", err)
	}

	snapshot, err := s.marshaler.UnmarshalObject(obj.Body)
	if err != nil {
		return nil, fmt.Errorf("unmarshaling snapshot: %w", err)
	}

	estoria.GetLogger().Debug("found snapshot", "aggregate_id", aggregateID, "aggregate_version", snapshot.AggregateVersion)

	return snapshot, nil
}

func versionFromS3Object(obj types.Object) (int64, error) {
	_, file := path.Split(*obj.Key)
	versionStr, ok := strings.CutSuffix(file, ".json")
	if !ok {
		return 0, fmt.Errorf("parsing version number: %w", fmt.Errorf("missing .json suffix"))
	}

	version, err := strconv.Atoi(versionStr)
	if err != nil {
		return 0, fmt.Errorf("parsing version number: %w", err)
	}

	return int64(version), nil
}

func (s *SnapshotStore) WriteSnapshot(_ context.Context, snap *snapshotstore.AggregateSnapshot) error {
	estoria.GetLogger().Debug("writing snapshot",
		"aggregate_id", snap.AggregateID,
		"aggregate_version",
		snap.AggregateVersion,
		"data_length", len(snap.Data))

	data, err := s.marshaler.MarshalObject(snap)
	if err != nil {
		return fmt.Errorf("marshaling snapshot: %w", err)
	}

	bucket := s.resolveBucket(snap.AggregateID)
	key := s.resolveKey(snap.AggregateID, snap.AggregateVersion)
	if _, err := s.s3.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: &bucket,
		Key:    &key,
		Body:   bytes.NewReader(data),
	}); err != nil {
		return fmt.Errorf("putting object: %w", err)
	}

	estoria.GetLogger().Debug("wrote snapshot", "aggregate_id", snap.AggregateID, "aggregate_version", snap.AggregateVersion)

	return nil
}
