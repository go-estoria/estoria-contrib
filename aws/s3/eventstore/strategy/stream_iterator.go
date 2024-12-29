package strategy

import (
	"context"
	"fmt"
	"log/slog"
	"path"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/go-estoria/estoria/eventstore"
	"github.com/go-estoria/estoria/typeid"
)

type streamIterator struct {
	streamID       typeid.UUID
	bucket         string
	paginator      *s3.ListObjectsV2Paginator
	s3             ObjectGetter
	objectMetadata map[int64]types.Object
	fromVersion    int64
	currentVersion int64
	toVersion      int64
	marshaler      ObjectMarshaler
}

func (i *streamIterator) Next(ctx context.Context) (*eventstore.Event, error) {
	if len(i.objectMetadata) == 0 {
		i.objectMetadata = make(map[int64]types.Object)
	}

	if i.toVersion > 0 && i.currentVersion == i.toVersion {
		slog.Debug("reached end of stream", "streamID", i.streamID, "currentVersion", i.currentVersion)
		return nil, eventstore.ErrEndOfEventStream
	}

	var versionObject types.Object
	var ok bool

	for versionObject, ok = i.objectMetadata[i.currentVersion]; !ok && i.paginator.HasMorePages(); {
		slog.Debug("object not found for version, getting next page of objects", "streamID", i.streamID, "currentVersion", i.currentVersion)
		page, err := i.paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("getting next page: %w", err)
		}

		for _, o := range page.Contents {
			slog.Debug("found object in fetched page", "streamID", i.streamID, "key", *o.Key)
			_, file := path.Split(*o.Key)
			versionStr, _ := strings.CutSuffix(file, ".json")
			version, err := strconv.Atoi(versionStr)
			if err != nil {
				return nil, fmt.Errorf("parsing version number: %w", err)
			}

			i.objectMetadata[int64(version)] = o

			if int64(version) == i.currentVersion {
				slog.Debug("found object for version in fetched page", "streamID", i.streamID, "version", i.currentVersion, "key", *o.Key)
				versionObject = o
				ok = true
			}
		}
	}

	if !ok {
		slog.Debug("no object found for version", "streamID", i.streamID, "currentVersion", i.currentVersion)
		return nil, eventstore.ErrEndOfEventStream
	}

	slog.Debug("found object for version", "streamID", i.streamID, "version", i.currentVersion, "key", *versionObject.Key)
	result, err := i.s3.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &i.bucket,
		Key:    versionObject.Key,
	})
	if err != nil {
		return nil, fmt.Errorf("getting object: %w", err)
	}

	defer result.Body.Close()

	evt, err := i.marshaler.UnmarshalObject(result.Body)
	if err != nil {
		return nil, fmt.Errorf("parsing event document: %w", err)
	}

	i.currentVersion++

	return evt, nil
}

func (i *streamIterator) Close(ctx context.Context) error {
	return nil
}
