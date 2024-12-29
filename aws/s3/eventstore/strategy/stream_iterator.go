package strategy

import (
	"context"
	"fmt"
	"path"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/go-estoria/estoria"
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
	log            estoria.Logger
}

func (i *streamIterator) Next(ctx context.Context) (*eventstore.Event, error) {
	if len(i.objectMetadata) == 0 {
		i.objectMetadata = make(map[int64]types.Object)
	}

	i.currentVersion++

	if i.toVersion > 0 && i.currentVersion == i.toVersion {
		i.log.Debug("reached end of stream", "stream_id", i.streamID, "current_version", i.currentVersion, "to_version", i.toVersion)
		return nil, eventstore.ErrEndOfEventStream
	}

	var versionObject types.Object
	var ok bool

	for versionObject, ok = i.objectMetadata[i.currentVersion]; !ok && i.paginator.HasMorePages(); {
		i.log.Debug("object not found for version, getting next page of objects", "stream_id", i.streamID, "current_version", i.currentVersion)
		page, err := i.paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("getting next page: %w", err)
		}

		for _, o := range page.Contents {
			_, file := path.Split(*o.Key)
			versionStr, _ := strings.CutSuffix(file, ".json")
			version, err := strconv.Atoi(versionStr)
			if err != nil {
				return nil, fmt.Errorf("parsing version number: %w", err)
			}

			i.objectMetadata[int64(version)] = o

			if int64(version) == i.currentVersion {
				i.log.Debug("found object for version in fetched page", "stream_id", i.streamID, "version", i.currentVersion)
				versionObject = o
				ok = true
			}
		}
	}

	if !ok {
		i.log.Debug("no object found for version", "stream_id", i.streamID, "current_version", i.currentVersion)
		return nil, eventstore.ErrEndOfEventStream
	}

	i.log.Debug("found object for version", "stream_id", i.streamID, "version", i.currentVersion)
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

	return evt, nil
}

func (i *streamIterator) Close(ctx context.Context) error {
	return nil
}
