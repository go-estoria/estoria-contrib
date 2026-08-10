package eventstore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"

	"github.com/go-estoria/estoria"
	"github.com/go-estoria/estoria/eventstore"
	"github.com/go-estoria/estoria/typeid"
	"github.com/gofrs/uuid/v5"
	"github.com/kurrent-io/KurrentDB-Client-Go/kurrentdb"
)

type streamIterator struct {
	streamID typeid.ID
	stream   *kurrentdb.ReadStream
	first    *eventstore.Event
}

func (i *streamIterator) Preload() error {
	event, err := i.scanEventRecord()
	if err != nil {
		return fmt.Errorf("scanning first event: %w", err)
	}

	i.first = event
	return nil
}

func (i *streamIterator) Next(ctx context.Context) (*eventstore.Event, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	if i.first != nil {
		event := i.first
		i.first = nil
		return event, nil
	}

	return i.scanEventRecord()
}

func (i *streamIterator) Close(_ context.Context) error {
	i.stream.Close()
	return nil
}

func (i *streamIterator) scanEventRecord() (*eventstore.Event, error) {
	resolvedEvent, err := i.stream.Recv()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil, eventstore.ErrEndOfEventStream
		} else if kdbErr, ok := kurrentdb.FromError(err); !ok && kdbErr != nil {
			switch kdbErr.Code() {
			case kurrentdb.ErrorCodeResourceNotFound:
				return nil, eventstore.ErrStreamNotFound
			case kurrentdb.ErrorCodeConnectionClosed:
				return nil, eventstore.ErrStreamIteratorClosed
			}
		}

		estoria.DefaultLogger().Error("unknown error receiving event", "error", err)
		return nil, fmt.Errorf("receiving event: %w", err)
	}

	return eventFromResolved(resolvedEvent, i.streamID)
}

// eventFromResolved maps a KurrentDB resolved event onto an estoria event for the given
// stream. The caller supplies the stream identity: per-stream reads address one known
// stream, and $all reads derive it from the record's stream name before calling here.
func eventFromResolved(resolved *kurrentdb.ResolvedEvent, streamID typeid.ID) (*eventstore.Event, error) {
	eventID, err := uuid.FromBytes(resolved.Event.EventID[:])
	if err != nil {
		return nil, fmt.Errorf("converting UUID: %w", err)
	}

	var globalPosition *int64
	if resolved.Commit != nil {
		if *resolved.Commit > math.MaxInt64 {
			return nil, fmt.Errorf("commit position %d overflows int64", *resolved.Commit)
		}

		position := int64(*resolved.Commit)
		globalPosition = &position
	}

	envelope := unmarshalEnvelope(resolved.Event.UserMetadata)

	return &eventstore.Event{
		ID:              typeid.New(resolved.Event.EventType, eventID),
		StreamID:        streamID,
		StreamVersion:   int64(resolved.Event.EventNumber + 1),
		GlobalPosition:  globalPosition,
		Timestamp:       resolved.Event.CreatedDate,
		Data:            resolved.Event.Data,
		DataContentType: envelope.DataContentType,
		Metadata:        envelope.Metadata,
	}, nil
}

// emptyStreamIterator is a StreamIterator that immediately returns ErrEndOfEventStream.
// ReadStream returns one when a read matched no events but the stream exists.
type emptyStreamIterator struct{}

func (emptyStreamIterator) Next(_ context.Context) (*eventstore.Event, error) {
	return nil, eventstore.ErrEndOfEventStream
}

func (emptyStreamIterator) Close(_ context.Context) error {
	return nil
}
