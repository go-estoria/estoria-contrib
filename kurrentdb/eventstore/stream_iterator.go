package eventstore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

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
		} else if kdbErr, ok := kurrentdb.FromError(err); !ok {
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

	parts := strings.Split(resolvedEvent.Event.StreamID, "_")
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid stream ID format: %s", resolvedEvent.Event.StreamID)
	}

	streamID := typeid.New(parts[0], uuid.Must(uuid.FromString(parts[1])))

	uidV5, err := uuid.FromBytes(resolvedEvent.Event.EventID[:])
	if err != nil {
		return nil, fmt.Errorf("converting UUID: %w", err)
	}

	return &eventstore.Event{
		ID:            typeid.New(resolvedEvent.Event.EventType, uidV5),
		StreamID:      streamID,
		StreamVersion: int64(resolvedEvent.Event.EventNumber + 1),
		Timestamp:     resolvedEvent.Event.CreatedDate,
		Data:          resolvedEvent.Event.Data,
	}, nil
}
