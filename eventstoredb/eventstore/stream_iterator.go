package eventstore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"

	"github.com/EventStore/EventStore-Client-Go/v3/esdb"
	"github.com/go-estoria/estoria/eventstore"
	"github.com/go-estoria/estoria/typeid"
	uuidv5 "github.com/gofrs/uuid/v5"
)

type streamIterator struct {
	streamID typeid.UUID
	stream   *esdb.ReadStream
}

func (i *streamIterator) Next(ctx context.Context) (*eventstore.Event, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	resolvedEvent, err := i.stream.Recv()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil, eventstore.ErrEndOfEventStream
		}

		var esdbErr *esdb.Error
		if errors.As(err, &esdbErr) {
			slog.Error("ESDB error", "code", esdbErr.Code(), "message", esdbErr.Err())
			switch esdbErr.Code() {
			case esdb.ErrorCodeConnectionClosed:
				return nil, eventstore.ErrStreamIteratorClosed
			}
		} else {
			slog.Error("unknown error receiving event", "error", err)
		}

		return nil, fmt.Errorf("receiving event: %w", err)
	}

	streamID, err := typeid.ParseUUID(resolvedEvent.Event.StreamID)
	if err != nil {
		return nil, fmt.Errorf("parsing stream ID: %w", err)
	}

	uidV5, err := uuidv5.FromBytes(resolvedEvent.Event.EventID.Bytes())
	if err != nil {
		return nil, fmt.Errorf("converting UUID: %w", err)
	}

	return &eventstore.Event{
		StreamID:  streamID,
		ID:        typeid.FromUUID(resolvedEvent.Event.EventType, uidV5),
		Timestamp: resolvedEvent.Event.CreatedDate,
		Data:      resolvedEvent.Event.Data,
	}, nil
}

func (i *streamIterator) Close(_ context.Context) error {
	i.stream.Close()
	return nil
}
