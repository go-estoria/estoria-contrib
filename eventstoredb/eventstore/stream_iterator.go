package eventstore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"

	"github.com/EventStore/EventStore-Client-Go/v3/esdb"
	"github.com/go-estoria/estoria"
	"go.jetpack.io/typeid"
)

type StreamIterator struct {
	streamID typeid.AnyID
	client   *esdb.Client
	stream   *esdb.ReadStream
}

func (i *StreamIterator) Next(ctx context.Context) (estoria.EventStoreEvent, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	resolvedEvent, err := i.stream.Recv()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil, io.EOF
		}

		var esdbErr *esdb.Error
		if errors.As(err, &esdbErr) {
			slog.Error("ESDB error", "code", esdbErr.Code(), "message", esdbErr.Err())
		} else {
			slog.Error("unknown error receiving event", "error", err)
		}

		return nil, fmt.Errorf("receiving event: %w", err)
	}

	streamID, err := typeid.FromString(resolvedEvent.Event.StreamID)
	if err != nil {
		return nil, fmt.Errorf("parsing stream ID: %w", err)
	}

	eventID, err := typeid.FromUUID[typeid.AnyID](resolvedEvent.Event.EventType, resolvedEvent.Event.EventID.String())
	if err != nil {
		return nil, fmt.Errorf("parsing event ID: %w", err)
	}

	return &event{
		streamID:  streamID,
		id:        eventID,
		timestamp: resolvedEvent.Event.CreatedDate,
		data:      resolvedEvent.Event.Data,
	}, nil
}
