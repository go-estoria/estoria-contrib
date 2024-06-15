package eventstore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"

	"github.com/EventStore/EventStore-Client-Go/v3/esdb"
	"github.com/go-estoria/estoria"
	"github.com/go-estoria/estoria/typeid"
	uuidv5 "github.com/gofrs/uuid/v5"
)

type StreamIterator struct {
	streamID typeid.TypeID
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

	streamID, err := typeid.ParseString(resolvedEvent.Event.StreamID)
	if err != nil {
		return nil, fmt.Errorf("parsing stream ID: %w", err)
	}

	uidV5, err := uuidv5.FromBytes(resolvedEvent.Event.EventID.Bytes())
	if err != nil {
		return nil, fmt.Errorf("converting UUID: %w", err)
	}

	return &event{
		streamID:  streamID,
		id:        typeid.FromUUID(resolvedEvent.Event.EventType, uidV5),
		timestamp: resolvedEvent.Event.CreatedDate,
		data:      resolvedEvent.Event.Data,
	}, nil
}
