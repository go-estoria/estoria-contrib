package eventstore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"

	"github.com/EventStore/EventStore-Client-Go/v3/esdb"
	"github.com/go-estoria/estoria"
)

type EventStream struct {
	id     estoria.Identifier
	client *esdb.Client
	stream *esdb.ReadStream
}

func (s *EventStream) Append(ctx context.Context, events ...estoria.Event) error {
	log := slog.Default().WithGroup("eventstream")
	log.Debug("appending events", "count", len(events))

	for _, evt := range events {
		data := esdb.EventData{
			ContentType: esdb.ContentTypeJson,
			EventType:   evt.ID().Type,
			Data:        evt.Data(),
		}

		if _, err := s.client.AppendToStream(
			ctx,
			s.id.String(),
			esdb.AppendToStreamOptions{},
			data,
		); err != nil {
			return fmt.Errorf("appending to stream: %w", err)
		}
	}

	return nil
}

func (s *EventStream) Close() error {
	s.stream.Close()
	return nil
}

func (s *EventStream) ID() estoria.Identifier {
	return s.id
}

func (s *EventStream) Next(_ context.Context) (estoria.Event, error) {

	resolvedEvent, err := s.stream.Recv()
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

	return &eventDocument{
		EventAggregateID: s.ID().String(),
		EventType:        resolvedEvent.Event.EventType,
		EventID:          resolvedEvent.Event.EventID.String(),
		EventTimestamp:   resolvedEvent.Event.CreatedDate,
		EventData:        resolvedEvent.Event.Data,
	}, nil
}
