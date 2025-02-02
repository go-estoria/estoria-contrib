package strategy

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/go-estoria/estoria/eventstore"
)

type (
	// MongoCursor provides an API for iterating over a set of documents returned by a query.
	MongoCursor interface {
		Next(ctx context.Context) bool
		Decode(v any) error
		Err() error
		Close(ctx context.Context) error
	}
)

type streamIterator struct {
	cursor    MongoCursor
	marshaler DocumentMarshaler
}

func (i *streamIterator) Next(ctx context.Context) (*eventstore.Event, error) {
	if i.cursor.Next(ctx) {
		evt, err := i.marshaler.UnmarshalDocument(i.cursor.Decode)
		if err != nil {
			return nil, fmt.Errorf("parsing event document: %w", err)
		}

		return &evt.Event, nil
	}

	if err := i.cursor.Err(); err != nil {
		return nil, fmt.Errorf("fetching document: %w", err)
	}

	return nil, eventstore.ErrEndOfEventStream
}

func (i *streamIterator) Close(ctx context.Context) error {
	return i.cursor.Close(ctx)
}

type multiStreamIterator struct {
	cursors             []*multiStreamIteratorCursor
	currentGlobalOffset int64
	marshaler           DocumentMarshaler
}

// Next returns the next event among all streams, ordered by global offset.
func (i *multiStreamIterator) Next(ctx context.Context) (*eventstore.Event, error) {
	var nextEvent *eventstore.Event
	for _, cursor := range i.cursors {
		if cursor.closed {
			continue
		}

		if cursor.nextEvent == nil {
			if cursor.cursor.Next(ctx) {
				evt, err := i.marshaler.UnmarshalDocument(cursor.cursor.Decode)
				if err != nil {
					return nil, fmt.Errorf("parsing event document: %w", err)
				}

				cursor.nextEvent = evt
			}

			if err := cursor.cursor.Err(); err != nil {
				return nil, fmt.Errorf("fetching document: %w", err)
			} else if cursor.nextEvent == nil {
				if err := cursor.cursor.Close(ctx); err != nil {
					return nil, fmt.Errorf("closing cursor: %w", err)
				}

				cursor.closed = true
				continue
			}
		}

		if cursor.nextEvent != nil && cursor.nextEvent.GlobalOffset == i.currentGlobalOffset+1 {
			nextEvent = &cursor.nextEvent.Event
			i.currentGlobalOffset++
			slog.Debug("multi-iterator returning event", "stream_id", cursor.nextEvent.StreamID, "global_offset", cursor.nextEvent.GlobalOffset)
			cursor.nextEvent = nil
			break
		}
	}

	if nextEvent == nil {
		return nil, eventstore.ErrEndOfEventStream
	}

	return nextEvent, nil
}

// Close closes all cursors.
func (i *multiStreamIterator) Close(ctx context.Context) error {
	for _, cursor := range i.cursors {
		if err := cursor.cursor.Close(ctx); err != nil {
			return err
		}
	}

	return nil
}

type multiStreamIteratorCursor struct {
	cursor    MongoCursor
	nextEvent *Event
	closed    bool
}
