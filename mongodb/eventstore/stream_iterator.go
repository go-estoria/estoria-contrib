package eventstore

import (
	"context"
	"fmt"

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

	// primed reports that the cursor already sits on a document nobody has been handed yet.
	// ReadStream advances it once to tell an absent stream from an empty filtered read, so
	// the first call here must decode in place rather than skipping that document.
	primed bool
}

func (i *streamIterator) Next(ctx context.Context) (*eventstore.Event, error) {
	advanced := i.primed || i.cursor.Next(ctx)
	i.primed = false

	if advanced {
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

		hasEvent, err := cursor.advance(ctx, i.marshaler)
		if err != nil {
			return nil, err
		} else if !hasEvent {
			continue
		}

		if cursor.nextEvent.GlobalOffset == i.currentGlobalOffset+1 {
			nextEvent = &cursor.nextEvent.Event
			i.currentGlobalOffset++
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

// advance loads the cursor's next event unless it already holds one, closing the cursor
// once it is exhausted. It reports whether the cursor still has an event to offer.
func (c *multiStreamIteratorCursor) advance(ctx context.Context, marshaler DocumentMarshaler) (bool, error) {
	if c.nextEvent != nil {
		return true, nil
	}

	if c.cursor.Next(ctx) {
		evt, err := marshaler.UnmarshalDocument(c.cursor.Decode)
		if err != nil {
			return false, fmt.Errorf("parsing event document: %w", err)
		}

		c.nextEvent = evt
	}

	if err := c.cursor.Err(); err != nil {
		return false, fmt.Errorf("fetching document: %w", err)
	}

	if c.nextEvent == nil {
		if err := c.cursor.Close(ctx); err != nil {
			return false, fmt.Errorf("closing cursor: %w", err)
		}

		c.closed = true
		return false, nil
	}

	return true, nil
}
