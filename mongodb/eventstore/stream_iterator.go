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

// A multiStreamIterator merges per-collection cursors, each already ordered by global
// offset, into one ascending sequence by always yielding the smallest offset on offer.
// Offsets may carry gaps, so the merge never assumes adjacency.
type multiStreamIterator struct {
	cursors   []*multiStreamIteratorCursor
	marshaler DocumentMarshaler

	// limit caps the total yielded across all cursors (0 = unlimited); each cursor's
	// server-side limit alone would allow up to limit events per collection.
	limit   int64
	yielded int64
}

// Next returns the next event among all streams, ordered by global offset.
func (i *multiStreamIterator) Next(ctx context.Context) (*eventstore.Event, error) {
	if i.limit > 0 && i.yielded >= i.limit {
		return nil, eventstore.ErrEndOfEventStream
	}

	var next *multiStreamIteratorCursor
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

		if next == nil || cursor.nextEvent.GlobalOffset < next.nextEvent.GlobalOffset {
			next = cursor
		}
	}

	if next == nil {
		return nil, eventstore.ErrEndOfEventStream
	}

	event := &next.nextEvent.Event
	next.nextEvent = nil
	i.yielded++

	return event, nil
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
