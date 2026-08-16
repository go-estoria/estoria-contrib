package eventstore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"strings"

	"github.com/go-estoria/estoria/eventstore"
	"github.com/go-estoria/estoria/typeid"
	"github.com/kurrent-io/KurrentDB-Client-Go/kurrentdb"
)

// An allStreamIterator yields a store's events from the server's $all stream in ascending
// commit-position order, up to a frontier fixed when the read was created. KurrentDB
// cannot filter reads server-side, so the iterator fetches raw records in windows and
// filters them through the store's ownership predicate; every raw record advances the
// cursor, so every window makes progress and the scan terminates.
type allStreamIterator struct {
	client KurrentClient
	owns   func(streamName string) (typeid.ID, bool)

	windowSize int64

	// bound is the exclusive lower bound on yielded positions, or -1 for none.
	bound int64

	// frontier is the inclusive upper bound on yielded positions: the server-wide $all
	// head commit position captured before ReadAll returned. Records commit only above
	// every position already readable, so the first raw record above the frontier proves
	// the read complete — the iterator exhausts there and stays exhausted, and commits
	// racing the drain can never extend it.
	frontier int64

	// cursor is where the next window resumes: the last raw record position seen, or -1
	// to read from the start of $all. The wire protocol's From position is inclusive, so
	// a record at the cursor is skipped when a window reopens on it.
	cursor int64

	// verified reports that the cursor lies on a known record boundary. A cursor seeded
	// from a caller's resume position is unverified: positions between records are
	// rejected by the server, and the recovery is a scan from the start of $all.
	verified bool

	// remaining is how many events are left to yield, or -1 for an unbounded read.
	remaining int64

	window     *kurrentdb.ReadStream
	windowSeen int64
	done       bool
	closed     bool
}

func (i *allStreamIterator) Next(ctx context.Context) (*eventstore.Event, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	if i.closed {
		return nil, eventstore.ErrStreamIteratorClosed
	}

	for {
		if i.remaining == 0 {
			return nil, eventstore.ErrEndOfEventStream
		}

		if i.window == nil {
			if i.done {
				return nil, eventstore.ErrEndOfEventStream
			}

			opts := kurrentdb.ReadAllOptions{Direction: kurrentdb.Forwards, From: kurrentdb.Start{}}
			if i.cursor >= 0 {
				opts.From = kurrentdb.Position{Commit: uint64(i.cursor), Prepare: uint64(i.cursor)}
			}

			window, err := i.client.ReadAll(ctx, opts, uint64(i.windowSize))
			if err != nil {
				return nil, fmt.Errorf("reading all streams: %w", err)
			}

			i.window = window
			i.windowSeen = 0
		}

		resolved, err := i.window.Recv()
		if errors.Is(err, io.EOF) {
			// A window that came back short saw the end of $all; a full one may have
			// more behind it, so reopen from the cursor.
			exhausted := i.windowSeen < i.windowSize
			i.window.Close()
			i.window = nil
			if exhausted {
				i.done = true
				return nil, eventstore.ErrEndOfEventStream
			}

			continue
		} else if err != nil {
			if !i.verified && isInvalidPosition(err) {
				// The resume position falls between records, which only a synthetic
				// position can: positions yielded by reads are record boundaries. The
				// bound still applies; only the scan restarts from the beginning.
				i.window.Close()
				i.window = nil
				i.cursor = -1
				i.verified = true
				continue
			}

			// A global read addresses no stream, so no error maps to ErrStreamNotFound.
			if kdbErr, ok := kurrentdb.FromError(err); !ok && kdbErr != nil && kdbErr.Code() == kurrentdb.ErrorCodeConnectionClosed {
				return nil, eventstore.ErrStreamIteratorClosed
			}

			return nil, fmt.Errorf("receiving event: %w", err)
		}

		i.windowSeen++

		if resolved.Commit == nil {
			return nil, errors.New("received $all record with no commit position")
		} else if *resolved.Commit > math.MaxInt64 {
			return nil, fmt.Errorf("commit position %d overflows int64", *resolved.Commit)
		}

		position := int64(*resolved.Commit)
		if position > i.frontier {
			// $all is scanned in ascending order, so everything at or below the
			// frontier has been seen: the read is complete, terminally.
			i.window.Close()
			i.window = nil
			i.done = true

			return nil, eventstore.ErrEndOfEventStream
		}

		if position <= i.cursor {
			continue
		}

		// Every raw record advances the cursor, owned or not: this is what guarantees a
		// reopened window starts past everything already seen.
		i.cursor = position
		i.verified = true

		if i.bound >= 0 && position <= i.bound {
			continue
		}

		if resolved.Event == nil {
			continue
		}

		streamID, ok := i.owns(resolved.Event.StreamID)
		if !ok {
			continue
		}

		event, err := eventFromResolved(resolved, streamID)
		if err != nil {
			return nil, fmt.Errorf("scanning event record: %w", err)
		}

		if i.remaining > 0 {
			i.remaining--
		}

		return event, nil
	}
}

func (i *allStreamIterator) Close(_ context.Context) error {
	i.closed = true
	if i.window != nil {
		i.window.Close()
		i.window = nil
	}

	return nil
}

// isInvalidPosition reports whether err is the server rejecting a $all read whose From
// position does not lie on a record boundary. The client surfaces it only as an unknown
// error wrapping the server's message, so the match is on the ReadAllResult name the
// message carries.
func isInvalidPosition(err error) bool {
	return err != nil && strings.Contains(err.Error(), "InvalidPosition")
}
