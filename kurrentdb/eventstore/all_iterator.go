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

	// cursor and cursorPrepare are where the next window resumes: the (commit, prepare)
	// position of the last raw record seen, or -1s to read from the start of $all. The
	// wire protocol's From position is inclusive, so the record at exactly this position
	// is skipped when a window reopens on it. Both halves are needed because $all orders
	// records by (commit, prepare): a legacy transaction's records share one commit
	// position, and commit alone cannot tell "already seen" from "same transaction,
	// not yet seen".
	cursor        int64
	cursorPrepare int64

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

			window, err := i.client.ReadAll(ctx, i.windowOptions(), uint64(i.windowSize))
			if err != nil {
				return nil, fmt.Errorf("reading all streams: %w", err)
			}

			i.window = window
			i.windowSeen = 0
		}

		resolved, err := i.window.Recv()
		if err != nil {
			terminal, recvErr := i.recvError(err)
			if recvErr != nil {
				return nil, recvErr
			} else if terminal {
				return nil, eventstore.ErrEndOfEventStream
			}

			continue
		}

		i.windowSeen++

		event, result, err := i.consume(resolved)
		if err != nil {
			return nil, err
		}

		switch result {
		case consumeEndOfRead:
			i.window.Close()
			i.window = nil
			i.done = true

			return nil, eventstore.ErrEndOfEventStream
		case consumeSkip:
			continue
		case consumeYield:
		}

		if i.remaining > 0 {
			i.remaining--
		}

		return event, nil
	}
}

// windowOptions returns the read options for the next window: from the start of $all,
// or resuming inclusively on the exact (commit, prepare) of the last record seen. The
// commit half alone would ask the server to skip past a legacy transaction group's
// unseen later members — server-side, where no client guard can fire.
func (i *allStreamIterator) windowOptions() kurrentdb.ReadAllOptions {
	opts := kurrentdb.ReadAllOptions{Direction: kurrentdb.Forwards, From: kurrentdb.Start{}}
	if i.cursor >= 0 {
		opts.From = kurrentdb.Position{Commit: uint64(i.cursor), Prepare: uint64(i.cursorPrepare)}
	}

	return opts
}

// recvError resolves a window Recv failure: io.EOF finishes the window; an unverified
// resume position the server rejected restarts the scan from the beginning with the
// bound still applied; everything else fails the read.
func (i *allStreamIterator) recvError(err error) (terminal bool, _ error) {
	if errors.Is(err, io.EOF) {
		return i.finishWindow()
	}

	if !i.verified && isInvalidPosition(err) {
		// The resume position falls between records, which only a synthetic position
		// can: positions yielded by reads are record boundaries. The bound still
		// applies; only the scan restarts from the beginning.
		i.window.Close()
		i.window = nil
		i.cursor = -1
		i.cursorPrepare = -1
		i.verified = true

		return false, nil
	}

	// A global read addresses no stream, so no error maps to ErrStreamNotFound.
	if kdbErr, ok := kurrentdb.FromError(err); !ok && kdbErr != nil && kdbErr.Code() == kurrentdb.ErrorCodeConnectionClosed {
		return false, eventstore.ErrStreamIteratorClosed
	}

	return false, fmt.Errorf("receiving event: %w", err)
}

// finishWindow closes a window that reported io.EOF. A window that came back short saw
// the end of $all, which is terminal — unless the log ended below the read's frontier,
// which a caught-up node cannot do, since the frontier is a record the server held when
// the read began: reporting end-of-stream would falsely certify the read complete, so a
// read against a lagging node fails closed instead. A full window may have more behind
// it, so the read continues from the cursor.
func (i *allStreamIterator) finishWindow() (terminal bool, _ error) {
	exhausted := i.windowSeen < i.windowSize
	i.window.Close()
	i.window = nil

	if !exhausted {
		return false, nil
	}

	if i.cursor < i.frontier {
		return false, fmt.Errorf("$all ended at position %d, below the read's frontier %d: the connected node is behind the frontier captured at ReadAll", i.cursor, i.frontier)
	}

	i.done = true

	return true, nil
}

// A consumeResult is what Next does with one raw $all record.
type consumeResult int

const (
	consumeSkip      consumeResult = iota // not eligible; read on
	consumeEndOfRead                      // past the frontier: the read is complete, terminally
	consumeYield                          // event is the read's next result
)

// consume applies one raw record to the cursor state and classifies it, holding every
// per-record rule: the frontier, (commit, prepare) cursor order, the resume bound,
// ownership, and the legacy-transaction guard.
func (i *allStreamIterator) consume(resolved *kurrentdb.ResolvedEvent) (*eventstore.Event, consumeResult, error) {
	if resolved.Commit == nil {
		return nil, 0, errors.New("received $all record with no commit position")
	} else if *resolved.Commit > math.MaxInt64 {
		return nil, 0, fmt.Errorf("commit position %d overflows int64", *resolved.Commit)
	}

	position := int64(*resolved.Commit)

	// The prepare position defaults to the commit position: only records carrying an
	// event can be legacy-transaction members, and modern records' positions are equal.
	prepare := position
	if resolved.Event != nil {
		if resolved.Event.Position.Prepare > math.MaxInt64 {
			return nil, 0, fmt.Errorf("prepare position %d overflows int64", resolved.Event.Position.Prepare)
		}

		prepare = int64(resolved.Event.Position.Prepare)
	}

	if position > i.frontier {
		// $all is scanned in ascending order, so everything at or below the frontier
		// has been seen.
		return nil, consumeEndOfRead, nil
	}

	if position < i.cursor || (position == i.cursor && prepare <= i.cursorPrepare) {
		return nil, consumeSkip, nil
	}

	// Every raw record advances the cursor, owned or not: this is what guarantees a
	// reopened window starts past everything already seen.
	i.cursor = position
	i.cursorPrepare = prepare
	i.verified = true

	if i.bound >= 0 && position <= i.bound {
		return nil, consumeSkip, nil
	}

	if resolved.Event == nil {
		return nil, consumeSkip, nil
	}

	streamID, ok := i.owns(resolved.Event.StreamID)
	if !ok {
		return nil, consumeSkip, nil
	}

	if prepare != position {
		// A legacy explicit-transaction record: its transaction's events share one
		// commit position across distinct prepare positions, which a scalar global
		// position cannot represent — yielding would give several events one position,
		// and resuming on it would skip all but the first. The read fails closed
		// rather than misrepresent the store.
		return nil, 0, fmt.Errorf("stream %s holds an event written by a legacy explicit transaction (commit %d, prepare %d): global reads cannot represent its position", resolved.Event.StreamID, position, prepare)
	}

	event, err := eventFromResolved(resolved, streamID)
	if err != nil {
		return nil, 0, fmt.Errorf("scanning event record: %w", err)
	}

	return event, consumeYield, nil
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
