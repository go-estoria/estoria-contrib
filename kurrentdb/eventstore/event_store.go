package eventstore

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/go-estoria/estoria"
	"github.com/go-estoria/estoria/eventstore"
	"github.com/go-estoria/estoria/typeid"
	"github.com/gofrs/uuid/v5"
	guuid "github.com/google/uuid"
	"github.com/kurrent-io/KurrentDB-Client-Go/kurrentdb"
)

type KurrentClient interface {
	ReadStream(context context.Context, streamID string, opts kurrentdb.ReadStreamOptions, count uint64) (*kurrentdb.ReadStream, error)
	AppendToStream(context context.Context, streamID string, opts kurrentdb.AppendToStreamOptions, events ...kurrentdb.EventData) (*kurrentdb.WriteResult, error)
}

type EventStore struct {
	kurrentDB KurrentClient
	log       estoria.Logger
}

var _ eventstore.StreamReader = (*EventStore)(nil)
var _ eventstore.StreamWriter = (*EventStore)(nil)

// New creates a new event store using the given KurrentDB client.
func New(kurrentDB KurrentClient, opts ...EventStoreOption) (*EventStore, error) {
	eventStore := &EventStore{
		kurrentDB: kurrentDB,
		log:       estoria.GetLogger().WithGroup("eventstore"),
	}

	for _, opt := range opts {
		if err := opt(eventStore); err != nil {
			return nil, fmt.Errorf("applying option: %w", err)
		}
	}

	return eventStore, nil
}

func (s *EventStore) ReadStream(ctx context.Context, streamID typeid.ID, opts eventstore.ReadStreamOptions) (eventstore.StreamIterator, error) {
	readOpts := kurrentdb.ReadStreamOptions{
		Direction: kurrentdb.Forwards,
		From:      kurrentdb.Start{},
	}

	if opts.Direction == eventstore.Reverse {
		readOpts.Direction = kurrentdb.Backwards
		readOpts.From = kurrentdb.End{}
	}

	if opts.Offset > 0 {
		readOpts.From = kurrentdb.StreamRevision{Value: uint64(opts.Offset)}
	}

	count := uint64(opts.Count)
	if count == 0 {
		// HACK: large value to read all events
		count = 1_000_000
	}

	result, err := s.kurrentDB.ReadStream(ctx, streamID.String(), readOpts, count)
	if err != nil {
		slog.Error("READ STREAM ERROR", "error", err.Error())
		if _, ok := kurrentdb.FromError(err); ok {
			return nil, eventstore.ErrStreamNotFound
		}

		return nil, fmt.Errorf("reading stream: %w", err)
	}

	slog.Info("READ STREAM", "stream", streamID.String())

	iter := &streamIterator{
		streamID: streamID,
		stream:   result,
	}

	if err := iter.Preload(); errors.Is(err, eventstore.ErrStreamNotFound) {
		return nil, eventstore.ErrStreamNotFound
	} else if err != nil {
		return nil, fmt.Errorf("preloading first event: %w", err)
	}

	return iter, nil
}

// AppendStream saves the given events to the event store.
func (s *EventStore) AppendStream(ctx context.Context, streamID typeid.ID, events []*eventstore.WritableEvent, opts eventstore.AppendStreamOptions) error {
	s.log.Debug("appending events to stream", "stream_id", streamID.String(), "events", len(events))

	appendOpts := kurrentdb.AppendToStreamOptions{}

	if opts.ExpectVersion > 0 {
		appendOpts.StreamState = kurrentdb.StreamRevision{Value: uint64(opts.ExpectVersion - 1)}
	}

	streamEvents := make([]kurrentdb.EventData, len(events))
	for i, e := range events {
		eventID, err := uuid.NewV4()
		if err != nil {
			return fmt.Errorf("generating event ID: %w", err)
		}

		streamEvents[i] = kurrentdb.EventData{
			EventID:     guuid.UUID(eventID),
			ContentType: kurrentdb.ContentTypeJson,
			EventType:   e.Type,
			Data:        e.Data,
		}
	}

	if _, err := s.kurrentDB.AppendToStream(ctx, streamID.String(), appendOpts, streamEvents...); err != nil {
		if kdbErr, ok := kurrentdb.FromError(err); !ok {
			switch kdbErr.Code() {
			case kurrentdb.ErrorCodeWrongExpectedVersion:
				var expected, actual int
				if _, scanErr := fmt.Fscanf(
					strings.NewReader(kdbErr.Unwrap().Error()),
					"wrong expected version: expecting '%d' but got '%d'",
					&expected,
					&actual,
				); scanErr != nil {
					slog.Error("append to stream: failed to parse version mismatch error",
						"stream_id", streamID.String(),
						"expected_version", opts.ExpectVersion,
						"scan_error", scanErr,
						"error", err,
						"code", kdbErr.Code(),
						"unwrap", kdbErr.Unwrap(),
					)
					return fmt.Errorf("appending to stream: %w", err)
				}

				return eventstore.StreamVersionMismatchError{
					StreamID:        streamID,
					ExpectedVersion: opts.ExpectVersion,
					ActualVersion:   int64(actual + 1), // convert to 1-based
				}
			}
		}

		return fmt.Errorf("appending to stream: %w", err)
	}

	return nil
}
