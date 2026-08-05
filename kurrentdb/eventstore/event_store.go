package eventstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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

var (
	_ eventstore.StreamReader = (*EventStore)(nil)
	_ eventstore.StreamWriter = (*EventStore)(nil)
)

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

	if opts.AfterVersion > 0 {
		// KurrentDB revisions are 0-based and stream versions are 1-based, and AfterVersion
		// means different things by direction. Reading forward it is an exclusive lower
		// bound, so the first event wanted is version AfterVersion+1, which is revision
		// AfterVersion. Reading backward it is an inclusive upper bound, so the first event
		// wanted is version AfterVersion, which is revision AfterVersion-1.
		revision := uint64(opts.AfterVersion)
		if opts.Direction == eventstore.Reverse {
			revision--
		}

		readOpts.From = kurrentdb.StreamRevision{Value: revision}
	}

	count := uint64(opts.Count)
	if count == 0 {
		// HACK: large value to read all events
		count = 1_000_000
	}

	result, err := s.kurrentDB.ReadStream(ctx, streamID.String(), readOpts, count)
	if err != nil {
		s.log.Error("reading stream", "stream_id", streamID.String(), "error", err.Error())
		if kdbErr, ok := kurrentdb.FromError(err); !ok && kdbErr != nil && kdbErr.Code() == kurrentdb.ErrorCodeResourceNotFound {
			return nil, eventstore.ErrStreamNotFound
		}

		return nil, fmt.Errorf("reading stream: %w", err)
	}

	s.log.Info("read stream", "stream_id", streamID.String())

	iter := &streamIterator{
		streamID: streamID,
		stream:   result,
	}

	// A stream read past its tip yields no events. That is an empty result, not a missing
	// stream: KurrentDB reports an absent stream distinctly, as a resource-not-found above.
	if err := iter.Preload(); errors.Is(err, eventstore.ErrStreamNotFound) {
		return nil, eventstore.ErrStreamNotFound
	} else if errors.Is(err, eventstore.ErrEndOfEventStream) {
		if closeErr := iter.Close(ctx); closeErr != nil {
			s.log.Warn("closing empty stream iterator", "stream_id", streamID.String(), "error", closeErr)
		}

		return emptyStreamIterator{}, nil
	} else if err != nil {
		return nil, fmt.Errorf("preloading first event: %w", err)
	}

	return iter, nil
}

// AppendStream saves the given events to the event store.
func (s *EventStore) AppendStream(ctx context.Context, streamID typeid.ID, events []*eventstore.WritableEvent, opts eventstore.AppendStreamOptions) error {
	s.log.Debug("appending events to stream", "stream_id", streamID.String(), "events", len(events))

	// Validate mutually exclusive options.
	if opts.ExpectVersion != nil && opts.StreamMustNotExist {
		return errors.New("ExpectVersion and StreamMustNotExist are mutually exclusive")
	}

	appendOpts := kurrentdb.AppendToStreamOptions{}

	if opts.StreamMustNotExist {
		appendOpts.StreamState = kurrentdb.NoStream{}
	} else if opts.ExpectVersion != nil {
		if *opts.ExpectVersion == 0 {
			// Version 0 means the stream must not exist yet.
			appendOpts.StreamState = kurrentdb.NoStream{}
		} else {
			// KurrentDB revisions are 0-based; version N corresponds to revision N-1.
			appendOpts.StreamState = kurrentdb.StreamRevision{Value: uint64(*opts.ExpectVersion - 1)}
		}
	}

	streamEvents := make([]kurrentdb.EventData, len(events))
	for i, e := range events {
		eventID, err := uuid.NewV4()
		if err != nil {
			return fmt.Errorf("generating event ID: %w", err)
		}

		metadata, err := marshalMetadata(e.Metadata)
		if err != nil {
			return eventstore.EventMarshalingError{StreamID: streamID, Err: err}
		}

		streamEvents[i] = kurrentdb.EventData{
			EventID:     guuid.UUID(eventID),
			ContentType: kurrentdb.ContentTypeJson,
			EventType:   e.Type,
			Data:        e.Data,
			Metadata:    metadata,
		}
	}

	if _, err := s.kurrentDB.AppendToStream(ctx, streamID.String(), appendOpts, streamEvents...); err != nil {
		if ok, mismatch := s.asVersionMismatch(err, streamID, opts); ok {
			return mismatch
		}

		return fmt.Errorf("appending to stream: %w", err)
	}

	return nil
}

// marshalMetadata encodes event metadata for KurrentDB's user-metadata slot, which is
// untyped bytes. Nil and empty metadata both encode to nil so an event written without
// metadata reads back with none, rather than with an empty map.
func marshalMetadata(metadata map[string]string) ([]byte, error) {
	if len(metadata) == 0 {
		return nil, nil
	}

	encoded, err := json.Marshal(metadata)
	if err != nil {
		return nil, fmt.Errorf("marshaling event metadata: %w", err)
	}

	return encoded, nil
}

// unmarshalMetadata decodes what marshalMetadata wrote. KurrentDB's user-metadata slot is
// writable by anything, so metadata that is absent or not a JSON object is reported as
// absent rather than failing the read of an otherwise intact event.
func unmarshalMetadata(encoded []byte) map[string]string {
	if len(encoded) == 0 {
		return nil
	}

	metadata := map[string]string{}
	if err := json.Unmarshal(encoded, &metadata); err != nil {
		estoria.GetLogger().Warn("ignoring unreadable event metadata", "error", err)
		return nil
	}

	return metadata
}

// asVersionMismatch converts a KurrentDB wrong-expected-version error into a
// StreamVersionMismatchError. The actual version is only available as text in the
// server's message; an unrecognized message reports no mismatch.
func (s *EventStore) asVersionMismatch(
	err error,
	streamID typeid.ID,
	opts eventstore.AppendStreamOptions,
) (bool, error) {
	kdbErr, ok := kurrentdb.FromError(err)
	if ok || kdbErr == nil || kdbErr.Code() != kurrentdb.ErrorCodeWrongExpectedVersion {
		return false, nil
	}

	message := kdbErr.Unwrap().Error()

	var expected, actual int

	_, scanErr := fmt.Fscanf(
		strings.NewReader(message),
		"wrong expected version: expecting '%d' but got '%d'",
		&expected,
		&actual,
	)
	if scanErr != nil {
		// StreamMustNotExist sends NoStream rather than a revision, and the server names it
		// in words: "expecting 'no_stream' but got '1'". Parsing only the numeric form left
		// that case reporting the raw KurrentDB error, so a caller could not tell a lost
		// creation race from a transport failure.
		_, scanErr = fmt.Fscanf(
			strings.NewReader(message),
			"wrong expected version: expecting 'no_stream' but got '%d'",
			&actual,
		)
	}

	if scanErr != nil {
		s.log.Error("append to stream: failed to parse version mismatch error",
			"stream_id", streamID.String(),
			"expected_version", derefInt64(opts.ExpectVersion),
			"scan_error", scanErr,
			"error", err,
			"code", kdbErr.Code(),
			"unwrap", kdbErr.Unwrap(),
		)

		return false, nil
	}

	return true, eventstore.StreamVersionMismatchError{
		StreamID:        streamID,
		ExpectedVersion: derefInt64(opts.ExpectVersion),
		ActualVersion:   int64(actual + 1), // convert to 1-based
	}
}

// derefInt64 safely dereferences an *int64, returning 0 for nil.
func derefInt64(p *int64) int64 {
	if p != nil {
		return *p
	}
	return 0
}
