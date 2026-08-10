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
	ReadAll(context context.Context, opts kurrentdb.ReadAllOptions, count uint64) (*kurrentdb.ReadStream, error)
	AppendToStream(context context.Context, streamID string, opts kurrentdb.AppendToStreamOptions, events ...kurrentdb.EventData) (*kurrentdb.WriteResult, error)
}

// defaultReadAllWindowSize is how many raw $all records ReadAll fetches per server read.
const defaultReadAllWindowSize = 1024

type EventStore struct {
	kurrentDB KurrentClient
	log       estoria.Logger

	streamPrefix      string
	readAllWindowSize int64
}

var (
	_ eventstore.StreamReader = (*EventStore)(nil)
	_ eventstore.StreamWriter = (*EventStore)(nil)
	_ eventstore.GlobalReader = (*EventStore)(nil)
)

// New creates a new event store using the given KurrentDB client.
func New(kurrentDB KurrentClient, opts ...EventStoreOption) (*EventStore, error) {
	eventStore := &EventStore{
		kurrentDB:         kurrentDB,
		log:               estoria.GetLogger().WithGroup("eventstore"),
		readAllWindowSize: defaultReadAllWindowSize,
	}

	for _, opt := range opts {
		if err := opt(eventStore); err != nil {
			return nil, fmt.Errorf("applying option: %w", err)
		}
	}

	return eventStore, nil
}

// streamName returns the underlying KurrentDB stream name for an estoria stream ID,
// applying the store's namespace prefix when one is configured.
func (s *EventStore) streamName(streamID typeid.ID) string {
	if s.streamPrefix != "" {
		return s.streamPrefix + "." + streamID.String()
	}

	return streamID.String()
}

// estoriaStreamID reports whether a KurrentDB stream name identifies a stream this store
// owns, returning the parsed stream ID when it does. The $-prefix check must precede
// parsing: a metadata stream name like $$user_<uuid> would otherwise parse as a valid
// stream ID. Name parsing is the only ownership signal available, so on a shared node,
// streams written by other applications whose names parse as stream IDs are
// indistinguishable from this store's; namespace stores with WithStreamPrefix to isolate
// them.
func (s *EventStore) estoriaStreamID(name string) (typeid.ID, bool) {
	if strings.HasPrefix(name, "$") {
		return typeid.ID{}, false
	}

	if s.streamPrefix != "" {
		trimmed, found := strings.CutPrefix(name, s.streamPrefix+".")
		if !found {
			return typeid.ID{}, false
		}

		name = trimmed
	}

	id, err := typeid.Parse(name)
	if err != nil {
		return typeid.ID{}, false
	}

	return id, true
}

// ReadAll creates an iterator over events from all streams in ascending global order,
// implementing eventstore.GlobalReader. Global positions are KurrentDB commit positions:
// gaps are normal, repeats cannot occur. KurrentDB offers no server-side read filtering,
// so the iterator scans the server's $all stream in windows and filters client-side to
// this store's streams.
func (s *EventStore) ReadAll(_ context.Context, opts eventstore.ReadAllOptions) (eventstore.StreamIterator, error) {
	if opts.AfterPosition < 0 {
		return nil, errors.New("AfterPosition must not be negative")
	} else if opts.Count < 0 {
		return nil, errors.New("count must not be negative")
	}

	// An unbounded read is -1 rather than 0 so a raw record at commit position zero
	// cannot be mistaken for one at the resume bound. The first window opens on the
	// first Next, so a read with nothing to yield returns a working, empty iterator.
	bound := int64(-1)
	if opts.AfterPosition > 0 {
		bound = opts.AfterPosition
	}

	remaining := int64(-1)
	if opts.Count > 0 {
		remaining = opts.Count
	}

	return &allStreamIterator{
		client:     s.kurrentDB,
		owns:       s.estoriaStreamID,
		windowSize: s.readAllWindowSize,
		bound:      bound,
		cursor:     bound,
		verified:   bound < 0,
		remaining:  remaining,
	}, nil
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

	result, err := s.kurrentDB.ReadStream(ctx, s.streamName(streamID), readOpts, count)
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
func (s *EventStore) AppendStream(ctx context.Context, streamID typeid.ID, events []*eventstore.WritableEvent, opts eventstore.AppendStreamOptions) ([]*eventstore.Event, error) {
	s.log.Debug("appending events to stream", "stream_id", streamID.String(), "events", len(events))

	// Validate mutually exclusive options.
	if opts.ExpectVersion != nil && opts.StreamMustNotExist {
		return nil, errors.New("ExpectVersion and StreamMustNotExist are mutually exclusive")
	}

	if len(events) == 0 {
		return nil, nil
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
			return nil, fmt.Errorf("generating event ID: %w", err)
		}

		envelope, err := marshalEnvelope(e)
		if err != nil {
			return nil, eventstore.EventMarshalingError{StreamID: streamID, Err: err}
		}

		streamEvents[i] = kurrentdb.EventData{
			EventID:     guuid.UUID(eventID),
			ContentType: nativeContentType(e.DataContentType),
			EventType:   e.Type,
			Data:        e.Data,
			Metadata:    envelope,
		}
	}

	result, err := s.kurrentDB.AppendToStream(ctx, s.streamName(streamID), appendOpts, streamEvents...)
	if err != nil {
		if ok, mismatch := s.asVersionMismatch(err, streamID, opts); ok {
			return nil, mismatch
		}

		return nil, fmt.Errorf("appending to stream: %w", err)
	}

	return s.readBack(ctx, streamID, result, len(events))
}

// readBack reads the just-appended range of a stream so AppendStream can return events
// exactly as a subsequent read yields them: the write result carries no server-assigned
// timestamps, so a read is the only source of the events of record.
func (s *EventStore) readBack(ctx context.Context, streamID typeid.ID, result *kurrentdb.WriteResult, count int) ([]*eventstore.Event, error) {
	lastVersion := int64(result.NextExpectedVersion) + 1

	iter, err := s.ReadStream(ctx, streamID, eventstore.ReadStreamOptions{
		AfterVersion: lastVersion - int64(count),
		Count:        int64(count),
	})
	if err != nil {
		return nil, fmt.Errorf("reading back appended events: %w", err)
	}

	defer func() {
		if err := iter.Close(ctx); err != nil {
			s.log.Warn("closing read-back iterator", "stream_id", streamID.String(), "error", err)
		}
	}()

	written, err := eventstore.Collect(ctx, iter)
	if err != nil {
		return nil, fmt.Errorf("reading back appended events: %w", err)
	}

	return written, nil
}

// nativeContentType maps a declared content type onto KurrentDB's native content types:
// JSON for "application/json" and for the empty declaration, binary for everything else.
func nativeContentType(declared string) kurrentdb.ContentType {
	if declared == "" || declared == estoria.ContentTypeJSON {
		return kurrentdb.ContentTypeJson
	}

	return kurrentdb.ContentTypeBinary
}

// An eventEnvelope is the JSON document stored in KurrentDB's user-metadata slot,
// carrying an event's metadata and declared payload content type.
type eventEnvelope struct {
	DataContentType string            `json:"data_content_type,omitempty"`
	Metadata        map[string]string `json:"metadata,omitempty"`
}

// marshalEnvelope encodes an event's metadata and content-type declaration for KurrentDB's
// user-metadata slot, which is untyped bytes. An event carrying neither encodes to nil so
// it reads back with none, rather than with an empty envelope.
func marshalEnvelope(event *eventstore.WritableEvent) ([]byte, error) {
	if len(event.Metadata) == 0 && event.DataContentType == "" {
		return nil, nil
	}

	encoded, err := json.Marshal(eventEnvelope{
		DataContentType: event.DataContentType,
		Metadata:        event.Metadata,
	})
	if err != nil {
		return nil, fmt.Errorf("marshaling event envelope: %w", err)
	}

	return encoded, nil
}

// unmarshalEnvelope decodes what marshalEnvelope wrote. KurrentDB's user-metadata slot is
// writable by anything, so bytes that do not decode as an envelope are reported as an
// absent envelope rather than failing the read of an otherwise intact event.
func unmarshalEnvelope(encoded []byte) eventEnvelope {
	envelope := eventEnvelope{}
	if len(encoded) == 0 {
		return envelope
	}

	if err := json.Unmarshal(encoded, &envelope); err != nil {
		estoria.GetLogger().Warn("ignoring unreadable event envelope", "error", err)
		return eventEnvelope{}
	}

	return envelope
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
