package eventstore

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/go-estoria/estoria"
	"github.com/go-estoria/estoria-contrib/mongodb/eventstore/strategy"
	"github.com/go-estoria/estoria/eventstore"
	"github.com/go-estoria/estoria/typeid"
	"github.com/gofrs/uuid/v5"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

const (
	DefaultDatabaseName   string = "estoria"
	DefaultCollectionName string = "events"
)

type (

	// MongoClient provides APIs for obtaining database handles and starting sessions.
	MongoClient interface {
		strategy.MongoSessionStarter
		Database(name string, opts ...options.Lister[options.DatabaseOptions]) *mongo.Database
	}

	// Strategy provides APIs for reading and writing events to an event store, enumerating streams, and marshaling events.
	Strategy interface {
		// ExecuteInsertTransaction executes the given function within a new session suitable
		// for inserting numEvents events. The function is executed within a transaction, after
		// offset and global offset ranges have been reserved in it, and is invoked with a
		// session context, a collection, and the offsets preceding each reserved range.
		ExecuteInsertTransaction(
			ctx context.Context,
			streamID typeid.ID,
			numEvents int,
			inTxnFn func(sessCtx context.Context, collection strategy.MongoCollection, offset int64, globalOffset int64) (any, error),
		) (any, error)

		// GetAllCursor returns one or more Mongo cursors for all events in the event store, each ordered by global offset.
		GetAllCursor(
			ctx context.Context,
			opts eventstore.ReadAllOptions,
		) ([]*mongo.Cursor, error)

		// GetStreamCursor returns a Mongo cursor for events in the specified stream, ordered by stream offset.
		GetStreamCursor(
			ctx context.Context,
			streamID typeid.ID,
			opts eventstore.ReadStreamOptions,
		) (*mongo.Cursor, error)

		// ListStreams returns a list of cursors for iterating over stream metadata.
		ListStreams(ctx context.Context) ([]*mongo.Cursor, error)

		// StreamExists reports whether any event has ever been written to the stream,
		// regardless of the options any particular read is filtered by.
		StreamExists(ctx context.Context, streamID typeid.ID) (bool, error)
	}

	// A TransactionHook is a function that is executed within the transaction used for appending events.
	// If a hook returns an error, the transaction is aborted and the error is returned to the caller.
	TransactionHook interface {
		HandleEvents(sessCtx context.Context, events []*eventstore.Event) error
	}
)

// An EventStore stores and retrieves events using MongoDB as the underlying storage.
type EventStore struct {
	mongoClient MongoClient
	strategy    Strategy
	marshaler   DocumentMarshaler
	txHooks     []TransactionHook

	log estoria.Logger
}

var (
	_ eventstore.StreamReader = (*EventStore)(nil)
	_ eventstore.StreamWriter = (*EventStore)(nil)
	_ eventstore.GlobalReader = (*EventStore)(nil)
)

// StreamInfo represents information about a single stream in the event store.
type StreamInfo struct {
	// StreamID is the typed ID of the stream.
	StreamID typeid.ID

	// Offset is the stream-specific offset of the most recent event in the stream.
	// Thus, it also represents the number of events in the stream.
	Offset int64

	// GlobalOffset is the global offset of the most recent event in the stream
	// among all events in the event store.
	GlobalOffset int64
}

// bsonField returns a BSON element's value as T, or an error naming the field and the
// type actually stored.
func bsonField[T any](key string, value any) (T, error) {
	typed, ok := value.(T)
	if !ok {
		var zero T
		return zero, fmt.Errorf("field %q is %T, want %T", key, value, zero)
	}

	return typed, nil
}

// UnmarshalBSON unmarshals a BSON document into a StreamInfo.
func (i *StreamInfo) UnmarshalBSON(b []byte) error {
	data := bson.D{}
	if err := bson.Unmarshal(b, &data); err != nil {
		return fmt.Errorf("unmarshaling BSON: %w", err)
	}

	id := uuid.Nil
	typ := ""
	for _, elem := range data {
		var err error

		switch elem.Key {
		case "_id":
			var raw string
			if raw, err = bsonField[string](elem.Key, elem.Value); err != nil {
				return fmt.Errorf("unmarshaling stream info: %w", err)
			}

			if id, err = uuid.FromString(raw); err != nil {
				return fmt.Errorf("parsing UUID: %w", err)
			}
		case "stream_type":
			typ, err = bsonField[string](elem.Key, elem.Value)
		case "offset":
			i.Offset, err = bsonField[int64](elem.Key, elem.Value)
		case "global_offset":
			i.GlobalOffset, err = bsonField[int64](elem.Key, elem.Value)
		}

		if err != nil {
			return fmt.Errorf("unmarshaling stream info: %w", err)
		}
	}

	i.StreamID = typeid.New(typ, id)
	return nil
}

// String returns a string representation of a StreamInfo.
func (i StreamInfo) String() string {
	return fmt.Sprintf("stream {ID: %s, Offset: %d, GlobalOffset: %d}", i.StreamID, i.Offset, i.GlobalOffset)
}

// New creates a new EventStore using the given MongoDB client.
func New(client MongoClient, opts ...EventStoreOption) (*EventStore, error) {
	if client == nil {
		return nil, errors.New("mongodb client is required")
	}

	eventStore := &EventStore{
		mongoClient: client,
		marshaler:   DefaultMarshaler{},
		log:         estoria.GetLogger().WithGroup("eventstore"),
	}

	for _, opt := range opts {
		if err := opt(eventStore); err != nil {
			return nil, fmt.Errorf("applying option: %w", err)
		}
	}

	// use a single collection strategy by default
	if eventStore.strategy == nil {
		database := client.Database(DefaultDatabaseName)
		strat, err := strategy.NewSingleCollectionStrategy(
			client,
			database.Collection(DefaultCollectionName),
			database.Collection(strategy.DefaultStreamsCollectionName),
		)
		if err != nil {
			return nil, fmt.Errorf("creating default strategy: %w", err)
		}

		eventStore.strategy = strat
	}

	return eventStore, nil
}

// ListStreams returns a list of metadata for all streams in the event store.
func (s *EventStore) ListStreams(ctx context.Context) ([]StreamInfo, error) {
	cursors, err := s.strategy.ListStreams(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing streams: %w", err)
	}

	streams := []StreamInfo{}
	for _, cursor := range cursors {
		defer cursor.Close(ctx)

		streamInfos := []StreamInfo{}
		if err := cursor.All(ctx, &streamInfos); err != nil {
			return nil, fmt.Errorf("decoding streams: %w", err)
		}

		streams = append(streams, streamInfos...)
	}

	return streams, nil
}

// ReadAll creates an iterator over events from all streams in ascending global order,
// implementing eventstore.GlobalReader. Global positions are counter-allocated global
// offsets: gaps can occur, repeats cannot. A read with nothing to yield returns an empty
// iterator rather than an error.
func (s *EventStore) ReadAll(ctx context.Context, opts eventstore.ReadAllOptions) (eventstore.StreamIterator, error) {
	s.log.Debug("reading events from MongoDB event store",
		"after_position", opts.AfterPosition,
		"count", opts.Count,
	)

	cursors, err := s.strategy.GetAllCursor(ctx, opts)
	if err != nil {
		return nil, fmt.Errorf("getting all events iterator: %w", err)
	}

	// A single cursor is already bounded server-side; the merged iterator applies the
	// count across cursors itself.
	if len(cursors) == 1 {
		return &streamIterator{
			cursor:    cursors[0],
			marshaler: s.marshaler,
		}, nil
	}

	iteratorCursors := make([]*multiStreamIteratorCursor, len(cursors))
	for i, cursor := range cursors {
		iteratorCursors[i] = &multiStreamIteratorCursor{
			cursor: cursor,
		}
	}

	return &multiStreamIterator{
		cursors:   iteratorCursors,
		marshaler: s.marshaler,
		limit:     opts.Count,
	}, nil
}

// ReadStream returns an iterator for reading events from the specified stream.
func (s *EventStore) ReadStream(ctx context.Context, streamID typeid.ID, opts eventstore.ReadStreamOptions) (eventstore.StreamIterator, error) {
	s.log.Debug("reading events from MongoDB stream",
		"stream_id", streamID.String(),
		"after_version", opts.AfterVersion,
		"count", opts.Count,
		"direction", opts.Direction,
	)

	cursor, err := s.strategy.GetStreamCursor(ctx, streamID, opts)
	if err != nil {
		return nil, fmt.Errorf("getting stream iterator: %w", err)
	}

	// An empty result has two meanings: the stream is absent, or it exists and the read
	// matched nothing. Only the first is ErrStreamNotFound, and callers depend on the
	// difference — EventSourcedStore reports ErrAggregateNotFound on that error alone, so
	// a store that never returns it hands back an empty aggregate for an ID that was
	// never written. The stream document is the authority; an empty unfiltered read is
	// not proof of absence, since a truncated stream can hold no events yet exist.
	primed := cursor.Next(ctx)
	if !primed {
		if err := cursor.Err(); err != nil {
			_ = cursor.Close(ctx)
			return nil, fmt.Errorf("reading stream events: %w", err)
		}

		exists, err := s.strategy.StreamExists(ctx, streamID)
		if err != nil {
			_ = cursor.Close(ctx)
			return nil, err
		} else if !exists {
			_ = cursor.Close(ctx)
			return nil, eventstore.ErrStreamNotFound
		}
	}

	return &streamIterator{
		cursor:    cursor,
		marshaler: s.marshaler,
		// True only when the advance above landed on a document. The iterator must deliver
		// that one before advancing again; an exhausted cursor leaves this false and the
		// iterator reports end-of-stream immediately.
		primed: primed,
	}, nil
}

// AppendStream appends events to the specified stream.
func (s *EventStore) AppendStream(ctx context.Context, streamID typeid.ID, events []*eventstore.WritableEvent, opts eventstore.AppendStreamOptions) ([]*eventstore.Event, error) {
	s.log.Debug("appending events to MongoDB stream",
		"stream_id", streamID.String(),
		"events", len(events),
		"expected_version", opts.ExpectVersion,
	)

	if opts.ExpectVersion != nil && opts.StreamMustNotExist {
		return nil, errors.New("ExpectVersion and StreamMustNotExist are mutually exclusive")
	}

	written := make([]*eventstore.Event, len(events))

	_, err := s.strategy.ExecuteInsertTransaction(ctx, streamID, len(events),
		func(sessCtx context.Context, collection strategy.MongoCollection, offset int64, globalOffset int64) (any, error) {
			if opts.StreamMustNotExist && offset > 0 {
				return nil, eventstore.StreamVersionMismatchError{
					StreamID:        streamID,
					ExpectedVersion: 0,
					ActualVersion:   offset,
				}
			}

			if opts.ExpectVersion != nil && offset != *opts.ExpectVersion {
				return nil, eventstore.StreamVersionMismatchError{
					StreamID:        streamID,
					ExpectedVersion: *opts.ExpectVersion,
					ActualVersion:   offset,
				}
			}

			// BSON datetimes hold milliseconds; truncate so the returned events carry
			// the timestamp a subsequent read yields.
			now := time.Now().UTC().Truncate(time.Millisecond)

			docs := make([]any, len(events))
			for i, we := range events {
				globalPosition := globalOffset + int64(i) + 1
				fullEvent := &Event{
					Event: eventstore.Event{
						ID:              typeid.NewV4(we.Type),
						StreamID:        streamID,
						StreamVersion:   offset + int64(i) + 1,
						GlobalPosition:  &globalPosition,
						Timestamp:       now,
						Data:            we.Data,
						DataContentType: we.DataContentType,
						Metadata:        we.Metadata,
					},
					GlobalOffset: globalPosition,
				}

				doc, err := s.marshaler.MarshalDocument(fullEvent)
				if err != nil {
					return nil, fmt.Errorf("marshaling event: %w", err)
				}

				docs[i] = doc
				written[i] = &fullEvent.Event
			}

			result, err := collection.InsertMany(sessCtx, docs)
			if err != nil {
				return result, fmt.Errorf("inserting events: %w", err)
			} else if len(result.InsertedIDs) != len(docs) {
				return result, fmt.Errorf("inserted %d events, but expected %d", len(result.InsertedIDs), len(docs))
			}

			for _, hook := range s.txHooks {
				if err := hook.HandleEvents(sessCtx, written); err != nil {
					return result, fmt.Errorf("executing transaction hook: %w", err)
				}
			}

			return result, nil
		},
	)
	if err != nil {
		return nil, err
	}

	return written, nil
}
