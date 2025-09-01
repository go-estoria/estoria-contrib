package eventstore

import (
	"context"
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
		// ExecuteInsertTransaction executes the given function within a new session suitable for inserting events.
		// The function is executed within a transaction and is invoked with a session context, a collection,
		// the current offset of the stream, and the global offset.
		ExecuteInsertTransaction(
			ctx context.Context,
			streamID typeid.UUID,
			inTxnFn func(sessCtx context.Context, collection strategy.MongoCollection, offset int64, globalOffset int64) (any, error),
		) (any, error)

		// GetAllCursor returns one or more Mongo cursors for all events in the event store, ordered by global offset.
		GetAllCursor(
			ctx context.Context,
			opts eventstore.ReadStreamOptions,
		) ([]*mongo.Cursor, error)

		// GetStreamCursor returns a Mongo cursor for events in the specified stream, ordered by stream offset.
		GetStreamCursor(
			ctx context.Context,
			streamID typeid.UUID,
			opts eventstore.ReadStreamOptions,
		) (*mongo.Cursor, error)

		// ListStreams returns a list of cursors for iterating over stream metadata.
		ListStreams(ctx context.Context) ([]*mongo.Cursor, error)
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

var _ eventstore.StreamReader = (*EventStore)(nil)
var _ eventstore.StreamWriter = (*EventStore)(nil)

// StreamInfo represents information about a single stream in the event store.
type StreamInfo struct {
	// StreamID is the typed ID of the stream.
	StreamID typeid.UUID

	// Offset is the stream-specific offset of the most recent event in the stream.
	// Thus, it also represents the number of events in the stream.
	Offset int64

	// GlobalOffset is the global offset of the most recent event in the stream
	// among all events in the event store.
	GlobalOffset int64
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
		switch elem.Key {
		case "_id":
			uid, err := uuid.FromString(elem.Value.(string))
			if err != nil {
				return fmt.Errorf("parsing UUID: %w", err)
			}
			id = uid
		case "stream_type":
			typ = elem.Value.(string)
		case "offset":
			i.Offset = elem.Value.(int64)
		case "global_offset":
			i.GlobalOffset = elem.Value.(int64)
		}
	}

	i.StreamID = typeid.FromUUID(typ, id)
	return nil
}

// String returns a string representation of a StreamInfo.
func (i StreamInfo) String() string {
	return fmt.Sprintf("stream {ID: %s, Offset: %d, GlobalOffset: %d}", i.StreamID, i.Offset, i.GlobalOffset)
}

// New creates a new EventStore using the given MongoDB client.
func New(client MongoClient, opts ...EventStoreOption) (*EventStore, error) {
	if client == nil {
		return nil, fmt.Errorf("mongodb client is required")
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
		strat, err := strategy.NewSingleCollectionStrategy(
			client,
			client.Database(DefaultDatabaseName).Collection(DefaultCollectionName),
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

// ReadAll returns an iterator for reading all events in the event store.
func (s *EventStore) ReadAll(ctx context.Context, opts eventstore.ReadStreamOptions) (eventstore.StreamIterator, error) {
	s.log.Debug("reading events from MongoDB event store",
		"offset", opts.Offset,
		"count", opts.Count,
		"direction", opts.Direction,
	)

	cursors, err := s.strategy.GetAllCursor(ctx, opts)
	if err != nil {
		return nil, fmt.Errorf("getting all events iterator: %w", err)
	}

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
	}, nil
}

// ReadStream returns an iterator for reading events from the specified stream.
func (s *EventStore) ReadStream(ctx context.Context, streamID typeid.UUID, opts eventstore.ReadStreamOptions) (eventstore.StreamIterator, error) {
	s.log.Debug("reading events from MongoDB stream",
		"stream_id", streamID.String(),
		"offset", opts.Offset,
		"count", opts.Count,
		"direction", opts.Direction,
	)

	cursor, err := s.strategy.GetStreamCursor(ctx, streamID, opts)
	if err != nil {
		return nil, fmt.Errorf("getting stream iterator: %w", err)
	}

	return &streamIterator{
		cursor:    cursor,
		marshaler: s.marshaler,
	}, nil
}

// AppendStream appends events to the specified stream.
func (s *EventStore) AppendStream(ctx context.Context, streamID typeid.UUID, events []*eventstore.WritableEvent, opts eventstore.AppendStreamOptions) error {
	s.log.Debug("appending events to MongoDB stream",
		"stream_id", streamID.String(),
		"events", len(events),
		"expected_version", opts.ExpectVersion,
	)

	_, err := s.strategy.ExecuteInsertTransaction(ctx, streamID,
		func(sessCtx context.Context, collection strategy.MongoCollection, offset int64, globalOffset int64) (any, error) {
			if opts.ExpectVersion > 0 && offset != opts.ExpectVersion {
				return nil, fmt.Errorf("expected offset %d, but stream has offset %d", opts.ExpectVersion, offset)
			}

			now := time.Now().UTC()

			fullEvents := make([]*Event, len(events))
			docs := make([]any, len(events))
			for i, we := range events {
				eventID, err := typeid.NewUUID(we.Type)
				if err != nil {
					return nil, fmt.Errorf("generating event ID: %w", err)
				}

				fullEvents[i] = &Event{
					Event: eventstore.Event{
						ID:            eventID,
						StreamID:      streamID,
						StreamVersion: offset + int64(i) + 1,
						Timestamp:     now,
						Data:          we.Data,
					},
					GlobalOffset: globalOffset + int64(i) + 1,
				}

				doc, err := s.marshaler.MarshalDocument(fullEvents[i])
				if err != nil {
					return nil, fmt.Errorf("marshaling event: %w", err)
				}

				docs[i] = doc
			}

			result, err := collection.InsertMany(ctx, docs)
			if err != nil {
				return result, fmt.Errorf("inserting events: %w", err)
			} else if len(result.InsertedIDs) != len(docs) {
				return result, fmt.Errorf("inserted %d events, but expected %d", len(result.InsertedIDs), len(docs))
			}

			return result, nil
		},
	)

	return err
}
