package eventstore

import (
	"context"
	"fmt"

	"github.com/go-estoria/estoria"
	"github.com/go-estoria/estoria-contrib/mongodb/eventstore/strategy"
	"github.com/go-estoria/estoria/eventstore"
	"github.com/go-estoria/estoria/typeid"
	"github.com/gofrs/uuid/v5"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
)

const (
	DefaultDatabaseName   string = "estoria"
	DefaultCollectionName string = "events"
)

type (
	// MongoClient provides APIs for obtaining database handles and starting sessions.
	MongoClient interface {
		Database(name string, opts ...options.Lister[options.DatabaseOptions]) *mongo.Database
		StartSession(opts ...options.Lister[options.SessionOptions]) (*mongo.Session, error)
	}

	// Strategy provides APIs for reading and writing events to an event store.
	Strategy interface {
		GetAllEventsIterator(
			ctx context.Context,
			opts eventstore.ReadStreamOptions,
		) (eventstore.StreamIterator, error)
		GetStreamIterator(
			ctx context.Context,
			streamID typeid.UUID,
			opts eventstore.ReadStreamOptions,
		) (eventstore.StreamIterator, error)
		InsertStreamEvents(
			ctx context.Context,
			streamID typeid.UUID,
			events []*eventstore.WritableEvent,
			opts eventstore.AppendStreamOptions,
		) (*strategy.InsertStreamEventsResult, error)
		ListStreams(ctx context.Context) (*mongo.Cursor, error)
	}

	// A TransactionHook is a function that is executed within the transaction used for appending events.
	// If a hook returns an error, the transaction is aborted and the error is returned to the caller.
	TransactionHook interface {
		HandleEvents(sessCtx context.Context, events []*eventstore.Event) error
	}
)

// An EventStore stores and retrieves events using MongoDB as the underlying storage.
type EventStore struct {
	mongoClient    MongoClient
	strategy       Strategy
	sessionOptions *options.SessionOptionsBuilder
	txOptions      *options.TransactionOptionsBuilder
	txHooks        []TransactionHook
	log            estoria.Logger
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
			id = uuid.FromStringOrNil(elem.Value.(string))
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
		mongoClient:    client,
		sessionOptions: DefaultSessionOptions(),
		txOptions:      DefaultTransactionOptions(),
		log:            estoria.GetLogger().WithGroup("eventstore"),
	}
	for _, opt := range opts {
		if err := opt(eventStore); err != nil {
			return nil, fmt.Errorf("applying option: %w", err)
		}
	}

	// use a single collection strategy by default
	if eventStore.strategy == nil {
		strat, err := strategy.NewSingleCollectionStrategy(
			client.Database(DefaultDatabaseName).Collection(DefaultCollectionName),
		)
		if err != nil {
			return nil, fmt.Errorf("creating default strategy: %w", err)
		}

		eventStore.strategy = strat
	}

	return eventStore, nil
}

// ListStreams returns a list containing metadata for all streams in the event store.
func (s *EventStore) ListStreams(ctx context.Context) ([]StreamInfo, error) {
	cursor, err := s.strategy.ListStreams(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing streams: %w", err)
	}

	defer cursor.Close(ctx)

	streams := []StreamInfo{}
	if err := cursor.All(ctx, &streams); err != nil {
		return nil, fmt.Errorf("decoding streams: %w", err)
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

	iter, err := s.strategy.GetAllEventsIterator(ctx, opts)
	if err != nil {
		return nil, fmt.Errorf("getting all events iterator: %w", err)
	}

	return iter, nil
}

// ReadStream returns an iterator for reading events from the specified stream.
func (s *EventStore) ReadStream(ctx context.Context, streamID typeid.UUID, opts eventstore.ReadStreamOptions) (eventstore.StreamIterator, error) {
	s.log.Debug("reading events from MongoDB stream",
		"stream_id", streamID.String(),
		"offset", opts.Offset,
		"count", opts.Count,
		"direction", opts.Direction,
	)

	iter, err := s.strategy.GetStreamIterator(ctx, streamID, opts)
	if err != nil {
		return nil, fmt.Errorf("getting stream iterator: %w", err)
	}

	return iter, nil
}

// AppendStream appends events to the specified stream.
func (s *EventStore) AppendStream(ctx context.Context, streamID typeid.UUID, events []*eventstore.WritableEvent, opts eventstore.AppendStreamOptions) error {
	s.log.Debug("appending events to MongoDB stream",
		"stream_id", streamID.String(),
		"events", len(events),
		"expected_version", opts.ExpectVersion,
	)

	result, txErr := s.doInTransaction(ctx, func(sessCtx context.Context) (any, error) {
		insertResult, err := s.strategy.InsertStreamEvents(sessCtx, streamID, events, opts)
		if err != nil {
			return nil, fmt.Errorf("inserting events: %w", err)
		}

		insertedEvents := make([]*eventstore.Event, len(insertResult.InsertedEvents))
		for i, evt := range insertResult.InsertedEvents {
			insertedEvents[i] = &evt.Event
		}

		for i, hook := range s.txHooks {
			s.log.Debug("executing transaction hook %d of %d", "hook", i+1, len(s.txHooks))
			if err := hook.HandleEvents(sessCtx, insertedEvents); err != nil {
				return nil, fmt.Errorf("executing transaction hook %d of %d: %w", i+1, len(s.txHooks), err)
			}
		}

		return insertResult, nil
	})
	if txErr != nil {
		return fmt.Errorf("executing transaction: %w", txErr)
	} else if result == nil {
		return fmt.Errorf("executing transaction: no result")
	}

	return nil
}

// DefaultSessionOptions returns the default session options used by the event store
// when starting a new MongoDB session.
func DefaultSessionOptions() *options.SessionOptionsBuilder {
	return options.Session()
}

// DefaultTransactionOptions returns the default transaction options used by the event store
// when starting a new MongoDB transaction on a session.
func DefaultTransactionOptions() *options.TransactionOptionsBuilder {
	return options.Transaction().SetReadPreference(readpref.Primary())
}

// Executes the given function within a session transaction.
// The function passed to this method must be idempotent, as the MongoDB transaction
// may be retried in the event of a transient error.
func (s *EventStore) doInTransaction(ctx context.Context, f func(sessCtx context.Context) (any, error)) (any, error) {
	session, err := s.mongoClient.StartSession(s.sessionOptions)
	if err != nil {
		return nil, fmt.Errorf("starting MongoDB session: %w", err)
	}

	defer session.EndSession(ctx)

	result, err := session.WithTransaction(ctx, func(sessCtx context.Context) (any, error) {
		return f(sessCtx)
	}, s.txOptions)
	if err != nil {
		return nil, fmt.Errorf("executing transaction: %w", err)
	}

	return result, nil
}
