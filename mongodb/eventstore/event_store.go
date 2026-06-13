package eventstore

import (
	"context"
	"fmt"

	"github.com/go-estoria/estoria"
	"github.com/go-estoria/estoria/eventstore"
	"github.com/go-estoria/estoria/typeid"
	"github.com/gofrs/uuid/v5"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readconcern"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"
)

const (
	// DefaultDatabaseName is the default MongoDB database name.
	DefaultDatabaseName string = "estoria"
	// DefaultEventsCollectionName is the default name of the events collection (the source of truth).
	DefaultEventsCollectionName string = "events"
	// DefaultStreamsCollectionName is the default name of the per-stream metadata collection.
	DefaultStreamsCollectionName string = "streams"
	// DefaultCountersCollectionName is the default name of the global counter collection.
	DefaultCountersCollectionName string = "counters"

	// globalCounterID is the _id of the single document in the counters collection
	// that holds the dense, gap-free, commit-ordered global offset.
	globalCounterID string = "global_offset"
)

type (
	// MongoClient provides APIs for obtaining database handles and starting sessions.
	MongoClient interface {
		MongoSessionStarter
		Database(name string, opts ...options.Lister[options.DatabaseOptions]) *mongo.Database
	}

	// MongoSessionStarter starts MongoDB sessions, which are required for multi-document transactions.
	MongoSessionStarter interface {
		StartSession(opts ...options.Lister[options.SessionOptions]) (*mongo.Session, error)
	}

	// MongoDatabase provides an API for obtaining collection handles.
	MongoDatabase interface {
		Collection(name string, opts ...options.Lister[options.CollectionOptions]) *mongo.Collection
	}

	// MongoCollection is the subset of *mongo.Collection used by the event store.
	MongoCollection interface {
		FindOneAndUpdate(ctx context.Context, filter, update any, opts ...options.Lister[options.FindOneAndUpdateOptions]) *mongo.SingleResult
		UpdateOne(ctx context.Context, filter, update any, opts ...options.Lister[options.UpdateOneOptions]) (*mongo.UpdateResult, error)
		InsertMany(ctx context.Context, docs any, opts ...options.Lister[options.InsertManyOptions]) (*mongo.InsertManyResult, error)
		Find(ctx context.Context, filter any, opts ...options.Lister[options.FindOptions]) (*mongo.Cursor, error)
		FindOne(ctx context.Context, filter any, opts ...options.Lister[options.FindOneOptions]) *mongo.SingleResult
		Indexes() mongo.IndexView
	}

	// A TransactionHook is executed within the same transaction used for appending events,
	// after the events (and their metadata) have been written but before the transaction commits.
	//
	// The hook is invoked with the session context: any writes it performs on that context are
	// part of the append transaction and are committed or rolled back atomically with the events.
	// If a hook returns an error, the entire append transaction is aborted.
	//
	// Transaction hooks are the foundation for the transactional outbox.
	TransactionHook interface {
		HandleEvents(sessCtx context.Context, events []*eventstore.Event) error
	}

	// TransactionHookFunc is a functional adapter for TransactionHook.
	TransactionHookFunc func(sessCtx context.Context, events []*eventstore.Event) error
)

// HandleEvents implements TransactionHook.
func (f TransactionHookFunc) HandleEvents(sessCtx context.Context, events []*eventstore.Event) error {
	return f(sessCtx, events)
}

// An EventStore stores and retrieves events using MongoDB as the underlying storage.
//
// All writes go through a single-collection, transactional append path. Per-stream and global
// offsets are assigned by atomic in-transaction counters held in dedicated metadata collections,
// so the store never scans for max(offset). Transactions require a replica set (or mongos).
type EventStore struct {
	client   MongoSessionStarter
	db       MongoDatabase
	events   MongoCollection
	streams  MongoCollection
	counters MongoCollection

	marshaler DocumentMarshaler
	txHooks   []TransactionHook
	sessOpts  options.Lister[options.SessionOptions]
	txOpts    options.Lister[options.TransactionOptions]
	log       estoria.Logger

	dbName           string
	eventsCollName   string
	streamsCollName  string
	countersCollName string
	autoEnsureIdx    bool
}

var _ eventstore.StreamReader = (*EventStore)(nil)
var _ eventstore.StreamWriter = (*EventStore)(nil)

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

// String returns a string representation of a StreamInfo.
func (i StreamInfo) String() string {
	return fmt.Sprintf("stream {ID: %s, Offset: %d, GlobalOffset: %d}", i.StreamID, i.Offset, i.GlobalOffset)
}

// streamDocument is the BSON shape of a document in the streams (metadata) collection.
// It is decoded with the driver's typed decoder; there is no hand-rolled BSON unmarshaling.
type streamDocument struct {
	StreamType       string `bson:"stream_type"`
	StreamID         string `bson:"stream_id"`
	LastOffset       int64  `bson:"last_offset"`
	LastGlobalOffset int64  `bson:"last_global_offset"`
}

func (d streamDocument) toStreamInfo() (StreamInfo, error) {
	id, err := uuid.FromString(d.StreamID)
	if err != nil {
		return StreamInfo{}, fmt.Errorf("parsing stream UUID: %w", err)
	}

	return StreamInfo{
		StreamID:     typeid.New(d.StreamType, id),
		Offset:       d.LastOffset,
		GlobalOffset: d.LastGlobalOffset,
	}, nil
}

// streamKey returns the compound natural-key _id used by the streams metadata collection.
func streamKey(streamID typeid.ID) bson.D {
	return bson.D{{Key: "t", Value: streamID.Type}, {Key: "s", Value: streamID.UUID.String()}}
}

// DefaultSessionOptions returns the default session options used when starting a MongoDB session.
func DefaultSessionOptions() options.Lister[options.SessionOptions] {
	return options.Session()
}

// DefaultTransactionOptions returns the default transaction options. They set snapshot read concern,
// majority write concern, and primary read preference so that appends are linearizable and durable.
func DefaultTransactionOptions() options.Lister[options.TransactionOptions] {
	return options.Transaction().
		SetReadConcern(readconcern.Snapshot()).
		SetWriteConcern(writeconcern.Majority()).
		SetReadPreference(readpref.Primary())
}

// New creates a new EventStore using the given MongoDB client.
//
// By default the store uses a database named "estoria" with three collections: "events"
// (the source of truth), "streams" (per-stream metadata), and "counters" (the global sequence).
// These can be overridden with the corresponding options.
//
// Operators must call EnsureIndexes once at deploy time (or pass WithAutoEnsureIndexes) to create
// the required unique indexes.
func New(client MongoClient, opts ...EventStoreOption) (*EventStore, error) {
	if client == nil {
		return nil, fmt.Errorf("mongodb client is required")
	}

	eventStore := &EventStore{
		client:           client,
		marshaler:        DefaultMarshaler{},
		sessOpts:         DefaultSessionOptions(),
		txOpts:           DefaultTransactionOptions(),
		log:              estoria.GetLogger().WithGroup("eventstore"),
		dbName:           DefaultDatabaseName,
		eventsCollName:   DefaultEventsCollectionName,
		streamsCollName:  DefaultStreamsCollectionName,
		countersCollName: DefaultCountersCollectionName,
	}

	for _, opt := range opts {
		if err := opt(eventStore); err != nil {
			return nil, fmt.Errorf("applying option: %w", err)
		}
	}

	db := client.Database(eventStore.dbName)
	eventStore.db = db
	eventStore.events = db.Collection(eventStore.eventsCollName)
	eventStore.streams = db.Collection(eventStore.streamsCollName)
	eventStore.counters = db.Collection(eventStore.countersCollName)

	if eventStore.autoEnsureIdx {
		if err := eventStore.EnsureIndexes(context.Background()); err != nil {
			return nil, fmt.Errorf("ensuring indexes: %w", err)
		}
	}

	return eventStore, nil
}

// ListStreams returns metadata for all streams in the event store.
//
// It reads directly from the streams metadata collection, with no aggregation over the events.
func (s *EventStore) ListStreams(ctx context.Context) ([]StreamInfo, error) {
	cursor, err := s.streams.Find(ctx, bson.D{})
	if err != nil {
		return nil, fmt.Errorf("finding streams: %w", err)
	}
	defer cursor.Close(ctx)

	docs := []streamDocument{}
	if err := cursor.All(ctx, &docs); err != nil {
		return nil, fmt.Errorf("decoding streams: %w", err)
	}

	streams := make([]StreamInfo, 0, len(docs))
	for _, doc := range docs {
		info, err := doc.toStreamInfo()
		if err != nil {
			return nil, fmt.Errorf("decoding streams: %w", err)
		}
		streams = append(streams, info)
	}

	return streams, nil
}

// ReadAll returns an iterator for reading all events in the event store, ordered by global offset.
func (s *EventStore) ReadAll(ctx context.Context, opts eventstore.ReadStreamOptions) (eventstore.StreamIterator, error) {
	s.log.Debug("reading all events from MongoDB event store",
		"after_version", opts.AfterVersion,
		"count", opts.Count,
		"direction", opts.Direction,
	)

	findOpts, versionFilter := findOptsFromReadStreamOptions(opts, "global_offset")
	filter := bson.D{}
	filter = append(filter, versionFilter...)

	cursor, err := s.events.Find(ctx, filter, findOpts)
	if err != nil {
		return nil, fmt.Errorf("finding events: %w", err)
	}

	return &streamIterator{cursor: cursor, marshaler: s.marshaler}, nil
}

// ReadStream returns an iterator for reading events from the specified stream, ordered by offset.
func (s *EventStore) ReadStream(ctx context.Context, streamID typeid.ID, opts eventstore.ReadStreamOptions) (eventstore.StreamIterator, error) {
	s.log.Debug("reading events from MongoDB stream",
		"stream_id", streamID.String(),
		"after_version", opts.AfterVersion,
		"count", opts.Count,
		"direction", opts.Direction,
	)

	findOpts, versionFilter := findOptsFromReadStreamOptions(opts, "offset")
	filter := bson.D{
		{Key: "stream_type", Value: streamID.Type},
		{Key: "stream_id", Value: streamID.UUID.String()},
	}
	filter = append(filter, versionFilter...)

	cursor, err := s.events.Find(ctx, filter, findOpts)
	if err != nil {
		return nil, fmt.Errorf("finding events: %w", err)
	}

	return &streamIterator{cursor: cursor, marshaler: s.marshaler}, nil
}

// findOptsFromReadStreamOptions translates estoria read options into MongoDB find options and a
// version filter, keyed on the given offset field ("offset" for a stream, "global_offset" for all).
func findOptsFromReadStreamOptions(opts eventstore.ReadStreamOptions, offsetKey string) (options.Lister[options.FindOptions], bson.D) {
	findOpts := options.Find()
	if opts.Direction == eventstore.Reverse {
		findOpts.SetSort(bson.D{{Key: offsetKey, Value: -1}})
	} else {
		findOpts.SetSort(bson.D{{Key: offsetKey, Value: 1}})
	}

	if opts.Count > 0 {
		findOpts.SetLimit(opts.Count)
	}

	var versionFilter bson.D
	if opts.AfterVersion > 0 {
		if opts.Direction == eventstore.Reverse {
			versionFilter = bson.D{{Key: offsetKey, Value: bson.D{{Key: "$lte", Value: opts.AfterVersion}}}}
		} else {
			versionFilter = bson.D{{Key: offsetKey, Value: bson.D{{Key: "$gt", Value: opts.AfterVersion}}}}
		}
	}

	return findOpts, versionFilter
}
