package strategy

import (
	"context"
	"fmt"

	"github.com/go-estoria/estoria"
	"github.com/go-estoria/estoria/eventstore"
	"github.com/go-estoria/estoria/typeid"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// A MultiCollectionStrategy stores events in multiple collections,
// with the collection name derived from the stream ID.
//
// By default, all events for a given stream type are stored in the same collection.
// This can be overridden by providing a custom CollectionSelector using the
// WithCollectionSelector option. For example, to store all events for a given
// stream ID in the same collection:
//
//	strategy, err := NewMultiCollectionStrategy(client, database, StreamIDCollectionSelector())
//
// The MultiCollectionStrategy is useful when the number of events in a single
// collection becomes too large, and you want to partition events across multiple
// collections.
type MultiCollectionStrategy struct {
	mongo    MongoClient
	database MongoDatabase
	selector CollectionSelector

	log       estoria.Logger
	marshaler DocumentMarshaler
	sessOpts  options.Lister[options.SessionOptions]
	txOpts    options.Lister[options.TransactionOptions]
}

type CollectionSelector interface {
	CollectionName(streamID typeid.UUID) string
}

type CollectionSelectorFunc func(streamID typeid.UUID) string

func (f CollectionSelectorFunc) CollectionName(streamID typeid.UUID) string {
	return f(streamID)
}

func StreamTypeCollectionSelector() CollectionSelectorFunc {
	return func(streamID typeid.UUID) string {
		return streamID.TypeName()
	}
}

func StreamIDCollectionSelector() CollectionSelectorFunc {
	return func(streamID typeid.UUID) string {
		return streamID.String()
	}
}

func NewMultiCollectionStrategy(client MongoClient, database MongoDatabase, opts ...MultiCollectionStrategyOption) (*MultiCollectionStrategy, error) {
	if client == nil {
		return nil, fmt.Errorf("client is required")
	} else if database == nil {
		return nil, fmt.Errorf("database is required")
	}

	strat := &MultiCollectionStrategy{
		mongo:    client,
		database: database,
		selector: StreamTypeCollectionSelector(),
	}

	for _, opt := range opts {
		if err := opt(strat); err != nil {
			return nil, fmt.Errorf("applying option: %w", err)
		}
	}

	return strat, nil
}

// Initialize initializes the strategy with the given logger, marshaler, session options, and transaction options.
func (s *MultiCollectionStrategy) Initialize(
	logger estoria.Logger,
	marshaler DocumentMarshaler,
	sessOpts *options.SessionOptionsBuilder,
	txOpts *options.TransactionOptionsBuilder,
) error {
	s.log = logger.WithGroup("strategy")
	s.marshaler = marshaler
	s.sessOpts = sessOpts
	s.txOpts = txOpts
	return nil
}

// ListStreams returns a list of cursors for iterating over stream metadata.
func (s *MultiCollectionStrategy) ListStreams(ctx context.Context) ([]*mongo.Cursor, error) {
	collections, err := s.database.ListCollectionNames(ctx, bson.D{})
	if err != nil {
		return nil, fmt.Errorf("listing collection names: %w", err)
	}

	cursors := make([]*mongo.Cursor, len(collections))
	for i, collectionName := range collections {
		collection := s.database.Collection(collectionName)
		cursor, err := getListStreamsCursor(ctx, collection)
		if err != nil {
			return nil, fmt.Errorf("getting streams cursor: %w", err)
		}

		cursors[i] = cursor
	}

	return cursors, nil
}

// GetAllIterator returns an iterator over all events in the event store, ordered by global offset.
func (s *MultiCollectionStrategy) GetAllIterator(
	ctx context.Context,
	opts eventstore.ReadStreamOptions,
) (eventstore.StreamIterator, error) {
	collectionNames, err := s.database.ListCollectionNames(ctx, bson.D{})
	if err != nil {
		return nil, fmt.Errorf("listing collection names: %w", err)
	}

	cursors := make([]*multiStreamIteratorCursor, len(collectionNames))
	for i, collectionName := range collectionNames {
		collection := s.database.Collection(collectionName)
		cursor, err := collection.Find(ctx, bson.D{}, findOptsFromReadStreamOptions(opts, "global_offset"))
		if err != nil {
			return nil, fmt.Errorf("finding events in collection %s: %w", collectionName, err)
		}

		cursors[i] = &multiStreamIteratorCursor{
			cursor: cursor,
		}
	}

	return &multiStreamIterator{
		cursors:   cursors,
		marshaler: s.marshaler,
	}, nil
}

// GetStreamIterator returns an iterator over events in the specified stream, ordered by stream offset.
func (s *MultiCollectionStrategy) GetStreamIterator(
	ctx context.Context,
	streamID typeid.UUID,
	opts eventstore.ReadStreamOptions,
) (eventstore.StreamIterator, error) {
	collection := s.database.Collection(s.selector.CollectionName(streamID))
	cursor, err := collection.Find(ctx, bson.D{
		{Key: "stream_type", Value: streamID.TypeName()},
		{Key: "stream_id", Value: streamID.Value()},
	}, findOptsFromReadStreamOptions(opts, "offset"))
	if err != nil {
		return nil, fmt.Errorf("finding events: %w", err)
	}

	return &streamIterator{
		cursor:    cursor,
		marshaler: s.marshaler,
	}, nil
}

// DoInInsertSession executes the given function within a new session suitable for inserting events.
// The function is executed within a transaction and is invoked with a session context, a collection,
// the current offset of the stream, and the global offset.
func (s *MultiCollectionStrategy) DoInInsertSession(
	ctx context.Context,
	streamID typeid.UUID,
	inTxnFn func(sessCtx context.Context, coll MongoCollection, offset int64, globalOffset int64) (any, error),
) (any, error) {
	session, err := s.mongo.StartSession(s.sessOpts)
	if err != nil {
		return nil, fmt.Errorf("starting insert session: %w", err)
	}

	defer session.EndSession(ctx)

	// cannot be done in the transaction; requires listing all collections
	globalOffset, err := s.getHighestGlobalOffset(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting highest global offset: %w", err)
	}

	result, err := session.WithTransaction(ctx, func(ctx context.Context) (any, error) {
		offset, err := s.getHighestOffset(ctx, streamID)
		if err != nil {
			return nil, fmt.Errorf("getting highest offset: %w", err)
		}

		collection := s.database.Collection(s.selector.CollectionName(streamID))

		return inTxnFn(ctx, collection, offset, globalOffset)
	}, s.txOpts)
	if err != nil {
		return nil, fmt.Errorf("executing transaction: %w", err)
	}

	return result, nil
}

func (s *MultiCollectionStrategy) getHighestOffset(ctx context.Context, streamID typeid.UUID) (int64, error) {
	collection := s.database.Collection(streamID.String())

	opts := options.FindOne().SetSort(bson.D{{Key: "version", Value: -1}})
	offsets := Offsets{}
	if err := collection.FindOne(ctx, bson.D{}, opts).Decode(&offsets); err != nil {
		if err == mongo.ErrNoDocuments {
			return 0, nil
		}

		return 0, fmt.Errorf("finding latest version: %w", err)
	}

	return offsets.Offset, nil
}

// Finds the highest global offset among all events in the event store.
func (s *MultiCollectionStrategy) getHighestGlobalOffset(ctx context.Context) (int64, error) {
	s.log.Debug("finding highest global offset in event store")

	streamIDs, err := s.database.ListCollectionNames(ctx, bson.D{})
	if err != nil {
		return 0, fmt.Errorf("listing collection names: %w", err)
	}

	opts := options.FindOne().SetSort(bson.D{{Key: "global_offset", Value: -1}})

	highestGlobalOffset := int64(0)
	for _, streamID := range streamIDs {
		collection := s.database.Collection(streamID)
		result := collection.FindOne(ctx, bson.D{}, opts)
		if result.Err() != nil {
			if result.Err() == mongo.ErrNoDocuments {
				s.log.Debug("collection for stream is empty", "collection", streamID)
				return 0, nil
			}
			return 0, fmt.Errorf("finding highest global offset in collection: %w", result.Err())
		}

		offsets := Offsets{}
		if err := result.Decode(&offsets); err != nil {
			return 0, fmt.Errorf("decoding highest global offset: %w", err)
		}

		s.log.Info("got highest global offset for collection", "collection", streamID, "global_offset", offsets.GlobalOffset)

		if offsets.GlobalOffset > highestGlobalOffset {
			s.log.Info("updating highest global offset", "global_offset", offsets.GlobalOffset)
			highestGlobalOffset = offsets.GlobalOffset
		}
	}

	s.log.Info("got highest global offset for event store", "global_offset", highestGlobalOffset)
	return highestGlobalOffset, nil
}

// A MultiCollectionStrategyOption configures a MultiCollectionStrategy.
type MultiCollectionStrategyOption func(*MultiCollectionStrategy) error

// WithCollectionSelector sets the collection selector used by the strategy.
func WithCollectionSelector(selector CollectionSelector) MultiCollectionStrategyOption {
	return func(s *MultiCollectionStrategy) error {
		s.selector = selector
		return nil
	}
}
