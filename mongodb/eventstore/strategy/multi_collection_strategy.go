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

type MultiCollectionStrategy struct {
	mongo     MongoClient
	database  MongoDatabase
	selector  CollectionSelector
	log       estoria.Logger
	marshaler DocumentMarshaler
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

func NewMultiCollectionStrategy(client MongoClient, database MongoDatabase, selector CollectionSelector) (*MultiCollectionStrategy, error) {
	if client == nil {
		return nil, fmt.Errorf("client is required")
	} else if database == nil {
		return nil, fmt.Errorf("database is required")
	}

	if selector == nil {
		// by default, store all streams of the same type in the same collection
		selector = StreamTypeCollectionSelector()
	}

	strategy := &MultiCollectionStrategy{
		mongo:     client,
		database:  database,
		selector:  selector,
		log:       estoria.GetLogger().WithGroup("eventstore"),
		marshaler: DefaultMarshaler{},
	}

	return strategy, nil
}

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

func (s *MultiCollectionStrategy) DoInInsertSession(
	ctx context.Context,
	streamID typeid.UUID,
	inTxnFn func(sessCtx context.Context, coll MongoCollection, offset int64, globalOffset int64) (any, error),
) (any, error) {
	session, err := s.mongo.StartSession()
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

// ListStreams returns a cursor over the streams in the event store.
func (s *MultiCollectionStrategy) ListStreams(ctx context.Context) ([]*mongo.Cursor, error) {
	collections, err := s.database.ListCollectionNames(ctx, bson.D{})
	if err != nil {
		return nil, fmt.Errorf("listing collection names: %w", err)
	}

	cursors := make([]*mongo.Cursor, len(collections))
	for i, collectionName := range collections {
		pipeline := mongo.Pipeline{
			{{Key: "$sort", Value: bson.D{
				{Key: "stream_id", Value: 1}, // Group documents together by stream_id.
				{Key: "offset", Value: -1},   // Highest offset comes first within each stream.
			}}},
			{{Key: "$group", Value: bson.D{
				{Key: "_id", Value: "$stream_id"}, // Group key is stream_id.
				{Key: "stream_type", Value: bson.D{{Key: "$first", Value: "$stream_type"}}},
				{Key: "offset", Value: bson.D{{Key: "$first", Value: "$offset"}}},
				{Key: "global_offset", Value: bson.D{{Key: "$first", Value: "$global_offset"}}},
			}}},
		}

		collection := s.database.Collection(collectionName)
		cursor, err := collection.Aggregate(ctx, pipeline)
		if err != nil {
			return nil, fmt.Errorf("aggregating collection: %w", err)
		}

		cursors[i] = cursor
	}

	return cursors, nil
}

func (s *MultiCollectionStrategy) SetLogger(l estoria.Logger) {
	s.log = l
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
