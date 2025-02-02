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

type CollectionPerStreamStrategy struct {
	mongo     MongoClient
	database  MongoDatabase
	log       estoria.Logger
	marshaler DocumentMarshaler
	txOpts    options.Lister[options.TransactionOptions]
}

func NewCollectionPerStreamStrategy(client MongoClient, db MongoDatabase) (*CollectionPerStreamStrategy, error) {
	if client == nil {
		return nil, fmt.Errorf("client is required")
	} else if db == nil {
		return nil, fmt.Errorf("database is required")
	}

	strategy := &CollectionPerStreamStrategy{
		mongo:     client,
		database:  db,
		log:       estoria.GetLogger().WithGroup("eventstore"),
		marshaler: DefaultMarshaler{},
	}

	return strategy, nil
}

func (s *CollectionPerStreamStrategy) GetAllEventsIterator(
	ctx context.Context,
	opts eventstore.ReadStreamOptions,
) (eventstore.StreamIterator, error) {
	streamIDs, err := s.database.ListCollectionNames(ctx, bson.D{})
	if err != nil {
		return nil, fmt.Errorf("listing collection names: %w", err)
	}

	cursors := make([]*multiStreamIteratorCursor, len(streamIDs))
	for i, streamID := range streamIDs {
		collection := s.database.Collection(streamID)
		cursor, err := collection.Find(ctx, bson.D{}, findOptsFromReadStreamOptions(opts, "global_offset"))
		if err != nil {
			return nil, fmt.Errorf("finding events in collection %s: %w", streamID, err)
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

func (s *CollectionPerStreamStrategy) GetStreamIterator(
	ctx context.Context,
	streamID typeid.UUID,
	opts eventstore.ReadStreamOptions,
) (eventstore.StreamIterator, error) {
	collection := s.database.Collection(streamID.String())

	findOpts := options.Find()
	if opts.Count > 0 {
		findOpts = findOpts.SetLimit(opts.Count)
	}

	cursor, err := collection.Find(ctx, bson.D{
		{Key: "version", Value: bson.D{{Key: "$gt", Value: opts.Offset}}},
	}, findOpts)
	if err != nil {
		return nil, fmt.Errorf("finding events: %w", err)
	}

	return &streamIterator{
		cursor:    cursor,
		marshaler: s.marshaler,
	}, nil
}

func (s *CollectionPerStreamStrategy) DoInInsertSession(
	ctx context.Context,
	streamID typeid.UUID,
	inTxnFn func(sessCtx context.Context, offset int64, globalOffset int64) (any, error),
) (any, error) {
	session, err := s.mongo.StartSession()
	if err != nil {
		return nil, fmt.Errorf("starting insert session: %w", err)
	}

	// cannot be done in a transaction; requires listing all collections
	offset, err := s.getHighestOffset(ctx, streamID)
	if err != nil {
		return nil, fmt.Errorf("getting highest offset: %w", err)
	}

	// cannot be done in a transaction; requires listing all collections
	globalOffset, err := s.getHighestGlobalOffset(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting highest global offset: %w", err)
	}

	result, err := session.WithTransaction(ctx, func(ctx context.Context) (any, error) {
		return inTxnFn(ctx, offset, globalOffset)
	}, s.txOpts)
	if err != nil {
		return nil, fmt.Errorf("executing transaction: %w", err)
	}

	return result, nil
}

func (s *CollectionPerStreamStrategy) InsertStreamDocs(
	ctx context.Context,
	streamID typeid.UUID,
	docs []any,
) (*InsertStreamEventsResult, error) {
	collection := s.database.Collection(streamID.String())
	result, err := collection.InsertMany(ctx, docs)
	if err != nil {
		s.log.Error("error while inserting events", "error", err)
		return nil, fmt.Errorf("inserting events: %w", err)
	} else if len(result.InsertedIDs) != len(docs) {
		return nil, fmt.Errorf("inserted %d events, but expected %d", len(result.InsertedIDs), len(docs))
	}

	return &InsertStreamEventsResult{
		MongoResult: result,
	}, nil
}

// ListStreams returns a cursor over the streams in the event store.
func (s *CollectionPerStreamStrategy) ListStreams(ctx context.Context) ([]*mongo.Cursor, error) {
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

func (s *CollectionPerStreamStrategy) SetLogger(l estoria.Logger) {
	s.log = l
}

func (s *CollectionPerStreamStrategy) getHighestOffset(ctx context.Context, streamID typeid.UUID) (int64, error) {
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
func (s *CollectionPerStreamStrategy) getHighestGlobalOffset(ctx context.Context) (int64, error) {
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
