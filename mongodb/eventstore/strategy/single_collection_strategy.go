package strategy

import (
	"context"
	"fmt"
	"log"

	"github.com/go-estoria/estoria"
	"github.com/go-estoria/estoria/eventstore"
	"github.com/go-estoria/estoria/typeid"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type SingleCollectionStrategy struct {
	mongo      MongoClient
	collection MongoCollection
	log        estoria.Logger
	marshaler  DocumentMarshaler
	txOpts     options.Lister[options.TransactionOptions]
}

// NewSingleCollectionStrategy creates a new SingleCollectionStrategy using the given collection.
func NewSingleCollectionStrategy(client MongoClient, collection MongoCollection) (*SingleCollectionStrategy, error) {
	if client == nil {
		return nil, fmt.Errorf("client is required")
	} else if collection == nil {
		return nil, fmt.Errorf("collection is required")
	}

	strategy := &SingleCollectionStrategy{
		mongo:      client,
		collection: collection,
		log:        estoria.GetLogger().WithGroup("eventstore"),
		marshaler:  DefaultMarshaler{},
		txOpts:     DefaultTransactionOptions(),
	}

	return strategy, nil
}

// GetAllIterator returns an iterator over all events in the event store.
func (s *SingleCollectionStrategy) GetAllIterator(
	ctx context.Context,
	opts eventstore.ReadStreamOptions,
) (eventstore.StreamIterator, error) {
	cursor, err := s.collection.Find(ctx, bson.D{}, findOptsFromReadStreamOptions(opts, "global_offset"))
	if err != nil {
		return nil, fmt.Errorf("finding events: %w", err)
	}

	return &streamIterator{
		cursor:    cursor,
		marshaler: s.marshaler,
	}, nil
}

// GetStreamIterator returns an iterator over events in the stream with the given ID.
func (s *SingleCollectionStrategy) GetStreamIterator(
	ctx context.Context,
	streamID typeid.UUID,
	opts eventstore.ReadStreamOptions,
) (eventstore.StreamIterator, error) {

	cursor, err := s.collection.Find(ctx, bson.D{
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

func (s *SingleCollectionStrategy) DoInInsertSession(
	ctx context.Context,
	streamID typeid.UUID,
	inTxnFn func(sessCtx context.Context, coll MongoCollection, offset int64, globalOffset int64) (any, error),
) (any, error) {
	session, err := s.mongo.StartSession()
	if err != nil {
		return nil, fmt.Errorf("starting insert session: %w", err)
	}

	defer session.EndSession(ctx)

	result, err := session.WithTransaction(ctx, func(ctx context.Context) (interface{}, error) {
		offset, err := s.getHighestOffset(ctx, streamID)
		if err != nil {
			return nil, fmt.Errorf("getting highest offset: %w", err)
		}

		globalOffset, err := s.getHighestGlobalOffset(ctx)
		if err != nil {
			return nil, fmt.Errorf("getting highest global offset: %w", err)
		}

		return inTxnFn(ctx, s.collection, offset, globalOffset)
	}, s.txOpts)
	if err != nil {
		return nil, fmt.Errorf("executing transaction: %w", err)
	}

	return result, nil
}

// ListStreams returns a cursor over the streams in the event store.
func (s *SingleCollectionStrategy) ListStreams(ctx context.Context) ([]*mongo.Cursor, error) {
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

	cursor, err := s.collection.Aggregate(ctx, pipeline)
	if err != nil {
		log.Fatal(err)
	}

	return []*mongo.Cursor{cursor}, nil
}

// SetLogger sets the logger used by the strategy.
func (s *SingleCollectionStrategy) SetLogger(l estoria.Logger) {
	s.log = l
}

// Finds the highest offset for the given stream.
func (s *SingleCollectionStrategy) getHighestOffset(ctx context.Context, streamID typeid.UUID) (int64, error) {
	s.log.Debug("finding highest offset for stream", "stream_id", streamID)
	opts := options.FindOne().SetSort(bson.D{{Key: "offset", Value: -1}})
	result := s.collection.FindOne(ctx, bson.D{
		{Key: "stream_type", Value: streamID.TypeName()},
		{Key: "stream_id", Value: streamID.Value()},
	}, opts)
	if result.Err() != nil {
		if result.Err() == mongo.ErrNoDocuments {
			s.log.Debug("stream not found", "stream_id", streamID)
			return 0, nil
		}
		return 0, fmt.Errorf("finding highest offset: %w", result.Err())
	}

	offsets := Offsets{}
	if err := result.Decode(&offsets); err != nil {
		return 0, fmt.Errorf("decoding highest offset: %w", err)
	}

	s.log.Info("got highest offset for stream", "stream_id", streamID, "offset", offsets.Offset)
	return offsets.Offset, nil
}

// Finds the highest global offset among all events in the event store.
func (s *SingleCollectionStrategy) getHighestGlobalOffset(ctx context.Context) (int64, error) {
	s.log.Debug("finding highest global offset in event store")
	opts := options.FindOne().SetSort(bson.D{{Key: "global_offset", Value: -1}})
	result := s.collection.FindOne(ctx, bson.D{}, opts)
	if result.Err() != nil {
		if result.Err() == mongo.ErrNoDocuments {
			s.log.Debug("event store is empty")
			return 0, nil
		}
		return 0, fmt.Errorf("finding highest global offset: %w", result.Err())
	}

	offsets := Offsets{}
	if err := result.Decode(&offsets); err != nil {
		return 0, fmt.Errorf("decoding highest global offset: %w", err)
	}

	s.log.Info("got highest global offset for event store", "global_offset", offsets.GlobalOffset)
	return offsets.GlobalOffset, nil
}
