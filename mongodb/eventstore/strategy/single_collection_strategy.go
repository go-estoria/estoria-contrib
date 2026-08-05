package strategy

import (
	"context"
	"errors"
	"fmt"

	"github.com/go-estoria/estoria"
	"github.com/go-estoria/estoria/eventstore"
	"github.com/go-estoria/estoria/typeid"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// A SingleCollectionStrategy stores all events for all streams in a single collection.
type SingleCollectionStrategy struct {
	mongo      MongoSessionStarter
	collection MongoCollection

	log      estoria.Logger
	sessOpts options.Lister[options.SessionOptions]
	txOpts   options.Lister[options.TransactionOptions]
}

// NewSingleCollectionStrategy creates a new SingleCollectionStrategy using the given client and collection.
func NewSingleCollectionStrategy(client MongoSessionStarter, collection MongoCollection, opts ...StrategyOption) (*SingleCollectionStrategy, error) {
	if client == nil {
		return nil, errors.New("client is required")
	} else if collection == nil {
		return nil, errors.New("collection is required")
	}

	config := newStrategyConfig()
	if err := config.apply(opts...); err != nil {
		return nil, fmt.Errorf("applying options: %w", err)
	}

	strat := &SingleCollectionStrategy{
		mongo:      client,
		collection: collection,

		log:      config.log,
		sessOpts: config.sessOpts,
		txOpts:   config.txOpts,
	}

	return strat, nil
}

// ListStreams returns a list of cursors for iterating over stream metadata.
func (s *SingleCollectionStrategy) ListStreams(ctx context.Context) ([]*mongo.Cursor, error) {
	cursor, err := getListStreamsCursor(ctx, s.collection)
	if err != nil {
		return nil, fmt.Errorf("getting streams cursor: %w", err)
	}

	return []*mongo.Cursor{cursor}, nil
}

// GetAllCursor returns an iterator over all events in the event store, ordered by global offset.
func (s *SingleCollectionStrategy) GetAllCursor(
	ctx context.Context,
	opts eventstore.ReadStreamOptions,
) ([]*mongo.Cursor, error) {
	findOpts, versionFilter := findOptsFromReadStreamOptions(opts, fieldGlobalOffset)
	filter := make(bson.D, 0, len(versionFilter))
	filter = append(filter, versionFilter...)
	cursor, err := s.collection.Find(ctx, filter, findOpts)
	if err != nil {
		return nil, fmt.Errorf("finding events: %w", err)
	}

	return []*mongo.Cursor{cursor}, nil
}

// GetStreamCursor returns an iterator over events in the specified stream, ordered by stream offset.
func (s *SingleCollectionStrategy) GetStreamCursor(
	ctx context.Context,
	streamID typeid.ID,
	opts eventstore.ReadStreamOptions,
) (*mongo.Cursor, error) {
	findOpts, versionFilter := findOptsFromReadStreamOptions(opts, fieldOffset)
	filter := make(bson.D, 0, 2+len(versionFilter))
	filter = append(filter,
		bson.E{Key: fieldStreamType, Value: streamID.Type},
		bson.E{Key: fieldStreamID, Value: streamID.UUID.String()},
	)
	filter = append(filter, versionFilter...)
	cursor, err := s.collection.Find(ctx, filter, findOpts)
	if err != nil {
		return nil, fmt.Errorf("finding events: %w", err)
	}

	return cursor, nil
}

// ExecuteInsertTransaction executes the given function within a new session suitable for inserting events.
// The function is executed within a transaction and is invoked with a session context, a collection,
// the current offset of the stream, and the global offset.
func (s *SingleCollectionStrategy) ExecuteInsertTransaction(
	ctx context.Context,
	streamID typeid.ID,
	inTxnFn func(sessCtx context.Context, coll MongoCollection, offset int64, globalOffset int64) (any, error),
) (any, error) {
	session, err := s.mongo.StartSession(s.sessOpts)
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

// StreamExists reports whether any event has ever been written to the stream. It exists so
// ReadStream can tell an absent stream from a filtered read that matched nothing.
func (s *SingleCollectionStrategy) StreamExists(ctx context.Context, streamID typeid.ID) (bool, error) {
	err := s.collection.FindOne(ctx, bson.D{
		{Key: fieldStreamType, Value: streamID.Type},
		{Key: fieldStreamID, Value: streamID.UUID.String()},
	}, options.FindOne().SetProjection(bson.D{{Key: fieldID, Value: 1}})).Err()
	if errors.Is(err, mongo.ErrNoDocuments) {
		return false, nil
	} else if err != nil {
		return false, fmt.Errorf("checking whether stream exists: %w", err)
	}

	return true, nil
}

// Finds the highest offset for the given stream.
func (s *SingleCollectionStrategy) getHighestOffset(ctx context.Context, streamID typeid.ID) (int64, error) {
	s.log.Debug("finding highest offset for stream", fieldStreamID, streamID)
	opts := options.FindOne().SetSort(bson.D{{Key: fieldOffset, Value: -1}})
	result := s.collection.FindOne(ctx, bson.D{
		{Key: fieldStreamType, Value: streamID.Type},
		{Key: fieldStreamID, Value: streamID.UUID.String()},
	}, opts)
	if result.Err() != nil {
		if errors.Is(result.Err(), mongo.ErrNoDocuments) {
			s.log.Debug("stream not found", fieldStreamID, streamID)
			return 0, nil
		}
		return 0, fmt.Errorf("finding highest offset: %w", result.Err())
	}

	offsets := Offsets{}
	if err := result.Decode(&offsets); err != nil {
		return 0, fmt.Errorf("decoding highest offset: %w", err)
	}

	s.log.Debug("got highest offset for stream", fieldStreamID, streamID, fieldOffset, offsets.Offset)
	return offsets.Offset, nil
}

// Finds the highest global offset among all events in the event store.
func (s *SingleCollectionStrategy) getHighestGlobalOffset(ctx context.Context) (int64, error) {
	s.log.Debug("finding highest global offset in event store")
	opts := options.FindOne().SetSort(bson.D{{Key: fieldGlobalOffset, Value: -1}})
	result := s.collection.FindOne(ctx, bson.D{}, opts)
	if result.Err() != nil {
		if errors.Is(result.Err(), mongo.ErrNoDocuments) {
			s.log.Debug("event store is empty")
			return 0, nil
		}
		return 0, fmt.Errorf("finding highest global offset: %w", result.Err())
	}

	offsets := Offsets{}
	if err := result.Decode(&offsets); err != nil {
		return 0, fmt.Errorf("decoding highest global offset: %w", err)
	}

	s.log.Debug("got highest global offset for event store", fieldGlobalOffset, offsets.GlobalOffset)
	return offsets.GlobalOffset, nil
}
