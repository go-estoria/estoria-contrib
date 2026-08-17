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

// A SingleCollectionStrategy stores all events for all streams in a single collection,
// alongside a streams collection holding one counter document per stream plus the global
// offset counter.
type SingleCollectionStrategy struct {
	mongo      MongoSessionStarter
	collection MongoCollection
	streams    MongoCollection

	autoEnsureIndexes bool
	indexes           *indexEnsurer

	log      estoria.Logger
	sessOpts options.Lister[options.SessionOptions]
	txOpts   options.Lister[options.TransactionOptions]
}

// NewSingleCollectionStrategy creates a new SingleCollectionStrategy using the given
// client, events collection, and streams collection.
func NewSingleCollectionStrategy(client MongoSessionStarter, events, streams MongoCollection, opts ...StrategyOption) (*SingleCollectionStrategy, error) {
	switch {
	case client == nil:
		return nil, errors.New("client is required")
	case events == nil:
		return nil, errors.New("events collection is required")
	case streams == nil:
		return nil, errors.New("streams collection is required")
	}

	config := newStrategyConfig()
	if err := config.apply(opts...); err != nil {
		return nil, fmt.Errorf("applying options: %w", err)
	}

	strat := &SingleCollectionStrategy{
		mongo:      client,
		collection: events,
		streams:    streams,

		autoEnsureIndexes: config.autoEnsureIndexes,
		indexes:           newIndexEnsurer(),

		log:      config.log,
		sessOpts: config.sessOpts,
		txOpts:   config.txOpts,
	}

	return strat, nil
}

// EnsureIndexes creates the events collection's unique indexes on
// (stream_type, stream_id, offset) and on global_offset. It is idempotent.
func (s *SingleCollectionStrategy) EnsureIndexes(ctx context.Context) error {
	return s.indexes.ensureNow(ctx, singleEventCollectionKey, s.collection)
}

// singleEventCollectionKey is the index-ensurer cache key for a SingleCollectionStrategy's
// sole event collection, whose real name the strategy never learns (it holds a handle).
const singleEventCollectionKey = ""

// ListStreams returns a list of cursors for iterating over stream metadata.
func (s *SingleCollectionStrategy) ListStreams(ctx context.Context) ([]*mongo.Cursor, error) {
	cursor, err := getListStreamsCursor(ctx, s.collection)
	if err != nil {
		return nil, fmt.Errorf("getting streams cursor: %w", err)
	}

	return []*mongo.Cursor{cursor}, nil
}

// GetAllCursor returns an iterator over all events in the event store, ordered by
// global offset and bounded above by the frontier captured here, so the cursor cannot
// chase appends committed after the read began.
func (s *SingleCollectionStrategy) GetAllCursor(
	ctx context.Context,
	opts eventstore.ReadAllOptions,
) ([]*mongo.Cursor, error) {
	frontier, err := readGlobalFrontier(ctx, s.streams)
	if err != nil {
		return nil, err
	}

	findOpts, positionFilter := findOptsFromReadAllOptions(opts, frontier)
	filter := make(bson.D, 0, len(positionFilter))
	filter = append(filter, positionFilter...)
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

// ExecuteInsertTransaction executes the given function within a new session suitable for
// inserting numEvents events. The function is executed within a transaction, after offset
// and global offset ranges have been reserved in it, and is invoked with a session
// context, a collection, and the offsets preceding each reserved range.
func (s *SingleCollectionStrategy) ExecuteInsertTransaction(
	ctx context.Context,
	streamID typeid.ID,
	numEvents int,
	inTxnFn func(sessCtx context.Context, coll MongoCollection, offset int64, globalOffset int64) (any, error),
) (any, error) {
	// Index creation cannot run inside the transaction, so it happens before the session starts.
	if s.autoEnsureIndexes {
		if err := s.indexes.ensureOnce(ctx, singleEventCollectionKey, s.collection); err != nil {
			return nil, fmt.Errorf("ensuring indexes: %w", err)
		}
	}

	session, err := s.mongo.StartSession(s.sessOpts)
	if err != nil {
		return nil, fmt.Errorf("starting insert session: %w", err)
	}

	defer session.EndSession(ctx)

	result, err := session.WithTransaction(ctx, func(ctx context.Context) (interface{}, error) {
		offset, err := reserveStreamOffsets(ctx, s.streams, streamID, numEvents)
		if err != nil {
			return nil, err
		}

		globalOffset, err := reserveGlobalOffsets(ctx, s.streams, numEvents)
		if err != nil {
			return nil, err
		}

		return inTxnFn(ctx, s.collection, offset, globalOffset)
	}, s.txOpts)
	if err != nil {
		return nil, fmt.Errorf("executing transaction: %w", err)
	}

	return result, nil
}

// DeleteStream deletes events from a stream within a transaction: all of them, and the
// stream itself, with zero options, or only events at or below ToVersion otherwise.
func (s *SingleCollectionStrategy) DeleteStream(ctx context.Context, streamID typeid.ID, opts eventstore.DeleteStreamOptions) error {
	session, err := s.mongo.StartSession(s.sessOpts)
	if err != nil {
		return fmt.Errorf("starting delete session: %w", err)
	}

	defer session.EndSession(ctx)

	if _, err := session.WithTransaction(ctx, func(ctx context.Context) (any, error) {
		return nil, deleteStreamDocs(ctx, s.streams, s.collection, streamID, opts)
	}, s.txOpts); err != nil {
		return err
	}

	return nil
}

// StreamExists reports whether any event has ever been written to the stream. It exists so
// ReadStream can tell an absent stream from a filtered read that matched nothing.
func (s *SingleCollectionStrategy) StreamExists(ctx context.Context, streamID typeid.ID) (bool, error) {
	err := s.streams.FindOne(ctx,
		bson.D{{Key: fieldID, Value: streamID.String()}},
		options.FindOne().SetProjection(bson.D{{Key: fieldID, Value: 1}}),
	).Err()
	if errors.Is(err, mongo.ErrNoDocuments) {
		return false, nil
	} else if err != nil {
		return false, fmt.Errorf("checking whether stream exists: %w", err)
	}

	return true, nil
}
