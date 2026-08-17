package strategy

import (
	"context"
	"errors"
	"fmt"
	"strings"

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
//	strategy, err := NewMultiCollectionStrategy(client, database, CollectionPerStreamID())
//
// The MultiCollectionStrategy is useful when the number of events in a single
// collection becomes too large, and you want to partition events across multiple
// collections.
//
// Alongside the event collections, the database holds a streams collection (named
// DefaultStreamsCollectionName unless overridden with WithStreamsCollectionName) with one
// counter document per stream plus the global offset counter. When event collections are
// enumerated, the streams collection and every collection whose name begins with an
// underscore are excluded: typeid type names cannot begin with an underscore, so that
// namespace is reserved for infrastructure collections (such as an outbox) sharing the
// database.
type MultiCollectionStrategy struct {
	mongo    MongoSessionStarter
	database MongoDatabase
	selector CollectionSelector
	streams  MongoCollection

	streamsCollectionName string
	autoEnsureIndexes     bool
	indexes               *indexEnsurer

	log      estoria.Logger
	sessOpts options.Lister[options.SessionOptions]
	txOpts   options.Lister[options.TransactionOptions]
}

// A CollectionSelector determines the collection name to use for a given stream ID
// when reading and storing events in a MultiCollectionStrategy.
type CollectionSelector interface {
	CollectionName(streamID typeid.ID) string
}

// A CollectionSelectorFunc is a function that returns a collection name for a given stream ID.
type CollectionSelectorFunc func(streamID typeid.ID) string

// CollectionName satisfies the CollectionSelector interface and returns the collection name
// for the given stream ID by invoking the CollectionSelectorFunc.
func (f CollectionSelectorFunc) CollectionName(streamID typeid.ID) string {
	return f(streamID)
}

// CollectionPerStreamType returns a CollectionSelector that returns the stream type name as the collection name.
func CollectionPerStreamType() CollectionSelector {
	return CollectionSelectorFunc(func(streamID typeid.ID) string {
		return streamID.Type
	})
}

// CollectionPerStreamID returns a CollectionSelector that returns the stream ID as the collection name.
func CollectionPerStreamID() CollectionSelector {
	return CollectionSelectorFunc(func(streamID typeid.ID) string {
		return streamID.String()
	})
}

// NewMultiCollectionStrategy creates a new MultiCollectionStrategy using the given client, database, and collection selector.
func NewMultiCollectionStrategy(client MongoSessionStarter, database MongoDatabase, selector CollectionSelector, opts ...StrategyOption) (*MultiCollectionStrategy, error) {
	switch {
	case client == nil:
		return nil, errors.New("client is required")
	case database == nil:
		return nil, errors.New("database is required")
	case selector == nil:
		return nil, errors.New("selector is required")
	}

	config := newStrategyConfig()
	if err := config.apply(opts...); err != nil {
		return nil, fmt.Errorf("applying options: %w", err)
	}

	strat := &MultiCollectionStrategy{
		mongo:    client,
		database: database,
		selector: selector,
		streams:  database.Collection(config.streamsCollectionName),

		streamsCollectionName: config.streamsCollectionName,
		autoEnsureIndexes:     config.autoEnsureIndexes,
		indexes:               newIndexEnsurer(),

		log:      config.log,
		sessOpts: config.sessOpts,
		txOpts:   config.txOpts,
	}

	return strat, nil
}

// EnsureIndexes creates the unique indexes on (stream_type, stream_id, offset) and on
// global_offset for every event collection currently in the database. It is idempotent,
// but covers only collections that exist when it is called; a selector that creates
// collections on the fly needs WithAutoEnsureIndexes for the collections it creates later.
func (s *MultiCollectionStrategy) EnsureIndexes(ctx context.Context) error {
	collectionNames, err := s.eventCollectionNames(ctx)
	if err != nil {
		return err
	}

	for _, name := range collectionNames {
		if err := s.indexes.ensureNow(ctx, name, s.database.Collection(name)); err != nil {
			return fmt.Errorf("collection %s: %w", name, err)
		}
	}

	return nil
}

// checkSelectedName rejects a selector-chosen collection name that a write path must not
// touch: the streams collection, or any name in the reserved underscore namespace (which
// event collection enumeration would then ignore, silently losing the stream's events
// from global reads).
func (s *MultiCollectionStrategy) checkSelectedName(collectionName string, streamID typeid.ID) error {
	if collectionName == s.streamsCollectionName || strings.HasPrefix(collectionName, "_") {
		return fmt.Errorf("collection selector chose reserved collection name %q for stream %s", collectionName, streamID)
	}

	return nil
}

// eventCollectionNames returns the names of the database's event collections, excluding
// the streams collection and the reserved underscore namespace.
func (s *MultiCollectionStrategy) eventCollectionNames(ctx context.Context) ([]string, error) {
	names, err := s.database.ListCollectionNames(ctx, bson.D{})
	if err != nil {
		return nil, fmt.Errorf("listing collection names: %w", err)
	}

	eventCollections := make([]string, 0, len(names))
	for _, name := range names {
		if name == s.streamsCollectionName || strings.HasPrefix(name, "_") {
			continue
		}
		eventCollections = append(eventCollections, name)
	}

	return eventCollections, nil
}

// ListStreams returns a list of cursors for iterating over stream metadata.
func (s *MultiCollectionStrategy) ListStreams(ctx context.Context) ([]*mongo.Cursor, error) {
	collections, err := s.eventCollectionNames(ctx)
	if err != nil {
		return nil, err
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

// GetAllCursor returns one cursor per event collection, each ordered by global offset
// and bounded above by the frontier captured here, so no cursor can chase appends
// committed after the read began. A Count limit applies per cursor; bounding the
// merged total is the iterator's job.
func (s *MultiCollectionStrategy) GetAllCursor(
	ctx context.Context,
	opts eventstore.ReadAllOptions,
) ([]*mongo.Cursor, error) {
	frontier, err := readGlobalFrontier(ctx, s.streams)
	if err != nil {
		return nil, err
	}

	collectionNames, err := s.eventCollectionNames(ctx)
	if err != nil {
		return nil, err
	}

	findOpts, positionFilter := findOptsFromReadAllOptions(opts, frontier)
	filter := make(bson.D, 0, len(positionFilter))
	filter = append(filter, positionFilter...)

	cursors := make([]*mongo.Cursor, len(collectionNames))
	for i, collectionName := range collectionNames {
		collection := s.database.Collection(collectionName)
		cursor, err := collection.Find(ctx, filter, findOpts)
		if err != nil {
			return nil, fmt.Errorf("finding events in collection %s: %w", collectionName, err)
		}

		cursors[i] = cursor
	}

	return cursors, nil
}

// GetStreamCursor returns an iterator over events in the specified stream, ordered by stream offset.
func (s *MultiCollectionStrategy) GetStreamCursor(
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
	collection := s.database.Collection(s.selector.CollectionName(streamID))
	cursor, err := collection.Find(ctx, filter, findOpts)
	if err != nil {
		return nil, fmt.Errorf("finding events: %w", err)
	}

	return cursor, nil
}

// ExecuteInsertTransaction executes the given function within a new session suitable for
// inserting numEvents events. The function is executed within a transaction, after offset
// and global offset ranges have been reserved in it, and is invoked with a session
// context, a collection, and the offsets preceding each reserved range.
func (s *MultiCollectionStrategy) ExecuteInsertTransaction(
	ctx context.Context,
	streamID typeid.ID,
	numEvents int,
	inTxnFn func(sessCtx context.Context, coll MongoCollection, offset int64, globalOffset int64) (any, error),
) (any, error) {
	collectionName := s.selector.CollectionName(streamID)
	if err := s.checkSelectedName(collectionName, streamID); err != nil {
		return nil, err
	}

	// Index creation cannot run inside the transaction, so it happens before the session starts.
	if s.autoEnsureIndexes {
		if err := s.indexes.ensureOnce(ctx, collectionName, s.database.Collection(collectionName)); err != nil {
			return nil, fmt.Errorf("ensuring indexes on collection %s: %w", collectionName, err)
		}
	}

	session, err := s.mongo.StartSession(s.sessOpts)
	if err != nil {
		return nil, fmt.Errorf("starting insert session: %w", err)
	}

	defer session.EndSession(ctx)

	result, err := session.WithTransaction(ctx, func(ctx context.Context) (any, error) {
		offset, err := reserveStreamOffsets(ctx, s.streams, streamID, numEvents)
		if err != nil {
			return nil, err
		}

		globalOffset, err := reserveGlobalOffsets(ctx, s.streams, numEvents)
		if err != nil {
			return nil, err
		}

		return inTxnFn(ctx, s.database.Collection(collectionName), offset, globalOffset)
	}, s.txOpts)
	if err != nil {
		return nil, fmt.Errorf("executing transaction: %w", err)
	}

	return result, nil
}

// DeleteStream deletes events from a stream within a transaction: all of them, and the
// stream itself, with zero options, or only events at or below ToVersion otherwise. A
// stream's events live in the single collection its selector names, so the transaction
// touches that collection and the streams collection.
func (s *MultiCollectionStrategy) DeleteStream(ctx context.Context, streamID typeid.ID, opts eventstore.DeleteStreamOptions) error {
	collectionName := s.selector.CollectionName(streamID)
	if err := s.checkSelectedName(collectionName, streamID); err != nil {
		return err
	}

	session, err := s.mongo.StartSession(s.sessOpts)
	if err != nil {
		return fmt.Errorf("starting delete session: %w", err)
	}

	defer session.EndSession(ctx)

	events := s.database.Collection(collectionName)

	if _, err := session.WithTransaction(ctx, func(ctx context.Context) (any, error) {
		return nil, deleteStreamDocs(ctx, s.streams, events, streamID, opts)
	}, s.txOpts); err != nil {
		return err
	}

	return nil
}

// StreamExists reports whether any event has ever been written to the stream. It exists so
// ReadStream can tell an absent stream from a filtered read that matched nothing.
func (s *MultiCollectionStrategy) StreamExists(ctx context.Context, streamID typeid.ID) (bool, error) {
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
