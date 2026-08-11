package strategy

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/go-estoria/estoria/eventstore"
	"github.com/go-estoria/estoria/typeid"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
)

// Document field names shared by both strategies.
const (
	// fieldID is MongoDB's own document key, not one of ours.
	fieldID           = "_id"
	fieldStreamType   = "stream_type"
	fieldStreamID     = "stream_id"
	fieldOffset       = "offset"
	fieldGlobalOffset = "global_offset"
	fieldLastOffset   = "last_offset"

	opFirst = "$first"
)

// DefaultStreamsCollectionName is the default name of the collection holding stream
// documents and the global offset counter. The leading underscore keeps it out of the
// namespace either collection selector can produce, since typeid type names cannot begin
// with an underscore.
const DefaultStreamsCollectionName = "_streams"

// globalCounterID is the _id of the streams collection's global offset counter document.
// No stream document can collide with it: stream _ids are typeid strings, whose type
// names cannot begin with an underscore.
const globalCounterID = "_global"

type (
	// MongoDatabase provides an API for obtaining a collection handle.
	MongoDatabase interface {
		Collection(string, ...options.Lister[options.CollectionOptions]) *mongo.Collection
		ListCollectionNames(ctx context.Context, filter any, opts ...options.Lister[options.ListCollectionsOptions]) ([]string, error)
	}

	// MongoCollection provides an API for querying, inserting, and deleting documents in a MongoDB collection.
	MongoCollection interface {
		Aggregate(context.Context, any, ...options.Lister[options.AggregateOptions]) (*mongo.Cursor, error)
		DeleteMany(context.Context, any, ...options.Lister[options.DeleteManyOptions]) (*mongo.DeleteResult, error)
		DeleteOne(context.Context, any, ...options.Lister[options.DeleteOneOptions]) (*mongo.DeleteResult, error)
		Find(context.Context, any, ...options.Lister[options.FindOptions]) (*mongo.Cursor, error)
		FindOne(context.Context, any, ...options.Lister[options.FindOneOptions]) *mongo.SingleResult
		FindOneAndUpdate(context.Context, any, any, ...options.Lister[options.FindOneAndUpdateOptions]) *mongo.SingleResult
		Indexes() mongo.IndexView
		InsertMany(context.Context, any, ...options.Lister[options.InsertManyOptions]) (*mongo.InsertManyResult, error)
	}

	MongoSessionStarter interface {
		StartSession(opts ...options.Lister[options.SessionOptions]) (*mongo.Session, error)
	}
)

// Names of the indexes ensured on every event collection.
const (
	uniqueStreamOffsetIndexName = "uniq_stream_offset"
	uniqueGlobalOffsetIndexName = "uniq_global_offset"
)

// ensureEventCollectionIndexes creates an event collection's two unique indexes: one on
// (stream_type, stream_id, offset), which serves per-stream reads and backstops offset
// reservation, and one on global_offset, which serves global reads. Creation is
// idempotent, but fails if a differently-specified index already uses either name, and
// cannot run inside a transaction.
func ensureEventCollectionIndexes(ctx context.Context, events MongoCollection) error {
	if _, err := events.Indexes().CreateMany(ctx, []mongo.IndexModel{
		{
			Keys: bson.D{
				{Key: fieldStreamType, Value: 1},
				{Key: fieldStreamID, Value: 1},
				{Key: fieldOffset, Value: 1},
			},
			Options: options.Index().SetUnique(true).SetName(uniqueStreamOffsetIndexName),
		},
		{
			Keys:    bson.D{{Key: fieldGlobalOffset, Value: 1}},
			Options: options.Index().SetUnique(true).SetName(uniqueGlobalOffsetIndexName),
		},
	}); err != nil {
		return fmt.Errorf("creating event collection indexes: %w", err)
	}

	return nil
}

// An indexEnsurer records which event collections have had their indexes ensured, so the
// auto-ensure path costs one round trip per collection per process rather than one per
// append.
type indexEnsurer struct {
	mu   sync.Mutex
	done map[string]bool
}

func newIndexEnsurer() *indexEnsurer {
	return &indexEnsurer{done: map[string]bool{}}
}

// ensureNow unconditionally (re)creates the named collection's indexes and records the fact.
func (e *indexEnsurer) ensureNow(ctx context.Context, name string, events MongoCollection) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if err := ensureEventCollectionIndexes(ctx, events); err != nil {
		return err
	}

	e.done[name] = true
	return nil
}

// ensureOnce creates the named collection's indexes unless a prior call already has.
func (e *indexEnsurer) ensureOnce(ctx context.Context, name string, events MongoCollection) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.done[name] {
		return nil
	}

	if err := ensureEventCollectionIndexes(ctx, events); err != nil {
		return err
	}

	e.done[name] = true
	return nil
}

// reserveStreamOffsets atomically claims n consecutive per-stream offsets by incrementing
// the stream document's counter, creating the document on a stream's first append. It
// returns the offset preceding the claimed range. Callers must invoke it inside the append
// transaction so an aborted append rolls the reservation back.
func reserveStreamOffsets(ctx context.Context, streams MongoCollection, streamID typeid.ID, n int) (int64, error) {
	result := streams.FindOneAndUpdate(ctx,
		bson.D{{Key: fieldID, Value: streamID.String()}},
		bson.D{
			{Key: "$inc", Value: bson.D{{Key: fieldLastOffset, Value: int64(n)}}},
			{Key: "$setOnInsert", Value: bson.D{
				{Key: fieldStreamType, Value: streamID.Type},
				{Key: fieldStreamID, Value: streamID.UUID.String()},
			}},
		},
		options.FindOneAndUpdate().SetUpsert(true).SetReturnDocument(options.After),
	)

	var doc struct {
		LastOffset int64 `bson:"last_offset"`
	}
	if err := result.Decode(&doc); err != nil {
		return 0, fmt.Errorf("reserving stream offsets: %w", err)
	}

	return doc.LastOffset - int64(n), nil
}

// reserveGlobalOffsets atomically claims n consecutive global offsets by incrementing the
// streams collection's counter document, returning the offset preceding the claimed range.
// Callers must invoke it inside the append transaction so an aborted append rolls the
// reservation back.
func reserveGlobalOffsets(ctx context.Context, streams MongoCollection, n int) (int64, error) {
	result := streams.FindOneAndUpdate(ctx,
		bson.D{{Key: fieldID, Value: globalCounterID}},
		bson.D{{Key: "$inc", Value: bson.D{{Key: fieldLastOffset, Value: int64(n)}}}},
		options.FindOneAndUpdate().SetUpsert(true).SetReturnDocument(options.After),
	)

	var doc struct {
		LastOffset int64 `bson:"last_offset"`
	}
	if err := result.Decode(&doc); err != nil {
		return 0, fmt.Errorf("reserving global offsets: %w", err)
	}

	return doc.LastOffset - int64(n), nil
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

// deleteStreamDocs deletes a stream's events and, on a full delete, its stream document,
// within the caller's transaction. Whether the stream exists is decided by its stream
// document, never by its event count — a truncated-empty stream holds no event documents
// yet exists — and an absent document reports eventstore.ErrStreamNotFound. With
// ToVersion 0 both the events and the stream document are deleted, so a subsequent
// append recreates the stream from version 1; with ToVersion > 0 only events at or below
// the bound are deleted and the document's last_offset survives, so appends continue
// from the existing tip even when truncation emptied the stream.
func deleteStreamDocs(ctx context.Context, streams, events MongoCollection, streamID typeid.ID, opts eventstore.DeleteStreamOptions) error {
	if err := streams.FindOne(ctx,
		bson.D{{Key: fieldID, Value: streamID.String()}},
		options.FindOne().SetProjection(bson.D{{Key: fieldID, Value: 1}}),
	).Err(); errors.Is(err, mongo.ErrNoDocuments) {
		return eventstore.ErrStreamNotFound
	} else if err != nil {
		return fmt.Errorf("querying stream: %w", err)
	}

	filter := bson.D{
		{Key: fieldStreamType, Value: streamID.Type},
		{Key: fieldStreamID, Value: streamID.UUID.String()},
	}
	if opts.ToVersion > 0 {
		filter = append(filter, bson.E{Key: fieldOffset, Value: bson.D{{Key: "$lte", Value: opts.ToVersion}}})
	}

	if _, err := events.DeleteMany(ctx, filter); err != nil {
		return fmt.Errorf("deleting events: %w", err)
	}

	if opts.ToVersion == 0 {
		if _, err := streams.DeleteOne(ctx, bson.D{{Key: fieldID, Value: streamID.String()}}); err != nil {
			return fmt.Errorf("deleting stream: %w", err)
		}
	}

	return nil
}

// findOptsFromReadAllOptions maps global-read options onto a Find: ascending global
// offset order, an exclusive lower bound when AfterPosition is set, and a per-cursor
// limit when Count is set.
func findOptsFromReadAllOptions(opts eventstore.ReadAllOptions) (options.Lister[options.FindOptions], bson.D) {
	findOpts := options.Find().SetSort(bson.D{{Key: fieldGlobalOffset, Value: 1}})
	if opts.Count > 0 {
		findOpts.SetLimit(opts.Count)
	}

	var filter bson.D
	if opts.AfterPosition > 0 {
		filter = bson.D{{Key: fieldGlobalOffset, Value: bson.D{{Key: "$gt", Value: opts.AfterPosition}}}}
	}

	return findOpts, filter
}

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

func getListStreamsCursor(ctx context.Context, collection MongoCollection) (*mongo.Cursor, error) {
	pipeline := mongo.Pipeline{
		{{Key: "$sort", Value: bson.D{
			{Key: fieldStreamID, Value: 1}, // Group documents together by stream_id.
			{Key: fieldOffset, Value: -1},  // Highest offset comes first within each stream.
		}}},
		{{Key: "$group", Value: bson.D{
			{Key: fieldID, Value: "$" + fieldStreamID}, // Group key is stream_id.
			{Key: fieldStreamType, Value: bson.D{{Key: opFirst, Value: "$stream_type"}}},
			{Key: fieldOffset, Value: bson.D{{Key: opFirst, Value: "$offset"}}},
			{Key: fieldGlobalOffset, Value: bson.D{{Key: opFirst, Value: "$global_offset"}}},
		}}},
	}

	cursor, err := collection.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, fmt.Errorf("aggregating collection: %w", err)
	}

	return cursor, nil
}
