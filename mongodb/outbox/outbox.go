package outbox

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/go-estoria/estoria"
	mongoeventstore "github.com/go-estoria/estoria-contrib/mongodb/eventstore"
	"github.com/go-estoria/estoria/eventstore"
	"github.com/go-estoria/estoria/typeid"
	"github.com/gofrs/uuid/v5"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// BSON document keys used by the outbox's collections.
const (
	fieldID          = "_id"
	fieldStreamType  = "stream_type"
	fieldStreamID    = "stream_id"
	fieldNextVersion = "next_version"
	fieldLastVersion = "last_version"
	fieldLeasedUntil = "leased_until"
	fieldLeasedBy    = "leased_by"
	fieldHalted      = "halted"

	opSet = "$set"
)

// epoch returns the sentinel "lease released / never leased" timestamp, which always
// sorts before now.
func epoch() time.Time {
	return time.Unix(0, 0).UTC()
}

// MongoCollection is the subset of *mongo.Collection used by the outbox.
type MongoCollection interface {
	InsertMany(ctx context.Context, docs any, opts ...options.Lister[options.InsertManyOptions]) (*mongo.InsertManyResult, error)
	UpdateOne(ctx context.Context, filter, update any, opts ...options.Lister[options.UpdateOneOptions]) (*mongo.UpdateResult, error)
	FindOne(ctx context.Context, filter any, opts ...options.Lister[options.FindOneOptions]) *mongo.SingleResult
	FindOneAndUpdate(ctx context.Context, filter, update any, opts ...options.Lister[options.FindOneAndUpdateOptions]) *mongo.SingleResult
	DeleteOne(ctx context.Context, filter any, opts ...options.Lister[options.DeleteOneOptions]) (*mongo.DeleteResult, error)
	Indexes() mongo.IndexView
}

// ItemHandler is a function that processes a single outbox item.
//
// Handlers must be idempotent: due to the at-least-once delivery guarantee, a handler may be
// called more than once for the same item if a crash occurs after the handler succeeds but before
// the item is acknowledged (or if a lease expires while the handler is still running).
//
// Handlers run OUTSIDE any MongoDB transaction (no session is held across user code), so they may
// take as long as needed — but a handler slower than the lease duration risks duplicate delivery.
type ItemHandler func(ctx context.Context, item *Item) error

// Outbox implements the transactional-outbox pattern on MongoDB. As an eventstore TransactionHook
// it writes one outbox document per event in the append transaction (the producer side), and a
// polling consumer claims, handles, and acknowledges those documents with strict per-stream FIFO.
type Outbox struct {
	coll         MongoCollection
	streamState  MongoCollection
	handler      ItemHandler
	pollInterval time.Duration
	maxRetries   int
	leaseDur     time.Duration
	leasedBy     string
	log          estoria.Logger
	running      atomic.Bool
}

var _ mongoeventstore.TransactionHook = (*Outbox)(nil)

// New creates a new Outbox.
//
// coll is the collection that holds outbox item documents; streamState is the collection that
// holds per-stream cursors and leases. Both must live in the same database as the event store so
// that producer writes participate in the append transaction.
func New(coll, streamState MongoCollection, handler ItemHandler, opts ...Option) (*Outbox, error) {
	if coll == nil {
		return nil, errors.New("outbox collection is required")
	}
	if streamState == nil {
		return nil, errors.New("stream state collection is required")
	}
	if handler == nil {
		return nil, errors.New("handler is required")
	}

	leasedBy, err := uuid.NewV4()
	if err != nil {
		return nil, fmt.Errorf("generating worker id: %w", err)
	}

	o := &Outbox{
		coll:         coll,
		streamState:  streamState,
		handler:      handler,
		pollInterval: 1 * time.Second,
		maxRetries:   10,
		leaseDur:     30 * time.Second,
		leasedBy:     leasedBy.String(),
		log:          estoria.GetLogger().WithGroup("outbox"),
	}

	for _, opt := range opts {
		if err := opt(o); err != nil {
			return nil, fmt.Errorf("applying option: %w", err)
		}
	}

	return o, nil
}

// streamStateDocument is the BSON shape of a per-stream outbox cursor/lease document.
type streamStateDocument struct {
	StreamType  string    `bson:"stream_type"`
	StreamID    string    `bson:"stream_id"`
	NextVersion int64     `bson:"next_version"`
	LastVersion int64     `bson:"last_version"`
	LeasedUntil time.Time `bson:"leased_until"`
	LeasedBy    string    `bson:"leased_by"`
	Halted      bool      `bson:"halted"`
}

// streamKey returns the compound natural-key _id for a stream's outbox cursor.
func streamKey(streamType, streamID string) bson.D {
	return bson.D{{Key: "t", Value: streamType}, {Key: "s", Value: streamID}}
}

// HandleEvents implements mongoeventstore.TransactionHook. It runs on the append session context,
// inserting one outbox document per event and advancing the stream's last enqueued version — all
// within the append transaction, so outbox writes commit or roll back atomically with the events.
func (o *Outbox) HandleEvents(sessCtx context.Context, events []*eventstore.Event) error {
	if len(events) == 0 {
		return nil
	}

	now := time.Now().UTC()
	docs := make([]any, len(events))
	for i, e := range events {
		var globalOffset int64
		if e.GlobalPosition != nil {
			globalOffset = *e.GlobalPosition
		}
		docs[i] = itemDocument{
			GlobalOffset:  globalOffset,
			StreamType:    e.StreamID.Type,
			StreamID:      e.StreamID.UUID.String(),
			StreamVersion: e.StreamVersion,
			EventID:       e.ID.UUID.String(),
			EventType:     e.ID.Type,
			Timestamp:     e.Timestamp,
			Data:          e.Data,
			Metadata:      e.Metadata,
			Status:        statusPending,
			RetryCount:    0,
			CreatedAt:     now,
		}
	}

	if _, err := o.coll.InsertMany(sessCtx, docs); err != nil {
		return fmt.Errorf("inserting outbox items: %w", err)
	}

	// All events in a single append share one stream. Advance that stream's cursor bounds:
	// on first sight, seed next_version with the lowest enqueued version; always raise last_version.
	streamType := events[0].StreamID.Type
	streamID := events[0].StreamID.UUID.String()
	minVersion := events[0].StreamVersion
	maxVersion := events[len(events)-1].StreamVersion

	if _, err := o.streamState.UpdateOne(sessCtx,
		bson.D{{Key: fieldID, Value: streamKey(streamType, streamID)}},
		bson.D{
			{Key: "$setOnInsert", Value: bson.D{
				{Key: fieldStreamType, Value: streamType},
				{Key: fieldStreamID, Value: streamID},
				{Key: fieldNextVersion, Value: minVersion},
				{Key: fieldLeasedUntil, Value: epoch()},
				{Key: fieldLeasedBy, Value: ""},
				{Key: fieldHalted, Value: false},
			}},
			{Key: "$max", Value: bson.D{{Key: fieldLastVersion, Value: maxVersion}}},
		},
		options.UpdateOne().SetUpsert(true),
	); err != nil {
		return fmt.Errorf("updating outbox stream state: %w", err)
	}

	return nil
}

// EnsureIndexes creates the indexes required by the outbox. It is idempotent.
func (o *Outbox) EnsureIndexes(ctx context.Context) error {
	itemModels := []mongo.IndexModel{
		{
			// Unique per-stream version: serves the head-of-stream lookup and guards against
			// duplicate enqueue.
			Keys: bson.D{
				{Key: fieldStreamType, Value: 1},
				{Key: fieldStreamID, Value: 1},
				{Key: "stream_version", Value: 1},
			},
			Options: options.Index().SetUnique(true).SetName("uniq_stream_version"),
		},
		{
			Keys:    bson.D{{Key: "global_offset", Value: 1}},
			Options: options.Index().SetName("global_offset"),
		},
	}
	if _, err := o.coll.Indexes().CreateMany(ctx, itemModels); err != nil {
		return fmt.Errorf("creating outbox item indexes: %w", err)
	}

	streamModels := []mongo.IndexModel{
		{
			Keys:    bson.D{{Key: fieldLeasedUntil, Value: 1}},
			Options: options.Index().SetName("leased_until"),
		},
	}
	if _, err := o.streamState.Indexes().CreateMany(ctx, streamModels); err != nil {
		return fmt.Errorf("creating outbox stream-state indexes: %w", err)
	}

	return nil
}

// streamIDString renders an item's stream identity for logging and error messages.
func streamIDString(streamType string, streamID string) string {
	id, err := uuid.FromString(streamID)
	if err != nil {
		return streamType + ":" + streamID
	}
	return typeid.New(streamType, id).String()
}
