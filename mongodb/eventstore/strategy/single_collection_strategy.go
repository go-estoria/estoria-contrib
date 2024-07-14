package strategy

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/go-estoria/estoria/eventstore"
	"github.com/go-estoria/estoria/typeid"
	"github.com/gofrs/uuid/v5"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type SingleCollectionStrategy struct {
	client     *mongo.Client
	database   *mongo.Database
	collection *mongo.Collection
	log        *slog.Logger
}

func NewSingleCollectionStrategy(client *mongo.Client, database, collection string) (*SingleCollectionStrategy, error) {
	if client == nil {
		return nil, fmt.Errorf("client is required")
	} else if database == "" {
		return nil, fmt.Errorf("database is required")
	} else if collection == "" {
		return nil, fmt.Errorf("collection is required")
	}

	db := client.Database(database)
	coll := db.Collection(collection)

	return &SingleCollectionStrategy{
		client:     client,
		database:   db,
		collection: coll,
		log:        slog.Default().WithGroup("eventstore"),
	}, nil
}

func (s *SingleCollectionStrategy) GetStreamIterator(
	ctx context.Context,
	streamID typeid.UUID,
	opts eventstore.ReadStreamOptions,
) (eventstore.StreamIterator, error) {
	offset := opts.Offset
	count := opts.Count
	sortDirection := 1
	versionFilterKey := "$gt"
	if opts.Direction == eventstore.Reverse {
		sortDirection = -1
		// versionFilterKey = "$lt"
	}

	findOpts := options.Find().SetSort(bson.D{{Key: "event_id", Value: sortDirection}})
	if count > 0 {
		findOpts = findOpts.SetLimit(count)
	}

	cursor, err := s.collection.Find(ctx, bson.D{
		{Key: "stream_type", Value: streamID.TypeName()},
		{Key: "stream_id", Value: streamID.Value()},
		{Key: "version", Value: bson.D{{Key: versionFilterKey, Value: offset}}},
	}, findOpts)
	if err != nil {
		return nil, fmt.Errorf("finding events: %w", err)
	}

	return &streamIterator[singleCollectionEventDocument]{
		streamID: streamID,
		cursor:   cursor,
	}, nil
}

func (s *SingleCollectionStrategy) InsertStreamEvents(
	ctx mongo.SessionContext,
	streamID typeid.UUID,
	events []*eventstore.EventStoreEvent,
	opts eventstore.AppendStreamOptions,
) (*mongo.InsertManyResult, error) {
	latestVersion, err := s.getLatestVersion(ctx, streamID)
	if err != nil {
		return nil, fmt.Errorf("getting latest version: %w", err)
	}

	if opts.ExpectVersion > 0 && latestVersion != opts.ExpectVersion {
		return nil, fmt.Errorf("expected version %d, but stream has version %d", opts.ExpectVersion, latestVersion)
	}

	docs := make([]any, len(events))
	for i, event := range events {
		docs[i] = singleCollectionEventDocumentFromEvent(event, latestVersion+int64(i+1))
	}

	result, err := s.collection.InsertMany(ctx, docs)
	if err != nil {
		return nil, fmt.Errorf("inserting events: %w", err)
	}

	return result, nil
}

func (s *SingleCollectionStrategy) getLatestVersion(ctx mongo.SessionContext, streamID typeid.UUID) (int64, error) {
	opts := options.FindOne().SetSort(bson.D{{Key: "version", Value: -1}})
	result := s.collection.FindOne(ctx, bson.D{
		{Key: "stream_type", Value: streamID.TypeName()},
		{Key: "stream_id", Value: streamID.Value()},
	}, opts)
	if result.Err() != nil {
		if result.Err() == mongo.ErrNoDocuments {
			return 0, nil
		}
		return 0, fmt.Errorf("finding latest version: %w", result.Err())
	}

	var doc singleCollectionEventDocument
	if err := result.Decode(&doc); err != nil {
		return 0, fmt.Errorf("decoding latest version: %w", err)
	}

	return doc.Version, nil
}

type singleCollectionEventDocument struct {
	StreamType string    `bson:"stream_type"`
	StreamID   uuid.UUID `bson:"stream_id"`
	EventID    uuid.UUID `bson:"event_id"`
	EventType  string    `bson:"event_type"`
	Timestamp  time.Time `bson:"timestamp"`
	Version    int64     `bson:"version"`
	Data       []byte    `bson:"data"`
}

func singleCollectionEventDocumentFromEvent(evt *eventstore.EventStoreEvent, version int64) singleCollectionEventDocument {
	return singleCollectionEventDocument{
		StreamType: evt.StreamID.TypeName(),
		StreamID:   evt.StreamID.UUID(),
		EventID:    evt.ID.UUID(),
		EventType:  evt.ID.TypeName(),
		Timestamp:  evt.Timestamp,
		Version:    version,
		Data:       evt.Data,
	}
}

func (d singleCollectionEventDocument) ToEvent(_ typeid.UUID) (*eventstore.EventStoreEvent, error) {
	return &eventstore.EventStoreEvent{
		ID:        typeid.FromUUID(d.EventType, d.EventID),
		StreamID:  typeid.FromUUID(d.StreamType, d.StreamID),
		Timestamp: d.Timestamp,
		Data:      d.Data,
	}, nil
}
