package strategy

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/go-estoria/estoria"
	"go.jetpack.io/typeid"
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
	streamID typeid.AnyID,
	opts estoria.ReadStreamOptions,
) (estoria.EventStreamIterator, error) {
	offset := opts.Offset
	count := opts.Count
	sortDirection := 1
	versionFilterKey := "$gt"
	if opts.Direction == estoria.Reverse {
		sortDirection = -1
		// versionFilterKey = "$lt"
	}

	findOpts := options.Find().SetSort(bson.D{{Key: "event_id", Value: sortDirection}})
	if count > 0 {
		findOpts = findOpts.SetLimit(count)
	}

	cursor, err := s.collection.Find(ctx, bson.D{
		{Key: "stream_type", Value: streamID.Prefix()},
		{Key: "stream_id", Value: streamID.Suffix()},
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
	streamID typeid.AnyID,
	events []estoria.Event,
	opts estoria.AppendStreamOptions,
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

func (s *SingleCollectionStrategy) getLatestVersion(ctx mongo.SessionContext, streamID typeid.AnyID) (int64, error) {
	opts := options.FindOne().SetSort(bson.D{{Key: "version", Value: -1}})
	result := s.collection.FindOne(ctx, bson.D{
		{Key: "stream_type", Value: streamID.Prefix()},
		{Key: "stream_id", Value: streamID.Suffix()},
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
	StreamID   string    `bson:"stream_id"`
	EventID    string    `bson:"event_id"`
	EventType  string    `bson:"event_type"`
	Timestamp  time.Time `bson:"timestamp"`
	Version    int64     `bson:"version"`
	Data       []byte    `bson:"data"`
}

func singleCollectionEventDocumentFromEvent(evt estoria.Event, version int64) singleCollectionEventDocument {
	return singleCollectionEventDocument{
		StreamType: evt.StreamID().Prefix(),
		StreamID:   evt.StreamID().Suffix(),
		EventID:    evt.ID().Suffix(),
		EventType:  evt.ID().Prefix(),
		Timestamp:  evt.Timestamp(),
		Version:    version,
		Data:       evt.Data(),
	}
}

func (d singleCollectionEventDocument) ToEvent(_ typeid.AnyID) (estoria.Event, error) {
	eventID, err := typeid.From(d.EventType, d.EventID)
	if err != nil {
		return nil, fmt.Errorf("parsing event ID: %w", err)
	}

	streamID, err := typeid.From(d.StreamType, d.StreamID)
	if err != nil {
		return nil, fmt.Errorf("parsing stream ID: %w", err)
	}

	return &event{
		id:        eventID,
		streamID:  streamID,
		timestamp: d.Timestamp,
		data:      d.Data,
	}, nil
}
