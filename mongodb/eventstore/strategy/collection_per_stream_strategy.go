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

type CollectionPerStreamStrategy struct {
	client   *mongo.Client
	database *mongo.Database
	log      *slog.Logger
}

func NewCollectionPerStreamStrategy(client *mongo.Client, database string) (*CollectionPerStreamStrategy, error) {
	if client == nil {
		return nil, fmt.Errorf("client is required")
	} else if database == "" {
		return nil, fmt.Errorf("database is required")
	}

	db := client.Database(database)

	return &CollectionPerStreamStrategy{
		client:   client,
		database: db,
		log:      slog.Default().WithGroup("eventstore"),
	}, nil
}

func (s *CollectionPerStreamStrategy) GetStreamIterator(
	ctx context.Context,
	streamID typeid.UUID,
	opts eventstore.ReadStreamOptions,
) (eventstore.StreamIterator, error) {
	collection := s.database.Collection(streamID.String())

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

	cursor, err := collection.Find(ctx, bson.D{
		{Key: "version", Value: bson.D{{Key: versionFilterKey, Value: offset}}},
	}, findOpts)
	if err != nil {
		return nil, fmt.Errorf("finding events: %w", err)
	}

	return &streamIterator[collectionPerStreamEventDocument]{
		streamID: streamID,
		cursor:   cursor,
	}, nil
}

func (s *CollectionPerStreamStrategy) InsertStreamEvents(
	ctx mongo.SessionContext,
	streamID typeid.UUID,
	events []*eventstore.EventStoreEvent,
	opts eventstore.AppendStreamOptions,
) (*mongo.InsertManyResult, error) {
	slog.Debug("inserting events into Mongo collection", "stream_id", streamID, "events", len(events))
	latestVersion, err := s.getLatestVersion(ctx, streamID)
	if err != nil {
		return nil, fmt.Errorf("getting latest version: %w", err)
	}

	if opts.ExpectVersion > 0 && latestVersion != opts.ExpectVersion {
		return nil, fmt.Errorf("expected version %d, but stream has version %d", opts.ExpectVersion, latestVersion)
	}

	docs := make([]any, len(events))
	for i, event := range events {
		docs[i] = collectionPerStreamEventDocumentFromEvent(event, latestVersion+int64(i+1))
	}

	collection := s.database.Collection(streamID.String())
	result, err := collection.InsertMany(ctx, docs)
	if err != nil {
		slog.Error("error while inserting events", "error", err)
		return nil, fmt.Errorf("inserting events: %w", err)
	}

	return result, nil
}

func (s *CollectionPerStreamStrategy) getLatestVersion(ctx context.Context, streamID typeid.UUID) (int64, error) {
	collection := s.database.Collection(streamID.String())

	opts := options.FindOne().SetSort(bson.D{{Key: "version", Value: -1}})
	var doc collectionPerStreamEventDocument
	if err := collection.FindOne(ctx, bson.D{}, opts).Decode(&doc); err != nil {
		if err == mongo.ErrNoDocuments {
			return 0, nil
		}

		return 0, fmt.Errorf("finding latest version: %w", err)
	}

	return doc.Version, nil
}

type collectionPerStreamEventDocument struct {
	EventID   uuid.UUID `bson:"event_id"`
	EventType string    `bson:"event_type"`
	Timestamp time.Time `bson:"timestamp"`
	Version   int64     `bson:"version"`
	Data      []byte    `bson:"data"` // QUESITON: should the event store API use []byte or any, to allow for different serialization strategies?
}

func collectionPerStreamEventDocumentFromEvent(evt *eventstore.EventStoreEvent, version int64) collectionPerStreamEventDocument {
	return collectionPerStreamEventDocument{
		EventID:   evt.ID.UUID(),
		EventType: evt.ID.TypeName(),
		Timestamp: evt.Timestamp,
		Version:   version,
		Data:      evt.Data,
	}
}

func (d collectionPerStreamEventDocument) ToEvent(streamID typeid.UUID) (*eventstore.EventStoreEvent, error) {
	return &eventstore.EventStoreEvent{
		ID:            typeid.FromUUID(d.EventType, d.EventID),
		StreamID:      streamID,
		StreamVersion: d.Version,
		Timestamp:     d.Timestamp,
		Data:          d.Data,
	}, nil
}
