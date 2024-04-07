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
	streamID typeid.AnyID,
	opts estoria.ReadStreamOptions,
) (estoria.EventStreamIterator, error) {
	collection := s.database.Collection(streamID.String())

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

	s.log.Debug("finding events", "stream_id", streamID, "offset", offset, "count", count, "direction", opts.Direction)

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
		docs[i] = collectionPerStreamEventDocumentFromEvent(event, latestVersion+int64(i+1))
	}

	collection := s.database.Collection(streamID.String())
	result, err := collection.InsertMany(ctx, docs)
	if err != nil {
		return nil, fmt.Errorf("inserting events: %w", err)
	}

	return result, nil
}

func (s *CollectionPerStreamStrategy) getLatestVersion(ctx context.Context, streamID typeid.AnyID) (int64, error) {
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
	EventID   string    `bson:"event_id"`
	EventType string    `bson:"event_type"`
	Timestamp time.Time `bson:"timestamp"`
	Version   int64     `bson:"version"`
	Data      []byte    `bson:"data"`
}

func collectionPerStreamEventDocumentFromEvent(evt estoria.Event, version int64) collectionPerStreamEventDocument {
	return collectionPerStreamEventDocument{
		EventID:   evt.ID().Suffix(),
		EventType: evt.ID().Prefix(),
		Timestamp: evt.Timestamp(),
		Version:   version,
		Data:      evt.Data(),
	}
}

func (d collectionPerStreamEventDocument) ToEvent(streamID typeid.AnyID) (estoria.Event, error) {
	eventID, err := typeid.From(d.EventType, d.EventID)
	if err != nil {
		return nil, fmt.Errorf("parsing event ID: %w", err)
	}

	return &event{
		id:        eventID,
		streamID:  streamID,
		timestamp: d.Timestamp,
		data:      d.Data,
	}, nil
}
