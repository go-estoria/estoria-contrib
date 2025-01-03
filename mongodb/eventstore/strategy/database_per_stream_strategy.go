package strategy

import (
	"context"
	"fmt"
	"time"

	"github.com/go-estoria/estoria"
	"github.com/go-estoria/estoria/eventstore"
	"github.com/go-estoria/estoria/typeid"
	"github.com/gofrs/uuid/v5"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type DatabasePerStreamStrategy struct {
	client         MongoClient
	collectionName string
	log            estoria.Logger
	marshaler      DocumentMarshaler
}

func NewDatabasePerStreamStrategy(client MongoClient, collectionName string) (*DatabasePerStreamStrategy, error) {
	if client == nil {
		return nil, fmt.Errorf("client is required")
	} else if collectionName == "" {
		return nil, fmt.Errorf("collectionName is required")
	}

	strategy := &DatabasePerStreamStrategy{
		client:         client,
		collectionName: collectionName,
		log:            estoria.GetLogger().WithGroup("eventstore"),
		marshaler:      DefaultDatabasePerStreamDocumentMarshaler{},
	}

	return strategy, nil
}

func (s *DatabasePerStreamStrategy) GetStreamIterator(
	ctx context.Context,
	streamID typeid.UUID,
	opts eventstore.ReadStreamOptions,
) (eventstore.StreamIterator, error) {
	database := s.client.Database(streamID.String())
	collection := database.Collection(s.collectionName)

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

	return &streamIterator{
		streamID:  streamID,
		cursor:    cursor,
		marshaler: s.marshaler,
	}, nil
}

func (s *DatabasePerStreamStrategy) InsertStreamEvents(
	ctx mongo.SessionContext,
	streamID typeid.UUID,
	events []*eventstore.WritableEvent,
	opts eventstore.AppendStreamOptions,
) (*InsertStreamEventsResult, error) {
	latestVersion, err := s.getLatestVersion(ctx, streamID)
	if err != nil {
		return nil, fmt.Errorf("getting latest version: %w", err)
	}

	if opts.ExpectVersion > 0 && latestVersion != opts.ExpectVersion {
		return nil, fmt.Errorf("expected version %d, but stream has version %d", opts.ExpectVersion, latestVersion)
	}

	now := time.Now()

	fullEvents := make([]*eventstore.Event, 0, len(events))
	docs := make([]any, len(events))
	for i, we := range events {
		fullEvents[i] = &eventstore.Event{
			ID:            we.ID,
			StreamID:      streamID,
			StreamVersion: latestVersion + int64(i) + 1,
			Timestamp:     now,
			Data:          we.Data,
		}

		doc, err := s.marshaler.MarshalDocument(fullEvents[i])
		if err != nil {
			return nil, fmt.Errorf("marshaling event: %w", err)
		}

		docs[i] = doc
	}

	database := s.client.Database(streamID.String())
	collection := database.Collection(s.collectionName)
	result, err := collection.InsertMany(ctx, docs)
	if err != nil {
		return nil, fmt.Errorf("inserting events: %w", err)
	} else if len(result.InsertedIDs) != len(fullEvents) {
		return nil, fmt.Errorf("inserted %d documents, but expected %d", len(result.InsertedIDs), len(docs))
	}

	return &InsertStreamEventsResult{
		MongoResult:    result,
		InsertedEvents: fullEvents,
	}, nil
}

func (s *DatabasePerStreamStrategy) SetDocumentMarshaler(marshaler DocumentMarshaler) {
	s.marshaler = marshaler
}

func (s *DatabasePerStreamStrategy) SetLogger(l estoria.Logger) {
	s.log = l
}

func (s *DatabasePerStreamStrategy) getLatestVersion(ctx context.Context, streamID typeid.UUID) (int64, error) {
	database := s.client.Database(streamID.String())
	collection := database.Collection(s.collectionName)

	opts := options.FindOne().SetSort(bson.D{{Key: "version", Value: -1}})
	var doc databasePerStreamEventDocument
	if err := collection.FindOne(ctx, bson.D{}, opts).Decode(&doc); err != nil {
		if err == mongo.ErrNoDocuments {
			return 0, nil
		}

		return 0, fmt.Errorf("finding latest version: %w", err)
	}

	return doc.Version, nil
}

type databasePerStreamEventDocument struct {
	StreamType string    `bson:"stream_type"`
	StreamID   string    `bson:"stream_id"`
	EventType  string    `bson:"event_type"`
	EventID    string    `bson:"event_id"`
	Version    int64     `bson:"version"`
	Timestamp  time.Time `bson:"timestamp"`
	EventData  []byte    `bson:"event_data"`
}

type DefaultDatabasePerStreamDocumentMarshaler struct{}

var _ DocumentMarshaler = DefaultDatabasePerStreamDocumentMarshaler{}

func (DefaultDatabasePerStreamDocumentMarshaler) MarshalDocument(event *eventstore.Event) (any, error) {
	return databasePerStreamEventDocument{
		StreamType: event.StreamID.TypeName(),
		StreamID:   event.StreamID.UUID().String(),
		EventType:  event.ID.TypeName(),
		EventID:    event.ID.UUID().String(),
		Version:    event.StreamVersion,
		Timestamp:  event.Timestamp,
		EventData:  event.Data,
	}, nil
}

func (DefaultDatabasePerStreamDocumentMarshaler) UnmarshalDocument(decode DecodeDocumentFunc) (*eventstore.Event, error) {
	doc := databasePerStreamEventDocument{}
	if err := decode(&doc); err != nil {
		return nil, fmt.Errorf("decoding event document: %w", err)
	}

	streamID, err := uuid.FromString(doc.StreamID)
	if err != nil {
		return nil, fmt.Errorf("parsing stream ID: %w", err)
	}

	eventID, err := uuid.FromString(doc.EventID)
	if err != nil {
		return nil, fmt.Errorf("parsing event ID: %w", err)
	}

	return &eventstore.Event{
		ID:            typeid.FromUUID(doc.EventType, eventID),
		StreamID:      typeid.FromUUID(doc.StreamType, streamID),
		StreamVersion: doc.Version,
		Timestamp:     doc.Timestamp,
		Data:          doc.EventData,
	}, nil
}

type DatabasePerStreamStrategyOption func(*DatabasePerStreamStrategy) error

func WithDPSSDocumentMarshaler(marshaler DocumentMarshaler) DatabasePerStreamStrategyOption {
	return func(s *DatabasePerStreamStrategy) error {
		s.marshaler = marshaler
		return nil
	}
}
