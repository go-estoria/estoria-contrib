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

type CollectionPerStreamStrategy struct {
	database  MongoDatabase
	log       estoria.Logger
	marshaler DocumentMarshaler
}

func NewCollectionPerStreamStrategy(db MongoDatabase) (*CollectionPerStreamStrategy, error) {
	if db == nil {
		return nil, fmt.Errorf("database is required")
	}

	strategy := &CollectionPerStreamStrategy{
		database:  db,
		log:       estoria.GetLogger().WithGroup("eventstore"),
		marshaler: DefaultCollectionPerStreamDocumentMarshaler{},
	}

	return strategy, nil
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

	return &streamIterator{
		streamID:  streamID,
		cursor:    cursor,
		marshaler: s.marshaler,
	}, nil
}

func (s *CollectionPerStreamStrategy) InsertStreamEvents(
	ctx mongo.SessionContext,
	streamID typeid.UUID,
	events []*eventstore.WritableEvent,
	opts eventstore.AppendStreamOptions,
) (*InsertStreamEventsResult, error) {
	s.log.Debug("inserting events into Mongo collection", "stream_id", streamID, "events", len(events))
	latestVersion, err := s.getLatestVersion(ctx, streamID)
	if err != nil {
		return nil, fmt.Errorf("getting latest version: %w", err)
	}

	if opts.ExpectVersion > 0 && latestVersion != opts.ExpectVersion {
		return nil, fmt.Errorf("expected version %d, but stream has version %d", opts.ExpectVersion, latestVersion)
	}

	now := time.Now()

	fullEvents := make([]*eventstore.Event, len(events))
	docs := make([]any, len(events))
	for i, we := range events {
		if we.Timestamp.IsZero() {
			we.Timestamp = now
		}

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

	collection := s.database.Collection(streamID.String())
	result, err := collection.InsertMany(ctx, docs)
	if err != nil {
		s.log.Error("error while inserting events", "error", err)
		return nil, fmt.Errorf("inserting events: %w", err)
	} else if len(result.InsertedIDs) != len(fullEvents) {
		return nil, fmt.Errorf("inserted %d events, but expected %d", len(result.InsertedIDs), len(docs))
	}

	return &InsertStreamEventsResult{
		MongoResult:    result,
		InsertedEvents: fullEvents,
	}, nil
}

func (s *CollectionPerStreamStrategy) SetDocumentMarshaler(marshaler DocumentMarshaler) {
	s.marshaler = marshaler
}

func (s *CollectionPerStreamStrategy) SetLogger(l estoria.Logger) {
	s.log = l
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

type DefaultCollectionPerStreamDocumentMarshaler struct{}

var _ DocumentMarshaler = DefaultCollectionPerStreamDocumentMarshaler{}

type collectionPerStreamEventDocument struct {
	StreamType string    `bson:"stream_type"`
	StreamID   string    `bson:"stream_id"`
	EventType  string    `bson:"event_type"`
	EventID    string    `bson:"event_id"`
	Version    int64     `bson:"version"`
	Timestamp  time.Time `bson:"timestamp"`
	EventData  []byte    `bson:"event_data"`
}

func (DefaultCollectionPerStreamDocumentMarshaler) MarshalDocument(event *eventstore.Event) (any, error) {
	return collectionPerStreamEventDocument{
		StreamType: event.StreamID.TypeName(),
		StreamID:   event.StreamID.UUID().String(),
		EventType:  event.ID.TypeName(),
		EventID:    event.ID.UUID().String(),
		Version:    event.StreamVersion,
		Timestamp:  event.Timestamp,
		EventData:  event.Data,
	}, nil
}

func (DefaultCollectionPerStreamDocumentMarshaler) UnmarshalDocument(decode DecodeDocumentFunc) (*eventstore.Event, error) {
	doc := collectionPerStreamEventDocument{}
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

type CollectionPerStreamStrategyOption func(*CollectionPerStreamStrategy) error

func WithCPSSDocumentMarshaler(marshaler DocumentMarshaler) CollectionPerStreamStrategyOption {
	return func(s *CollectionPerStreamStrategy) error {
		s.marshaler = marshaler
		return nil
	}
}
