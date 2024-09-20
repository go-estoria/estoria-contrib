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

type SingleCollectionStrategy struct {
	collection MongoCollection
	log        estoria.Logger
	marshaler  DocumentMarshaler
}

func NewSingleCollectionStrategy(collection MongoCollection) (*SingleCollectionStrategy, error) {
	if collection == nil {
		return nil, fmt.Errorf("collection is required")
	}

	strategy := &SingleCollectionStrategy{
		collection: collection,
		log:        estoria.GetLogger().WithGroup("eventstore"),
		marshaler:  DefaultSingleCollectionDocumentMarshaler{},
	}

	return strategy, nil
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

	return &streamIterator{
		streamID:  streamID,
		cursor:    cursor,
		marshaler: s.marshaler,
	}, nil
}

func (s *SingleCollectionStrategy) InsertStreamEvents(
	ctx mongo.SessionContext,
	streamID typeid.UUID,
	events []*eventstore.Event,
	opts eventstore.AppendStreamOptions,
) (*InsertResult, error) {
	latestVersion, err := s.getLatestVersion(ctx, streamID)
	if err != nil {
		return nil, fmt.Errorf("getting latest version: %w", err)
	}

	if opts.ExpectVersion > 0 && latestVersion != opts.ExpectVersion {
		return nil, fmt.Errorf("expected version %d, but stream has version %d", opts.ExpectVersion, latestVersion)
	}

	docs := make([]any, len(events))
	appended := make([]*eventstore.Event, len(events))
	for i, event := range events {
		event.StreamVersion = latestVersion + int64(i) + 1
		doc, err := s.marshaler.MarshalDocument(event)
		if err != nil {
			return nil, fmt.Errorf("marshaling event: %w", err)
		}

		docs[i] = doc
		appended[i] = event
	}

	result, err := s.collection.InsertMany(ctx, docs)
	if err != nil {
		return nil, fmt.Errorf("inserting events: %w", err)
	}

	return &InsertResult{
		MongoResult:    result,
		InsertedEvents: appended,
	}, nil
}

func (s *SingleCollectionStrategy) SetDocumentMarshaler(marshaler DocumentMarshaler) {
	s.marshaler = marshaler
}

func (s *SingleCollectionStrategy) SetLogger(l estoria.Logger) {
	s.log = l
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

type DefaultSingleCollectionDocumentMarshaler struct{}

var _ DocumentMarshaler = DefaultSingleCollectionDocumentMarshaler{}

type singleCollectionEventDocument struct {
	StreamType string    `bson:"stream_type"`
	StreamID   string    `bson:"stream_id"`
	EventType  string    `bson:"event_type"`
	EventID    string    `bson:"event_id"`
	Version    int64     `bson:"version"`
	Timestamp  time.Time `bson:"timestamp"`
	EventData  []byte    `bson:"event_data"`
}

func (DefaultSingleCollectionDocumentMarshaler) MarshalDocument(event *eventstore.Event) (any, error) {
	return singleCollectionEventDocument{
		StreamType: event.StreamID.TypeName(),
		StreamID:   event.StreamID.UUID().String(),
		EventType:  event.ID.TypeName(),
		EventID:    event.ID.UUID().String(),
		Version:    event.StreamVersion,
		Timestamp:  event.Timestamp,
		EventData:  event.Data,
	}, nil
}

func (DefaultSingleCollectionDocumentMarshaler) UnmarshalDocument(decode DecodeDocumentFunc) (*eventstore.Event, error) {
	doc := singleCollectionEventDocument{}
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
