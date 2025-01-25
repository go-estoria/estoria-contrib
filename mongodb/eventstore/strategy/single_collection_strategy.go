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

	cursor, err := s.collection.Find(ctx, bson.D{
		{Key: "stream_type", Value: streamID.TypeName()},
		{Key: "stream_id", Value: streamID.Value()},
	}, findOptsFromReadStreamOptions(opts))
	if err != nil {
		return nil, fmt.Errorf("finding events: %w", err)
	}

	return &streamIterator{
		cursor:    cursor,
		marshaler: s.marshaler,
	}, nil
}

func (s *SingleCollectionStrategy) InsertStreamEvents(
	ctx mongo.SessionContext,
	streamID typeid.UUID,
	events []*eventstore.WritableEvent,
	opts eventstore.AppendStreamOptions,
) (*InsertStreamEventsResult, error) {
	latestVersion, err := s.getHighestOffset(ctx, streamID)
	if err != nil {
		return nil, fmt.Errorf("getting highest offset: %w", err)
	}

	if opts.ExpectVersion > 0 && latestVersion != opts.ExpectVersion {
		return nil, fmt.Errorf("expected offset %d, but stream's highest offset is %d", opts.ExpectVersion, latestVersion)
	}

	now := time.Now()

	fullEvents := make([]*eventstore.Event, len(events))
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

	result, err := s.collection.InsertMany(ctx, docs)
	if err != nil {
		return nil, fmt.Errorf("inserting events: %w", err)
	} else if len(result.InsertedIDs) != len(fullEvents) {
		return nil, fmt.Errorf("inserted %d events, but expected %d", len(result.InsertedIDs), len(fullEvents))
	}

	return &InsertStreamEventsResult{
		MongoResult:    result,
		InsertedEvents: fullEvents,
	}, nil
}

func (s *SingleCollectionStrategy) SetDocumentMarshaler(marshaler DocumentMarshaler) {
	s.marshaler = marshaler
}

func (s *SingleCollectionStrategy) SetLogger(l estoria.Logger) {
	s.log = l
}

func (s *SingleCollectionStrategy) getHighestOffset(ctx mongo.SessionContext, streamID typeid.UUID) (int64, error) {
	s.log.Info("finding highest offset for stream", "stream_id", streamID)
	opts := options.FindOne().SetSort(bson.D{{Key: "offset", Value: -1}})
	result := s.collection.FindOne(ctx, bson.D{
		{Key: "stream_type", Value: streamID.TypeName()},
		{Key: "stream_id", Value: streamID.Value()},
	}, opts)
	if result.Err() != nil {
		if result.Err() == mongo.ErrNoDocuments {
			return 0, nil
		}
		return 0, fmt.Errorf("finding highest offset: %w", result.Err())
	}

	s.log.Info("found highest offset for stream", "stream_id", streamID)

	var doc singleCollectionEventDocument
	if err := result.Decode(&doc); err != nil {
		return 0, fmt.Errorf("decoding highest offset: %w", err)
	}

	s.log.Info("got highest offset for stream", "stream_id", streamID, "offset", doc.Offset)
	return doc.Offset, nil
}

type DefaultSingleCollectionDocumentMarshaler struct{}

var _ DocumentMarshaler = DefaultSingleCollectionDocumentMarshaler{}

type singleCollectionEventDocument struct {
	StreamType string    `bson:"stream_type"`
	StreamID   string    `bson:"stream_id"`
	EventType  string    `bson:"event_type"`
	EventID    string    `bson:"event_id"`
	Offset     int64     `bson:"offset"`
	Timestamp  time.Time `bson:"timestamp"`
	EventData  []byte    `bson:"event_data"`
}

func (DefaultSingleCollectionDocumentMarshaler) MarshalDocument(event *eventstore.Event) (any, error) {
	return singleCollectionEventDocument{
		StreamType: event.StreamID.TypeName(),
		StreamID:   event.StreamID.UUID().String(),
		EventType:  event.ID.TypeName(),
		EventID:    event.ID.UUID().String(),
		Offset:     event.StreamVersion,
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
		StreamVersion: doc.Offset,
		Timestamp:     doc.Timestamp,
		Data:          doc.EventData,
	}, nil
}

type SingleCollectionStrategyOption func(*SingleCollectionStrategy) error

func WithSCSDocumentMarshaler(marshaler DocumentMarshaler) SingleCollectionStrategyOption {
	return func(s *SingleCollectionStrategy) error {
		s.marshaler = marshaler
		return nil
	}
}
