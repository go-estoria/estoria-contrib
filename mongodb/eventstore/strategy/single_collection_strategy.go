package strategy

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/go-estoria/estoria"
	"github.com/go-estoria/estoria/eventstore"
	"github.com/go-estoria/estoria/typeid"
	"github.com/gofrs/uuid/v5"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type SingleCollectionStrategy struct {
	collection MongoCollection
	log        estoria.Logger
	marshaler  DocumentMarshaler
}

// NewSingleCollectionStrategy creates a new SingleCollectionStrategy using the given collection.
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

// GetAllEventsIterator returns an iterator over all events in the event store.
func (s *SingleCollectionStrategy) GetAllEventsIterator(
	ctx context.Context,
	opts eventstore.ReadStreamOptions,
) (eventstore.StreamIterator, error) {
	cursor, err := s.collection.Find(ctx, bson.D{}, findOptsFromReadStreamOptions(opts, "global_offset"))
	if err != nil {
		return nil, fmt.Errorf("finding events: %w", err)
	}

	return &streamIterator{
		cursor:    cursor,
		marshaler: s.marshaler,
	}, nil
}

// GetStreamIterator returns an iterator over events in the stream with the given ID.
func (s *SingleCollectionStrategy) GetStreamIterator(
	ctx context.Context,
	streamID typeid.UUID,
	opts eventstore.ReadStreamOptions,
) (eventstore.StreamIterator, error) {

	cursor, err := s.collection.Find(ctx, bson.D{
		{Key: "stream_type", Value: streamID.TypeName()},
		{Key: "stream_id", Value: streamID.Value()},
	}, findOptsFromReadStreamOptions(opts, "offset"))
	if err != nil {
		return nil, fmt.Errorf("finding events: %w", err)
	}

	return &streamIterator{
		cursor:    cursor,
		marshaler: s.marshaler,
	}, nil
}

// InsertStreamEvents inserts the given events into the stream with the given ID.
func (s *SingleCollectionStrategy) InsertStreamEvents(
	ctx context.Context,
	streamID typeid.UUID,
	events []*eventstore.WritableEvent,
	opts eventstore.AppendStreamOptions,
) (*InsertStreamEventsResult, error) {
	highestStreamOffset, err := s.getHighestOffset(ctx, streamID)
	if err != nil {
		return nil, fmt.Errorf("getting highest offset: %w", err)
	}

	highestGlobalOffset, err := s.getHighestGlobalOffset(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting highest global offset: %w", err)
	}

	if opts.ExpectVersion > 0 && highestStreamOffset != opts.ExpectVersion {
		return nil, fmt.Errorf("expected offset %d, but stream's highest offset is %d", opts.ExpectVersion, highestStreamOffset)
	}

	fullEvents := make([]*Event, len(events))
	docs := make([]any, len(events))
	for i, we := range events {
		fullEvents[i] = &Event{
			Event: eventstore.Event{
				ID:            we.ID,
				StreamID:      streamID,
				StreamVersion: highestStreamOffset + int64(i) + 1,
				Timestamp:     we.Timestamp,
				Data:          we.Data,
			},
			GlobalOffset: highestGlobalOffset + int64(i) + 1,
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

// ListStreams returns a cursor over the streams in the event store.
func (s *SingleCollectionStrategy) ListStreams(ctx context.Context) (*mongo.Cursor, error) {
	pipeline := mongo.Pipeline{
		{{Key: "$sort", Value: bson.D{
			{Key: "stream_id", Value: 1}, // Group documents together by stream_id.
			{Key: "offset", Value: -1},   // Highest offset comes first within each stream.
		}}},
		{{Key: "$group", Value: bson.D{
			{Key: "_id", Value: "$stream_id"}, // Group key is stream_id.
			{Key: "stream_type", Value: bson.D{{Key: "$first", Value: "$stream_type"}}},
			{Key: "offset", Value: bson.D{{Key: "$first", Value: "$offset"}}},
			{Key: "global_offset", Value: bson.D{{Key: "$first", Value: "$global_offset"}}},
		}}},
	}

	cursor, err := s.collection.Aggregate(ctx, pipeline)
	if err != nil {
		log.Fatal(err)
	}

	return cursor, nil
}

// SetDocumentMarshaler sets the marshaler used to encode and decode event documents.
func (s *SingleCollectionStrategy) SetDocumentMarshaler(marshaler DocumentMarshaler) {
	s.marshaler = marshaler
}

// SetLogger sets the logger used by the strategy.
func (s *SingleCollectionStrategy) SetLogger(l estoria.Logger) {
	s.log = l
}

// Finds the highest offset for the given stream.
func (s *SingleCollectionStrategy) getHighestOffset(ctx context.Context, streamID typeid.UUID) (int64, error) {
	s.log.Debug("finding highest offset for stream", "stream_id", streamID)
	opts := options.FindOne().SetSort(bson.D{{Key: "offset", Value: -1}})
	result := s.collection.FindOne(ctx, bson.D{
		{Key: "stream_type", Value: streamID.TypeName()},
		{Key: "stream_id", Value: streamID.Value()},
	}, opts)
	if result.Err() != nil {
		if result.Err() == mongo.ErrNoDocuments {
			s.log.Debug("stream not found", "stream_id", streamID)
			return 0, nil
		}
		return 0, fmt.Errorf("finding highest offset: %w", result.Err())
	}

	var doc singleCollectionEventDocument
	if err := result.Decode(&doc); err != nil {
		return 0, fmt.Errorf("decoding highest offset: %w", err)
	}

	s.log.Info("got highest offset for stream", "stream_id", streamID, "offset", doc.Offset)
	return doc.Offset, nil
}

// Finds the highest global offset among all events in the event store.
func (s *SingleCollectionStrategy) getHighestGlobalOffset(ctx context.Context) (int64, error) {
	s.log.Debug("finding highest global offset in event store")
	opts := options.FindOne().SetSort(bson.D{{Key: "global_offset", Value: -1}})
	result := s.collection.FindOne(ctx, bson.D{}, opts)
	if result.Err() != nil {
		if result.Err() == mongo.ErrNoDocuments {
			s.log.Debug("event store is empty")
			return 0, nil
		}
		return 0, fmt.Errorf("finding highest global offset: %w", result.Err())
	}

	var doc singleCollectionEventDocument
	if err := result.Decode(&doc); err != nil {
		return 0, fmt.Errorf("decoding highest global offset: %w", err)
	}

	s.log.Info("got highest global offset for event store", "global_offset", doc.GlobalOffset)
	return doc.GlobalOffset, nil
}

// DefaultSingleCollectionDocumentMarshaler is the default marshaler used by SingleCollectionStrategy.
type DefaultSingleCollectionDocumentMarshaler struct{}

var _ DocumentMarshaler = DefaultSingleCollectionDocumentMarshaler{}

// singleCollectionEventDocument is the document format used by the default marshaler.
type singleCollectionEventDocument struct {
	StreamType   string    `bson:"stream_type"`
	StreamID     string    `bson:"stream_id"`
	EventType    string    `bson:"event_type"`
	EventID      string    `bson:"event_id"`
	Offset       int64     `bson:"offset"`
	GlobalOffset int64     `bson:"global_offset"`
	Timestamp    time.Time `bson:"timestamp"`
	EventData    []byte    `bson:"event_data"`
}

// MarshalDocument encodes an event into a document.
func (DefaultSingleCollectionDocumentMarshaler) MarshalDocument(event *Event) (any, error) {
	return singleCollectionEventDocument{
		StreamType:   event.StreamID.TypeName(),
		StreamID:     event.StreamID.UUID().String(),
		EventType:    event.ID.TypeName(),
		EventID:      event.ID.UUID().String(),
		Offset:       event.StreamVersion,
		GlobalOffset: event.GlobalOffset,
		Timestamp:    event.Timestamp,
		EventData:    event.Data,
	}, nil
}

// UnmarshalDocument decodes a document into an event.
func (DefaultSingleCollectionDocumentMarshaler) UnmarshalDocument(decode DecodeDocumentFunc) (*Event, error) {
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

	return &Event{
		Event: eventstore.Event{
			ID:            typeid.FromUUID(doc.EventType, eventID),
			StreamID:      typeid.FromUUID(doc.StreamType, streamID),
			StreamVersion: doc.Offset,
			Timestamp:     doc.Timestamp,
			Data:          doc.EventData,
		},
		GlobalOffset: doc.GlobalOffset,
	}, nil
}

// SingleCollectionStrategyOption is an option for configuring a SingleCollectionStrategy.
type SingleCollectionStrategyOption func(*SingleCollectionStrategy) error

func WithSCSDocumentMarshaler(marshaler DocumentMarshaler) SingleCollectionStrategyOption {
	return func(s *SingleCollectionStrategy) error {
		s.marshaler = marshaler
		return nil
	}
}
