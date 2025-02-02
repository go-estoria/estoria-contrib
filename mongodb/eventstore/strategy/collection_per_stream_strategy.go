package strategy

import (
	"context"
	"fmt"
	"time"

	"github.com/go-estoria/estoria"
	"github.com/go-estoria/estoria/eventstore"
	"github.com/go-estoria/estoria/typeid"
	"github.com/gofrs/uuid/v5"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
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

func (s *CollectionPerStreamStrategy) GetAllEventsIterator(
	ctx context.Context,
	opts eventstore.ReadStreamOptions,
) (eventstore.StreamIterator, error) {
	streamIDs, err := s.database.ListCollectionNames(ctx, bson.D{})
	if err != nil {
		return nil, fmt.Errorf("listing collection names: %w", err)
	}

	cursors := make(map[string]multiStreamIteratorCursor, len(streamIDs))
	for _, streamID := range streamIDs {
		collection := s.database.Collection(streamID)
		cursor, err := collection.Find(ctx, bson.D{}, findOptsFromReadStreamOptions(opts, "global_offset"))
		if err != nil {
			return nil, fmt.Errorf("finding events in collection %s: %w", streamID, err)
		}

		cursors[streamID] = multiStreamIteratorCursor{
			cursor: cursor,
		}
	}

	return &multiStreamIterator{
		cursors:   cursors,
		marshaler: s.marshaler,
	}, nil
}

func (s *CollectionPerStreamStrategy) GetStreamIterator(
	ctx context.Context,
	streamID typeid.UUID,
	opts eventstore.ReadStreamOptions,
) (eventstore.StreamIterator, error) {
	collection := s.database.Collection(streamID.String())

	findOpts := options.Find()
	if opts.Count > 0 {
		findOpts = findOpts.SetLimit(opts.Count)
	}

	cursor, err := collection.Find(ctx, bson.D{
		{Key: "version", Value: bson.D{{Key: "$gt", Value: opts.Offset}}},
	}, findOpts)
	if err != nil {
		return nil, fmt.Errorf("finding events: %w", err)
	}

	return &streamIterator{
		cursor:    cursor,
		marshaler: s.marshaler,
	}, nil
}

func (s *CollectionPerStreamStrategy) InsertStreamEvents(
	ctx context.Context,
	streamID typeid.UUID,
	events []*eventstore.WritableEvent,
	opts eventstore.AppendStreamOptions,
) (*InsertStreamEventsResult, error) {
	s.log.Debug("inserting events into Mongo collection", "stream_id", streamID, "events", len(events))
	highestOffset, err := s.getHighestOffset(ctx, streamID)
	if err != nil {
		return nil, fmt.Errorf("getting latest version: %w", err)
	}

	highestGlobalOffset, err := s.getHighestGlobalOffset(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting latest global offset: %w", err)
	}

	if opts.ExpectVersion > 0 && highestOffset != opts.ExpectVersion {
		return nil, fmt.Errorf("expected version %d, but stream has version %d", opts.ExpectVersion, highestOffset)
	}

	now := time.Now()

	fullEvents := make([]*Event, len(events))
	docs := make([]any, len(events))
	for i, we := range events {
		if we.Timestamp.IsZero() {
			we.Timestamp = now
		}

		fullEvents[i] = &Event{
			Event: eventstore.Event{
				ID:            we.ID,
				StreamID:      streamID,
				StreamVersion: highestOffset + int64(i) + 1,
				Timestamp:     now,
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

// ListStreams returns a cursor over the streams in the event store.
func (s *CollectionPerStreamStrategy) ListStreams(ctx context.Context) (*mongo.Cursor, error) {
	collections, err := s.database.ListCollectionNames(ctx, bson.D{})
	if err != nil {
		return nil, fmt.Errorf("listing collection names: %w", err)
	}

	docs := make([]any, len(collections))
	for i, collectionName := range collections {
		collection := s.database.Collection(collectionName)
		res := collection.FindOne(ctx, bson.D{}, options.FindOne().SetSort(bson.D{{Key: "offset", Value: -1}}))
		if res.Err() != nil {
			return nil, fmt.Errorf("finding events in collection %s: %w", collectionName, err)
		}

		doc, err := s.marshaler.UnmarshalDocument(res.Decode)
		if err != nil {
			return nil, fmt.Errorf("decoding event document: %w", err)
		}

		docs[i] = doc
	}

	cursor, err := mongo.NewCursorFromDocuments(docs, nil, nil)
	if err != nil {
		return nil, fmt.Errorf("creating aggregate cursor: %w", err)
	}

	return cursor, nil
}

func (s *CollectionPerStreamStrategy) SetDocumentMarshaler(marshaler DocumentMarshaler) {
	s.marshaler = marshaler
}

func (s *CollectionPerStreamStrategy) SetLogger(l estoria.Logger) {
	s.log = l
}

func (s *CollectionPerStreamStrategy) getHighestOffset(ctx context.Context, streamID typeid.UUID) (int64, error) {
	collection := s.database.Collection(streamID.String())

	opts := options.FindOne().SetSort(bson.D{{Key: "version", Value: -1}})
	var doc collectionPerStreamEventDocument
	if err := collection.FindOne(ctx, bson.D{}, opts).Decode(&doc); err != nil {
		if err == mongo.ErrNoDocuments {
			return 0, nil
		}

		return 0, fmt.Errorf("finding latest version: %w", err)
	}

	return doc.Offset, nil
}

// Finds the highest global offset among all events in the event store.
func (s *CollectionPerStreamStrategy) getHighestGlobalOffset(ctx context.Context) (int64, error) {
	s.log.Debug("finding highest global offset in event store")

	streamIDs, err := s.database.ListCollectionNames(ctx, bson.D{})
	if err != nil {
		return 0, fmt.Errorf("listing collection names: %w", err)
	}

	opts := options.FindOne().SetSort(bson.D{{Key: "global_offset", Value: -1}})

	highestGlobalOffset := int64(0)
	for _, streamID := range streamIDs {
		collection := s.database.Collection(streamID)
		result := collection.FindOne(ctx, bson.D{}, opts)
		if result.Err() != nil {
			if result.Err() == mongo.ErrNoDocuments {
				s.log.Debug("collection for stream is empty", "collection", streamID)
				return 0, nil
			}
			return 0, fmt.Errorf("finding highest global offset in collection: %w", result.Err())
		}

		var doc singleCollectionEventDocument
		if err := result.Decode(&doc); err != nil {
			return 0, fmt.Errorf("decoding highest global offset: %w", err)
		}

		s.log.Info("got highest global offset for collection", "collection", streamID, "global_offset", doc.GlobalOffset)

		if doc.GlobalOffset > highestGlobalOffset {
			highestGlobalOffset = doc.GlobalOffset
		}
	}

	s.log.Info("got highest global offset for event store", "global_offset", highestGlobalOffset)
	return highestGlobalOffset, nil
}

type DefaultCollectionPerStreamDocumentMarshaler struct{}

var _ DocumentMarshaler = DefaultCollectionPerStreamDocumentMarshaler{}

type collectionPerStreamEventDocument struct {
	StreamType   string    `bson:"stream_type"`
	StreamID     string    `bson:"stream_id"`
	EventType    string    `bson:"event_type"`
	EventID      string    `bson:"event_id"`
	Offset       int64     `bson:"offset"`
	GlobalOffset int64     `bson:"global_offset"`
	Timestamp    time.Time `bson:"timestamp"`
	EventData    []byte    `bson:"event_data"`
}

func (DefaultCollectionPerStreamDocumentMarshaler) MarshalDocument(event *Event) (any, error) {
	return collectionPerStreamEventDocument{
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

func (DefaultCollectionPerStreamDocumentMarshaler) UnmarshalDocument(decode DecodeDocumentFunc) (*Event, error) {
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

type CollectionPerStreamStrategyOption func(*CollectionPerStreamStrategy) error

func WithCPSSDocumentMarshaler(marshaler DocumentMarshaler) CollectionPerStreamStrategyOption {
	return func(s *CollectionPerStreamStrategy) error {
		s.marshaler = marshaler
		return nil
	}
}
