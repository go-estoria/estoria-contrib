package outbox

import (
	"context"
	"fmt"
	"time"

	"github.com/go-estoria/estoria/outbox"
	"go.jetpack.io/typeid"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Iterator struct {
	client       *mongo.Client
	database     string
	collection   string
	changeStream *mongo.ChangeStream
}

func NewIterator(client *mongo.Client, database, collection string) (*Iterator, error) {
	iterator := &Iterator{
		client:     client,
		database:   database,
		collection: collection,
	}

	return iterator, nil
}

func (i *Iterator) Next(ctx context.Context) (outbox.OutboxEntry, error) {
	if i.changeStream == nil {
		collection := i.client.Database(i.database).Collection(i.collection)
		opts := options.ChangeStream().SetFullDocument("updateLookup")
		changeStream, err := collection.Watch(ctx, mongo.Pipeline{}, opts)
		if err != nil {
			return nil, fmt.Errorf("creating change stream: %w", err)
		}

		i.changeStream = changeStream
	}

	if i.changeStream.Next(ctx) {
		changeStreamDoc := map[string]any{}
		if err := i.changeStream.Decode(&changeStreamDoc); err != nil {
			return nil, fmt.Errorf("decoding change stream event: %w", err)
		}

		fullDocument, ok := changeStreamDoc["fullDocument"].(map[string]any)
		if !ok {
			return nil, fmt.Errorf("can't get full document from change stream document")
		}

		doc := outboxDocument{
			Timestamp: fullDocument["timestamp"].(primitive.DateTime),
			StreamID:  fullDocument["stream_id"].(string),
			EventID:   fullDocument["event_id"].(string),
			EventData: fullDocument["event_data"].(primitive.Binary),
		}

		streamID, err := typeid.FromString(doc.StreamID)
		if err != nil {
			return nil, fmt.Errorf("parsing stream ID: %w", err)
		}

		eventID, err := typeid.FromString(doc.EventID)
		if err != nil {
			return nil, fmt.Errorf("parsing event ID: %w", err)
		}

		entry := outboxEntry{
			timestamp: doc.Timestamp.Time(),
			streamID:  streamID,
			eventID:   eventID,
			eventData: doc.EventData.Data,
		}

		return entry, nil
	}

	return nil, fmt.Errorf("iterating change stream: %w", i.changeStream.Err())
}

type outboxEntry struct {
	timestamp time.Time
	streamID  typeid.AnyID
	eventID   typeid.AnyID
	eventData []byte
}

func (e outboxEntry) Timestamp() time.Time {
	return e.timestamp
}

func (e outboxEntry) StreamID() typeid.AnyID {
	return e.streamID
}

func (e outboxEntry) EventID() typeid.AnyID {
	return e.eventID
}

func (e outboxEntry) EventData() []byte {
	return e.eventData
}
