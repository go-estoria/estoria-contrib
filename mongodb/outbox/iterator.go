package outbox

import (
	"context"
	"fmt"
	"time"

	"github.com/go-estoria/estoria/outbox"
	"github.com/go-estoria/estoria/typeid"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Iterator struct {
	client       *mongo.Client
	database     string
	collection   string
	changeStream *mongo.ChangeStream
}

func (i *Iterator) Next(ctx context.Context) (outbox.OutboxItem, error) {
	if i.changeStream == nil {
		changeStream, err := i.getChangeStream(ctx)
		if err != nil {
			return nil, fmt.Errorf("getting change stream: %w", err)
		}

		i.changeStream = changeStream
	}

	if i.changeStream.Next(ctx) {
		changeStreamDoc := changeStreamDocument{}
		if err := i.changeStream.Decode(&changeStreamDoc); err != nil {
			return nil, fmt.Errorf("decoding change stream event: %w", err)
		}

		outboxDoc := changeStreamDoc.OutboxDocument

		streamID, err := typeid.ParseString(outboxDoc.StreamID)
		if err != nil {
			return nil, fmt.Errorf("parsing stream ID: %w", err)
		}

		eventID, err := typeid.ParseUUID(outboxDoc.EventID)
		if err != nil {
			return nil, fmt.Errorf("parsing event ID: %w", err)
		}

		entry := outboxEntry{
			timestamp: outboxDoc.Timestamp.Time(),
			streamID:  streamID,
			eventID:   eventID,
			eventData: outboxDoc.EventData.Data,
		}

		return entry, nil
	}

	return nil, fmt.Errorf("iterating change stream: %w", i.changeStream.Err())
}

func (i *Iterator) getChangeStream(ctx context.Context) (*mongo.ChangeStream, error) {
	collection := i.client.Database(i.database).Collection(i.collection)
	opts := options.ChangeStream().SetFullDocument("updateLookup")
	changeStream, err := collection.Watch(ctx, mongo.Pipeline{
		{{Key: "$match", Value: bson.M{"operationType": "insert"}}},
	}, opts)
	if err != nil {
		return nil, fmt.Errorf("creating change stream: %w", err)
	}

	return changeStream, nil
}

type changeStreamDocument struct {
	OutboxDocument outboxDocument `bson:"fullDocument"`
}

type outboxEntry struct {
	timestamp time.Time
	streamID  typeid.TypeID
	eventID   typeid.UUID
	handlers  map[string]outbox.HandlerResult
	eventData []byte
}

func (e outboxEntry) Timestamp() time.Time {
	return e.timestamp
}

func (e outboxEntry) StreamID() typeid.TypeID {
	return e.streamID
}

func (e outboxEntry) EventID() typeid.UUID {
	return e.eventID
}

func (e outboxEntry) EventData() []byte {
	return e.eventData
}

func (e outboxEntry) Handlers() map[string]outbox.HandlerResult {
	return e.handlers
}

func (e outboxEntry) Lock() {}

func (e outboxEntry) Unlock() {}

func (e outboxEntry) SetHandlerError(handlerName string, err error) {}

func (e outboxEntry) SetCompletedAt(handlerName string, at time.Time) {}
