package outbox

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/go-estoria/estoria/outbox"
	"github.com/go-estoria/estoria/typeid"
	"github.com/gofrs/uuid/v5"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type Iterator struct {
	client       *mongo.Client
	database     string
	collection   string
	changeStream *mongo.ChangeStream
}

func (i *Iterator) Next(ctx context.Context) (outbox.Item, error) {
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

		parts := strings.Split(outboxDoc.StreamID, "_")
		if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
			return nil, fmt.Errorf("invalid stream ID format: %s", outboxDoc.StreamID)
		}

		streamID := typeid.New(parts[0], uuid.Must(uuid.FromString(parts[1])))

		parts = strings.Split(outboxDoc.StreamID, "_")
		if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
			return nil, fmt.Errorf("invalid stream ID format: %s", outboxDoc.StreamID)
		}

		eventID := typeid.New(parts[0], uuid.Must(uuid.FromString(parts[1])))

		entry := outboxEntry{
			timestamp: outboxDoc.Timestamp,
			streamID:  streamID,
			eventID:   eventID,
			eventData: outboxDoc.EventData,
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
	id        uuid.UUID
	timestamp time.Time
	streamID  typeid.ID
	eventID   typeid.ID
	handlers  map[string]*outbox.HandlerResult
	eventData []byte
}

func (e outboxEntry) ID() uuid.UUID {
	return e.id
}

func (e outboxEntry) Timestamp() time.Time {
	return e.timestamp
}

func (e outboxEntry) StreamID() typeid.ID {
	return e.streamID
}

func (e outboxEntry) EventID() typeid.ID {
	return e.eventID
}

func (e outboxEntry) EventData() []byte {
	return e.eventData
}

func (e outboxEntry) Handlers() outbox.HandlerResultMap {
	return e.handlers
}

func (e outboxEntry) Lock() {}

func (e outboxEntry) Unlock() {}

func (e outboxEntry) SetHandlerError(handlerName string, err error) {}

func (e outboxEntry) SetCompletedAt(handlerName string, at time.Time) {}

func (e outboxEntry) FullyProcessed() bool {
	return false
}
