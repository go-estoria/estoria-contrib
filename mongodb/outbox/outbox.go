package outbox

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/go-estoria/estoria"
	"github.com/go-estoria/estoria/outbox"
	"github.com/gofrs/uuid/v5"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

type Outbox struct {
	client     *mongo.Client
	database   string
	collection *mongo.Collection
}

func New(client *mongo.Client, database, collection string) *Outbox {
	return &Outbox{
		client:     client,
		database:   database,
		collection: client.Database(database).Collection(collection),
	}
}

func (o *Outbox) Iterator() (outbox.Iterator, error) {
	return &Iterator{
		client:     o.client,
		database:   o.database,
		collection: o.collection.Name(),
	}, nil
}

func (o *Outbox) HandleEvents(sess mongo.SessionContext, events []estoria.EventStoreEvent) error {
	slog.Debug("inserting events into outbox", "tx", "inherited", "events", len(events))

	documents := make([]any, len(events))
	for i, event := range events {
		documents[i] = &outboxDocument{
			Timestamp: primitive.NewDateTimeFromTime(time.Now()),
			StreamID:  event.StreamID().String(),
			EventID:   event.ID().String(),
			EventData: primitive.Binary{Data: event.Data()},
		}
	}

	_, err := o.collection.InsertMany(sess, documents)
	if err != nil {
		return fmt.Errorf("inserting outbox documents: %w", err)
	}

	return nil
}

func (o *Outbox) MarkHandled(ctx context.Context, itemID uuid.UUID, result outbox.HandlerResult) error {
	return nil
}

type outboxDocument struct {
	Timestamp primitive.DateTime `bson:"timestamp"`
	StreamID  string             `bson:"stream_id"`
	EventID   string             `bson:"event_id"`
	EventData primitive.Binary   `bson:"event_data"`
}
