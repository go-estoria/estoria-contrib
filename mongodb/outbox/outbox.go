package outbox

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/go-estoria/estoria"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MongoCollection interface {
	InsertMany(ctx context.Context, documents []any, opts ...*options.InsertManyOptions) (*mongo.InsertManyResult, error)
}

type Outbox struct {
	client     *mongo.Client
	collection MongoCollection
}

func New(client *mongo.Client, database, collection string) *Outbox {
	return &Outbox{
		client:     client,
		collection: client.Database(database).Collection(collection),
	}
}

func (o *Outbox) HandleEvents(sess mongo.SessionContext, events []estoria.Event) error {
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

type outboxDocument struct {
	Timestamp primitive.DateTime `bson:"timestamp"`
	StreamID  string             `bson:"stream_id"`
	EventID   string             `bson:"event_id"`
	EventData primitive.Binary   `bson:"event_data"`
}
