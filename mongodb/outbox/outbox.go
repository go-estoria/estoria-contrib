package outbox

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/go-estoria/estoria"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

type Outbox struct {
	client     *mongo.Client
	database   string
	collection string
}

func New(client *mongo.Client, database, collection string) *Outbox {
	return &Outbox{
		client:     client,
		database:   database,
		collection: collection,
	}
}

func (o *Outbox) HandleEvents(sess mongo.SessionContext, events []estoria.Event) error {
	slog.Debug("inserting events into outbox", "tx", "inherited", "events", len(events))

	db := o.client.Database(o.database)
	coll := db.Collection(o.collection)

	documents := make([]any, len(events))
	for i, event := range events {
		documents[i] = &outboxDocument{
			Timestamp: primitive.NewDateTimeFromTime(time.Now()),
			StreamID:  event.StreamID().String(),
			EventID:   event.ID().String(),
			EventData: primitive.Binary{Data: event.Data()},
		}
	}

	_, err := coll.InsertMany(sess, documents)
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
