package outbox

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/go-estoria/estoria"
	"go.jetpack.io/typeid"
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
			StreamID:  event.StreamID(),
			Timestamp: time.Now(),
			Event:     event,
		}
	}

	_, err := coll.InsertMany(sess, documents)
	if err != nil {
		return fmt.Errorf("inserting outbox documents: %w", err)
	}

	return nil
}

type outboxDocument struct {
	StreamID  typeid.AnyID  `bson:"stream_id"`
	Timestamp time.Time     `bson:"timestamp"`
	Event     estoria.Event `bson:"event"`
}
