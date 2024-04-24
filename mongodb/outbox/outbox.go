package outbox

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/go-estoria/estoria"
	"go.jetpack.io/typeid"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
	"go.mongodb.org/mongo-driver/mongo/readpref"
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

func (o *Outbox) HandleEvents(ctx context.Context, events []estoria.Event) error {
	slog.Debug("inserting events into outbox", "tx", "separate", "events", len(events))
	sessionOpts := options.Session().
		SetDefaultReadConcern(readconcern.Majority()).
		SetDefaultReadPreference(readpref.Primary())
	session, err := o.client.StartSession(sessionOpts)
	if err != nil {
		return fmt.Errorf("starting MongoDB session: %w", err)
	}
	defer session.EndSession(ctx)
	slog.Debug("started MongoDB session", "tx", session.ID())

	transactionFn := func(sessCtx mongo.SessionContext) (any, error) {
		if err := o.HandleEventsInTransaction(sessCtx, events); err != nil {
			return nil, fmt.Errorf("appending events in transaction: %w", err)
		}

		return nil, nil
	}

	txOpts := options.Transaction().SetReadPreference(readpref.Primary())
	_, err = session.WithTransaction(ctx, transactionFn, txOpts)
	if err != nil {
		return fmt.Errorf("executing transaction: %w", err)
	}

	return nil
}

func (o *Outbox) HandleEventsInTransaction(sess mongo.SessionContext, events []estoria.Event) error {
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
