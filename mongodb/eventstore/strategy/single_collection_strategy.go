package strategy

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/go-estoria/estoria"
	"go.jetpack.io/typeid"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

type SingleCollectionStrategy struct {
	DatabaseName   string
	CollectionName string

	client     *mongo.Client
	database   *mongo.Database
	collection *mongo.Collection
	log        *slog.Logger
}

func (s *SingleCollectionStrategy) Initialize(client *mongo.Client) error {
	s.log = slog.Default().WithGroup("eventstore").WithGroup("strategy").WithGroup("single_collection")
	s.client = client
	s.database = client.Database(s.DatabaseName)
	s.collection = s.database.Collection(s.CollectionName)
	return nil
}

func (s *SingleCollectionStrategy) ReadStream(ctx context.Context, streamID typeid.AnyID, opts estoria.ReadStreamOptions) (estoria.EventStreamIterator, error) {
	s.log.Debug("reading events from stream", "stream_id", streamID.String())

	findOpts := options.Find().SetSort(bson.D{{Key: "timestamp", Value: 1}})
	cursor, err := s.collection.Find(ctx, bson.M{"stream_id": streamID.Suffix()}, findOpts)
	if err != nil {
		s.log.Error("MongoDB error while finding events", "error", err)
		return nil, fmt.Errorf("finding events: %w", err)
	}

	iter := &StreamIterator{
		cursor: cursor,
	}

	return iter, nil
}

func (s *SingleCollectionStrategy) AppendStream(ctx context.Context, streamID typeid.AnyID, opts estoria.AppendStreamOptions, events ...estoria.Event) error {
	s.log.Debug("appending events to stream", "stream_id", streamID.String(), "events", len(events))

	docs := make([]any, len(events))
	for i, e := range events {
		docs[i] = documentFromEvent(e)
	}

	s.log.Debug("starting MongoDB session")
	sessionOpts := options.Session().SetDefaultReadConcern(readconcern.Majority())
	session, err := s.client.StartSession(sessionOpts)
	if err != nil {
		return fmt.Errorf("starting MongoDB session: %w", err)
	}
	defer session.EndSession(ctx)

	transactionFn := func(sessCtx mongo.SessionContext) (any, error) {
		s.log.Debug("inserting events", "events", len(docs))
		result, err := s.collection.InsertMany(sessCtx, docs)
		if err != nil {
			return nil, fmt.Errorf("inserting events: %w", err)
		}

		return result, nil
	}

	s.log.Debug("executing transaction")
	txOpts := options.Transaction().SetReadPreference(readpref.PrimaryPreferred())
	_, err = session.WithTransaction(ctx, transactionFn, txOpts)
	if err != nil {
		return fmt.Errorf("executing transaction: %w", err)
	}

	return nil
}
