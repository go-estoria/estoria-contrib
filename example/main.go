package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"os"
	"time"

	"github.com/go-estoria/estoria"
	esdbes "github.com/go-estoria/estoria-contrib/eventstoredb/eventstore"
	mongoes "github.com/go-estoria/estoria-contrib/mongodb/eventstore"
	mongooutbox "github.com/go-estoria/estoria-contrib/mongodb/outbox"
	postgres "github.com/go-estoria/estoria-contrib/postgres"
	pges "github.com/go-estoria/estoria-contrib/postgres/eventstore"
	pgoutbox "github.com/go-estoria/estoria-contrib/postgres/outbox"
	"github.com/go-estoria/estoria/aggregatestore"
	memoryes "github.com/go-estoria/estoria/eventstore/memory"
	"github.com/go-estoria/estoria/outbox"
	"github.com/go-estoria/estoria/snapshot"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	configureLogging()

	// 1. Create an Event Store to store events.
	eventStores := map[string]estoria.EventStore{
		"memory": newInMemoryEventStore(ctx),
		// "esdb": newESDBEventStore(ctx),
		// "mongo": newMongoEventStore(ctx),
		// "pg": newPostgresEventStore(ctx),
	}

	for name, eventStore := range eventStores {
		fmt.Println("Event Store:", name)
		var err error

		// 2. Create an AggregateStore to load and store aggregates.
		var aggregateStore estoria.AggregateStore[*Account]
		aggregateStore, err = aggregatestore.New(eventStore, eventStore, NewAccount)
		if err != nil {
			panic(err)
		}

		// Enable aggregate snapshots (optional)
		snapshotReader := snapshot.NewEventStreamReader(eventStore)
		snapshotWriter := snapshot.NewEventStreamWriter(eventStore)
		snapshotPolicy := estoria.EventCountSnapshotPolicy{N: 8}
		aggregateStore = aggregatestore.NewSnapshottingAggregateStore(aggregateStore, snapshotReader, snapshotWriter, snapshotPolicy)

		hookableStore := aggregatestore.NewHookableAggregateStore(aggregateStore)
		hookableStore.AddHook(aggregatestore.BeforeSave, func(ctx context.Context, aggregate *estoria.Aggregate[*Account]) error {
			slog.Info("before save", "aggregate_id", aggregate.ID())
			return nil
		})
		hookableStore.AddHook(aggregatestore.AfterSave, func(ctx context.Context, aggregate *estoria.Aggregate[*Account]) error {
			slog.Info("after save", "aggregate_id", aggregate.ID())
			return nil
		})

		aggregateStore = hookableStore

		// 4. Create an aggregate instance.
		aggregate, err := aggregateStore.NewAggregate()
		if err != nil {
			panic(err)
		}

		// fmt.Println("created new aggregate with ID", aggregate.ID())

		if err := aggregate.Append(
			&UserCreatedEvent{Username: "jdoe"},
			&BalanceChangedEvent{Amount: 100},
			&BalanceChangedEvent{Amount: -72},
			&UserCreatedEvent{Username: "rlowe"},
			&UserDeletedEvent{Username: "rlowe"},
			&BalanceChangedEvent{Amount: 34},
			&BalanceChangedEvent{Amount: 60},
			&BalanceChangedEvent{Amount: -23},
			&BalanceChangedEvent{Amount: 1},
			&UserCreatedEvent{Username: "bschmoe"},
			&BalanceChangedEvent{Amount: -14},
			&BalanceChangedEvent{Amount: -696},
			&BalanceChangedEvent{Amount: 334},
			&BalanceChangedEvent{Amount: 63},
			&UserDeletedEvent{Username: "jdoe"},
			&BalanceChangedEvent{Amount: -92},
			&BalanceChangedEvent{Amount: -125},
			&BalanceChangedEvent{Amount: 883},
			&BalanceChangedEvent{Amount: 626},
			&BalanceChangedEvent{Amount: -620},
		); err != nil {
			panic(err)
		}

		// save the aggregate
		if err := aggregateStore.Save(ctx, aggregate, estoria.SaveAggregateOptions{}); err != nil {
			panic(err)
		}

		// load the aggregate
		loadedAggregate, err := aggregateStore.Load(ctx, aggregate.ID(), estoria.LoadAggregateOptions{
			// ToVersion: 15,
		})
		if err != nil {
			panic(err)
		}

		// fmt.Println("loaded aggregate with ID", loadedAggregate.ID())

		// newEvents, err := aggregate.Data.Diff(&Account{
		// 	ID:      "123",
		// 	Users:   []string{"bschmoe", "rlowe"},
		// 	Balance: 80,
		// })
		// if err != nil {
		// 	panic(err)
		// }

		// if err := aggregate.Append(newEvents...); err != nil {
		// 	panic(err)
		// }

		// if err := aggregateStore.Save(ctx, aggregate); err != nil {
		// 	panic(err)
		// }

		// aggregate, err = aggregateStore.Load(ctx, estoria.StringID("123"))
		// if err != nil {
		// 	panic(err)
		// }

		// get the aggregate data
		account := loadedAggregate.Entity()
		// fmt.Println()
		fmt.Println(account)
		// fmt.Println()

		<-time.After(5 * time.Second)
	}
}

func configureLogging() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelInfo,
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			switch a.Key {
			case "time":
				t := a.Value.Time()
				return slog.Attr{
					Key:   "t",
					Value: slog.StringValue(t.Format(time.TimeOnly)),
				}
			case "level":
				return slog.Attr{
					Key:   "l",
					Value: a.Value,
				}
			}

			return a
		},
	})))
}

func newInMemoryEventStore(ctx context.Context) estoria.EventStore {
	inMemoryOutbox := memoryes.NewOutbox()

	logger := &OutboxLogger{}
	inMemoryOutbox.RegisterHandlers(UserCreatedEvent{}, logger)
	inMemoryOutbox.RegisterHandlers(UserDeletedEvent{}, logger)
	inMemoryOutbox.RegisterHandlers(BalanceChangedEvent{}, logger)

	outboxProcessor := outbox.NewProcessor(inMemoryOutbox)
	outboxProcessor.RegisterHandlers(logger)

	if err := outboxProcessor.Start(ctx); err != nil {
		panic(err)
	}

	return memoryes.NewEventStore(
		memoryes.WithOutbox(inMemoryOutbox),
	)
}

func newESDBEventStore(ctx context.Context) estoria.EventStore {
	esdbClient, err := esdbes.NewDefaultEventStoreDBClient(
		ctx,
		os.Getenv("EVENTSTOREDB_URI"),
		os.Getenv("EVENTSTOREDB_USERNAME"),
		os.Getenv("EVENTSTOREDB_PASSWORD"),
	)
	if err != nil {
		panic(err)
	}

	esdbEventStore, err := esdbes.NewEventStore(esdbClient)
	if err != nil {
		panic(err)
	}

	return esdbEventStore
}

func newMongoEventStore(ctx context.Context) estoria.EventStore {
	mongoClient, err := mongoes.NewDefaultMongoDBClient(ctx, "example-app", os.Getenv("MONGODB_URI"))
	if err != nil {
		panic(err)
	}

	slog.Info("pinging MongoDB", "uri", os.Getenv("MONGODB_URI"))
	pingCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if err := mongoClient.Ping(pingCtx, nil); err != nil {
		log.Fatalf("failed to ping MongoDB: %v", err)
	}

	mongoOutbox := mongooutbox.New(mongoClient, "example-app", "outbox")
	outboxProcessor := outbox.NewProcessor(mongoOutbox)
	logger := &OutboxLogger{}
	outboxProcessor.RegisterHandlers(logger)

	if err := outboxProcessor.Start(ctx); err != nil {
		panic(err)
	}

	mongoEventStore, err := mongoes.NewEventStore(
		mongoClient,
		mongoes.WithTransactionHook(mongoOutbox),
	)
	if err != nil {
		panic(err)
	}

	return mongoEventStore
}

func newPostgresEventStore(ctx context.Context) estoria.EventStore {
	db, err := postgres.NewDefaultDB(ctx, os.Getenv("POSTGRES_URI"))
	if err != nil {
		panic(err)
	}

	// create the 'events' table if it doesn't exist
	if _, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS events (
			event_id    TEXT PRIMARY KEY,
			stream_type TEXT NOT NULL,
			stream_id   TEXT NOT NULL,
			event_type  TEXT NOT NULL,
			timestamp   TIMESTAMPTZ NOT NULL,
			version     BIGINT NOT NULL,
			data        BYTEA NOT NULL
		);
	`); err != nil {
		panic(err)
	}

	outbox, err := pgoutbox.New(db, "outbox", pgoutbox.WithFullEventData(false))
	if err != nil {
		panic(err)
	}

	// create the 'outbox' table if it doesn't exist
	if _, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS outbox (
			id         SERIAL PRIMARY KEY,
			timestamp  TIMESTAMPTZ NOT NULL,
			stream_id  TEXT NOT NULL,
			event_id   TEXT NOT NULL,
			event_data BYTEA
		);
	`); err != nil {
		panic(err)
	}

	postgresEventStore, err := pges.NewEventStore(db, pges.WithOutbox(outbox))
	if err != nil {
		panic(err)
	}

	return postgresEventStore
}

type OutboxLogger struct{}

func (l OutboxLogger) Name() string {
	return "logger"
}

func (l OutboxLogger) Handle(_ context.Context, item outbox.OutboxItem) error {
	slog.Info("OUTBOX LOGGER HANDLER", "event_id", item.EventID(), "handlers", len(item.Handlers()))
	return nil
}
