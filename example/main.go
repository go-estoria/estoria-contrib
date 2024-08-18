package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"log/slog"
	"os"
	"time"

	_ "github.com/lib/pq"

	"github.com/go-estoria/estoria"
	esdbes "github.com/go-estoria/estoria-contrib/eventstoredb/eventstore"
	mongoes "github.com/go-estoria/estoria-contrib/mongodb/eventstore"
	mongooutbox "github.com/go-estoria/estoria-contrib/mongodb/outbox"
	pges "github.com/go-estoria/estoria-contrib/postgres/eventstore"
	pgoutbox "github.com/go-estoria/estoria-contrib/postgres/outbox"
	"github.com/go-estoria/estoria/aggregatestore"
	"github.com/go-estoria/estoria/eventstore"
	memoryes "github.com/go-estoria/estoria/eventstore/memory"
	"github.com/go-estoria/estoria/outbox"
	"github.com/go-estoria/estoria/snapshotstore"
	"github.com/go-estoria/estoria/typeid"
	"github.com/gofrs/uuid/v5"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	configureLogging()

	// 1. Create an Event Store to store events.
	eventStores := map[string]eventstore.Store{
		// "memory": newInMemoryEventStore(ctx),
		// "esdb": newESDBEventStore(ctx),
		// "mongo": newMongoEventStore(ctx),
		"pg": newPostgresEventStore(ctx),
	}

	for name, eventStore := range eventStores {
		fmt.Println("Event Store:", name)
		var err error

		// 2. Create an AggregateStore to load and store aggregates.
		var aggregateStore aggregatestore.Store[*Account]
		aggregateStore, err = aggregatestore.NewEventSourcedStore(eventStore, NewAccount,
			aggregatestore.WithStreamReader[*Account](eventStore), // optional: override the stream reader
			aggregatestore.WithStreamWriter[*Account](eventStore), // optional: override the stream writer
		)
		if err != nil {
			panic(err)
		}

		// Enable aggregate snapshots (optional)
		snapshotStores := map[string]aggregatestore.SnapshotStore{
			"memory":      snapshotstore.NewMemoryStore(),
			"eventstream": snapshotstore.NewEventStreamStore(eventStore),
		}

		snapshotStore := snapshotStores["memory"] // choose a snapshot store
		snapshotPolicy := snapshotstore.EventCountSnapshotPolicy{N: 8}
		aggregateStore = aggregatestore.NewSnapshottingStore(aggregateStore, snapshotStore, snapshotPolicy)

		hookableStore := aggregatestore.NewHookableStore(aggregateStore)
		hookableStore.AddHook(aggregatestore.BeforeSave, func(ctx context.Context, aggregate *aggregatestore.Aggregate[*Account]) error {
			slog.Info("before save", "aggregate_id", aggregate.ID())
			return nil
		})
		hookableStore.AddHook(aggregatestore.AfterSave, func(ctx context.Context, aggregate *aggregatestore.Aggregate[*Account]) error {
			slog.Info("after save", "aggregate_id", aggregate.ID())
			return nil
		})

		aggregateStore = hookableStore

		uid, err := uuid.NewV4()
		if err != nil {
			panic(err)
		}

		// 4. Create an aggregate instance.
		aggregate, err := aggregateStore.New(uid)
		if err != nil {
			panic(err)
		}

		events := []estoria.EntityEvent{
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
		}

		for _, event := range events {
			eventID, err := typeid.NewUUID(event.EventType())
			if err != nil {
				panic(err)
			}

			if err := aggregate.Append(&aggregatestore.AggregateEvent{
				ID:          eventID,
				Timestamp:   time.Now(),
				EntityEvent: event,
			}); err != nil {
				panic(err)
			}
		}

		// save the aggregate
		if err := aggregateStore.Save(ctx, aggregate, aggregatestore.SaveOptions{}); err != nil {
			panic(err)
		}

		// load the aggregate
		loadedAggregate, err := aggregateStore.Load(ctx, aggregate.ID(), aggregatestore.LoadOptions{
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
		Level: slog.LevelDebug,
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

func newInMemoryEventStore(ctx context.Context) eventstore.Store {
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

func newESDBEventStore(ctx context.Context) eventstore.Store {
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

func newMongoEventStore(ctx context.Context) eventstore.Store {
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

func newPostgresEventStore(ctx context.Context) eventstore.Store {
	db, err := sql.Open("postgres", os.Getenv("POSTGRES_URI"))
	if err != nil {
		panic(fmt.Errorf("opening database connection: %w", err))
	}

	if err := db.PingContext(ctx); err != nil {
		panic(fmt.Errorf("pinging database: %w", err))
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
	// slog.Info("OUTBOX LOGGER HANDLER", "event_id", item.EventID(), "handlers", len(item.Handlers()))
	return nil
}
