package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"os"
	"time"

	"github.com/go-estoria/estoria"

	// memoryes "github.com/go-estoria/estoria/eventstore/memory"
	mongoes "github.com/go-estoria/estoria-contrib/mongodb/eventstore"
	// redises "github.com/go-estoria/estoria-contrib/redis/eventstore"
)

func main() {
	ctx := context.Background()
	configureLogging()

	// 1. Create an Event Store to store events.
	var eventReader estoria.EventStreamReader
	var eventWriter estoria.EventStreamWriter

	// // EventStoreDB Event Store
	// {
	// 	esdbClient, err := esdbes.NewDefaultEventStoreDBClient(
	// 		ctx,
	// 		os.Getenv("EVENTSTOREDB_URI"),
	// 		os.Getenv("EVENTSTOREDB_USERNAME"),
	// 		os.Getenv("EVENTSTOREDB_PASSWORD"),
	// 	)
	// 	if err != nil {
	// 		panic(err)
	// 	}
	// 	defer esdbClient.Close()

	// 	eventStore, err := esdbes.NewEventStore(esdbClient)
	// 	if err != nil {
	// 		panic(err)
	// 	}

	// 	eventReader = eventStore
	// 	eventWriter = eventStore
	// }

	// MongoDB Event Store
	{
		mongoClient, err := mongoes.NewDefaultMongoDBClient(ctx, "example-app", os.Getenv("MONGODB_URI"))
		if err != nil {
			panic(err)
		}
		defer mongoClient.Disconnect(ctx)

		slog.Info("pinging MongoDB", "uri", os.Getenv("MONGODB_URI"))
		pingCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()
		if err := mongoClient.Ping(pingCtx, nil); err != nil {
			log.Fatalf("failed to ping MongoDB: %v", err)
		}

		eventStore, err := mongoes.NewEventStore(mongoClient)
		if err != nil {
			panic(err)
		}

		eventReader = eventStore
		eventWriter = eventStore
	}

	// // Redis Event Store
	// {
	// 	redisClient, err := redises.NewDefaultRedisClient(
	// 		ctx,
	// 		os.Getenv("REDIS_URI"),
	// 		os.Getenv("REDIS_USERNAME"),
	// 		os.Getenv("REDIS_PASSWORD"),
	// 	)
	// 	if err != nil {
	// 		panic(err)
	// 	}
	// 	defer redisClient.Close()

	// 	slog.Info("pinging Redis", "uri", os.Getenv("REDIS_URI"))
	// 	pingCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	// 	defer cancel()
	// 	if _, err := redisClient.Ping(pingCtx).Result(); err != nil {
	// 		log.Fatalf("failed to ping Redis: %v", err)
	// 	}

	// 	eventStore, err := redises.NewEventStore(redisClient)
	// 	if err != nil {
	// 		panic(err)
	// 	}

	// 	eventReader = eventStore
	// 	eventWriter = eventStore
	// }

	// // Memory Event Store
	// {
	// 	eventStore = &memoryes.EventStore{
	// 		Events: []estoria.Event{},
	// 	}
	// }

	// 2. Create an AggregateStore store aggregates.
	aggregateStore := estoria.NewAggregateStore(eventReader, eventWriter, NewAccount)

	// Enable aggregate snapshots (optional)
	// snapshotReader := snapshotter.NewEventStreamSnapshotReader(eventReader)
	// snapshotWriter := snapshotter.NewEventStreamSnapshotWriter(eventWriter)
	// snapshotPolicy := estoria.EventCountSnapshotPolicy{N: 2}
	// ssAggregateStore := aggregatestore.NewSnapshottingAggregateStore(aggregateStore, snapshotReader, snapshotWriter, snapshotPolicy)

	// 3. Allow the aggregate store to store events of a specific type.
	aggregateStore.Allow(&UserCreatedEvent{})
	aggregateStore.Allow(&UserDeletedEvent{})
	aggregateStore.Allow(&BalanceChangedEvent{})

	// 4. Create an aggregate instance.
	aggregate, err := aggregateStore.NewAggregate()
	if err != nil {
		panic(err)
	}

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
	if err := aggregateStore.Save(ctx, aggregate); err != nil {
		panic(err)
	}

	// load the aggregate
	loadedAggregate, err := aggregateStore.Load(ctx, aggregate.ID())
	if err != nil {
		panic(err)
	}

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
	fmt.Println(account)
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
