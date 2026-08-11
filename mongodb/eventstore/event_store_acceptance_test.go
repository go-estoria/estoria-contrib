package eventstore_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/go-estoria/estoria-contrib/mongodb/eventstore"
	"github.com/go-estoria/estoria-contrib/mongodb/eventstore/strategy"
	coreeventstore "github.com/go-estoria/estoria/eventstore"
	"github.com/go-estoria/estoria/eventstore/storetest"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

func TestEventStore_AcceptanceTest(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	t.Parallel()

	ctx := t.Context()

	mongoClient, err := createMongoDBContainer(t)
	if err != nil {
		t.Fatalf("failed to create MongoDB container: %v", err)
	}

	for _, tt := range []struct {
		name     string
		haveOpts func(*testing.T, *mongo.Database) []eventstore.EventStoreOption
	}{
		{
			name: "store with default options",
			haveOpts: func(*testing.T, *mongo.Database) []eventstore.EventStoreOption {
				return []eventstore.EventStoreOption{}
			},
		},
		{
			name: "store with single collection strategy",
			haveOpts: func(t *testing.T, db *mongo.Database) []eventstore.EventStoreOption {
				t.Helper()
				strat, err := strategy.NewSingleCollectionStrategy(mongoClient,
					db.Collection("events"),
					db.Collection(strategy.DefaultStreamsCollectionName),
				)
				if err != nil {
					t.Fatalf("tc setup: failed to create SingleCollectionStrategy: %v", err)
				}
				// The explicit deploy-time indexing flow; the multi-collection case uses
				// auto-ensure and the default-options case runs unindexed, so all three
				// arrangements stay covered.
				if err := strat.EnsureIndexes(t.Context()); err != nil {
					t.Fatalf("tc setup: failed to ensure indexes: %v", err)
				}
				return []eventstore.EventStoreOption{eventstore.WithStrategy(strat)}
			},
		},
		{
			// One collection per stream, so multi-cursor reads are genuinely multi-cursor;
			// a constant selector would collapse this case into the single-collection one.
			name: "store with multi collection strategy",
			haveOpts: func(t *testing.T, db *mongo.Database) []eventstore.EventStoreOption {
				t.Helper()
				strat, err := strategy.NewMultiCollectionStrategy(mongoClient, db, strategy.CollectionPerStreamID(),
					strategy.WithAutoEnsureIndexes())
				if err != nil {
					t.Fatalf("tc setup: failed to create MultiCollectionStrategy: %v", err)
				}
				return []eventstore.EventStoreOption{eventstore.WithStrategy(strat)}
			},
		},
	} {
		// Each strategy runs as its own subtest so the suite's clause names nest under the
		// strategy that failed them; previously all three shared one flat scope.
		t.Run(tt.name, func(t *testing.T) {
			database := mongoClient.Database("estoria")
			t.Cleanup(func() {
				if err := database.Drop(context.WithoutCancel(ctx)); err != nil {
					t.Fatalf("tc cleanup: failed to drop database: %v", err)
				}
			})

			eventStore, err := eventstore.New(mongoClient, tt.haveOpts(t, database)...)
			if err != nil {
				t.Fatalf("tc setup: failed to create EventStore: %v", err)
			}

			storetest.RunEventStoreSuite(t, func(*testing.T) coreeventstore.Store {
				return eventStore
			})
		})
	}
}

func TestEventStore_StreamDeleterAcceptanceTest(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping acceptance test")
	}

	t.Parallel()

	ctx := t.Context()

	mongoClient, err := createMongoDBContainer(t)
	if err != nil {
		t.Fatalf("failed to create MongoDB container: %v", err)
	}

	for _, tt := range []struct {
		name     string
		haveOpts func(*testing.T, *mongo.Database) []eventstore.EventStoreOption
	}{
		{
			name: "store with default options",
			haveOpts: func(*testing.T, *mongo.Database) []eventstore.EventStoreOption {
				return []eventstore.EventStoreOption{}
			},
		},
		{
			name: "store with single collection strategy",
			haveOpts: func(t *testing.T, db *mongo.Database) []eventstore.EventStoreOption {
				t.Helper()
				strat, err := strategy.NewSingleCollectionStrategy(mongoClient,
					db.Collection("events"),
					db.Collection(strategy.DefaultStreamsCollectionName),
				)
				if err != nil {
					t.Fatalf("tc setup: failed to create SingleCollectionStrategy: %v", err)
				}
				// The explicit deploy-time indexing flow; the multi-collection case uses
				// auto-ensure and the default-options case runs unindexed, so all three
				// arrangements stay covered.
				if err := strat.EnsureIndexes(t.Context()); err != nil {
					t.Fatalf("tc setup: failed to ensure indexes: %v", err)
				}
				return []eventstore.EventStoreOption{eventstore.WithStrategy(strat)}
			},
		},
		{
			name: "store with multi collection strategy",
			haveOpts: func(t *testing.T, db *mongo.Database) []eventstore.EventStoreOption {
				t.Helper()
				strat, err := strategy.NewMultiCollectionStrategy(mongoClient, db, strategy.CollectionPerStreamID(),
					strategy.WithAutoEnsureIndexes())
				if err != nil {
					t.Fatalf("tc setup: failed to create MultiCollectionStrategy: %v", err)
				}
				return []eventstore.EventStoreOption{eventstore.WithStrategy(strat)}
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			database := mongoClient.Database("estoria")
			t.Cleanup(func() {
				if err := database.Drop(context.WithoutCancel(ctx)); err != nil {
					t.Fatalf("tc cleanup: failed to drop database: %v", err)
				}
			})

			eventStore, err := eventstore.New(mongoClient, tt.haveOpts(t, database)...)
			if err != nil {
				t.Fatalf("tc setup: failed to create EventStore: %v", err)
			}

			storetest.RunStreamDeleterSuite(t, func(*testing.T) storetest.DeleterStore {
				return eventStore
			})
		})
	}
}

// The global reader suite requires exclusive ownership of the store's history, so unlike
// the suite above, every clause gets a fresh database.
func TestEventStore_GlobalReaderAcceptanceTest(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping acceptance test")
	}

	t.Parallel()

	ctx := t.Context()

	mongoClient, err := createMongoDBContainer(t)
	if err != nil {
		t.Fatalf("failed to create MongoDB container: %v", err)
	}

	dbCount := 0
	newDatabase := func(t *testing.T) *mongo.Database {
		t.Helper()
		dbCount++
		database := mongoClient.Database(fmt.Sprintf("estoria_global_%d", dbCount))
		t.Cleanup(func() {
			if err := database.Drop(context.WithoutCancel(ctx)); err != nil {
				t.Fatalf("tc cleanup: failed to drop database: %v", err)
			}
		})
		return database
	}

	newStore := func(t *testing.T, opts ...eventstore.EventStoreOption) storetest.GlobalStore {
		t.Helper()
		eventStore, err := eventstore.New(mongoClient, opts...)
		if err != nil {
			t.Fatalf("tc setup: failed to create EventStore: %v", err)
		}
		return eventStore
	}

	for _, tt := range []struct {
		name     string
		newStore storetest.NewGlobalStoreFunc
	}{
		{
			// The default configuration hardwires the "estoria" database, so exclusivity
			// comes from dropping it before each clause; the suite's clauses run
			// sequentially.
			name: "store with default options",
			newStore: func(t *testing.T) storetest.GlobalStore {
				t.Helper()
				if err := mongoClient.Database(eventstore.DefaultDatabaseName).Drop(ctx); err != nil {
					t.Fatalf("tc setup: failed to drop database: %v", err)
				}
				return newStore(t)
			},
		},
		{
			name: "store with single collection strategy",
			newStore: func(t *testing.T) storetest.GlobalStore {
				t.Helper()
				db := newDatabase(t)
				strat, err := strategy.NewSingleCollectionStrategy(mongoClient,
					db.Collection("events"),
					db.Collection(strategy.DefaultStreamsCollectionName),
				)
				if err != nil {
					t.Fatalf("tc setup: failed to create SingleCollectionStrategy: %v", err)
				}
				if err := strat.EnsureIndexes(t.Context()); err != nil {
					t.Fatalf("tc setup: failed to ensure indexes: %v", err)
				}
				return newStore(t, eventstore.WithStrategy(strat))
			},
		},
		{
			// One collection per stream, so global reads exercise the multi-cursor merge.
			name: "store with multi collection strategy",
			newStore: func(t *testing.T) storetest.GlobalStore {
				t.Helper()
				db := newDatabase(t)
				strat, err := strategy.NewMultiCollectionStrategy(mongoClient, db, strategy.CollectionPerStreamID(),
					strategy.WithAutoEnsureIndexes())
				if err != nil {
					t.Fatalf("tc setup: failed to create MultiCollectionStrategy: %v", err)
				}
				return newStore(t, eventstore.WithStrategy(strat))
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			storetest.RunGlobalReaderSuite(t, tt.newStore)
		})
	}
}
