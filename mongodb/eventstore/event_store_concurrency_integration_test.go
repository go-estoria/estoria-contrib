package eventstore_test

import (
	"fmt"
	"sync"
	"testing"

	"github.com/go-estoria/estoria-contrib/mongodb/eventstore"
	"github.com/go-estoria/estoria-contrib/mongodb/eventstore/strategy"
	coreeventstore "github.com/go-estoria/estoria/eventstore"
	"github.com/go-estoria/estoria/typeid"
)

// Concurrent appends must allocate unique global positions and contiguous per-stream
// versions, which is the property the counter documents exist to provide: deriving
// offsets from document maxima allowed concurrent multi-collection appends to claim the
// same global offset.
func TestEventStore_Integration_ConcurrentAppendOffsets(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	t.Parallel()

	ctx := t.Context()

	mongoClient, err := createMongoDBContainer(t)
	if err != nil {
		t.Fatalf("failed to create MongoDB container: %v", err)
	}

	const (
		numStreams       = 8
		appendsPerStream = 5
		eventsPerAppend  = 2
	)

	for _, tt := range []struct {
		name     string
		haveOpts func(*testing.T, string) []eventstore.EventStoreOption
	}{
		{
			name: "single collection strategy",
			haveOpts: func(t *testing.T, dbName string) []eventstore.EventStoreOption {
				t.Helper()
				db := mongoClient.Database(dbName)
				strat, err := strategy.NewSingleCollectionStrategy(mongoClient,
					db.Collection("events"),
					db.Collection(strategy.DefaultStreamsCollectionName),
				)
				if err != nil {
					t.Fatalf("tc setup: failed to create SingleCollectionStrategy: %v", err)
				}
				return []eventstore.EventStoreOption{eventstore.WithStrategy(strat)}
			},
		},
		{
			name: "multi collection strategy",
			haveOpts: func(t *testing.T, dbName string) []eventstore.EventStoreOption {
				t.Helper()
				db := mongoClient.Database(dbName)
				strat, err := strategy.NewMultiCollectionStrategy(mongoClient, db, strategy.CollectionPerStreamID())
				if err != nil {
					t.Fatalf("tc setup: failed to create MultiCollectionStrategy: %v", err)
				}
				return []eventstore.EventStoreOption{eventstore.WithStrategy(strat)}
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			dbName := "estoria_concurrency_" + typeid.NewV4("db").UUID.String()[0:8]
			t.Cleanup(func() {
				if err := mongoClient.Database(dbName).Drop(t.Context()); err != nil {
					t.Logf("tc cleanup: failed to drop database %s: %v", dbName, err)
				}
			})

			store, err := eventstore.New(mongoClient, tt.haveOpts(t, dbName)...)
			if err != nil {
				t.Fatalf("tc setup: failed to create EventStore: %v", err)
			}

			streamIDs := make([]typeid.ID, numStreams)
			for i := range streamIDs {
				streamIDs[i] = typeid.NewV4("concurrencytest")
			}

			var wg sync.WaitGroup
			appendErrs := make([]error, numStreams)
			for i, streamID := range streamIDs {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for range appendsPerStream {
						events := make([]*coreeventstore.WritableEvent, eventsPerAppend)
						for j := range events {
							events[j] = &coreeventstore.WritableEvent{
								Type: "testevent",
								Data: fmt.Appendf(nil, `{"stream":%d}`, i),
							}
						}
						if _, err := store.AppendStream(ctx, streamID, events, coreeventstore.AppendStreamOptions{}); err != nil {
							appendErrs[i] = err
							return
						}
					}
				}()
			}
			wg.Wait()

			for i, err := range appendErrs {
				if err != nil {
					t.Fatalf("appending to stream %d: %v", i, err)
				}
			}

			// Every stream must hold contiguous versions from 1, each event carrying a
			// globally unique position.
			seenPositions := map[int64]typeid.ID{}
			for _, streamID := range streamIDs {
				iter, err := store.ReadStream(ctx, streamID, coreeventstore.ReadStreamOptions{})
				if err != nil {
					t.Fatalf("reading stream %s: %v", streamID, err)
				}
				events, err := coreeventstore.Collect(ctx, iter)
				if err != nil {
					t.Fatalf("collecting stream %s: %v", streamID, err)
				}
				if len(events) != appendsPerStream*eventsPerAppend {
					t.Fatalf("stream %s holds %d events, want %d", streamID, len(events), appendsPerStream*eventsPerAppend)
				}
				for i, event := range events {
					if event.StreamVersion != int64(i)+1 {
						t.Errorf("stream %s event %d has version %d, want %d", streamID, i, event.StreamVersion, i+1)
					}
					if event.GlobalPosition == nil {
						t.Errorf("stream %s event %d has nil global position", streamID, i)
						continue
					}
					if holder, taken := seenPositions[*event.GlobalPosition]; taken {
						t.Errorf("global position %d allocated to both %s and %s", *event.GlobalPosition, holder, streamID)
					}
					seenPositions[*event.GlobalPosition] = streamID
				}
			}

			// A global read must yield every event in strictly ascending position order.
			iter, err := store.ReadAll(ctx, coreeventstore.ReadAllOptions{})
			if err != nil {
				t.Fatalf("reading all events: %v", err)
			}
			all, err := coreeventstore.Collect(ctx, iter)
			if err != nil {
				t.Fatalf("collecting all events: %v", err)
			}
			if want := numStreams * appendsPerStream * eventsPerAppend; len(all) != want {
				t.Fatalf("global read yielded %d events, want %d", len(all), want)
			}
			lastPosition := int64(0)
			for i, event := range all {
				if event.GlobalPosition == nil {
					t.Fatalf("global read event %d has nil global position", i)
				}
				if *event.GlobalPosition <= lastPosition {
					t.Errorf("global read event %d has position %d, not greater than previous %d", i, *event.GlobalPosition, lastPosition)
				}
				lastPosition = *event.GlobalPosition
			}
		})
	}
}
