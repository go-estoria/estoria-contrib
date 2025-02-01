package strategy_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/go-estoria/estoria-contrib/mongodb/eventstore/strategy"
	"github.com/go-estoria/estoria/eventstore"
	"github.com/go-estoria/estoria/typeid"
	"github.com/gofrs/uuid/v5"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/mongodb"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
)

func TestSingleCollectionStrategy_Integration_GetStreamIterator(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	t.Parallel()

	ctx := context.Background()

	mongoClient, err := createMongoDBContainer(t, ctx)
	if err != nil {
		t.Fatalf("failed to create MongoDB container: %v", err)
	}

	collection := mongoClient.Database("estoria").Collection("events")

	res, err := collection.InsertMany(ctx, []any{
		bson.M{
			"stream_id": "a422f08c-0981-49cd-8249-7a48e66a4e8c", "stream_type": "mockstreamtype",
			"event_id": "b112c50d-0834-4b78-a9e7-009d80b79001", "event_type": "mockeventtypeA",
			"offset": 1, "timestamp": "2025-11-05T12:34:01Z", "data": bson.M{},
		},
		bson.M{
			"stream_id": "a422f08c-0981-49cd-8249-7a48e66a4e8c", "stream_type": "mockstreamtype",
			"event_id": "b112c50d-0834-4b78-a9e7-009d80b79002", "event_type": "mockeventtypeB",
			"offset": 2, "timestamp": "2025-11-05T12:34:02Z", "data": bson.M{},
		},
		bson.M{
			"stream_id": "a422f08c-0981-49cd-8249-7a48e66a4e8c", "stream_type": "mockstreamtype",
			"event_id": "b112c50d-0834-4b78-a9e7-009d80b79003", "event_type": "mockeventtypeC",
			"offset": 3, "timestamp": "2025-11-05T12:34:03Z", "data": bson.M{},
		},
		bson.M{
			"stream_id": "a422f08c-0981-49cd-8249-7a48e66a4e8c", "stream_type": "mockstreamtype",
			"event_id": "b112c50d-0834-4b78-a9e7-009d80b79002", "event_type": "mockeventtypeB",
			"offset": 4, "timestamp": "2025-11-05T12:34:04Z", "data": bson.M{},
		},
		bson.M{
			"stream_id": "a422f08c-0981-49cd-8249-7a48e66a4e8c", "stream_type": "mockstreamtype",
			"event_id": "b112c50d-0834-4b78-a9e7-009d80b79003", "event_type": "mockeventtypeC",
			"offset": 5, "timestamp": "2025-11-05T12:34:05Z", "data": bson.M{},
		},
	})
	if err != nil {
		t.Fatalf("failed to insert events into MongoDB: %v", err)
	} else if len(res.InsertedIDs) != 5 {
		t.Fatalf("unexpected number of inserted IDs, want: 5, got: %d", len(res.InsertedIDs))
	}

	events := []*eventstore.Event{
		{
			ID:            typeid.FromUUID("mockeventtype", uuid.Must(uuid.FromString("b112c50d-0834-4b78-a9e7-009d80b79001"))),
			StreamID:      typeid.FromUUID("mockstreamtype", uuid.Must(uuid.FromString("a422f08c-0981-49cd-8249-7a48e66a4e8c"))),
			StreamVersion: 1,
			Timestamp:     time.Date(2025, 11, 5, 12, 34, 01, 0, time.UTC),
			Data:          []byte{},
		},
		{
			ID:            typeid.FromUUID("mockeventtype", uuid.Must(uuid.FromString("b112c50d-0834-4b78-a9e7-009d80b79002"))),
			StreamID:      typeid.FromUUID("mockstreamtype", uuid.Must(uuid.FromString("a422f08c-0981-49cd-8249-7a48e66a4e8c"))),
			StreamVersion: 2,
			Timestamp:     time.Date(2025, 11, 5, 12, 34, 02, 0, time.UTC),
			Data:          []byte{},
		},
		{
			ID:            typeid.FromUUID("mockeventtype", uuid.Must(uuid.FromString("b112c50d-0834-4b78-a9e7-009d80b79003"))),
			StreamID:      typeid.FromUUID("mockstreamtype", uuid.Must(uuid.FromString("a422f08c-0981-49cd-8249-7a48e66a4e8c"))),
			StreamVersion: 3,
			Timestamp:     time.Date(2025, 11, 5, 12, 34, 03, 0, time.UTC),
			Data:          []byte{},
		},
		{
			ID:            typeid.FromUUID("mockeventtype", uuid.Must(uuid.FromString("b112c50d-0834-4b78-a9e7-009d80b79002"))),
			StreamID:      typeid.FromUUID("mockstreamtype", uuid.Must(uuid.FromString("a422f08c-0981-49cd-8249-7a48e66a4e8c"))),
			StreamVersion: 4,
			Timestamp:     time.Date(2025, 11, 5, 12, 34, 04, 0, time.UTC),
			Data:          []byte{},
		},
		{
			ID:            typeid.FromUUID("mockeventtype", uuid.Must(uuid.FromString("b112c50d-0834-4b78-a9e7-009d80b79003"))),
			StreamID:      typeid.FromUUID("mockstreamtype", uuid.Must(uuid.FromString("a422f08c-0981-49cd-8249-7a48e66a4e8c"))),
			StreamVersion: 5,
			Timestamp:     time.Date(2025, 11, 5, 12, 34, 05, 0, time.UTC),
			Data:          []byte{},
		},
	}

	for _, tt := range []struct {
		name         string
		haveStreamID typeid.UUID
		haveOpts     eventstore.ReadStreamOptions
		wantEvents   []*eventstore.Event
		wantErr      error
	}{
		{
			name:         "returns a default stream iterator",
			haveStreamID: typeid.FromUUID("mockstreamtype", uuid.Must(uuid.FromString("a422f08c-0981-49cd-8249-7a48e66a4e8c"))),
			wantEvents:   []*eventstore.Event{events[0], events[1], events[2], events[3], events[4]},
		},
		{
			name:         "returns a forward stream iterator",
			haveStreamID: typeid.FromUUID("mockstreamtype", uuid.Must(uuid.FromString("a422f08c-0981-49cd-8249-7a48e66a4e8c"))),
			haveOpts: eventstore.ReadStreamOptions{
				Direction: eventstore.Forward,
			},
			wantEvents: []*eventstore.Event{events[0], events[1], events[2], events[3], events[4]},
		},
		{
			name:         "returns a reverse stream iterator",
			haveStreamID: typeid.FromUUID("mockstreamtype", uuid.Must(uuid.FromString("a422f08c-0981-49cd-8249-7a48e66a4e8c"))),
			haveOpts: eventstore.ReadStreamOptions{
				Direction: eventstore.Reverse,
			},
			wantEvents: []*eventstore.Event{events[4], events[3], events[2], events[1], events[0]},
		},
		{
			name:         "returns a forward stream iterator with an offset",
			haveStreamID: typeid.FromUUID("mockstreamtype", uuid.Must(uuid.FromString("a422f08c-0981-49cd-8249-7a48e66a4e8c"))),
			haveOpts: eventstore.ReadStreamOptions{
				Direction: eventstore.Forward,
				Offset:    2,
			},
			wantEvents: []*eventstore.Event{events[2], events[3], events[4]},
		},
		{
			name:         "returns a reverse stream iterator with an offset",
			haveStreamID: typeid.FromUUID("mockstreamtype", uuid.Must(uuid.FromString("a422f08c-0981-49cd-8249-7a48e66a4e8c"))),
			haveOpts: eventstore.ReadStreamOptions{
				Direction: eventstore.Reverse,
				Offset:    2,
			},
			wantEvents: []*eventstore.Event{events[2], events[1], events[0]},
		},
		{
			name:         "returns a forward stream iterator with a count",
			haveStreamID: typeid.FromUUID("mockstreamtype", uuid.Must(uuid.FromString("a422f08c-0981-49cd-8249-7a48e66a4e8c"))),
			haveOpts: eventstore.ReadStreamOptions{
				Direction: eventstore.Forward,
				Count:     2,
			},
			wantEvents: []*eventstore.Event{events[0], events[1]},
		},
		{
			name:         "returns a reverse stream iterator with a count",
			haveStreamID: typeid.FromUUID("mockstreamtype", uuid.Must(uuid.FromString("a422f08c-0981-49cd-8249-7a48e66a4e8c"))),
			haveOpts: eventstore.ReadStreamOptions{
				Direction: eventstore.Reverse,
				Count:     2,
			},
			wantEvents: []*eventstore.Event{events[4], events[3]},
		},
		{
			name:         "returns a forward stream iterator with an offset and a count",
			haveStreamID: typeid.FromUUID("mockstreamtype", uuid.Must(uuid.FromString("a422f08c-0981-49cd-8249-7a48e66a4e8c"))),
			haveOpts: eventstore.ReadStreamOptions{
				Direction: eventstore.Forward,
				Offset:    2,
				Count:     2,
			},
			wantEvents: []*eventstore.Event{events[2], events[3]},
		},
		{
			name:         "returns a reverse stream iterator with an offset and a count",
			haveStreamID: typeid.FromUUID("mockstreamtype", uuid.Must(uuid.FromString("a422f08c-0981-49cd-8249-7a48e66a4e8c"))),
			haveOpts: eventstore.ReadStreamOptions{
				Direction: eventstore.Reverse,
				Offset:    2,
				Count:     2,
			},
			wantEvents: []*eventstore.Event{events[2], events[1]},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			haveStrategy, err := strategy.NewSingleCollectionStrategy(collection)
			if err != nil {
				t.Fatalf("unexpected error creating strategy: %v", err)
			}

			gotIter, err := haveStrategy.GetStreamIterator(context.Background(), tt.haveStreamID, tt.haveOpts)
			if err != nil {
				if tt.wantErr == nil {
					t.Errorf("unexpected no error creating stream iterator, but got: %v", err)
				} else if err.Error() != tt.wantErr.Error() {
					t.Errorf("unexpected error creating stream iterator, want: %v, got: %v", tt.wantErr, err)
				}
			} else if gotIter == nil {
				t.Fatalf("unexpected nil stream iterator")
			}

			gotEvents := []*eventstore.Event{}
			for {
				event, err := gotIter.Next(context.Background())
				if errors.Is(err, eventstore.ErrEndOfEventStream) {
					break
				} else if err != nil {
					t.Errorf("unexpected error reading event from stream iterator: %v", err)
				}

				gotEvents = append(gotEvents, event)
			}

			if len(gotEvents) != len(tt.wantEvents) {
				t.Errorf("unexpected number of events, want: %d, got: %d", len(tt.wantEvents), len(gotEvents))
			}

			for i, wantEvent := range tt.wantEvents {
				if gotEvents[i].StreamVersion != wantEvent.StreamVersion {
					t.Errorf("unexpected event version at index %d, want: %d, got: %d", i, wantEvent.StreamVersion, gotEvents[i].StreamVersion)
				}
			}
		})
	}
}

func TestSingleCollectionStrategy_Integration_InsertStreamEvents(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	t.Parallel()

	ctx := context.Background()

	mongoClient, err := createMongoDBContainer(t, ctx)
	if err != nil {
		t.Fatalf("failed to create MongoDB container: %v", err)
	}

	collection := mongoClient.Database("estoria").Collection("events")

	t.Log("MongoDB collection:", collection.Name())

	writableEvents := []*eventstore.WritableEvent{
		{
			ID:        typeid.FromUUID("mockeventtypeA", uuid.Must(uuid.FromString("b112c50d-0834-4b78-a9e7-009d80b79001"))),
			Timestamp: time.Date(2025, 11, 5, 12, 34, 01, 0, time.UTC),
			Data:      []byte{0x01, 0x02, 0x03},
		},
		{
			ID:        typeid.FromUUID("mockeventtypeB", uuid.Must(uuid.FromString("b112c50d-0834-4b78-a9e7-009d80b79002"))),
			Timestamp: time.Date(2025, 11, 5, 12, 34, 02, 0, time.UTC),
			Data:      []byte{0x04, 0x05, 0x06},
		},
		{
			ID:        typeid.FromUUID("mockeventtypeC", uuid.Must(uuid.FromString("b112c50d-0834-4b78-a9e7-009d80b79003"))),
			Timestamp: time.Date(2025, 11, 5, 12, 34, 03, 0, time.UTC),
			Data:      []byte{0x07, 0x08, 0x09},
		},
		{
			ID:        typeid.FromUUID("mockeventtypeB", uuid.Must(uuid.FromString("b112c50d-0834-4b78-a9e7-009d80b79002"))),
			Timestamp: time.Date(2025, 11, 5, 12, 34, 04, 0, time.UTC),
			Data:      []byte{0x0a, 0x0b, 0x0c},
		},
		{
			ID:        typeid.FromUUID("mockeventtypeC", uuid.Must(uuid.FromString("b112c50d-0834-4b78-a9e7-009d80b79003"))),
			Timestamp: time.Date(2025, 11, 5, 12, 34, 05, 0, time.UTC),
			Data:      []byte{0x0d, 0x0e, 0x0f},
		},
	}

	events := []*eventstore.Event{
		{
			ID:            typeid.FromUUID("mockeventtypeA", uuid.Must(uuid.FromString("b112c50d-0834-4b78-a9e7-009d80b79001"))),
			StreamID:      typeid.FromUUID("mockstreamtype", uuid.Must(uuid.FromString("a422f08c-0981-49cd-8249-7a48e66a4e8c"))),
			StreamVersion: 1,
			Timestamp:     time.Date(2025, 11, 5, 12, 34, 01, 0, time.UTC),
			Data:          []byte{0x01, 0x02, 0x03},
		},
		{
			ID:            typeid.FromUUID("mockeventtypeB", uuid.Must(uuid.FromString("b112c50d-0834-4b78-a9e7-009d80b79002"))),
			StreamID:      typeid.FromUUID("mockstreamtype", uuid.Must(uuid.FromString("a422f08c-0981-49cd-8249-7a48e66a4e8c"))),
			StreamVersion: 2,
			Timestamp:     time.Date(2025, 11, 5, 12, 34, 02, 0, time.UTC),
			Data:          []byte{0x04, 0x05, 0x06},
		},
		{
			ID:            typeid.FromUUID("mockeventtypeC", uuid.Must(uuid.FromString("b112c50d-0834-4b78-a9e7-009d80b79003"))),
			StreamID:      typeid.FromUUID("mockstreamtype", uuid.Must(uuid.FromString("a422f08c-0981-49cd-8249-7a48e66a4e8c"))),
			StreamVersion: 3,
			Timestamp:     time.Date(2025, 11, 5, 12, 34, 03, 0, time.UTC),
			Data:          []byte{0x07, 0x08, 0x09},
		},
		{
			ID:            typeid.FromUUID("mockeventtypeB", uuid.Must(uuid.FromString("b112c50d-0834-4b78-a9e7-009d80b79002"))),
			StreamID:      typeid.FromUUID("mockstreamtype", uuid.Must(uuid.FromString("a422f08c-0981-49cd-8249-7a48e66a4e8c"))),
			StreamVersion: 4,
			Timestamp:     time.Date(2025, 11, 5, 12, 34, 04, 0, time.UTC),
			Data:          []byte{0x0a, 0x0b, 0x0c},
		},
		{
			ID:            typeid.FromUUID("mockeventtypeC", uuid.Must(uuid.FromString("b112c50d-0834-4b78-a9e7-009d80b79003"))),
			StreamID:      typeid.FromUUID("mockstreamtype", uuid.Must(uuid.FromString("a422f08c-0981-49cd-8249-7a48e66a4e8c"))),
			StreamVersion: 5,
			Timestamp:     time.Date(2025, 11, 5, 12, 34, 05, 0, time.UTC),
			Data:          []byte{0x0d, 0x0e, 0x0f},
		},
	}

	for _, tt := range []struct {
		name         string
		haveStreamID typeid.UUID
		haveEvents   []*eventstore.WritableEvent
		haveOpts     eventstore.AppendStreamOptions
		// wantDocuments []bson.M
		wantEvents []*eventstore.Event
		wantErr    error
	}{
		{
			name:         "inserts events into a new stream with default options",
			haveStreamID: typeid.FromUUID("mockstreamtype", uuid.Must(uuid.FromString("a422f08c-0981-49cd-8249-7a48e66a4e8c"))),
			haveEvents:   []*eventstore.WritableEvent{writableEvents[0], writableEvents[1], writableEvents[2], writableEvents[3], writableEvents[4]},
			// wantDocuments: []bson.M{documents[0], documents[1], documents[2], documents[3], documents[4]},
			wantEvents: []*eventstore.Event{events[0], events[1], events[2], events[3], events[4]},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			haveStrategy, err := strategy.NewSingleCollectionStrategy(collection)
			if err != nil {
				t.Fatalf("unexpected error creating strategy: %v", err)
			}

			session, err := mongoClient.StartSession()
			if err != nil {
				t.Fatalf("unexpected error starting session: %v", err)
			}

			t.Log("MongoDB session:", session)

			defer func() {
				t.Log("Ending MongoDB session")
				session.EndSession(ctx)
				t.Log("Ended MongoDB session")
			}()

			txOpts := options.Transaction().SetReadPreference(readpref.Primary())

			gotResult, gotErr := session.WithTransaction(context.Background(), func(sessionCtx context.Context) (any, error) {
				t.Log("MongoDB session context:", sessionCtx)
				t.Log("Inserting events into stream:", tt.haveStreamID)
				return haveStrategy.InsertStreamEvents(sessionCtx, tt.haveStreamID, tt.haveEvents, tt.haveOpts)
			}, txOpts)
			if gotErr != nil {
				if tt.wantErr == nil {
					t.Errorf("unexpected no error inserting events, but got: %v", gotErr)
				} else if err.Error() != tt.wantErr.Error() {
					t.Errorf("unexpected error inserting events, want: %v, got: %v", tt.wantErr, gotErr)
				}
				return
			} else if gotResult == nil {
				t.Fatalf("unexpected nil result")
			}

			if numInserted := len(gotResult.(*strategy.InsertStreamEventsResult).InsertedEvents); numInserted != len(tt.haveEvents) {
				t.Errorf("unexpected number of inserted events, want: %d, got: %d", len(tt.haveEvents), numInserted)
			}

			cursor, err := collection.Find(context.Background(), bson.M{"stream_id": tt.haveStreamID.Value()})
			if err != nil {
				t.Fatalf("unexpected error finding events: %v", err)
			}

			gotEvents := []*strategy.Event{}
			for cursor.Next(context.Background()) {
				event, err := (&strategy.DefaultSingleCollectionDocumentMarshaler{}).UnmarshalDocument(cursor.Decode)
				if err != nil {
					t.Fatalf("unexpected error decoding event: %v", err)
				}

				gotEvents = append(gotEvents, event)
			}

			if len(gotEvents) != len(tt.wantEvents) {
				t.Errorf("unexpected number of events, want: %d, got: %d", len(tt.wantEvents), len(gotEvents))
			}

			for i, wantEvent := range tt.wantEvents {
				if gotEvents[i].StreamID != wantEvent.StreamID {
					t.Errorf("unexpected stream ID at index %d, want: %s, got: %s", i, wantEvent.StreamID, gotEvents[i].StreamID)
				}
				if gotEvents[i].StreamVersion != wantEvent.StreamVersion {
					t.Errorf("unexpected event version at index %d, want: %d, got: %d", i, wantEvent.StreamVersion, gotEvents[i].StreamVersion)
				}
				if gotEvents[i].ID != wantEvent.ID {
					t.Errorf("unexpected stream ID at index %d, want: %s, got: %s", i, wantEvent.ID, gotEvents[i].ID)
				}
				if gotEvents[i].Timestamp != wantEvent.Timestamp {
					t.Errorf("unexpected timestamp at index %d, want: %v, got: %v", i, wantEvent.Timestamp, gotEvents[i].Timestamp)
				}
				if string(gotEvents[i].Data) != string(wantEvent.Data) {
					t.Errorf("unexpected data at index %d, want: %s, got: %s", i, wantEvent.Data, gotEvents[i].Data)
				}
			}
		})
	}
}

func createMongoDBContainer(t *testing.T, ctx context.Context) (*mongo.Client, error) {
	t.Helper()

	mongodbContainer, err := mongodb.Run(ctx, "mongo:7", mongodb.WithReplicaSet("rs0"))
	if err != nil {
		return nil, fmt.Errorf("starting MongoDB container: %w", err)
	}

	t.Cleanup(func() {
		if err := testcontainers.TerminateContainer(mongodbContainer); err != nil {
			t.Fatalf("failed to terminate MongoDB container: %v", err)
		}
	})

	connStr, err := mongodbContainer.ConnectionString(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get MongoDB connection string: %w", err)
	}

	// connStr := "mongodb://localhost:27017"

	t.Log("MongoDB container connection string:", connStr)

	mongoClient, err := mongo.Connect(options.Client().
		ApplyURI(connStr).
		SetReplicaSet("rs0").
		SetDirect(true),
	)
	if err != nil {
		t.Fatalf("failed to create MongoDB client: %v", err)
	}

	t.Log("Created MongoDB client")

	if err := mongoClient.Ping(ctx, nil); err != nil {
		return nil, fmt.Errorf("failed to ping MongoDB: %w", err)
	}

	t.Log("Successfully pinged MongoDB")

	return mongoClient, nil
}
