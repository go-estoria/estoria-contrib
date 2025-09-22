package strategy_test

import (
	"context"
	"testing"

	"github.com/go-estoria/estoria-contrib/mongodb/eventstore/strategy"
	"github.com/go-estoria/estoria/eventstore"
	"github.com/go-estoria/estoria/typeid"
	"github.com/gofrs/uuid/v5"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
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

	docs := []bson.M{
		{
			"stream_id": "a422f08c-0981-49cd-8249-7a48e66a4e8c", "stream_type": "mockstreamtype",
			"event_id": "b112c50d-0834-4b78-a9e7-009d80b79001", "event_type": "mockeventtypeA",
			"offset": int64(1), "timestamp": "2025-11-05T12:34:01Z", "data": bson.M{},
		},
		{
			"stream_id": "a422f08c-0981-49cd-8249-7a48e66a4e8c", "stream_type": "mockstreamtype",
			"event_id": "b112c50d-0834-4b78-a9e7-009d80b79002", "event_type": "mockeventtypeB",
			"offset": int64(2), "timestamp": "2025-11-05T12:34:02Z", "data": bson.M{},
		},
		{
			"stream_id": "a422f08c-0981-49cd-8249-7a48e66a4e8c", "stream_type": "mockstreamtype",
			"event_id": "b112c50d-0834-4b78-a9e7-009d80b79003", "event_type": "mockeventtypeC",
			"offset": int64(3), "timestamp": "2025-11-05T12:34:03Z", "data": bson.M{},
		},
		{
			"stream_id": "a422f08c-0981-49cd-8249-7a48e66a4e8c", "stream_type": "mockstreamtype",
			"event_id": "b112c50d-0834-4b78-a9e7-009d80b79002", "event_type": "mockeventtypeB",
			"offset": int64(4), "timestamp": "2025-11-05T12:34:04Z", "data": bson.M{},
		},
		{
			"stream_id": "a422f08c-0981-49cd-8249-7a48e66a4e8c", "stream_type": "mockstreamtype",
			"event_id": "b112c50d-0834-4b78-a9e7-009d80b79003", "event_type": "mockeventtypeC",
			"offset": int64(5), "timestamp": "2025-11-05T12:34:05Z", "data": bson.M{},
		},
	}

	res, err := collection.InsertMany(ctx, docs)
	if err != nil {
		t.Fatalf("failed to insert events into MongoDB: %v", err)
	} else if len(res.InsertedIDs) != 5 {
		t.Fatalf("unexpected number of inserted IDs, want: 5, got: %d", len(res.InsertedIDs))
	}

	for _, tt := range []struct {
		name         string
		haveStreamID typeid.ID
		haveOpts     eventstore.ReadStreamOptions
		wantEvents   []bson.M
		wantErr      error
	}{
		{
			name:         "returns a default stream iterator",
			haveStreamID: typeid.New("mockstreamtype", uuid.Must(uuid.FromString("a422f08c-0981-49cd-8249-7a48e66a4e8c"))),
			wantEvents:   []bson.M{docs[0], docs[1], docs[2], docs[3], docs[4]},
		},
		{
			name:         "returns a forward stream iterator",
			haveStreamID: typeid.New("mockstreamtype", uuid.Must(uuid.FromString("a422f08c-0981-49cd-8249-7a48e66a4e8c"))),
			haveOpts: eventstore.ReadStreamOptions{
				Direction: eventstore.Forward,
			},
			wantEvents: []bson.M{docs[0], docs[1], docs[2], docs[3], docs[4]},
		},
		{
			name:         "returns a reverse stream iterator",
			haveStreamID: typeid.New("mockstreamtype", uuid.Must(uuid.FromString("a422f08c-0981-49cd-8249-7a48e66a4e8c"))),
			haveOpts: eventstore.ReadStreamOptions{
				Direction: eventstore.Reverse,
			},
			wantEvents: []bson.M{docs[4], docs[3], docs[2], docs[1], docs[0]},
		},
		{
			name:         "returns a forward stream iterator with an offset",
			haveStreamID: typeid.New("mockstreamtype", uuid.Must(uuid.FromString("a422f08c-0981-49cd-8249-7a48e66a4e8c"))),
			haveOpts: eventstore.ReadStreamOptions{
				Direction: eventstore.Forward,
				Offset:    2,
			},
			wantEvents: []bson.M{docs[2], docs[3], docs[4]},
		},
		{
			name:         "returns a reverse stream iterator with an offset",
			haveStreamID: typeid.New("mockstreamtype", uuid.Must(uuid.FromString("a422f08c-0981-49cd-8249-7a48e66a4e8c"))),
			haveOpts: eventstore.ReadStreamOptions{
				Direction: eventstore.Reverse,
				Offset:    2,
			},
			wantEvents: []bson.M{docs[2], docs[1], docs[0]},
		},
		{
			name:         "returns a forward stream iterator with a count",
			haveStreamID: typeid.New("mockstreamtype", uuid.Must(uuid.FromString("a422f08c-0981-49cd-8249-7a48e66a4e8c"))),
			haveOpts: eventstore.ReadStreamOptions{
				Direction: eventstore.Forward,
				Count:     2,
			},
			wantEvents: []bson.M{docs[0], docs[1]},
		},
		{
			name:         "returns a reverse stream iterator with a count",
			haveStreamID: typeid.New("mockstreamtype", uuid.Must(uuid.FromString("a422f08c-0981-49cd-8249-7a48e66a4e8c"))),
			haveOpts: eventstore.ReadStreamOptions{
				Direction: eventstore.Reverse,
				Count:     2,
			},
			wantEvents: []bson.M{docs[4], docs[3]},
		},
		{
			name:         "returns a forward stream iterator with an offset and a count",
			haveStreamID: typeid.New("mockstreamtype", uuid.Must(uuid.FromString("a422f08c-0981-49cd-8249-7a48e66a4e8c"))),
			haveOpts: eventstore.ReadStreamOptions{
				Direction: eventstore.Forward,
				Offset:    2,
				Count:     2,
			},
			wantEvents: []bson.M{docs[2], docs[3]},
		},
		{
			name:         "returns a reverse stream iterator with an offset and a count",
			haveStreamID: typeid.New("mockstreamtype", uuid.Must(uuid.FromString("a422f08c-0981-49cd-8249-7a48e66a4e8c"))),
			haveOpts: eventstore.ReadStreamOptions{
				Direction: eventstore.Reverse,
				Offset:    2,
				Count:     2,
			},
			wantEvents: []bson.M{docs[2], docs[1]},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			haveStrategy, err := strategy.NewSingleCollectionStrategy(mongoClient, collection)
			if err != nil {
				t.Fatalf("unexpected error creating strategy: %v", err)
			}

			gotCursor, err := haveStrategy.GetStreamCursor(context.Background(), tt.haveStreamID, tt.haveOpts)
			if err != nil {
				if tt.wantErr == nil {
					t.Errorf("unexpected no error creating stream iterator, but got: %v", err)
				} else if err.Error() != tt.wantErr.Error() {
					t.Errorf("unexpected error creating stream iterator, want: %v, got: %v", tt.wantErr, err)
				}
			} else if gotCursor == nil {
				t.Fatalf("unexpected nil stream cursor")
			}

			gotDocs := []bson.M{}
			if err := gotCursor.All(context.Background(), &gotDocs); err != nil {
				t.Fatalf("failed to decode stream cursor: %v", err)
			}

			if len(gotDocs) != len(tt.wantEvents) {
				t.Errorf("unexpected number of events, want: %d, got: %d", len(tt.wantEvents), len(gotDocs))
			}

			for i, wantEvent := range tt.wantEvents {
				if gotDocs[i]["offset"].(int64) != wantEvent["offset"].(int64) {
					t.Errorf("unexpected offset at index %d, want: %d, got: %d", i, wantEvent["offset"], gotDocs[i]["offset"])
				}
			}
		})
	}
}

func TestSingleCollectionStrategy_Integration_InsertStreamDocs(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	t.Parallel()

	ctx := context.Background()

	mongoClient, err := createMongoDBContainer(t, ctx)
	if err != nil {
		t.Fatalf("failed to create MongoDB container: %v", err)
	}

	for _, tt := range []struct {
		name                  string
		haveExistingDocuments []bson.M
		haveStreamID          typeid.ID
		haveDocuments         []bson.M
		wantDocuments         []bson.M
		wantErr               error
	}{
		{
			name:         "inserts documents into an empty collection",
			haveStreamID: typeid.New("mockstreamtypeA", uuid.Must(uuid.FromString("b610ff0b-5bb0-4e8f-9d2b-9cfb9818065d"))),
			haveDocuments: []bson.M{
				{"stream_id": "b610ff0b-5bb0-4e8f-9d2b-9cfb9818065d", "stream_type": "mockstreamtypeA", "data": "mockdata1"},
				{"stream_id": "b610ff0b-5bb0-4e8f-9d2b-9cfb9818065d", "stream_type": "mockstreamtypeA", "data": "mockdata2"},
				{"stream_id": "b610ff0b-5bb0-4e8f-9d2b-9cfb9818065d", "stream_type": "mockstreamtypeA", "data": "mockdata3"},
			},
			wantDocuments: []bson.M{
				{"stream_id": "b610ff0b-5bb0-4e8f-9d2b-9cfb9818065d", "stream_type": "mockstreamtypeA", "data": "mockdata1", "offset": int64(1), "global_offset": int64(1)},
				{"stream_id": "b610ff0b-5bb0-4e8f-9d2b-9cfb9818065d", "stream_type": "mockstreamtypeA", "data": "mockdata2", "offset": int64(2), "global_offset": int64(2)},
				{"stream_id": "b610ff0b-5bb0-4e8f-9d2b-9cfb9818065d", "stream_type": "mockstreamtypeA", "data": "mockdata3", "offset": int64(3), "global_offset": int64(3)},
			},
		},
		{
			name: "inserts documents into a non-empty collection with no matching stream type (ok for IDs to match in disparate streams)",
			haveExistingDocuments: []bson.M{
				{"stream_id": "b610ff0b-5bb0-4e8f-9d2b-9cfb9818065d", "stream_type": "mockstreamtypeA", "data": "mockdata1", "offset": int64(1), "global_offset": int64(1)},
				{"stream_id": "b610ff0b-5bb0-4e8f-9d2b-9cfb9818065d", "stream_type": "mockstreamtypeA", "data": "mockdata2", "offset": int64(2), "global_offset": int64(2)},
				{"stream_id": "b610ff0b-5bb0-4e8f-9d2b-9cfb9818065d", "stream_type": "mockstreamtypeA", "data": "mockdata3", "offset": int64(3), "global_offset": int64(3)},
			},
			haveStreamID: typeid.New("mockstreamtypeB", uuid.Must(uuid.FromString("b610ff0b-5bb0-4e8f-9d2b-9cfb9818065d"))),
			haveDocuments: []bson.M{
				{"stream_id": "b610ff0b-5bb0-4e8f-9d2b-9cfb9818065d", "stream_type": "mockstreamtypeB", "data": "mockdata1"},
				{"stream_id": "b610ff0b-5bb0-4e8f-9d2b-9cfb9818065d", "stream_type": "mockstreamtypeB", "data": "mockdata2"},
				{"stream_id": "b610ff0b-5bb0-4e8f-9d2b-9cfb9818065d", "stream_type": "mockstreamtypeB", "data": "mockdata3"},
			},
			wantDocuments: []bson.M{
				{"stream_id": "b610ff0b-5bb0-4e8f-9d2b-9cfb9818065d", "stream_type": "mockstreamtypeA", "data": "mockdata1", "offset": int64(1), "global_offset": int64(1)},
				{"stream_id": "b610ff0b-5bb0-4e8f-9d2b-9cfb9818065d", "stream_type": "mockstreamtypeA", "data": "mockdata2", "offset": int64(2), "global_offset": int64(2)},
				{"stream_id": "b610ff0b-5bb0-4e8f-9d2b-9cfb9818065d", "stream_type": "mockstreamtypeA", "data": "mockdata3", "offset": int64(3), "global_offset": int64(3)},
				{"stream_id": "b610ff0b-5bb0-4e8f-9d2b-9cfb9818065d", "stream_type": "mockstreamtypeB", "data": "mockdata1", "offset": int64(1), "global_offset": int64(4)},
				{"stream_id": "b610ff0b-5bb0-4e8f-9d2b-9cfb9818065d", "stream_type": "mockstreamtypeB", "data": "mockdata2", "offset": int64(2), "global_offset": int64(5)},
				{"stream_id": "b610ff0b-5bb0-4e8f-9d2b-9cfb9818065d", "stream_type": "mockstreamtypeB", "data": "mockdata3", "offset": int64(3), "global_offset": int64(6)},
			},
		},
		{
			name: "inserts documents into a non-empty collection with no matching stream ID",
			haveExistingDocuments: []bson.M{
				{"stream_id": "b610ff0b-5bb0-4e8f-9d2b-9cfb9818065d", "stream_type": "mockstreamtypeA", "data": "mockdata1", "offset": int64(1), "global_offset": int64(1)},
				{"stream_id": "b610ff0b-5bb0-4e8f-9d2b-9cfb9818065d", "stream_type": "mockstreamtypeA", "data": "mockdata2", "offset": int64(2), "global_offset": int64(2)},
				{"stream_id": "b610ff0b-5bb0-4e8f-9d2b-9cfb9818065d", "stream_type": "mockstreamtypeA", "data": "mockdata3", "offset": int64(3), "global_offset": int64(3)},
			},
			haveStreamID: typeid.New("mockstreamtypeA", uuid.Must(uuid.FromString("7de3ee60-1e2a-4169-a6a9-3ce2b298350e"))),
			haveDocuments: []bson.M{
				{"stream_id": "7de3ee60-1e2a-4169-a6a9-3ce2b298350e", "stream_type": "mockstreamtypeA", "data": "mockdata1"},
				{"stream_id": "7de3ee60-1e2a-4169-a6a9-3ce2b298350e", "stream_type": "mockstreamtypeA", "data": "mockdata2"},
				{"stream_id": "7de3ee60-1e2a-4169-a6a9-3ce2b298350e", "stream_type": "mockstreamtypeA", "data": "mockdata3"},
			},
			wantDocuments: []bson.M{
				{"stream_id": "b610ff0b-5bb0-4e8f-9d2b-9cfb9818065d", "stream_type": "mockstreamtypeA", "data": "mockdata1", "offset": int64(1), "global_offset": int64(1)},
				{"stream_id": "b610ff0b-5bb0-4e8f-9d2b-9cfb9818065d", "stream_type": "mockstreamtypeA", "data": "mockdata2", "offset": int64(2), "global_offset": int64(2)},
				{"stream_id": "b610ff0b-5bb0-4e8f-9d2b-9cfb9818065d", "stream_type": "mockstreamtypeA", "data": "mockdata3", "offset": int64(3), "global_offset": int64(3)},
				{"stream_id": "7de3ee60-1e2a-4169-a6a9-3ce2b298350e", "stream_type": "mockstreamtypeA", "data": "mockdata1", "offset": int64(1), "global_offset": int64(4)},
				{"stream_id": "7de3ee60-1e2a-4169-a6a9-3ce2b298350e", "stream_type": "mockstreamtypeA", "data": "mockdata2", "offset": int64(2), "global_offset": int64(5)},
				{"stream_id": "7de3ee60-1e2a-4169-a6a9-3ce2b298350e", "stream_type": "mockstreamtypeA", "data": "mockdata3", "offset": int64(3), "global_offset": int64(6)},
			},
		},
		{
			name: "inserts documents into a non-empty collection with a matching stream",
			haveExistingDocuments: []bson.M{
				{"stream_id": "b610ff0b-5bb0-4e8f-9d2b-9cfb9818065d", "stream_type": "mockstreamtypeA", "data": "mockdata1", "offset": int64(1), "global_offset": int64(1)},
				{"stream_id": "b610ff0b-5bb0-4e8f-9d2b-9cfb9818065d", "stream_type": "mockstreamtypeA", "data": "mockdata2", "offset": int64(2), "global_offset": int64(2)},
				{"stream_id": "b610ff0b-5bb0-4e8f-9d2b-9cfb9818065d", "stream_type": "mockstreamtypeA", "data": "mockdata3", "offset": int64(3), "global_offset": int64(3)},
			},
			haveStreamID: typeid.New("mockstreamtypeA", uuid.Must(uuid.FromString("b610ff0b-5bb0-4e8f-9d2b-9cfb9818065d"))),
			haveDocuments: []bson.M{
				{"stream_id": "b610ff0b-5bb0-4e8f-9d2b-9cfb9818065d", "stream_type": "mockstreamtypeA", "data": "mockdata4"},
				{"stream_id": "b610ff0b-5bb0-4e8f-9d2b-9cfb9818065d", "stream_type": "mockstreamtypeA", "data": "mockdata5"},
				{"stream_id": "b610ff0b-5bb0-4e8f-9d2b-9cfb9818065d", "stream_type": "mockstreamtypeA", "data": "mockdata6"},
			},
			wantDocuments: []bson.M{
				{"stream_id": "b610ff0b-5bb0-4e8f-9d2b-9cfb9818065d", "stream_type": "mockstreamtypeA", "data": "mockdata1", "offset": int64(1), "global_offset": int64(1)},
				{"stream_id": "b610ff0b-5bb0-4e8f-9d2b-9cfb9818065d", "stream_type": "mockstreamtypeA", "data": "mockdata2", "offset": int64(2), "global_offset": int64(2)},
				{"stream_id": "b610ff0b-5bb0-4e8f-9d2b-9cfb9818065d", "stream_type": "mockstreamtypeA", "data": "mockdata3", "offset": int64(3), "global_offset": int64(3)},
				{"stream_id": "b610ff0b-5bb0-4e8f-9d2b-9cfb9818065d", "stream_type": "mockstreamtypeA", "data": "mockdata4", "offset": int64(4), "global_offset": int64(4)},
				{"stream_id": "b610ff0b-5bb0-4e8f-9d2b-9cfb9818065d", "stream_type": "mockstreamtypeA", "data": "mockdata5", "offset": int64(5), "global_offset": int64(5)},
				{"stream_id": "b610ff0b-5bb0-4e8f-9d2b-9cfb9818065d", "stream_type": "mockstreamtypeA", "data": "mockdata6", "offset": int64(6), "global_offset": int64(6)},
			},
		},
		{
			name: "inserts documents into a non-empty collection with a stream whose highest offset is equal to the global offset",
			haveExistingDocuments: []bson.M{
				{"stream_id": "b610ff0b-5bb0-4e8f-9d2b-9cfb9818065d", "stream_type": "mockstreamtypeA", "data": "mockdata1", "offset": int64(1), "global_offset": int64(1)},
				{"stream_id": "7de3ee60-1e2a-4169-a6a9-3ce2b298350e", "stream_type": "mockstreamtypeA", "data": "mockdata2", "offset": int64(1), "global_offset": int64(2)},
				{"stream_id": "5f9abd8a-6c25-49b2-a7c6-f0eda89a9a5d", "stream_type": "mockstreamtypeA", "data": "mockdata3", "offset": int64(1), "global_offset": int64(3)},
				{"stream_id": "b610ff0b-5bb0-4e8f-9d2b-9cfb9818065d", "stream_type": "mockstreamtypeA", "data": "mockdata4", "offset": int64(2), "global_offset": int64(4)},
			},
			haveStreamID: typeid.New("mockstreamtypeA", uuid.Must(uuid.FromString("b610ff0b-5bb0-4e8f-9d2b-9cfb9818065d"))),
			haveDocuments: []bson.M{
				{"stream_id": "b610ff0b-5bb0-4e8f-9d2b-9cfb9818065d", "stream_type": "mockstreamtypeA", "data": "mockdata5"},
			},
			wantDocuments: []bson.M{
				{"stream_id": "b610ff0b-5bb0-4e8f-9d2b-9cfb9818065d", "stream_type": "mockstreamtypeA", "data": "mockdata1", "offset": int64(1), "global_offset": int64(1)},
				{"stream_id": "7de3ee60-1e2a-4169-a6a9-3ce2b298350e", "stream_type": "mockstreamtypeA", "data": "mockdata2", "offset": int64(1), "global_offset": int64(2)},
				{"stream_id": "5f9abd8a-6c25-49b2-a7c6-f0eda89a9a5d", "stream_type": "mockstreamtypeA", "data": "mockdata3", "offset": int64(1), "global_offset": int64(3)},
				{"stream_id": "b610ff0b-5bb0-4e8f-9d2b-9cfb9818065d", "stream_type": "mockstreamtypeA", "data": "mockdata4", "offset": int64(2), "global_offset": int64(4)},
				{"stream_id": "b610ff0b-5bb0-4e8f-9d2b-9cfb9818065d", "stream_type": "mockstreamtypeA", "data": "mockdata5", "offset": int64(3), "global_offset": int64(5)},
			},
		},
		{
			name: "inserts documents into a non-empty collection with a stream whose highest offset is less than the global offset",
			haveExistingDocuments: []bson.M{
				{"stream_id": "b610ff0b-5bb0-4e8f-9d2b-9cfb9818065d", "stream_type": "mockstreamtypeA", "data": "mockdata1", "offset": int64(1), "global_offset": int64(1)},
				{"stream_id": "7de3ee60-1e2a-4169-a6a9-3ce2b298350e", "stream_type": "mockstreamtypeA", "data": "mockdata2", "offset": int64(1), "global_offset": int64(2)},
				{"stream_id": "5f9abd8a-6c25-49b2-a7c6-f0eda89a9a5d", "stream_type": "mockstreamtypeA", "data": "mockdata3", "offset": int64(1), "global_offset": int64(3)},
				{"stream_id": "b610ff0b-5bb0-4e8f-9d2b-9cfb9818065d", "stream_type": "mockstreamtypeA", "data": "mockdata4", "offset": int64(2), "global_offset": int64(4)},
			},
			haveStreamID: typeid.New("mockstreamtypeA", uuid.Must(uuid.FromString("7de3ee60-1e2a-4169-a6a9-3ce2b298350e"))),
			haveDocuments: []bson.M{
				{"stream_id": "7de3ee60-1e2a-4169-a6a9-3ce2b298350e", "stream_type": "mockstreamtypeA", "data": "mockdata5"},
			},
			wantDocuments: []bson.M{
				{"stream_id": "b610ff0b-5bb0-4e8f-9d2b-9cfb9818065d", "stream_type": "mockstreamtypeA", "data": "mockdata1", "offset": int64(1), "global_offset": int64(1)},
				{"stream_id": "7de3ee60-1e2a-4169-a6a9-3ce2b298350e", "stream_type": "mockstreamtypeA", "data": "mockdata2", "offset": int64(1), "global_offset": int64(2)},
				{"stream_id": "5f9abd8a-6c25-49b2-a7c6-f0eda89a9a5d", "stream_type": "mockstreamtypeA", "data": "mockdata3", "offset": int64(1), "global_offset": int64(3)},
				{"stream_id": "b610ff0b-5bb0-4e8f-9d2b-9cfb9818065d", "stream_type": "mockstreamtypeA", "data": "mockdata4", "offset": int64(2), "global_offset": int64(4)},
				{"stream_id": "7de3ee60-1e2a-4169-a6a9-3ce2b298350e", "stream_type": "mockstreamtypeA", "data": "mockdata5", "offset": int64(2), "global_offset": int64(5)},
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			collection := mongoClient.Database("estoria").Collection("events-" + uuid.Must(uuid.NewV4()).String()[0:8])

			haveStrategy, err := strategy.NewSingleCollectionStrategy(mongoClient, collection)
			if err != nil {
				t.Fatalf("tc setup: unexpected error creating strategy: %v", err)
			}

			if len(tt.haveExistingDocuments) > 0 {
				res, err := collection.InsertMany(ctx, tt.haveExistingDocuments)
				if err != nil {
					t.Fatalf("tc setup: failed to insert events into MongoDB: %v", err)
				} else if len(res.InsertedIDs) != len(tt.haveExistingDocuments) {
					t.Fatalf("tc setup: unexpected number of inserted IDs, want: %d, got: %d", len(tt.haveExistingDocuments), len(res.InsertedIDs))
				}
			}

			gotResult, gotErr := haveStrategy.ExecuteInsertTransaction(context.Background(), tt.haveStreamID,
				func(sessCtx context.Context, coll strategy.MongoCollection, offset, globalOffset int64) (any, error) {
					for i := range tt.haveDocuments {
						tt.haveDocuments[i]["offset"] = offset + int64(i) + 1
						tt.haveDocuments[i]["global_offset"] = globalOffset + int64(i) + 1
					}
					return coll.InsertMany(sessCtx, tt.haveDocuments)
				},
			)
			if gotErr != nil {
				if tt.wantErr == nil {
					t.Fatalf("expected no error inserting events, but got: %v", gotErr)
				} else if err.Error() != tt.wantErr.Error() {
					t.Fatalf("unexpected error inserting events, want: %v, got: %v", tt.wantErr, gotErr)
				}
				return
			} else if tt.wantErr != nil {
				t.Fatalf("expected error inserting events, but got nil")
			} else if gotResult == nil {
				t.Fatalf("unexpected nil result")
			}

			gotInsertManyResult, ok := gotResult.(*mongo.InsertManyResult)
			if !ok {
				t.Fatalf("unexpected result type, want: *mongo.InsertManyResult, got: %T", gotResult)
			} else if len(gotInsertManyResult.InsertedIDs) != len(tt.haveDocuments) {
				t.Errorf("unexpected number of inserted IDs, want: %d, got: %d", len(tt.haveDocuments), len(gotInsertManyResult.InsertedIDs))
			}

			cursor, err := collection.Find(ctx, bson.D{})
			if err != nil {
				t.Fatalf("failed to find documents in collection: %v", err)
			}

			gotStreamDocs := []bson.M{}
			if err := cursor.All(ctx, &gotStreamDocs); err != nil {
				t.Fatalf("failed to decode documents from cursor: %v", err)
			}

			if len(gotStreamDocs) != len(tt.haveExistingDocuments)+len(tt.haveDocuments) {
				t.Errorf("unexpected number of documents, want: %d, got: %d", len(tt.haveExistingDocuments)+len(tt.haveDocuments), len(gotStreamDocs))
			}

			for i, wantDoc := range tt.wantDocuments {
				switch {
				case gotStreamDocs[i]["_id"] == nil:
					t.Errorf("unexpected nil ID at index %d", i)
				case gotStreamDocs[i]["stream_id"] != tt.wantDocuments[i]["stream_id"]:
					t.Errorf("unexpected stream ID at index %d, want: %s, got: %s", i, wantDoc["stream_id"], gotStreamDocs[i]["stream_id"])
				case gotStreamDocs[i]["stream_type"] != tt.wantDocuments[i]["stream_type"]:
					t.Errorf("unexpected stream type at index %d, want: %s, got: %s", i, wantDoc["stream_type"], gotStreamDocs[i]["stream_type"])
				case gotStreamDocs[i]["data"] != tt.wantDocuments[i]["data"]:
					t.Errorf("unexpected data at index %d, want: %v, got: %v", i, wantDoc["data"], gotStreamDocs[i]["data"])
				case gotStreamDocs[i]["offset"] != tt.wantDocuments[i]["offset"]:
					t.Errorf("unexpected offset at index %d, want: %d, got: %d", i, wantDoc["offset"], gotStreamDocs[i]["offset"])
				case gotStreamDocs[i]["global_offset"] != tt.wantDocuments[i]["global_offset"]:
					t.Errorf("unexpected global offset at index %d, want: %d, got: %d", i, wantDoc["global_offset"], gotStreamDocs[i]["global_offset"])
				}
			}
		})
	}
}
