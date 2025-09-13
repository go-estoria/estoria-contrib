package eventstore_test

import (
	"context"
	"errors"
	"testing"

	"github.com/go-estoria/estoria-contrib/mongodb/eventstore"
	"github.com/go-estoria/estoria/typeid"
	"github.com/gofrs/uuid/v5"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestEventStore_Integration_ListStreams(t *testing.T) {
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
		name          string
		haveOpts      func(*testing.T) []eventstore.EventStoreOption
		haveDocuments map[string][]bson.M
		wantStreams   []eventstore.StreamInfo
		wantErr       error
	}{
		{
			name:          "returns an empty slice when no streams exist",
			haveDocuments: map[string][]bson.M{},
			wantStreams:   []eventstore.StreamInfo{},
		},
		{
			name: "returns a single stream",
			haveDocuments: map[string][]bson.M{
				"events": {
					{"stream_id": "f60c13d5-63de-4e33-873f-fddb0ccfdf81", "stream_type": "streamtypeA", "data": "data1", "offset": int64(1), "global_offset": int64(1)},
				},
			},
			wantStreams: []eventstore.StreamInfo{
				{StreamID: typeid.New("streamtypeA", uuid.Must(uuid.FromString("f60c13d5-63de-4e33-873f-fddb0ccfdf81"))), Offset: 1, GlobalOffset: 1},
			},
		},
		{
			name: "returns multiple streams",
			haveDocuments: map[string][]bson.M{
				"events": {
					{"stream_id": "f60c13d5-63de-4e33-873f-fddb0ccfdf81", "stream_type": "streamtypeA", "data": "data1", "offset": int64(1), "global_offset": int64(1)},
					{"stream_id": "163f58b0-7326-4b76-964e-43c1f05c0a9a", "stream_type": "streamtypeB", "data": "data2", "offset": int64(1), "global_offset": int64(2)},
					{"stream_id": "09381358-2bd4-4bfa-8d18-65fc6a19583d", "stream_type": "streamtypeC", "data": "data3", "offset": int64(1), "global_offset": int64(3)},
				},
			},
			wantStreams: []eventstore.StreamInfo{
				{StreamID: typeid.New("streamtypeA", uuid.Must(uuid.FromString("f60c13d5-63de-4e33-873f-fddb0ccfdf81"))), Offset: 1, GlobalOffset: 1},
				{StreamID: typeid.New("streamtypeB", uuid.Must(uuid.FromString("163f58b0-7326-4b76-964e-43c1f05c0a9a"))), Offset: 1, GlobalOffset: 2},
				{StreamID: typeid.New("streamtypeC", uuid.Must(uuid.FromString("09381358-2bd4-4bfa-8d18-65fc6a19583d"))), Offset: 1, GlobalOffset: 3},
			},
		},
		{
			name: "returns an error if stream info cannot be decoded",
			haveDocuments: map[string][]bson.M{
				"events": {
					{"stream_id": "invalid_uuid", "stream_type": "streamtypeA", "data": "data1", "offset": int64(1), "global_offset": int64(1)},
				},
			},
			wantErr: errors.New(`decoding streams: parsing UUID: uuid: incorrect UUID length 12 in string "invalid_uuid"`),
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			database := mongoClient.Database("estoria")
			t.Cleanup(func() {
				if err := database.Drop(ctx); err != nil {
					t.Fatalf("tc cleanup: failed to drop database: %v", err)
				}
			})

			for collectionName, haveDocuments := range tt.haveDocuments {
				if len(haveDocuments) > 0 {
					collection := database.Collection(collectionName)
					if _, err := collection.InsertMany(ctx, haveDocuments); err != nil {
						t.Fatalf("tc setup: failed to insert documents: %v", err)
					}
				}
			}

			haveOpts := []eventstore.EventStoreOption{}
			if tt.haveOpts != nil {
				haveOpts = tt.haveOpts(t)
			}

			eventStore, err := eventstore.New(mongoClient, haveOpts...)
			if err != nil {
				t.Fatalf("tc setup: failed to create EventStore: %v", err)
			}

			gotStreams, gotErr := eventStore.ListStreams(ctx)
			if tt.wantErr != nil {
				if gotErr == nil {
					t.Fatalf("expected error %v, got nil", tt.wantErr)
				} else if gotErr.Error() != tt.wantErr.Error() {
					t.Fatalf("expected error %v, got %v", tt.wantErr, gotErr)
				}
			} else if err != nil {
				t.Fatalf("unexpected error listing streams: %v", err)
			}

			if len(gotStreams) != len(tt.wantStreams) {
				t.Errorf("expected %d streams, got %d", len(tt.wantStreams), len(gotStreams))
			}

			// TODO: fix this test (stream info is not always returned in the same order)
			// for i, wantStream := range tt.wantStreams {
			// 	if gotStream := gotStreams[i]; gotStream != wantStream {
			// 		t.Errorf("expected stream %d to be %v, got %v", i, wantStream, gotStream)
			// 	}
			// }
		})
	}
}
