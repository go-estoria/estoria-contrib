package strategy_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/mongodb"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// seedStreamDocs backfills stream and global counter documents from already-seeded event
// documents, mirroring the backfill an existing deployment performs when upgrading to the
// counter-based storage format.
func seedStreamDocs(ctx context.Context, t *testing.T, streams *mongo.Collection, eventDocs ...[]bson.M) {
	t.Helper()

	type streamKey struct{ streamType, streamID string }
	lastOffsets := map[streamKey]int64{}
	globalOffset := int64(0)
	for _, docs := range eventDocs {
		for _, doc := range docs {
			key := streamKey{doc["stream_type"].(string), doc["stream_id"].(string)}
			if offset, ok := doc["offset"].(int64); ok && offset > lastOffsets[key] {
				lastOffsets[key] = offset
			}
			if offset, ok := doc["global_offset"].(int64); ok && offset > globalOffset {
				globalOffset = offset
			}
		}
	}

	for key, lastOffset := range lastOffsets {
		if _, err := streams.InsertOne(ctx, bson.M{
			"_id":         key.streamType + "_" + key.streamID,
			"stream_type": key.streamType,
			"stream_id":   key.streamID,
			"last_offset": lastOffset,
		}); err != nil {
			t.Fatalf("tc setup: failed to seed stream document: %v", err)
		}
	}

	if globalOffset > 0 {
		if _, err := streams.InsertOne(ctx, bson.M{"_id": "_global", "last_offset": globalOffset}); err != nil {
			t.Fatalf("tc setup: failed to seed global counter document: %v", err)
		}
	}
}

func createMongoDBContainer(t *testing.T) (*mongo.Client, error) {
	t.Helper()

	ctx := t.Context()

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
