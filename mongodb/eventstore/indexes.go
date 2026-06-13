package eventstore

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// EnsureIndexes creates the indexes required by the event store. It is idempotent and safe to call
// repeatedly; operators must call it once at deploy time (the analog of the Postgres Schema() step),
// unless the store was created with WithAutoEnsureIndexes.
//
// Two unique indexes are created on the events collection:
//
//   - uniq_stream_offset on {stream_type, stream_id, offset} — serves ReadStream and is the
//     optimistic-concurrency / idempotency backstop for appends.
//   - uniq_global_offset on {global_offset} — serves ReadAll and enforces global-offset uniqueness.
func (s *EventStore) EnsureIndexes(ctx context.Context) error {
	models := []mongo.IndexModel{
		{
			Keys: bson.D{
				{Key: "stream_type", Value: 1},
				{Key: "stream_id", Value: 1},
				{Key: "offset", Value: 1},
			},
			Options: options.Index().SetUnique(true).SetName("uniq_stream_offset"),
		},
		{
			Keys:    bson.D{{Key: "global_offset", Value: 1}},
			Options: options.Index().SetUnique(true).SetName("uniq_global_offset"),
		},
	}

	if _, err := s.events.Indexes().CreateMany(ctx, models); err != nil {
		return fmt.Errorf("creating event indexes: %w", err)
	}

	return nil
}
