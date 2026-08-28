// Package checkpointstore provides a MongoDB-backed projection checkpoint store.
package checkpointstore

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/go-estoria/estoria/projection"
	"github.com/go-estoria/estoria/projection/checkpointstore"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
)

// fieldID is the BSON field name of a document's natural key.
const fieldID = "_id"

// CheckpointStore persists projection checkpoints in a MongoDB collection, one
// document per projection ID. Reads are pinned to the primary regardless of the
// collection's read preference: a checkpoint may legitimately rewind to a lower
// position, and a lagging secondary serving the pre-rewind position would cause
// the events between the two to be skipped. Writes must use an acknowledged
// write concern; Save and Delete report an error otherwise. Deployments that
// must survive replica-set failovers should pair majority read and write
// concerns on the collection — a rewind write rolled back during a failover
// resurrects the higher pre-rewind checkpoint with the same skip effect.
type CheckpointStore struct {
	coll *mongo.Collection
}

var _ checkpointstore.Store = (*CheckpointStore)(nil)

// New creates a new checkpoint store using the provided collection.
//
// When the collection shares a database with an event store using a
// MultiCollectionStrategy, give it a name with a leading underscore (e.g.
// "_projection_checkpoints") to keep it out of the namespace the strategy's
// collection selector can produce.
func New(coll *mongo.Collection) (*CheckpointStore, error) {
	if coll == nil {
		return nil, errors.New("collection is required")
	}

	return &CheckpointStore{
		coll: coll.Clone(options.Collection().SetReadPreference(readpref.Primary())),
	}, nil
}

// checkpointDocument is the BSON shape of a checkpoint document.
type checkpointDocument struct {
	Position  int64     `bson:"position"`
	UpdatedAt time.Time `bson:"updated_at"`
}

// checkpointKey returns the compound natural-key _id for a projection's checkpoint.
func checkpointKey(id projection.ID) bson.D {
	return bson.D{{Key: "n", Value: id.Name}, {Key: "v", Value: id.Version}}
}

// Load returns the projection's checkpoint, or
// checkpointstore.ErrCheckpointNotFound if none has been saved.
func (s *CheckpointStore) Load(ctx context.Context, id projection.ID) (checkpointstore.Checkpoint, error) {
	var doc checkpointDocument
	if err := s.coll.FindOne(ctx, bson.D{{Key: fieldID, Value: checkpointKey(id)}}).
		Decode(&doc); errors.Is(err, mongo.ErrNoDocuments) {
		return checkpointstore.Checkpoint{}, checkpointstore.ErrCheckpointNotFound
	} else if err != nil {
		return checkpointstore.Checkpoint{}, fmt.Errorf("finding checkpoint: %w", err)
	}

	return checkpointstore.Checkpoint{
		ProjectionID: id,
		Position:     doc.Position,
		UpdatedAt:    doc.UpdatedAt.UTC(),
	}, nil
}

// Save records position as the projection's checkpoint, assigning updated_at
// from the database clock even when the position is unchanged.
func (s *CheckpointStore) Save(ctx context.Context, id projection.ID, position int64) error {
	update := bson.D{
		{Key: "$set", Value: bson.D{{Key: "position", Value: position}}},
		{Key: "$currentDate", Value: bson.D{{Key: "updated_at", Value: true}}},
	}

	result, err := s.coll.UpdateOne(ctx,
		bson.D{{Key: fieldID, Value: checkpointKey(id)}},
		update,
		options.UpdateOne().SetUpsert(true),
	)
	if err != nil {
		return fmt.Errorf("upserting checkpoint: %w", err)
	}

	if !result.Acknowledged {
		return errors.New("upserting checkpoint: write was not acknowledged; checkpoint writes require an acknowledged write concern")
	}

	return nil
}

// Delete removes the projection's checkpoint, or reports
// checkpointstore.ErrCheckpointNotFound if none exists.
func (s *CheckpointStore) Delete(ctx context.Context, id projection.ID) error {
	result, err := s.coll.DeleteOne(ctx, bson.D{{Key: fieldID, Value: checkpointKey(id)}})
	if err != nil {
		return fmt.Errorf("deleting checkpoint: %w", err)
	}

	// An unacknowledged delete has no meaningful DeletedCount, so it must be
	// rejected before the not-found check.
	if !result.Acknowledged {
		return errors.New("deleting checkpoint: write was not acknowledged; checkpoint writes require an acknowledged write concern")
	}

	if result.DeletedCount == 0 {
		return checkpointstore.ErrCheckpointNotFound
	}

	return nil
}
