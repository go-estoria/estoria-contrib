package eventstore

import (
	"context"
	"fmt"
	"time"

	"github.com/go-estoria/estoria/eventstore"
	"github.com/go-estoria/estoria/typeid"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// AppendStream appends events to the specified stream.
//
// The entire operation runs inside a single MongoDB transaction on a session context:
//
//  1. Per-stream offsets are reserved (and the prior version read) with one atomic
//     findOneAndUpdate on the streams collection, which also serializes concurrent appends
//     to the same stream via a write conflict.
//  2. The optimistic-concurrency checks (ExpectVersion / StreamMustNotExist) run against the
//     prior version.
//  3. Global offsets are reserved by incrementing the single global counter document — the
//     serialization point that makes global_offset dense, gap-free, and commit-ordered.
//  4. The event documents are inserted. The unique {stream_type, stream_id, offset} index is the
//     idempotency/OCC backstop; a duplicate key is translated to a StreamVersionMismatchError.
//  5. Registered transaction hooks run on the session context.
//
// Any error aborts (rolls back) the whole transaction. WithTransaction retries transient errors
// (e.g. write conflicts), so a losing concurrent appender re-reads the bumped offset and fails its
// OCC check rather than producing a duplicate.
func (s *EventStore) AppendStream(ctx context.Context, streamID typeid.ID, events []*eventstore.WritableEvent, opts eventstore.AppendStreamOptions) error {
	if opts.ExpectVersion != nil && opts.StreamMustNotExist {
		return fmt.Errorf("ExpectVersion and StreamMustNotExist are mutually exclusive")
	}

	if streamID.Type == "" {
		return fmt.Errorf("stream type is required")
	}

	if len(events) == 0 {
		return nil
	}

	s.log.Debug("appending events to MongoDB stream",
		"stream_id", streamID.String(),
		"events", len(events),
		"expected_version", opts.ExpectVersion,
	)

	sess, err := s.client.StartSession(s.sessOpts)
	if err != nil {
		return fmt.Errorf("starting session: %w", err)
	}
	defer sess.EndSession(ctx)

	_, err = sess.WithTransaction(ctx, func(sc context.Context) (any, error) {
		return nil, s.appendInTransaction(sc, streamID, events, opts)
	}, s.txOpts)
	if err != nil {
		return err
	}

	return nil
}

// appendInTransaction performs the append work on the session context sc. It is run by
// WithTransaction and may be invoked multiple times if the transaction is retried.
func (s *EventStore) appendInTransaction(sc context.Context, streamID typeid.ID, events []*eventstore.WritableEvent, opts eventstore.AppendStreamOptions) error {
	n := int64(len(events))
	now := time.Now().UTC()
	key := streamKey(streamID)

	// (a) Reserve per-stream offsets and read the prior version in one atomic op.
	//     ReturnDocument(Before) yields the pre-update document (absent for a new stream).
	priorRes := s.streams.FindOneAndUpdate(sc,
		bson.D{{Key: "_id", Value: key}},
		bson.D{
			{Key: "$inc", Value: bson.D{{Key: "last_offset", Value: n}}},
			{Key: "$setOnInsert", Value: bson.D{
				{Key: "stream_type", Value: streamID.Type},
				{Key: "stream_id", Value: streamID.UUID.String()},
			}},
			{Key: "$set", Value: bson.D{{Key: "updated_at", Value: now}}},
		},
		options.FindOneAndUpdate().SetUpsert(true).SetReturnDocument(options.Before),
	)

	priorOffset, err := decodeLastOffset(priorRes)
	if err != nil {
		return fmt.Errorf("reserving stream offsets: %w", err)
	}

	// (b) Optimistic-concurrency checks against the prior version.
	if opts.StreamMustNotExist && priorOffset > 0 {
		return eventstore.StreamVersionMismatchError{
			StreamID:        streamID,
			ExpectedVersion: 0,
			ActualVersion:   priorOffset,
		}
	}
	if opts.ExpectVersion != nil && priorOffset != *opts.ExpectVersion {
		return eventstore.StreamVersionMismatchError{
			StreamID:        streamID,
			ExpectedVersion: *opts.ExpectVersion,
			ActualVersion:   priorOffset,
		}
	}

	// (c) Reserve global offsets from the hot counter (serializes all appends, gap-free).
	globalRes := s.counters.FindOneAndUpdate(sc,
		bson.D{{Key: "_id", Value: globalCounterID}},
		bson.D{{Key: "$inc", Value: bson.D{{Key: "value", Value: n}}}},
		options.FindOneAndUpdate().SetUpsert(true).SetReturnDocument(options.After),
	)
	newGlobal, err := decodeCounterValue(globalRes)
	if err != nil {
		return fmt.Errorf("reserving global offsets: %w", err)
	}
	startGlobal := newGlobal - n

	// (d) Build and insert the event documents on the session context.
	fullEvents := make([]*Event, n)
	docs := make([]any, n)
	for i, we := range events {
		globalOffset := startGlobal + int64(i) + 1
		gp := globalOffset
		fullEvents[i] = &Event{
			Event: eventstore.Event{
				ID:             typeid.NewV4(we.Type),
				StreamID:       streamID,
				StreamVersion:  priorOffset + int64(i) + 1,
				GlobalPosition: &gp,
				Timestamp:      now,
				Data:           we.Data,
				Metadata:       we.Metadata,
			},
			GlobalOffset: globalOffset,
		}

		doc, err := s.marshaler.MarshalDocument(fullEvents[i])
		if err != nil {
			return fmt.Errorf("marshaling event: %w", err)
		}
		docs[i] = doc
	}

	if res, err := s.events.InsertMany(sc, docs); err != nil {
		// A duplicate key on the unique {stream_type, stream_id, offset} index means a concurrent
		// writer reserved the same offset; surface it as a deterministic version mismatch so the
		// caller (not WithTransaction's retry loop) handles it.
		if mongo.IsDuplicateKeyError(err) {
			return eventstore.StreamVersionMismatchError{
				StreamID:        streamID,
				ExpectedVersion: priorOffset,
				ActualVersion:   priorOffset,
			}
		}
		return fmt.Errorf("inserting events: %w", err)
	} else if len(res.InsertedIDs) != int(n) {
		return fmt.Errorf("inserted %d events, but expected %d", len(res.InsertedIDs), n)
	}

	// (e) Record the stream's most recent global offset for ListStreams / StreamInfo.
	if _, err := s.streams.UpdateOne(sc,
		bson.D{{Key: "_id", Value: key}},
		bson.D{{Key: "$set", Value: bson.D{{Key: "last_global_offset", Value: newGlobal}}}},
	); err != nil {
		return fmt.Errorf("updating stream metadata: %w", err)
	}

	// (f) Run transaction hooks in-transaction on the session context.
	if len(s.txHooks) > 0 {
		hookEvents := make([]*eventstore.Event, n)
		for i := range fullEvents {
			hookEvents[i] = &fullEvents[i].Event
		}
		for _, h := range s.txHooks {
			if err := h.HandleEvents(sc, hookEvents); err != nil {
				return fmt.Errorf("transaction hook: %w", err)
			}
		}
	}

	return nil
}

// decodeLastOffset returns the last_offset from a streams document, or 0 if the document did not
// exist (the ReturnDocument(Before) result for a newly upserted stream is ErrNoDocuments).
func decodeLastOffset(res *mongo.SingleResult) (int64, error) {
	if err := res.Err(); err != nil {
		if err == mongo.ErrNoDocuments {
			return 0, nil
		}
		return 0, err
	}

	var doc struct {
		LastOffset int64 `bson:"last_offset"`
	}
	if err := res.Decode(&doc); err != nil {
		return 0, fmt.Errorf("decoding stream document: %w", err)
	}

	return doc.LastOffset, nil
}

// decodeCounterValue returns the value from a counters document.
func decodeCounterValue(res *mongo.SingleResult) (int64, error) {
	if err := res.Err(); err != nil {
		return 0, err
	}

	var doc struct {
		Value int64 `bson:"value"`
	}
	if err := res.Decode(&doc); err != nil {
		return 0, fmt.Errorf("decoding counter document: %w", err)
	}

	return doc.Value, nil
}
