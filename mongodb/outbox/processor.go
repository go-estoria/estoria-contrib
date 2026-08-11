package outbox

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// ErrNoItems is returned by ProcessNext when no stream has claimable, eligible work.
var ErrNoItems = errors.New("no eligible outbox items")

// ProcessNext claims one stream, processes its head item, and acknowledges the result.
//
// Per-stream FIFO is enforced by a per-stream lease: a stream is claimed with an atomic
// findOneAndUpdate that grants a time-bounded lease, guaranteeing at most one active worker per
// stream while permitting cross-stream parallelism. The claimed stream's head item (the pending
// item whose stream_version equals the stream cursor) is then handled OUTSIDE any transaction.
//
// On success the item is deleted (delete-on-ack) and the cursor advances. On failure the retry
// count and error are recorded; once the retry limit is exceeded the item is marked failed and the
// stream halts (its later events are not delivered until an operator intervenes). In both cases the
// lease is released so the stream is reconsidered on the next tick.
//
// ProcessNext is safe to call concurrently across processes; the lease ensures each stream is
// worked by at most one caller at a time. It returns ErrNoItems when there is nothing to do.
func (o *Outbox) ProcessNext(ctx context.Context) error {
	now := time.Now().UTC()

	// (1) Claim a stream that has eligible work (next_version <= last_version), is not halted,
	//     and has no live lease.
	claimRes := o.streamState.FindOneAndUpdate(ctx,
		bson.D{
			{Key: fieldHalted, Value: bson.D{{Key: "$ne", Value: true}}},
			{Key: fieldLeasedUntil, Value: bson.D{{Key: "$lt", Value: now}}},
			{Key: "$expr", Value: bson.D{{Key: "$lte", Value: bson.A{"$next_version", "$last_version"}}}},
		},
		bson.D{{Key: opSet, Value: bson.D{
			{Key: fieldLeasedUntil, Value: now.Add(o.leaseDur)},
			{Key: fieldLeasedBy, Value: o.leasedBy},
		}}},
		options.FindOneAndUpdate().SetReturnDocument(options.After),
	)

	var stream streamStateDocument
	if err := claimRes.Decode(&stream); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return ErrNoItems
		}
		return fmt.Errorf("claiming stream: %w", err)
	}

	key := streamKey(stream.StreamType, stream.StreamID)

	// (2) Load the head item: the pending item at the cursor position.
	headRes := o.coll.FindOne(ctx, bson.D{
		{Key: fieldStreamType, Value: stream.StreamType},
		{Key: fieldStreamID, Value: stream.StreamID},
		{Key: "stream_version", Value: stream.NextVersion},
	})

	var doc itemDocument
	if err := headRes.Decode(&doc); err != nil {
		// Release the lease before returning so the stream is retried.
		o.releaseLease(ctx, key)
		if errors.Is(err, mongo.ErrNoDocuments) {
			// The cursor points at a version that has no item — inconsistent state. Surface it.
			return fmt.Errorf("head item for stream %s version %d not found",
				streamIDString(stream.StreamType, stream.StreamID), stream.NextVersion)
		}
		return fmt.Errorf("loading head item: %w", err)
	}

	item, err := doc.toItem()
	if err != nil {
		o.releaseLease(ctx, key)
		return fmt.Errorf("decoding head item: %w", err)
	}

	// (3) Run the handler outside any transaction.
	if handlerErr := o.handler(ctx, item); handlerErr != nil {
		if ackErr := o.ackFailure(ctx, key, doc, handlerErr); ackErr != nil {
			o.log.Error("acknowledging failed item", "error", ackErr)
		}
		return fmt.Errorf("handling outbox item (stream %s version %d): %w",
			item.StreamID.String(), item.StreamVersion, handlerErr)
	}

	// (4) Success: delete the item, advance the cursor, release the lease.
	if err := o.ackSuccess(ctx, key, doc.ID, stream.NextVersion); err != nil {
		return fmt.Errorf("acknowledging processed item: %w", err)
	}

	return nil
}

// ackSuccess advances the stream cursor past the processed item (releasing the lease), then deletes
// the item. The cursor is advanced before the delete so that a crash between the two operations
// leaves only a harmless orphan (a pending item below the cursor, never re-selected) rather than a
// stuck stream whose cursor points at a missing item.
func (o *Outbox) ackSuccess(ctx context.Context, key bson.D, itemID bson.ObjectID, version int64) error {
	res, err := o.streamState.UpdateOne(ctx,
		bson.D{{Key: fieldID, Value: key}, {Key: fieldNextVersion, Value: version}},
		bson.D{{Key: opSet, Value: bson.D{
			{Key: fieldNextVersion, Value: version + 1},
			{Key: fieldLeasedUntil, Value: epoch()},
			{Key: fieldLeasedBy, Value: ""},
		}}},
	)
	if err != nil {
		return fmt.Errorf("advancing stream cursor: %w", err)
	}

	// The cursor guard ({next_version: version}) should always match under the lease invariant
	// (only the lease holder reaches here, and it advances the cursor exactly once). If it does not,
	// the stream state changed unexpectedly; deleting the item would leave the cursor pointing at a
	// missing item and stall the stream, so skip the delete and surface the anomaly instead.
	if res.MatchedCount == 0 {
		return fmt.Errorf("stream cursor for version %d advanced unexpectedly; leaving item in place", version)
	}

	if _, err := o.coll.DeleteOne(ctx, bson.D{{Key: fieldID, Value: itemID}}); err != nil {
		return fmt.Errorf("deleting item: %w", err)
	}

	return nil
}

// ackFailure records a failed processing attempt. If the retry budget is exhausted, the item is
// marked permanently failed and its stream halts. The lease is always released.
func (o *Outbox) ackFailure(ctx context.Context, key bson.D, doc itemDocument, handlerErr error) error {
	newRetry := doc.RetryCount + 1
	errMsg := handlerErr.Error()

	if o.maxRetries > 0 && newRetry > o.maxRetries {
		// Marking the item failed and halting the stream are two writes to different collections,
		// so they cannot be made atomic without a transaction. If a crash occurs between them the
		// item is failed but the stream is not yet halted; the next claim re-loads the same head and
		// re-runs the handler, which fails again and re-halts. That re-delivery is within the
		// documented at-least-once contract and never violates per-stream FIFO.
		now := time.Now().UTC()
		if _, err := o.coll.UpdateOne(ctx,
			bson.D{{Key: fieldID, Value: doc.ID}},
			bson.D{{Key: opSet, Value: bson.D{
				{Key: "status", Value: statusFailed},
				{Key: "retry_count", Value: newRetry},
				{Key: "last_error", Value: errMsg},
				{Key: "failed_at", Value: now},
			}}},
		); err != nil {
			return fmt.Errorf("marking item failed: %w", err)
		}

		// Halt the stream and release the lease.
		if _, err := o.streamState.UpdateOne(ctx,
			bson.D{{Key: fieldID, Value: key}},
			bson.D{{Key: opSet, Value: bson.D{
				{Key: fieldHalted, Value: true},
				{Key: fieldLeasedUntil, Value: epoch()},
				{Key: fieldLeasedBy, Value: ""},
			}}},
		); err != nil {
			return fmt.Errorf("halting stream: %w", err)
		}

		o.log.Error("outbox item permanently failed",
			"stream", streamIDString(doc.StreamType, doc.StreamID),
			"version", doc.StreamVersion,
			"retry_count", newRetry,
			"max_retries", o.maxRetries,
			"error", errMsg,
		)
		return nil
	}

	if _, err := o.coll.UpdateOne(ctx,
		bson.D{{Key: fieldID, Value: doc.ID}},
		bson.D{{Key: opSet, Value: bson.D{
			{Key: "retry_count", Value: newRetry},
			{Key: "last_error", Value: errMsg},
		}}},
	); err != nil {
		return fmt.Errorf("recording retry: %w", err)
	}

	o.releaseLease(ctx, key)
	return nil
}

// releaseLease makes a stream immediately claimable again.
func (o *Outbox) releaseLease(ctx context.Context, key bson.D) {
	if _, err := o.streamState.UpdateOne(ctx,
		bson.D{{Key: fieldID, Value: key}},
		bson.D{{Key: opSet, Value: bson.D{
			{Key: fieldLeasedUntil, Value: epoch()},
			{Key: fieldLeasedBy, Value: ""},
		}}},
	); err != nil {
		o.log.Error("releasing stream lease", "error", err)
	}
}

// Run starts the outbox polling loop. On each tick it drains all available items by calling
// ProcessNext repeatedly until ErrNoItems. A handler error stops the current drain; the failing
// stream is retried on a later tick. The loop runs until the context is canceled.
//
// Run returns an error if another Run is already active on the same Outbox. Multiple processes may
// each run their own Outbox concurrently; per-stream leases keep delivery FIFO per stream while
// allowing different streams to be processed in parallel.
func (o *Outbox) Run(ctx context.Context) error {
	if !o.running.CompareAndSwap(false, true) {
		return errors.New("outbox processor is already running")
	}
	defer o.running.Store(false)

	o.log.Info("outbox processor starting", "poll_interval", o.pollInterval, "lease_duration", o.leaseDur)

	ticker := time.NewTicker(o.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			o.log.Info("outbox processor stopped")
			return nil

		case <-ticker.C:
			for {
				if err := o.ProcessNext(ctx); err != nil {
					if errors.Is(err, ErrNoItems) {
						break
					}
					if ctx.Err() != nil {
						return nil
					}
					o.log.Error("processing outbox item", "error", err)
					break
				}
			}
		}
	}
}
