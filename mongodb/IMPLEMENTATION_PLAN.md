# MongoDB Event Store — Production-Readiness Redesign: Implementation Plan

**Date:** 2026-06-12
**Status:** Approved design; ready to implement.
**Companion doc:** `PRODUCTION_READINESS_REVIEW.md` (the findings this plan resolves).
**Audience:** the implementing engineer/agent. This is a build spec, not a discussion doc.

---

## 0. Context and ground rules

The current MongoDB event store derives stream/global positions by scanning the events
collection (`max(offset)+1`), runs its insert **outside** the append transaction, never invokes
registered transaction hooks, has **no indexes**, and ships a multi-collection strategy whose
global-ordering machinery is unsound. The Postgres event store (production-proven) is the parity
reference for *behavior*, not for *mechanism*.

This redesign is a **breaking, greenfield rewrite** of the `mongodb/eventstore` package plus a new
`mongodb/outbox` package. Confirmed constraints:

- **No data migration / no backward compatibility.** There are no known MongoDB adopters and no
  production Mongo data. Treat the on-disk schema as new. Document the break in the README/CHANGELOG.
- **Frozen public surface:** the estoria core `Store` contract (`ReadStream`, `AppendStream`) and the
  existing package-public extras (`ReadAll`, `ListStreams`, `New`, `WithLogger`,
  `WithDocumentMarshaler`, `WithTransactionHook`, the `StreamInfo` type). Any change to these
  must be raised before merging.
- **Removed public surface (approved):** the exported `Strategy` interface, `WithStrategy`, the entire
  `strategy/` package, and the multi-collection strategy.
- **Transactions require a replica set** (or mongos). Single-node replica set is fine for tests.

---

## 1. Design decisions (locked)

| Axis | Decision |
|---|---|
| Sequencing | Dedicated metadata collections with **atomic in-transaction counters**; delete the scan-for-max model. |
| Global order | **G-strict** — dense, gap-free, **commit-ordered** `int64` from an in-txn global counter doc. `Event.GlobalPosition` stays a real `int64`. |
| Event `_id` | **S1** — opaque auto `ObjectID` surrogate; meaning lives in separate logical keys/indexes. |
| Strategy abstraction | **Removed.** Single-collection logic folded directly into `EventStore`. No pluggable strategy. |
| Write path | Owned end-to-end inside `EventStore`; the append closure runs entirely on the session context. The C1 foot-gun (closure capturing the wrong ctx) is structurally eliminated. |
| Hooks | Executed **in-transaction** on the session context (fixes C3); foundation for the outbox. |
| Outbox | New `mongodb/outbox` package. Producer writes one doc per event in the append txn. Consumer uses **claim → handle-outside-txn → ack** (lease/visibility-timeout). **Strict per-stream FIFO (hard requirement)** via per-stream lease. Delete-on-ack; retain failed. |
| Init | Explicit `EnsureIndexes`/`Init` (the analog of Postgres `Schema()`). |
| Deferred | Change streams and checkpoint-on-events subscriptions — separate future feature, out of scope here. |

**Surrogate vs natural keys (the principle behind S1):** the **events** collection is the source of
truth → its `_id` is an opaque `ObjectID`, referenced by nothing, regenerable on restore/re-shard.
The **metadata** collections (`streams`, `counters`) are *derived, rebuildable caches* → they use
natural-key `_id`s because we upsert by identity and can reconstruct them from the events at any time.

---

## 2. Target data model

Database: configurable (default `estoria`). Three collections.

### 2.1 `events` (default name `events`) — source of truth

```jsonc
{
  "_id":           ObjectID,          // opaque surrogate (auto)
  "stream_type":   "user",
  "stream_id":     "<uuid string>",
  "event_type":    "UserRegistered",
  "event_id":      "<uuid string>",
  "offset":        12,                // per-stream version, 1-based
  "global_offset": 4096,             // dense, gap-free, commit-ordered
  "timestamp":     ISODate,           // UTC
  "event_data":    BinData,
  "metadata":      { "k": "v" }      // omitempty
}
```

Indexes (created by `EnsureIndexes`):
- **`uniq_stream_offset`**: `{ stream_type: 1, stream_id: 1, offset: 1 }`, **unique**.
  Serves `ReadStream` (equality on stream + sort/range on `offset`), and is the OCC + idempotency
  backstop.
- **`uniq_global_offset`**: `{ global_offset: 1 }`, **unique**.
  Serves `ReadAll` (sort/range on `global_offset`) and enforces global uniqueness.

The `EventDocument` BSON struct keeps its current field tags. `_id` is left unset on insert so the
server assigns the `ObjectID`.

### 2.2 `streams` (default name `streams`) — per-stream metadata (derived)

```jsonc
{
  "_id":                { "t": "user", "s": "<uuid string>" },  // compound natural key
  "stream_type":        "user",
  "stream_id":          "<uuid string>",
  "last_offset":        12,    // highest per-stream offset assigned
  "last_global_offset": 4096,  // global offset of this stream's most recent event
  "updated_at":         ISODate
}
```

- Natural-key `_id` makes the per-append upsert atomic and idempotent.
- Powers `ListStreams` directly (no aggregation over `events`) and the OCC offset reservation.
- `last_global_offset` lets `StreamInfo.GlobalOffset` be populated without touching `events`.
- No index needed beyond `_id` for the hot paths.

### 2.3 `counters` (default name `counters`) — global sequence (derived)

```jsonc
{ "_id": "global_offset", "value": 4096 }
```

A single hot document. `$inc`-ed inside every append transaction → this is the serialization point
that makes `global_offset` dense, gap-free, and commit-ordered (see §3.3). No extra index.

> **Rebuild note (ops):** both `streams` and `counters` are reconstructable from `events`
> (`last_offset` = max `offset` per stream; `value` = max `global_offset`). Worth a future
> `Rebuild(ctx)` helper, but out of scope for this plan.

---

## 3. `mongodb/eventstore` redesign

### 3.1 Type & file layout

Remove:
- `strategy/` package entirely (`strategy.go`, `single_collection_strategy.go`,
  `multi_collection_strategy.go`, `options.go`, and their tests).
- `multiStreamIterator` and `multiStreamIteratorCursor` from `stream_iterator.go`.
- `Strategy` interface and `WithStrategy` from `event_store.go` / `options.go`.
- `StreamInfo.UnmarshalBSON`'s unchecked type assertions (replace with a typed decode, fixes M1).

Keep/modify:
- `event_store.go` — `EventStore` now holds the three collection handles, marshaler, hooks, logger.
- `event_document.go` — `Event`, `EventDocument`, `DefaultMarshaler` unchanged.
- `stream_iterator.go` — keep the single-cursor `streamIterator` only.
- `options.go` — keep `WithLogger`, `WithDocumentMarshaler`, `WithTransactionHook`; remove
  `WithStrategy`; add collection/db-name and session/txn options (below).
- `append.go` *(new)* — the transactional append path, kept separate for readability/testing.
- `indexes.go` *(new)* — `EnsureIndexes`/`Init`.

### 3.2 Struct & constructor

```go
type EventStore struct {
    db       MongoDatabase     // small seam, default-named collections derived from it
    events   MongoCollection
    streams  MongoCollection
    counters MongoCollection
    client   MongoSessionStarter

    marshaler DocumentMarshaler
    txHooks   []TransactionHook
    sessOpts  options.Lister[options.SessionOptions]
    txOpts    options.Lister[options.TransactionOptions]
    log       estoria.Logger
}
```

- `New(client MongoClient, opts ...EventStoreOption) (*EventStore, error)` — **signature unchanged.**
  Derives the database (default `estoria`) and three collections (default names) unless overridden.
- New options (additive, all optional):
  `WithDatabaseName`, `WithEventsCollectionName`, `WithStreamsCollectionName`,
  `WithCountersCollectionName`, `WithSessionOptions`, `WithTransactionOptions`.
- Transaction/session defaults (fixes H4): `readConcern: snapshot`, `writeConcern: majority`,
  `readPreference: primary`. Expose via the options above for advanced users.

### 3.3 Append path (`AppendStream`) — the core of the rewrite

Resolves C1, C2, C3, H1, H4, M2. Pseudocode (mongo-driver v2, illustrative):

```go
func (s *EventStore) AppendStream(ctx, streamID, events, opts) error {
    if opts.ExpectVersion != nil && opts.StreamMustNotExist {
        return errors.New("ExpectVersion and StreamMustNotExist are mutually exclusive")
    }
    if len(events) == 0 { return nil } // no-op

    sess, err := s.client.StartSession(s.sessOpts)
    if err != nil { return ... }
    defer sess.EndSession(ctx)

    _, err = sess.WithTransaction(ctx, func(sc context.Context) (any, error) {
        n := int64(len(events))
        streamKey := bson.D{{"t", streamID.Type}, {"s", streamID.UUID.String()}}

        // (a) Reserve per-stream offsets AND read prior version in one op.
        //     returnDocument:Before => prior doc (nil if the stream is new).
        prior := s.streams.FindOneAndUpdate(sc, bson.D{{"_id", streamKey}},
            bson.D{{"$inc", bson.D{{"last_offset", n}}},
                   {"$setOnInsert", bson.D{{"stream_type", streamID.Type}, {"stream_id", streamID.UUID.String()}}},
                   {"$set", bson.D{{"updated_at", now}}}},
            options.FindOneAndUpdate().SetUpsert(true).SetReturnDocument(options.Before))
        priorOffset := decodeLastOffsetOrZero(prior) // 0 when newly upserted

        // (b) OCC checks against the prior version.
        if opts.StreamMustNotExist && priorOffset > 0 {
            return nil, eventstore.StreamVersionMismatchError{StreamID: streamID, ExpectedVersion: 0, ActualVersion: priorOffset}
        }
        if opts.ExpectVersion != nil && priorOffset != *opts.ExpectVersion {
            return nil, eventstore.StreamVersionMismatchError{StreamID: streamID, ExpectedVersion: *opts.ExpectVersion, ActualVersion: priorOffset}
        }

        // (c) Reserve global offsets (the hot counter — serializes all appends, gap-free).
        gres := s.counters.FindOneAndUpdate(sc, bson.D{{"_id", "global_offset"}},
            bson.D{{"$inc", bson.D{{"value", n}}}},
            options.FindOneAndUpdate().SetUpsert(true).SetReturnDocument(options.After))
        newGlobal := decodeValue(gres)        // == priorGlobal + n
        startGlobal := newGlobal - n

        // (d) Build + insert event docs on the SESSION context (fixes C1).
        full := make([]*Event, n); docs := make([]any, n)
        for i, we := range events {
            full[i] = &Event{Event: eventstore.Event{
                ID: typeid.NewV4(we.Type), StreamID: streamID,
                StreamVersion: priorOffset + int64(i) + 1, Timestamp: now,
                Data: we.Data, Metadata: we.Metadata,
            }, GlobalOffset: startGlobal + int64(i) + 1}
            docs[i], err = s.marshaler.MarshalDocument(full[i]); if err != nil { return nil, err }
        }
        if res, err := s.events.InsertMany(sc, docs); err != nil {
            return nil, fmt.Errorf("inserting events: %w", err)   // unique-index violation => OCC backstop
        } else if len(res.InsertedIDs) != int(n) { return nil, ... }

        // (e) Update stream's last_global_offset for ListStreams.
        s.streams.UpdateOne(sc, bson.D{{"_id", streamKey}},
            bson.D{{"$set", bson.D{{"last_global_offset", newGlobal}}}})

        // (f) Run transaction hooks in-txn on the session ctx (fixes C3).
        for _, h := range s.txHooks {
            if err := h.HandleEvents(sc, toEventSlice(full)); err != nil {
                return nil, fmt.Errorf("transaction hook: %w", err) // aborts the whole append
            }
        }
        return nil, nil
    }, s.txOpts)
    return err
}
```

Key properties this gives us:
- **C1 fixed:** every write uses the session context; the insert is genuinely in the transaction.
- **C2 fixed:** concurrent same-stream appends write-conflict on the `streams` doc → `WithTransaction`
  retries the loser, which re-reads the bumped offset and fails its OCC check; the unique
  `{stream_type, stream_id, offset}` index is the backstop if anything slips through.
- **C3 fixed:** hooks run in-txn; a hook error rolls the whole append back.
- **H1 fixed:** no scans — two `findOneAndUpdate`s by `_id` and an `InsertMany`.
- **H4 fixed:** `snapshot`/`majority` concerns set by default.
- **M2 fixed:** a retried append computes the same `offset` → duplicate-key on the unique index →
  `StreamVersionMismatch`-class rejection; no silent duplicate.
- **Contention (documented, accepted):** the global counter serializes all appends across streams.
  This is the deliberate cost of G-strict ordering. Note it in the README; future mitigations
  (sharded/batched counters) are out of scope.

> **Driver detail to verify during implementation:** confirm `WithTransaction`'s retry behavior on
> `WriteConflict` (code 112) / `TransientTransactionError` for both the counter `$inc` and the unique
> index violation, and that `StreamVersionMismatchError` (a deterministic app error) is *not*
> retried into a spurious loop. If the unique-index path needs explicit duplicate-key detection,
> translate code 11000 → `StreamVersionMismatchError` before returning.

### 3.4 Read paths

- **`ReadStream`** — unchanged signature. Query `{stream_type, stream_id}` + version filter, sort by
  `offset` (asc/desc per `Direction`), `limit` = `Count`. Single cursor → `streamIterator`. Reuse the
  existing `findOptsFromReadStreamOptions` helper (keyed on `offset`). Uses `uniq_stream_offset`.
- **`ReadAll`** — unchanged signature. Single query sorted by `global_offset` + version filter +
  limit. Single cursor → `streamIterator`. **The multi-cursor merge is deleted** (H2 gone). Uses
  `uniq_global_offset`.
- **`ListStreams`** — unchanged signature/return type. Now a plain `Find({})` over the `streams`
  collection, decoded into `[]StreamInfo` with a typed struct (no hand-rolled `UnmarshalBSON`, no
  unchecked assertions → M1 gone). Close the cursor properly (no defer-in-loop).

### 3.5 `EnsureIndexes` / `Init`

```go
func (s *EventStore) EnsureIndexes(ctx context.Context) error
```
Creates the two `events` indexes via `Indexes().CreateMany` (idempotent). Optionally seed the
`counters` doc (or let the first append upsert it). Document that operators must call this once at
deploy, mirroring Postgres `Schema()`. Optionally add `WithAutoEnsureIndexes()` to call it from
`New`, defaulting **off** for parity with the explicit Postgres flow.

### 3.6 Testability seam (fixes the driver-type leak)

With `Strategy` gone, define the minimal interfaces the store needs and depend on those:
`MongoSessionStarter`, `MongoDatabase`, `MongoCollection` (with `FindOneAndUpdate`, `UpdateOne`,
`InsertMany`, `Find`, `FindOne`, `Indexes`), and the existing `MongoCursor`. Where the driver returns
concrete types (`*mongo.SingleResult`, `*mongo.Cursor`), wrap at the seam boundary so the iterator and
append logic depend only on interfaces. Do not over-invest — the concurrency guarantees can only be
proven against a live replica set, so integration tests carry most of the weight.

---

## 4. `mongodb/outbox` package (new)

Mirrors the Postgres outbox **API**; replaces its Postgres-specific **mechanics**.

### 4.1 Public API (consistent with `postgres/outbox`)

```go
type ItemHandler func(ctx context.Context, item *Item) error

type Item struct {
    GlobalOffset  int64     // insertion-order key (was Postgres `ID int64`)
    EventID       typeid.ID
    StreamID      typeid.ID
    StreamVersion int64
    Timestamp     time.Time
    Data          []byte
    Metadata      map[string]string
    CreatedAt     time.Time
    RetryCount    int
    LastError     *string
    FailedAt      *time.Time
}

func New(coll MongoCollection, streamState MongoCollection, handler ItemHandler, opts ...Option) (*Outbox, error)
func (o *Outbox) HandleEvents(sessCtx context.Context, events []*eventstore.Event) error // TransactionHook
func (o *Outbox) EnsureIndexes(ctx context.Context) error
func (o *Outbox) ProcessNext(ctx context.Context) error // returns ErrNoItems when idle
func (o *Outbox) Run(ctx context.Context) error          // single active Run guard (atomic.Bool)

var ErrNoItems = errors.New("no eligible outbox items")

// Options: WithCollectionName, WithStreamStateCollectionName, WithPollInterval,
//          WithLogger, WithMaxRetries (default 10; 0 = unlimited),
//          WithLeaseDuration (default 30s).
```

`Outbox` implements the eventstore `TransactionHook`, so registration is unchanged:
`eventstore.New(client, eventstore.WithTransactionHook(outbox))`.

### 4.2 Producer side — `HandleEvents`

Inside the append transaction (session ctx), insert one outbox document per event:

```jsonc
{
  "_id":           ObjectID,
  "global_offset": 4096,           // ordering key (from event.GlobalPosition)
  "stream_type":   "user",
  "stream_id":     "<uuid>",
  "stream_version":12,             // per-stream FIFO key
  "event_id":      "<uuid>",
  "event_type":    "UserRegistered",
  "timestamp":     ISODate,
  "data":          BinData,
  "metadata":      { ... },
  "status":        "pending",       // pending | failed
  "retry_count":   0,
  "last_error":    null,
  "created_at":    ISODate,
  "leased_until":  null,
  "leased_by":     null
}
```

Because this runs in the same transaction as the event insert, outbox rows are written atomically
with the events (and rolled back together on any failure). This is the transactional-outbox
guarantee, identical in spirit to Postgres.

### 4.3 Consumer side — claim → handle-outside-txn → ack, with per-stream FIFO

**Per-stream FIFO is a hard requirement.** Achieved via **per-stream leasing** (one worker owns a
stream at a time; processes that stream's items in `stream_version` order). A second
`outbox_streams` state collection tracks per-stream cursors and leases:

```jsonc
{
  "_id":              { "t": "user", "s": "<uuid>" }, // stream key
  "next_version":     12,        // next stream_version eligible for processing
  "leased_until":     ISODate,   // lease expiry (null/elapsed => claimable)
  "leased_by":        "worker-1"
}
```

`ProcessNext(ctx)`:

1. **Claim a stream.** `findOneAndUpdate` on `outbox_streams` matching a stream with eligible work and
   no live lease — filter `{ leased_until: {$lt: now} }` (or null) — `$set leased_until = now + lease,
   leased_by = id`. Discovering "has eligible work" cheaply: maintain `outbox_streams` from the
   producer hook (upsert the stream key with `next_version` initialized on first event), or derive
   the candidate set from `pending` items. **Implementation choice to finalize in code review:** prefer
   producer-maintained `outbox_streams` so the claim is a single indexed `findOneAndUpdate` rather than
   a distinct-scan. (Index `outbox_streams` on `{ leased_until: 1 }`.)
2. **Load the head item** for that stream: the `pending` item with the lowest `stream_version`
   (== the cursor's `next_version`). Index `outbox` on `{ stream_type, stream_id, stream_version }`
   and a partial index on `status: "pending"`.
3. **Run the handler OUTSIDE any transaction** (no session held across user code).
4. **Ack:**
   - success → delete the item (delete-on-ack); advance the stream cursor `next_version++`; if no more
     `pending` items for the stream, release the lease.
   - failure → `retry_count++`, record `last_error`; if `retry_count > maxRetries` set
     `status: "failed", failed_at: now` (the stream **halts** here — a failed head blocks its stream,
     matching Postgres semantics); release the lease so the stream is retried next tick (until failed).
5. Return `ErrNoItems` when no stream has claimable eligible work.

`Run(ctx)`: ticker loop; each tick drains via repeated `ProcessNext` until `ErrNoItems`; single active
run enforced with `atomic.Bool`; stops on ctx cancellation. Multiple processes may run concurrently —
stream leases guarantee at-most-one active worker **per stream** while allowing cross-stream
parallelism.

**Delivery contract:** at-least-once (a crash between handler success and ack re-delivers after lease
expiry). Handlers must be idempotent — same contract the Postgres outbox already documents.

**Cleanup:** delete-on-ack keeps the working set small with no cron. Failed items are retained for
operator inspection; offer `WithFailedRetention(d)` later via a TTL index on `failed_at` (note as a
follow-up, not required for v1).

### 4.4 Why not the Postgres mechanics

- No `FOR UPDATE SKIP LOCKED` → atomic `findOneAndUpdate` lease instead.
- No handler-inside-txn → can't hold a Mongo session across user code; the claim/ack inversion is
  mandatory, hence the lease.
- No correlated `NOT EXISTS` head-of-stream probe → the `outbox_streams` cursor encodes head-of-stream
  position directly, which is cheaper and also frees us from retaining processed rows for ordering.

---

## 5. Test matrix

Integration tests run against a **single-node replica set** (transactions require it). Update
`testutil_test.go` / `createMongoDBContainer` to start `mongod --replSet` + `rs.initiate()` and wait
for primary. Gate with `testing.Short()` as today. Run the race-sensitive tests with `-race`.

### 5.1 Event store — correctness
- Acceptance suite (`tests.EventStoreAcceptanceTest`) passes for the default store.
- `ReadStream`: order, `AfterVersion` (forward exclusive / reverse inclusive), `Count`, `Reverse`.
- `ReadAll`: global order, `AfterVersion`, `Count`, `Reverse`; multi-stream interleaving.
- `ListStreams`: returns correct `Offset` and `GlobalOffset` per stream from the `streams` collection.
- `GlobalPosition` is populated, dense, and gap-free across a sequential workload.
- Malformed/edge: empty append is a no-op; metadata round-trips; large batch.

### 5.2 Event store — concurrency (the centerpiece, `-race`)
- **Same-stream race:** N goroutines append to one stream with `ExpectVersion`. Exactly one succeeds
  per version; losers get `StreamVersionMismatchError`; final read shows a contiguous, unique
  `offset` sequence with **no duplicates** and no gaps.
- **Different-stream race:** N goroutines append to N streams concurrently. All succeed;
  `global_offset` values across the store are **unique, dense, gap-free**, and contain no hole below
  any observed high-water mark (the commit-order property).
- **`StreamMustNotExist` race:** concurrent creators of the same new stream → exactly one wins.
- **Idempotency:** replaying an identical append (same expected version) is rejected by the unique
  index, not silently duplicated.

### 5.3 Transaction hooks
- A hook's writes are visible iff the append commits (write a marker doc in the hook; assert it is
  absent when the append is forced to fail after the hook).
- A hook returning an error aborts the append (no events, no marker persisted).
- Hook receives events with correct `StreamVersion` and `GlobalPosition`.

### 5.4 Outbox
- Producer: outbox docs written in the append txn; rolled back when the append fails.
- **Per-stream FIFO:** interleaved multi-stream load, multiple concurrent `Run` workers; assert each
  stream's items are delivered in `stream_version` order; assert cross-stream parallelism occurred.
- Lease expiry: a worker that "crashes" (drops its lease) → item re-delivered after the lease window;
  handler idempotency assumed.
- Retry/fail: a handler that always errors increments `retry_count`, then `status: failed` after
  `maxRetries`, and **halts the stream** (later versions on that stream are not delivered).
- Delete-on-ack: processed items are removed; `ProcessNext` returns `ErrNoItems` when drained.
- `EnsureIndexes` is idempotent.

### 5.5 Init / indexes
- `EnsureIndexes` creates exactly the specified indexes and is safe to call repeatedly.
- Unique constraints actually reject duplicate `offset` / `global_offset` at the driver level.

---

## 6. Branching & PR sequencing

**Integration model:** all five PRs merge into a single long-lived **feature branch**
(e.g. `mongodb-rewrite`), cut from `main`. The feature branch is merged to `main` **once, as a single
release**, when the whole rewrite is complete and green. No partial slice reaches `main` on its own.

- Cut `mongodb-rewrite` from `main` (current default). Each PR below is a topic branch off
  `mongodb-rewrite`, reviewed and merged **into `mongodb-rewrite`** — never into `main`.
- Keep `mongodb-rewrite` current with `main` (merge/rebase `main` in) if `main` moves during the work,
  so the eventual release merge is a clean fast-forward-ish diff.
- The breaking change (schema rewrite, removed `Strategy`/multi-collection) lands on `main` atomically
  at the final feature-branch merge — so `main` is never in a half-migrated state.

PRs into `mongodb-rewrite`, each building on the prior:

1. **PR1 — schema + init + read paths.** New collections/data model, `EnsureIndexes`, RS-aware test
   harness, `ListStreams` from `streams`, `ReadStream`/`ReadAll` single-cursor. No write path yet
   (or a temporary naive writer) — gets the test infra and indexes in place.
2. **PR2 — transactional append + counters.** The §3.3 append path; delete the old strategy package
   and multi-collection iterator; wire OCC + global counter. Land §5.1–5.2 tests (the proof).
3. **PR3 — transaction hooks.** In-txn hook execution; §5.3 tests.
4. **PR4 — outbox package.** Producer hook, claim/lease/ack consumer, per-stream FIFO, §5.4 tests.
5. **PR5 — docs.** README rewrite (replica-set requirement, `EnsureIndexes` step, accurate strategy
   list, global-counter contention note), CHANGELOG breaking-change entry, package docs.

Each PR (and the feature branch tip): `go build ./... && go vet ./... && go test -race ./mongodb/...`
green. The final `mongodb-rewrite → main` release merge is gated on the **full** matrix in §5 passing
on the feature branch tip — that merge is the release.

---

## 7. Risks & things to verify during implementation

- **`WithTransaction` retry semantics** (driver v2): confirm write-conflicts and transient errors
  retry, and that deterministic `StreamVersionMismatchError` and duplicate-key (11000) do **not** spin.
  Decide where to translate 11000 → `StreamVersionMismatchError`.
- **Global-counter contention** under high cross-stream concurrency: validate retry rates in the
  different-stream race test; document the ceiling. Mitigation (sharded counters) is future work.
- **Lease duration vs handler latency** in the outbox: a handler slower than the lease causes
  duplicate delivery (acceptable under at-least-once, but pick a safe 30s default and document it).
- **`outbox_streams` candidate discovery:** finalize producer-maintained cursor vs pending-scan in
  PR4 review; the former keeps the claim a single indexed op.
- **Replica-set test startup flakiness:** ensure the harness waits for primary election before tests
  run; budget for `rs.initiate()` latency in container setup.
- **Public-surface freeze:** if any Tier-B signature (`ReadAll`/`ListStreams`/`New`/options/
  `StreamInfo`) needs to change, stop and raise it before merging.
```
