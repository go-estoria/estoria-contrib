# Production-Readiness Review: MongoDB Event Store (`estoria-contrib/mongodb`)

**Date:** 2026-06-12
**Scope:** `mongodb/eventstore/` (8 production files, ~1,100 LOC) + strategies.
**Method:** full manual read of all production code, contract check against `estoria/eventstore`,
parity comparison against the production-proven `postgres/eventstore`, `go build`/`go vet`
(both clean), and a test-coverage inventory.

**Bottom line:** The package compiles and passes the happy-path acceptance test, but its core
promise — safe concurrent appends with optimistic concurrency control — **is not actually enforced
at any layer**. There are two independent, compounding defects that each defeat concurrency safety,
plus a silently-dead feature and a global-offset reset bug. Not production-ready in its current
state. The worst offender is a one-token fix, and the architecture is sound enough that the rest is
additive.

## Severity summary

| ID | Severity | Finding | Location |
|----|----------|---------|----------|
| C1 | Critical | Event insert runs **outside** the transaction (`ctx` vs `sessCtx`) | `event_store.go:292` |
| C2 | Critical | No unique index -> no concurrency backstop; offsets via read-then-write with no atomicity | all strategies (no index anywhere) |
| C3 | Critical | Registered `TransactionHook`s are **never invoked** (silent dead feature) | `event_store.go:238`, `options.go:57` |
| C4 | Critical | Multi-collection global-offset **resets to 0** if any collection is empty | `multi_collection_strategy.go:250` |
| H1 | High | `getHighestGlobalOffset` is a full **collection scan on every append** (no index) | `single...:157`, `multi...:235` |
| H2 | High | `multiStreamIterator` breaks on gaps, `AfterVersion`, and `Reverse` -> `ReadAll` returns truncated/empty | `stream_iterator.go:53` |
| H3 | High | Multi-collection global-offset race -> duplicate global offsets (self-documented) | `multi_collection_strategy.go:29` |
| H4 | High | No `readConcern: snapshot` / `writeConcern: majority` -> failover can roll back acked writes | `strategy.go:52` |
| H5 | High | Multi-collection operates over **all** collections in the DB; cursor leaks on error paths | `multi_collection_strategy.go:104,129,238` |
| M1 | Medium | Unchecked type assertions in `UnmarshalBSON` -> panic on legacy/odd BSON | `event_store.go:105,111,113,115` |
| M2 | Medium | No idempotency / duplicate-append protection (`_id` not derived from event identity) | `event_document.go` |
| M3 | Medium | Collection names from **unvalidated** stream type/ID (Postgres validates) | `multi_collection_strategy.go:62,69` |
| M4 | Medium | No concurrency tests; ExpectVersion/hooks/global-order untested; multi-collection test ~entirely commented out | test files |
| L1-L5 | Low | README drift, no index/setup helper, replica-set requirement undocumented, etc. | various |

---

## Critical findings

### C1 - The insert never runs inside the transaction
`event_store.go:292`
```go
result, err := collection.InsertMany(ctx, docs)   // ctx is AppendStream's outer context
```
The closure is handed the session context as `sessCtx` (line 250), but `InsertMany` uses the
**outer** `ctx`. In mongo-driver v2 an operation is only part of a transaction if it runs with the
session-bearing context. So:
- The offset reads (`getHighestOffset`, `getHighestGlobalOffset`) *do* run in the transaction (they
  use the callback's session ctx), but **the write escapes it**.
- The transaction therefore contains only reads -> no write-conflict detection -> it commits trivially.
- Two concurrent appends can both read `offset=5` and both insert `offset=6`.

**Fix:** change `ctx` -> `sessCtx` on line 292. Effectively one token - but on its own it's
necessary-not-sufficient (see C2).

### C2 - No unique index, so even a correct transaction wouldn't protect you
There is **no index creation anywhere** in the package. MongoDB transactions only detect write
conflicts when two transactions touch the *same document*. Two appends inserting *different* new
documents to the same stream do **not** conflict. Optimistic concurrency for an append-only log
therefore *requires* a unique index on `(stream_type, stream_id, offset)` as the backstop - the
racing transaction that loses gets a duplicate-key error, which is exactly the desired
`StreamVersionMismatch` behavior.

This is the central parity gap with Postgres, which enforces:
```sql
CONSTRAINT event_stream_offset_unique UNIQUE (stream_id, stream_type, stream_offset)
```
plus an atomic counter (`INSERT ... ON CONFLICT DO UPDATE ... RETURNING last_offset`) so offsets are
*reserved* atomically rather than read-then-written. The Mongo version has neither. **C1 and C2
together mean AppendStream's `ExpectVersion`/`StreamMustNotExist` checks provide no real concurrency
protection.**

**Fix:** create a unique index on `(stream_type, stream_id, offset)` (and one on `global_offset`);
fix C1; ideally adopt an atomic counter document (`findOneAndUpdate` with `$inc`) instead of
`max()+1` reads.

### C3 - Transaction hooks are registered and then silently ignored
`options.go:57` appends to `s.txHooks`; the `TransactionHook` doc comment promises execution "within
the transaction used for appending events." But `AppendStream` (`event_store.go:238`) never iterates
`s.txHooks` or calls `HandleEvents`. Anyone using this for a transactional outbox or projection
update gets **silent data loss** - the most dangerous kind of bug because it looks wired up.

**Fix:** invoke hooks inside the insert closure (with `sessCtx`), after the insert; abort the txn on
hook error. Add a test.

### C4 - Multi-collection global offset resets to 0 on any empty collection
`multi_collection_strategy.go:248-255`
```go
if result.Err() == mongo.ErrNoDocuments {
    return 0, nil   // <- aborts the whole scan, reports global offset 0
}
```
This should `continue`, not `return`. One empty collection in the database (a freshly created one, a
drained stream, or an unrelated collection - see H5) makes the next append start global offset at 1
again -> mass duplication of global offsets across the store. This is a real bug *beyond* the
documented race in H3.

---

## High-severity findings

**H1 - O(N) read on every write.** `getHighestGlobalOffset` does `FindOne({}, sort: global_offset
desc)` over the entire events collection on *every append*. With no index it's a COLLSCAN +
in-memory sort that scales linearly with total store size - a hard performance cliff.
`getHighestOffset` has the same issue per-stream. (An atomic counter document, per C2, removes both
reads entirely.)

**H2 - `multiStreamIterator` is fragile and partially incorrect.** `stream_iterator.go:53-95` merges
per-collection cursors by requiring the next event's `GlobalOffset == currentGlobalOffset+1`,
starting from 0. Consequences:
- Any gap in global offsets -> premature `ErrEndOfEventStream` (and gaps are *guaranteed* by C4/H3).
- `ReadAll` with `AfterVersion=N` returns **nothing** (cursors yield offsets > N, but the merger
  still expects 1 first).
- `Direction: Reverse` is effectively unsupported (logic only ever increments).

(Single-collection `ReadAll` is fine - it uses the simple `streamIterator` with query-level
sort/limit.)

**H3 - Documented multi-collection global-offset race.** Acknowledged at
`multi_collection_strategy.go:29-35`: the global offset is computed outside the transaction, so
concurrent writes to different streams compute the same value. Honestly documented, but still a
correctness limitation that interacts badly with H2/C4.

**H4 - Durability/isolation not configured.** `DefaultTransactionOptions()` sets only
`ReadPreference(Primary)` (`strategy.go:52`). For a correct read-then-write you want `readConcern:
snapshot` and `writeConcern: majority`; without majority write concern, an acknowledged append can
be rolled back on primary failover, producing offset gaps or duplicates.

**H5 - Multi-collection scans the whole database; leaks cursors on error.**
`ListCollectionNames(ctx, bson.D{})` returns *every* collection
(`multi_collection_strategy.go:105,129,238`), so a shared database or any non-event collection
breaks `ListStreams`/`GetAllCursor`/`getHighestGlobalOffset`. Additionally, when `Find`/`Aggregate`
fails partway through those loops, already-opened cursors are never closed (resource leak).
`event_store.go:171` also uses `defer cursor.Close` inside a loop.

---

## Medium-severity findings

- **M1 - Panic on malformed BSON.** `StreamInfo.UnmarshalBSON` (`event_store.go:105-115`) uses
  unchecked type assertions (`elem.Value.(string)`, `.(int64)`). A legacy/odd document (e.g., an
  `offset` stored as `int32`, or a non-string `_id`) panics the calling goroutine - a robustness/DoS
  risk on untrusted or evolved data. Use checked assertions and return errors.
- **M2 - No idempotency.** Events get an auto-generated `_id`; nothing dedups a retried append. With
  C1/C2 fixed the unique offset index gives natural idempotency-on-conflict, but it's worth an
  explicit decision/test.
- **M3 - Unvalidated collection names.** `CollectionPerStreamType`/`CollectionPerStreamID` feed
  `streamID.Type`/`streamID.String()` straight into collection names. MongoDB rejects/clashes on
  names with `$`, `system.` prefixes, or excess length. Postgres validates analogously
  (`validateTableName`); Mongo should too.
- **M4 - Test coverage gaps.** The shared acceptance test is single-threaded happy-path only (append
  10, read back). **Zero** concurrency tests (no goroutines/`sync` anywhere). Untested:
  `ExpectVersion`, `StreamMustNotExist`, global-position correctness, transaction hooks, `ReadAll`
  cross-stream ordering, reverse/`AfterVersion`/`Count`, error paths. The multi-collection
  integration test is **~entirely commented out** - 205 of 538 lines are comments and only one test
  function (`TestMultiCollectionStrategy_Integration_InsertStreamDocs`) is active. Given the
  concurrency claims, a concurrent-append race test is the single highest-value test to add.

## Low-severity findings

- **L1** - README describes a "Collection Per Database" strategy that doesn't exist in code (only
  single + multi-collection); docs don't mention required indexes.
- **L2** - No operator setup helper to create indexes (Postgres exposes `Schema()`).
- **L3** - Multi-document transactions **require a replica set / mongos**; a standalone `mongod`
  fails at `WithTransaction`. Undocumented operational footgun.
- **L4** - Partial-insert `result` from `InsertMany` is discarded on the happy path (minor).
- **L5** - `StreamVersionMismatchError` for `StreamMustNotExist` reports `ExpectedVersion: 0`,
  indistinguishable from an explicit `ExpectVersion(0)` (already noted as a core caveat upstream).

---

## Recommended remediation order

1. **Make appends safe (C1 + C2 + H4 together)** - they're one coherent change: insert with
   `sessCtx`, add the unique `(stream_type, stream_id, offset)` index, set `writeConcern: majority` +
   `readConcern: snapshot`. Ideally replace `max()+1` reads with an atomic counter document. Land a
   **concurrent-append race test** (M4) in the same PR - it's the proof the fix works.
2. **C3** - wire up transaction hooks (or remove the API). Silent data loss outranks everything below
   it once appends are safe.
3. **C4 + H5** - fix the empty-collection reset and scope multi-collection to actual event
   collections; close cursors on error.
4. **H1** - eliminate the per-append full scan (falls out of the atomic-counter change).
5. **H2 + H3** - decide multi-collection's global-ordering story; fix or constrain
   `multiStreamIterator`.
6. **M1, M3** - harden BSON decoding and collection-name validation.
7. **L1-L3** - docs: required indexes, replica-set requirement, correct strategy list; add an
   index-setup helper.

A defensible MVP "production-ready" cut is items 1-3. Items 4-7 are the difference between "works
under load" and "operable and trustworthy long-term."
