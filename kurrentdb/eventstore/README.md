# KurrentDB

Event store implementation for [estoria](https://github.com/go-estoria/estoria) using [KurrentDB](https://www.kurrent.io).

## Storage Strategies

### 1:1 Stream

Each Estoria event stream maps 1:1 to a KurrentDB stream, named `<type>_<uuid>` — or `<prefix>.<type>_<uuid>` when the store is configured with `WithStreamPrefix`.

## Global Reads

The store implements `eventstore.GlobalReader` over the server's `$all` stream, with KurrentDB commit positions as global positions. KurrentDB offers no server-side filtering on reads, so `ReadAll` fetches raw records in windows (see `WithReadAllWindowSize`) and filters them client-side by stream name: system streams (`$`-prefixed) are skipped, and the remainder must parse as an Estoria stream ID carrying the store's prefix, if one is configured.

Stream-name parsing is the only ownership signal available. On a node shared with other applications, any foreign stream named like `<type>_<uuid>` appears in an unprefixed store's global reads; prefix stores to isolate them. Prefix isolation is one-way: prefixed stores never see each other's streams, but an unprefixed store sees every stream whose name parses, prefixed ones included.

These behaviors were established empirically against `kurrentdb:26.1`, the image the test suites pin.

## Unsupported: StreamDeleter

The store deliberately does not implement `eventstore.StreamDeleter`. Estoria's full-delete contract requires that a deleted stream's ID be reusable with versions restarting at 1, and neither KurrentDB deletion primitive provides that: a soft delete leaves appends continuing from the prior revision, and a tombstone retires the stream name permanently. Claiming the interface with either semantic would make type-assertion discovery lie.
