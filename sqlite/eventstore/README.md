# SQLite

Event store implementation for [estoria](https://github.com/go-estoria/estoria) using [SQLite](https://www.sqlite.org).

The implementation is driver-agnostic: callers pass a configured `*sql.DB`, so any
SQLite driver that registers with `database/sql` (e.g., `modernc.org/sqlite`,
`github.com/mattn/go-sqlite3`) is supported. Tests use `modernc.org/sqlite` so the
suite runs without cgo or Docker.

SQLite 3.35.0+ is required (for `RETURNING` and `ON CONFLICT ... DO UPDATE ... RETURNING`).
Using WAL journal mode with more than one connection additionally requires SQLite
3.51.3+, or a build carrying the WAL-reset fix (such as 3.50.7 or 3.44.6): older
versions can corrupt the database under exactly this workload. The `modernc.org/sqlite`
release pinned by this module's tests embeds a fixed SQLite (3.53.3 as of v1.54.0).
WAL also requires every connected process to run on one host against a local
filesystem whose locking SQLite can rely on; network filesystems are unsupported.

## Storage Strategies

### Single Table

All events for all Estoria event streams are stored in a single table. Streams query
this table, filtering on stream ID. A separate `stream` table records the highwater
mark per stream and is updated atomically with each append.
