# SQLite

Event store implementation for [estoria](https://github.com/go-estoria/estoria) using [SQLite](https://www.sqlite.org).

The implementation is driver-agnostic: callers pass a configured `*sql.DB`, so any
SQLite driver that registers with `database/sql` (e.g., `modernc.org/sqlite`,
`github.com/mattn/go-sqlite3`) is supported. Tests use `modernc.org/sqlite` so the
suite runs without cgo or Docker.

SQLite 3.35.0+ is required (for `RETURNING` and `ON CONFLICT ... DO UPDATE ... RETURNING`).

## Storage Strategies

### Single Table

All events for all Estoria event streams are stored in a single table. Streams query
this table, filtering on stream ID. A separate `stream` table records the highwater
mark per stream and is updated atomically with each append.
