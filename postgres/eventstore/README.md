# Postgres

Event store implementation for [estoria](https://github.com/go-estoria/estoria) using [Postgres](https://www.postgresql.org).

## Storage Strategies

### Single Table

All events for all Estoria event streams are stored in a single table. Streams query this table, filtering on stream ID.
