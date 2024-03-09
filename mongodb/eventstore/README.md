# MongoDB

Event store implementation for [estoria](https://github.com/go-estoria/estoria) using [MongoDB](https://www.mongodb.com).

## Storage Strategies

### Single-Collection

All events for all streams are stored in a single collection. Streams query this collection, filtering on stream ID.

### Collection-Per-Stream

Each Estoria event stream maps to its own Mongo collection.
