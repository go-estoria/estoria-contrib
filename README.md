# estoria-contrib

>**Note: This project is in early development and is not yet ready for production use.**

Third party implementatons for [estoria](https://github.com/go-estoria/estoria) components:

- [Event Stores](#event-stores)

## Event Stores

| Name | Strategy | Description |
|------|----------|-------------|
| [EventStoreDB](./eventstoredb/eventstore) | 1:1 Stream | Estoria streams map 1:1 to EventStoreDB streams. |
| [MongoDB](./mongodb/eventstore) | Single-Collection | A single Mongo collection is used to store all events for all streams. Streams query this collection, filtering on stream ID. |
| [Redis](./redis/eventstore) | 1:1 Stream | Estoria streams map 1:1 to Redis streams. |

## Storage Strategies

### 1:1 Stream

Each Estoria event stream maps 1:1 to the implementation's concept of a stream.

### Single-Collection

All events for all streams are stored in a single collection. Streams query this collection, filtering on stream ID.

### Collection-Per-Stream

Each Estoria event stream maps to a collection in the implementation's storage. Streams query their respective collections.

## Examples

See the [example](./example) directory for usage examples.
