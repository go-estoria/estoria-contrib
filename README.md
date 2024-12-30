# estoria-contrib

Third party implementatons for [Estoria](https://github.com/go-estoria/estoria) components:

>**Note**: This project is in early beta. While functional, the API is not yet stable and is not suitable for production use.

- [Event Stores](#event-stores)
- [Aggregate Caches](#aggregate-caches)
- [Snapshot Stores](#snapshot-stores)

## Event Stores

| Name | Description | Outbox Support |
|------|-------------| -------------- |
| [AWS S3](./aws/s3/eventstore) | Estoria streams map to subdirectories within a bucket. | No |
| [EventStoreDB](./eventstoredb/eventstore) | Estoria streams map 1:1 to EventStoreDB streams. | No |
| [MongoDB](./mongodb/eventstore) | Estoria streams map to databases, collections, or a single collection for all streams, depending on the strategy chosen. | Yes |
| [SQL](./sql/eventstore) | Estoria streams use a single table for all events. | Yes |

## Aggregate Caches

| Name | Description | Type |
|------|-------------| ---- |
| [bigcache](./bigcache/aggregatecache) | Memory-based cache using [bigcache](https://github.com/allegro/bigcache). | In-memory |
| [freecache](./freecache/aggregatecache) | Memory-based cache using [freecache](https://github.com/coocood/freecache). | In-memory |
| [Redis](./redis/aggregatecache) | Memory and/or distributed cache using [Redis](https://github.com/redis/redis) key/value storage. | Distributed |

## Snapshot Stores

| Name | Description |
|------|-------------|
| [AWS S3](./aws/s3/snapshotstore) | Snapshots are stored as JSON files in S3, and snapshot data is encoded in base64. |

## License

Estoria is licensed under the MIT License.
