# estoria-contrib

>**Note: This project is in early development and is not yet ready for production use.**

Third party implementatons for [estoria](https://github.com/go-estoria/estoria) components:

- [Event Stores](#event-stores)

## Event Stores

| Name | Description | Outbox |
|------|-------------| ------ |
| [EventStoreDB](./eventstoredb/eventstore) | Estoria streams map 1:1 to EventStoreDB streams. | No |
| [MongoDB](./mongodb/eventstore) | Estoria streams map to databases, collections, or a single collection for all streams, depending on the strategy chosen. | Yes |
| [Postgres](./postgres/eventstore) | Estoria streams use a single table for all events. | Yes |
