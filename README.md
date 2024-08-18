# estoria-contrib

Third party implementatons for [Estoria](https://github.com/go-estoria/estoria) components:

>**Note**: This project is in early beta. While functional, the API is not yet stable and is not suitable for production use.

- [Event Stores](#event-stores)

## Event Stores

| Name | Description | Outbox Support |
|------|-------------| ------ |
| [EventStoreDB](./eventstoredb/eventstore) | Estoria streams map 1:1 to EventStoreDB streams. | No |
| [MongoDB](./mongodb/eventstore) | Estoria streams map to databases, collections, or a single collection for all streams, depending on the strategy chosen. | Yes |
| [SQL](./sql/eventstore) | Estoria streams use a single table for all events. | Yes |

## License

Estoria is licensed under the MIT License.
