# EventStoreDB

Event store implementation for [estoria](https://github.com/go-estoria/estoria) using [EventStoreDB](https://eventstore.com).

## Storage Strategies

### 1:1 Stream

Each Estoria event stream maps 1:1 to an EventStoreDB stream.
