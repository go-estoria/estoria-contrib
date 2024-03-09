# MongoDB

Event store implementation for [estoria](https://github.com/go-estoria/estoria) using [Redis Streams](https://redis.io/docs/data-types/streams/).

## Storage Strategies

### 1:1 Stream

Each Estoria event stream maps 1:1 to a Redis stream.
