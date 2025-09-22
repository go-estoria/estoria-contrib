# KurrentDB

Event store implementation for [estoria](https://github.com/go-estoria/estoria) using [KurrentDB](https://www.kurrent.io).

## Storage Strategies

### 1:1 Stream

Each Estoria event stream maps 1:1 to a KurrentDB stream.
