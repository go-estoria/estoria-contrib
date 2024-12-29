# S3 Event Store

Event store implementation for [estoria](https://github.com/go-estoria/estoria) using [AWS S3](https://aws.amazon.com/s3/).

## Storage Strategies

### Single Bucket

All events for all Estoria event streams are stored in a single bucket. Each Estoria event stream maps to a subdirectory in the bucket.

### Bucket Per Stream

Each Estoria event stream maps to its own S3 bucket.
