package aggregatecache

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/go-estoria/estoria"
	"github.com/go-estoria/estoria/aggregatestore"
	"github.com/go-estoria/estoria/typeid"
	"github.com/redis/go-redis/v9"
)

type Snapshot[E estoria.Entity] struct {
	Entity  E     `json:"e"`
	Version int64 `json:"v"`
}

type SnapshotMarshaler[E estoria.Entity] interface {
	Marshal(snapshot Snapshot[E]) ([]byte, error)
	Unmarshal(data []byte, snapshot *Snapshot[E]) error
}

type JSONSnapshotMarshaler[E estoria.Entity] struct{}

func (m JSONSnapshotMarshaler[E]) Marshal(snapshot Snapshot[E]) ([]byte, error) {
	return json.Marshal(snapshot)
}

func (m JSONSnapshotMarshaler[E]) Unmarshal(data []byte, snapshot *Snapshot[E]) error {
	return json.Unmarshal(data, snapshot)
}

type Cache[E estoria.Entity] struct {
	redis     *redis.Client
	marshaler SnapshotMarshaler[E]
}

func New[E estoria.Entity](client *redis.Client, opts ...CacheOption[E]) *Cache[E] {
	aggregateCache := &Cache[E]{
		redis:     client,
		marshaler: JSONSnapshotMarshaler[E]{},
	}

	for _, opt := range opts {
		opt(aggregateCache)
	}

	return aggregateCache
}

func (c *Cache[E]) GetAggregate(ctx context.Context, aggregateID typeid.ID) (*aggregatestore.Aggregate[E], error) {
	res := c.redis.Get(ctx, aggregateID.String())
	if err := res.Err(); errors.Is(err, redis.Nil) {
		return nil, nil
	} else if err != nil {
		return nil, fmt.Errorf("getting data from Redis: %w", err)
	}

	data, err := res.Bytes()
	if err != nil {
		return nil, fmt.Errorf("getting data from Redis: %w", err)
	}

	snapshot := Snapshot[E]{}
	if err := c.marshaler.Unmarshal(data, &snapshot); err != nil {
		return nil, fmt.Errorf("unmarshaling data: %w", err)
	}

	return aggregatestore.NewAggregate(snapshot.Entity, snapshot.Version), nil
}

func (c *Cache[E]) PutAggregate(ctx context.Context, aggregate *aggregatestore.Aggregate[E]) error {
	snapshot := Snapshot[E]{
		Entity:  aggregate.Entity(),
		Version: aggregate.Version(),
	}

	data, err := c.marshaler.Marshal(snapshot)
	if err != nil {
		return fmt.Errorf("marshaling snapshot: %w", err)
	}

	if err := c.redis.Set(ctx, aggregate.ID().String(), data, time.Second).Err(); err != nil {
		return fmt.Errorf("setting data in Redis: %w", err)
	}

	return nil
}

type CacheOption[E estoria.Entity] func(*Cache[E])

func WithMarshaler[E estoria.Entity](marshaler SnapshotMarshaler[E]) CacheOption[E] {
	return func(c *Cache[E]) {
		c.marshaler = marshaler
	}
}
