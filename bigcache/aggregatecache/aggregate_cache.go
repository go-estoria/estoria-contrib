package aggregatecache

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/allegro/bigcache/v3"
	"github.com/go-estoria/estoria"
	"github.com/go-estoria/estoria/aggregatestore"
	"github.com/go-estoria/estoria/typeid"
)

type BigCache interface {
	Get(key string) ([]byte, error)
	Set(key string, value []byte) error
}

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
	cache     BigCache
	marshaler SnapshotMarshaler[E]
}

func New[E estoria.Entity](cache BigCache, opts ...CacheOption[E]) *Cache[E] {
	aggregateCache := &Cache[E]{
		cache:     cache,
		marshaler: JSONSnapshotMarshaler[E]{},
	}

	for _, opt := range opts {
		opt(aggregateCache)
	}

	return aggregateCache
}

func (c *Cache[E]) GetAggregate(ctx context.Context, aggregateID typeid.UUID) (*aggregatestore.Aggregate[E], error) {
	data, err := c.cache.Get(aggregateID.String())
	if errors.Is(err, bigcache.ErrEntryNotFound) {
		return nil, nil
	} else if err != nil {
		return nil, fmt.Errorf("getting data from cache: %w", err)
	}

	snapshot := Snapshot[E]{}
	if err := c.marshaler.Unmarshal(data, &snapshot); err != nil {
		return nil, fmt.Errorf("unmarshaling data: %w", err)
	}

	return aggregatestore.NewAggregate(snapshot.Entity, snapshot.Version), nil
}

func (c *Cache[E]) PutAggregate(ctx context.Context, aggregate *aggregatestore.Aggregate[E]) error {
	data, err := c.marshaler.Marshal(Snapshot[E]{
		Entity:  aggregate.Entity(),
		Version: aggregate.Version(),
	})
	if err != nil {
		return fmt.Errorf("marshaling snapshot: %w", err)
	}

	if err := c.cache.Set(aggregate.ID().String(), data); err != nil {
		return fmt.Errorf("setting data in cache: %w", err)
	}

	return nil
}

type CacheOption[E estoria.Entity] func(*Cache[E])

func WithMarshaler[E estoria.Entity](marshaler SnapshotMarshaler[E]) CacheOption[E] {
	return func(c *Cache[E]) {
		c.marshaler = marshaler
	}
}
