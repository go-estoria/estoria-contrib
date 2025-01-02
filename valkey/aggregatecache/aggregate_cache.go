package aggregatecache

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/go-estoria/estoria"
	"github.com/go-estoria/estoria/aggregatestore"
	"github.com/go-estoria/estoria/typeid"
	"github.com/valkey-io/valkey-go"
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
	valkey    valkey.Client
	marshaler SnapshotMarshaler[E]
}

func New[E estoria.Entity](client valkey.Client, opts ...CacheOption[E]) *Cache[E] {
	aggregateCache := &Cache[E]{
		valkey:    client,
		marshaler: JSONSnapshotMarshaler[E]{},
	}

	for _, opt := range opts {
		opt(aggregateCache)
	}

	return aggregateCache
}

func (c *Cache[E]) GetAggregate(ctx context.Context, aggregateID typeid.UUID) (*aggregatestore.Aggregate[E], error) {
	res := c.valkey.Do(ctx, c.valkey.B().Get().Key(aggregateID.String()).Build())
	if err := res.Error(); errors.Is(err, valkey.Nil) {
		return nil, nil
	} else if err != nil {
		return nil, fmt.Errorf("getting data from Valkey: %w", err)
	}

	data, err := res.AsBytes()
	if err != nil {
		return nil, fmt.Errorf("getting data from Valkey: %w", err)
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

	if err := c.valkey.Do(ctx, c.valkey.B().Set().Key(aggregate.ID().String()).Value(string(data)).Build()).Error(); err != nil {
		return fmt.Errorf("setting data in Valkey: %w", err)
	}

	return nil
}

type CacheOption[E estoria.Entity] func(*Cache[E])

func WithMarshaler[E estoria.Entity](marshaler SnapshotMarshaler[E]) CacheOption[E] {
	return func(c *Cache[E]) {
		c.marshaler = marshaler
	}
}
