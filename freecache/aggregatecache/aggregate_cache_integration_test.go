package aggregatecache_test

import (
	"context"
	"testing"

	"github.com/coocood/freecache"
	"github.com/go-estoria/estoria-contrib/freecache/aggregatecache"
	"github.com/go-estoria/estoria/aggregatestore"
	"github.com/go-estoria/estoria/typeid"
	"github.com/gofrs/uuid/v5"
)

type mockEntity struct {
	ID   uuid.UUID
	Name string
}

func (e mockEntity) EntityID() typeid.UUID {
	return typeid.FromUUID("mockentity", e.ID)
}

func TestCache_GetAggregate(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name            string
		haveCache       func(*testing.T) *freecache.Cache
		haveMarshaler   aggregatecache.SnapshotMarshaler[mockEntity]
		haveAggregateID typeid.UUID
		wantAggregate   *aggregatestore.Aggregate[mockEntity]
		wantErr         error
	}{
		{
			name: "returns nil when aggregate is not found",
			haveCache: func(t *testing.T) *freecache.Cache {
				t.Helper()
				return freecache.NewCache(1000)
			},
			haveMarshaler:   aggregatecache.JSONSnapshotMarshaler[mockEntity]{},
			haveAggregateID: typeid.FromUUID("type", uuid.Must(uuid.NewV4())),
			wantAggregate:   nil,
			wantErr:         nil,
		},
		{
			name: "returns aggregate when found",
			haveCache: func(t *testing.T) *freecache.Cache {
				t.Helper()
				cache := freecache.NewCache(1000)

				snapshot := aggregatecache.Snapshot[mockEntity]{
					Entity:  mockEntity{Name: "test"},
					Version: 1,
				}

				data, err := aggregatecache.JSONSnapshotMarshaler[mockEntity]{}.Marshal(snapshot)
				if err != nil {
					t.Fatal(err)
				}

				if err := cache.Set([]byte("type_9fbcfd12-fffa-4e43-8168-9e107db5c800"), data, 1); err != nil {
					t.Fatal(err)
				}

				return cache
			},
			haveMarshaler:   aggregatecache.JSONSnapshotMarshaler[mockEntity]{},
			haveAggregateID: typeid.FromUUID("type", uuid.Must(uuid.FromString("9fbcfd12-fffa-4e43-8168-9e107db5c800"))),
			wantAggregate: func() *aggregatestore.Aggregate[mockEntity] {
				return aggregatestore.NewAggregate(mockEntity{Name: "test"}, 1)
			}(),
			wantErr: nil,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cache := aggregatecache.New(tt.haveCache(t), aggregatecache.WithMarshaler(tt.haveMarshaler))
			aggregate, err := cache.GetAggregate(context.Background(), tt.haveAggregateID)

			if tt.wantErr != nil {
				if err != tt.wantErr {
					t.Errorf("want error %v, got %v", tt.wantErr, err)
				}

				return
			} else if tt.wantAggregate == nil {
				return
			}

			if aggregate.Version() != tt.wantAggregate.Version() {
				t.Errorf("want aggregate version %d, got %d", tt.wantAggregate.Version(), aggregate.Version())
			}

			if aggregate.Entity().Name != tt.wantAggregate.Entity().Name {
				t.Errorf("want aggregate entity name %s, got %s", tt.wantAggregate.Entity().Name, aggregate.Entity().Name)
			}
		})
	}
}

func TestCache_PutAggregate(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name          string
		haveCache     func(*testing.T) *freecache.Cache
		haveMarshaler aggregatecache.SnapshotMarshaler[mockEntity]
		haveAggregate *aggregatestore.Aggregate[mockEntity]
		wantErr       error
	}{
		{
			name: "puts aggregate in cache",
			haveCache: func(t *testing.T) *freecache.Cache {
				t.Helper()
				return freecache.NewCache(1000)
			},
			haveMarshaler: aggregatecache.JSONSnapshotMarshaler[mockEntity]{},
			haveAggregate: func() *aggregatestore.Aggregate[mockEntity] {
				return aggregatestore.NewAggregate(mockEntity{Name: "test"}, 1)
			}(),
			wantErr: nil,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cache := aggregatecache.New(tt.haveCache(t), aggregatecache.WithMarshaler(tt.haveMarshaler))
			err := cache.PutAggregate(context.Background(), tt.haveAggregate)

			if tt.wantErr != nil {
				if err != tt.wantErr {
					t.Errorf("want error %v, got %v", tt.wantErr, err)
				}
			}
		})
	}
}
