package aggregatestore

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/DataDog/datadog-go/v5/statsd"
	"github.com/DataDog/dd-trace-go/v2/ddtrace/ext"
	"github.com/DataDog/dd-trace-go/v2/ddtrace/mocktracer"
	"github.com/go-estoria/estoria"
	"github.com/go-estoria/estoria/aggregatestore"
	"github.com/go-estoria/estoria/typeid"
	"github.com/gofrs/uuid/v5"
)

// mockAggregateStore is a mock for aggregatestore.Store.
type mockAggregateStore[E estoria.Entity] struct {
	NewFn     func(uuid.UUID) *aggregatestore.Aggregate[E]
	LoadFn    func(context.Context, uuid.UUID, *aggregatestore.LoadOptions) (*aggregatestore.Aggregate[E], error)
	HydrateFn func(context.Context, *aggregatestore.Aggregate[E], *aggregatestore.HydrateOptions) error
	SaveFn    func(context.Context, *aggregatestore.Aggregate[E], *aggregatestore.SaveOptions) error
}

func (s *mockAggregateStore[E]) New(id uuid.UUID) *aggregatestore.Aggregate[E] {
	if s.NewFn != nil {
		return s.NewFn(id)
	}
	return nil
}

func (s *mockAggregateStore[E]) Load(ctx context.Context, id uuid.UUID, opts *aggregatestore.LoadOptions) (*aggregatestore.Aggregate[E], error) {
	if s.LoadFn != nil {
		return s.LoadFn(ctx, id, opts)
	}
	return nil, errors.New("LoadFn not set")
}

func (s *mockAggregateStore[E]) Hydrate(ctx context.Context, agg *aggregatestore.Aggregate[E], opts *aggregatestore.HydrateOptions) error {
	if s.HydrateFn != nil {
		return s.HydrateFn(ctx, agg, opts)
	}
	return errors.New("HydrateFn not set")
}

func (s *mockAggregateStore[E]) Save(ctx context.Context, agg *aggregatestore.Aggregate[E], opts *aggregatestore.SaveOptions) error {
	if s.SaveFn != nil {
		return s.SaveFn(ctx, agg, opts)
	}
	return errors.New("SaveFn not set")
}

// mockEntity is a minimal estoria.Entity used to parameterize the store in tests.
type mockEntity struct {
	id typeid.ID
}

func (e mockEntity) EntityID() typeid.ID { return e.id }

func newMockAggregate(t *testing.T, version int64) *aggregatestore.Aggregate[mockEntity] {
	t.Helper()
	u, err := uuid.NewV4()
	if err != nil {
		t.Fatalf("generating uuid: %v", err)
	}
	return aggregatestore.NewAggregate(mockEntity{id: typeid.New("mockentity", u)}, version)
}

// incrCall records a single statsd Incr invocation.
type incrCall struct {
	name string
	tags []string
	rate float64
}

// mockStatsd is a hand-rolled statsd.ClientInterface that records Incr calls.
// It embeds NoOpClient so unused methods no-op rather than panic.
type mockStatsd struct {
	statsd.NoOpClient
	mu    sync.Mutex
	incrs []incrCall
}

func (m *mockStatsd) Incr(name string, tags []string, rate float64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.incrs = append(m.incrs, incrCall{name: name, tags: tags, rate: rate})
	return nil
}

func (m *mockStatsd) incrCalls() []incrCall {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]incrCall, len(m.incrs))
	copy(out, m.incrs)
	return out
}

func TestNewInstrumentedStore(t *testing.T) {
	t.Parallel()

	inner := &mockAggregateStore[mockEntity]{}

	for _, tt := range []struct {
		name    string
		opts    []InstrumentedStoreOption[mockEntity]
		wantErr bool
	}{
		{
			name:    "creates store with metrics disabled",
			opts:    []InstrumentedStoreOption[mockEntity]{WithMetricsEnabled[mockEntity](false)},
			wantErr: false,
		},
		{
			name:    "creates store with custom metrics client",
			opts:    []InstrumentedStoreOption[mockEntity]{WithMetricsClient[mockEntity](&statsd.NoOpClient{})},
			wantErr: false,
		},
		{
			name: "creates store with custom metric namespace",
			opts: []InstrumentedStoreOption[mockEntity]{
				WithMetricNamespace[mockEntity]("custom.metrics"),
				WithMetricsClient[mockEntity](&statsd.NoOpClient{}),
			},
			wantErr: false,
		},
		{
			name: "creates store with custom trace namespace",
			opts: []InstrumentedStoreOption[mockEntity]{
				WithTraceNamespace[mockEntity]("custom.trace"),
				WithMetricsClient[mockEntity](&statsd.NoOpClient{}),
			},
			wantErr: false,
		},
		{
			name: "option returning error fails construction",
			opts: []InstrumentedStoreOption[mockEntity]{
				func(*InstrumentedStore[mockEntity]) error { return errors.New("boom") },
			},
			wantErr: true,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			store, err := NewInstrumentedStore(inner, tt.opts...)

			if tt.wantErr {
				if err == nil {
					t.Errorf("expected error, got nil")
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if store == nil {
				t.Fatalf("expected non-nil store")
			}

			if store.inner != inner {
				t.Errorf("expected inner store to be set")
			}

			if store.meter == nil {
				t.Errorf("expected non-nil meter")
			}
		})
	}
}

func TestInstrumentedStoreOptions(t *testing.T) {
	t.Parallel()

	inner := &mockAggregateStore[mockEntity]{}

	for _, tt := range []struct {
		name                string
		opts                []InstrumentedStoreOption[mockEntity]
		wantMetricsEnabled  bool
		wantMetricNamespace string
		wantTraceNamespace  string
	}{
		{
			name:                "defaults",
			opts:                []InstrumentedStoreOption[mockEntity]{WithMetricsClient[mockEntity](&statsd.NoOpClient{})},
			wantMetricsEnabled:  true,
			wantMetricNamespace: "aggregatestore",
			wantTraceNamespace:  "aggregatestore",
		},
		{
			name: "WithMetricsEnabled(false)",
			opts: []InstrumentedStoreOption[mockEntity]{
				WithMetricsEnabled[mockEntity](false),
			},
			wantMetricsEnabled:  false,
			wantMetricNamespace: "aggregatestore",
			wantTraceNamespace:  "aggregatestore",
		},
		{
			name: "WithMetricNamespace",
			opts: []InstrumentedStoreOption[mockEntity]{
				WithMetricNamespace[mockEntity]("custom.metrics"),
				WithMetricsClient[mockEntity](&statsd.NoOpClient{}),
			},
			wantMetricsEnabled:  true,
			wantMetricNamespace: "custom.metrics",
			wantTraceNamespace:  "aggregatestore",
		},
		{
			name: "WithTraceNamespace",
			opts: []InstrumentedStoreOption[mockEntity]{
				WithTraceNamespace[mockEntity]("custom.trace"),
				WithMetricsClient[mockEntity](&statsd.NoOpClient{}),
			},
			wantMetricsEnabled:  true,
			wantMetricNamespace: "aggregatestore",
			wantTraceNamespace:  "custom.trace",
		},
		{
			name: "all options combined",
			opts: []InstrumentedStoreOption[mockEntity]{
				WithMetricsEnabled[mockEntity](false),
				WithMetricNamespace[mockEntity]("my.metrics"),
				WithTraceNamespace[mockEntity]("my.traces"),
			},
			wantMetricsEnabled:  false,
			wantMetricNamespace: "my.metrics",
			wantTraceNamespace:  "my.traces",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			store, err := NewInstrumentedStore(inner, tt.opts...)
			if err != nil {
				t.Fatalf("failed to create store: %v", err)
			}

			if store.metricsEnabled != tt.wantMetricsEnabled {
				t.Errorf("expected metricsEnabled %v, got %v", tt.wantMetricsEnabled, store.metricsEnabled)
			}

			if store.metricNamespace != tt.wantMetricNamespace {
				t.Errorf("expected metricNamespace %q, got %q", tt.wantMetricNamespace, store.metricNamespace)
			}

			if store.traceNamespace != tt.wantTraceNamespace {
				t.Errorf("expected traceNamespace %q, got %q", tt.wantTraceNamespace, store.traceNamespace)
			}
		})
	}
}

func TestInstrumentedStore_New(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name       string
		innerAgg   *aggregatestore.Aggregate[mockEntity]
		wantInvoke bool
	}{
		{
			name:       "delegates to inner and returns the aggregate",
			innerAgg:   aggregatestore.NewAggregate(mockEntity{id: typeid.NewV4("mockentity")}, 0),
			wantInvoke: true,
		},
		{
			name:       "returns nil when inner returns nil",
			innerAgg:   nil,
			wantInvoke: true,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			gotID := uuid.Nil
			inner := &mockAggregateStore[mockEntity]{
				NewFn: func(id uuid.UUID) *aggregatestore.Aggregate[mockEntity] {
					gotID = id
					return tt.innerAgg
				},
			}

			store, err := NewInstrumentedStore(inner,
				WithMetricsClient[mockEntity](&statsd.NoOpClient{}),
			)
			if err != nil {
				t.Fatalf("failed to create store: %v", err)
			}

			callID, err := uuid.NewV4()
			if err != nil {
				t.Fatalf("generating uuid: %v", err)
			}

			got := store.New(callID)

			if tt.wantInvoke && gotID != callID {
				t.Errorf("expected inner New to receive id %v, got %v", callID, gotID)
			}

			if got != tt.innerAgg {
				t.Errorf("expected inner aggregate to be returned, got %v", got)
			}
		})
	}
}

func TestInstrumentedStore_Load(t *testing.T) {
	mt := mocktracer.Start()
	defer mt.Stop()

	aggID, err := uuid.NewV4()
	if err != nil {
		t.Fatalf("generating uuid: %v", err)
	}
	loadedAgg := aggregatestore.NewAggregate(mockEntity{id: typeid.New("mockentity", aggID)}, 3)

	sentinelErr := errors.New("load failed")

	for _, tt := range []struct {
		name             string
		opts             []InstrumentedStoreOption[mockEntity]
		loadOpts         *aggregatestore.LoadOptions
		innerReturnAgg   *aggregatestore.Aggregate[mockEntity]
		innerReturnErr   error
		wantAgg          *aggregatestore.Aggregate[mockEntity]
		wantErr          error
		wantSpanName     string
		wantMetricName   string
		wantToVersionTag bool
		wantToVersionVal int64
	}{
		{
			name:             "success with load options",
			loadOpts:         &aggregatestore.LoadOptions{ToVersion: 7},
			innerReturnAgg:   loadedAgg,
			wantAgg:          loadedAgg,
			wantSpanName:     "aggregatestore.Load",
			wantMetricName:   "aggregatestore.load",
			wantToVersionTag: true,
			wantToVersionVal: 7,
		},
		{
			name:             "nil opts does not panic (regression)",
			loadOpts:         nil,
			innerReturnAgg:   loadedAgg,
			wantAgg:          loadedAgg,
			wantSpanName:     "aggregatestore.Load",
			wantMetricName:   "aggregatestore.load",
			wantToVersionTag: false,
		},
		{
			name:             "inner error is propagated and span records error",
			loadOpts:         &aggregatestore.LoadOptions{},
			innerReturnErr:   sentinelErr,
			wantErr:          sentinelErr,
			wantSpanName:     "aggregatestore.Load",
			wantMetricName:   "aggregatestore.load",
			wantToVersionTag: true,
			wantToVersionVal: 0,
		},
		{
			name: "custom namespaces are applied",
			opts: []InstrumentedStoreOption[mockEntity]{
				WithTraceNamespace[mockEntity]("custom.trace"),
				WithMetricNamespace[mockEntity]("custom.metrics"),
			},
			loadOpts:         &aggregatestore.LoadOptions{ToVersion: 2},
			innerReturnAgg:   loadedAgg,
			wantAgg:          loadedAgg,
			wantSpanName:     "custom.trace.Load",
			wantMetricName:   "custom.metrics.load",
			wantToVersionTag: true,
			wantToVersionVal: 2,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			mt.Reset()

			var gotID uuid.UUID
			var gotOpts *aggregatestore.LoadOptions
			inner := &mockAggregateStore[mockEntity]{
				LoadFn: func(_ context.Context, id uuid.UUID, opts *aggregatestore.LoadOptions) (*aggregatestore.Aggregate[mockEntity], error) {
					gotID = id
					gotOpts = opts
					return tt.innerReturnAgg, tt.innerReturnErr
				},
			}

			meter := &mockStatsd{}
			opts := append([]InstrumentedStoreOption[mockEntity]{WithMetricsClient[mockEntity](meter)}, tt.opts...)

			store, err := NewInstrumentedStore(inner, opts...)
			if err != nil {
				t.Fatalf("failed to create store: %v", err)
			}

			gotAgg, gotErr := store.Load(context.Background(), aggID, tt.loadOpts)

			if tt.wantErr != nil {
				if !errors.Is(gotErr, tt.wantErr) {
					t.Errorf("expected error %v, got %v", tt.wantErr, gotErr)
				}
			} else if gotErr != nil {
				t.Errorf("unexpected error: %v", gotErr)
			}

			if gotAgg != tt.wantAgg {
				t.Errorf("expected aggregate %v, got %v", tt.wantAgg, gotAgg)
			}

			if gotID != aggID {
				t.Errorf("expected inner to receive id %v, got %v", aggID, gotID)
			}

			if gotOpts != tt.loadOpts {
				t.Errorf("expected inner to receive opts %v, got %v", tt.loadOpts, gotOpts)
			}

			incrs := meter.incrCalls()
			if len(incrs) != 1 {
				t.Fatalf("expected 1 metric increment, got %d", len(incrs))
			}
			if incrs[0].name != tt.wantMetricName {
				t.Errorf("expected metric name %q, got %q", tt.wantMetricName, incrs[0].name)
			}

			spans := mt.FinishedSpans()
			if len(spans) != 1 {
				t.Fatalf("expected 1 finished span, got %d", len(spans))
			}
			span := spans[0]
			if span.OperationName() != tt.wantSpanName {
				t.Errorf("expected span name %q, got %q", tt.wantSpanName, span.OperationName())
			}
			if got := span.Tag("aggregate.id"); got != aggID.String() {
				t.Errorf("expected aggregate.id tag %q, got %v", aggID.String(), got)
			}
			if tt.wantToVersionTag {
				if got := span.Tag("load_options.to_version"); got != float64(tt.wantToVersionVal) {
					t.Errorf("expected load_options.to_version tag %d, got %v", tt.wantToVersionVal, got)
				}
			} else {
				if got := span.Tag("load_options.to_version"); got != nil {
					t.Errorf("expected load_options.to_version tag to be unset, got %v", got)
				}
			}
			if tt.wantErr != nil {
				if got := span.Tag(ext.ErrorMsg); got == nil {
					t.Errorf("expected error to be recorded on span")
				}
			}
		})
	}
}

func TestInstrumentedStore_Hydrate(t *testing.T) {
	mt := mocktracer.Start()
	defer mt.Stop()

	sentinelErr := errors.New("hydrate failed")

	for _, tt := range []struct {
		name             string
		opts             []InstrumentedStoreOption[mockEntity]
		hydrateOpts      *aggregatestore.HydrateOptions
		innerReturnErr   error
		wantErr          error
		wantSpanName     string
		wantMetricName   string
		wantToVersionTag bool
		wantToVersionVal int64
	}{
		{
			name:             "success with hydrate options",
			hydrateOpts:      &aggregatestore.HydrateOptions{ToVersion: 5},
			wantSpanName:     "aggregatestore.Hydrate",
			wantMetricName:   "aggregatestore.hydrate",
			wantToVersionTag: true,
			wantToVersionVal: 5,
		},
		{
			name:             "nil opts does not panic (regression)",
			hydrateOpts:      nil,
			wantSpanName:     "aggregatestore.Hydrate",
			wantMetricName:   "aggregatestore.hydrate",
			wantToVersionTag: false,
		},
		{
			name:             "inner error is propagated and span records error",
			hydrateOpts:      &aggregatestore.HydrateOptions{},
			innerReturnErr:   sentinelErr,
			wantErr:          sentinelErr,
			wantSpanName:     "aggregatestore.Hydrate",
			wantMetricName:   "aggregatestore.hydrate",
			wantToVersionTag: true,
			wantToVersionVal: 0,
		},
		{
			name: "custom namespaces are applied",
			opts: []InstrumentedStoreOption[mockEntity]{
				WithTraceNamespace[mockEntity]("custom.trace"),
				WithMetricNamespace[mockEntity]("custom.metrics"),
			},
			hydrateOpts:      &aggregatestore.HydrateOptions{ToVersion: 1},
			wantSpanName:     "custom.trace.Hydrate",
			wantMetricName:   "custom.metrics.hydrate",
			wantToVersionTag: true,
			wantToVersionVal: 1,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			mt.Reset()

			agg := newMockAggregate(t, 3)

			var gotAgg *aggregatestore.Aggregate[mockEntity]
			var gotOpts *aggregatestore.HydrateOptions
			inner := &mockAggregateStore[mockEntity]{
				HydrateFn: func(_ context.Context, a *aggregatestore.Aggregate[mockEntity], o *aggregatestore.HydrateOptions) error {
					gotAgg = a
					gotOpts = o
					return tt.innerReturnErr
				},
			}

			meter := &mockStatsd{}
			opts := append([]InstrumentedStoreOption[mockEntity]{WithMetricsClient[mockEntity](meter)}, tt.opts...)

			store, err := NewInstrumentedStore(inner, opts...)
			if err != nil {
				t.Fatalf("failed to create store: %v", err)
			}

			gotErr := store.Hydrate(context.Background(), agg, tt.hydrateOpts)

			if tt.wantErr != nil {
				if !errors.Is(gotErr, tt.wantErr) {
					t.Errorf("expected error %v, got %v", tt.wantErr, gotErr)
				}
			} else if gotErr != nil {
				t.Errorf("unexpected error: %v", gotErr)
			}

			if gotAgg != agg {
				t.Errorf("expected inner to receive aggregate %v, got %v", agg, gotAgg)
			}
			if gotOpts != tt.hydrateOpts {
				t.Errorf("expected inner to receive opts %v, got %v", tt.hydrateOpts, gotOpts)
			}

			incrs := meter.incrCalls()
			if len(incrs) != 1 {
				t.Fatalf("expected 1 metric increment, got %d", len(incrs))
			}
			if incrs[0].name != tt.wantMetricName {
				t.Errorf("expected metric name %q, got %q", tt.wantMetricName, incrs[0].name)
			}

			spans := mt.FinishedSpans()
			if len(spans) != 1 {
				t.Fatalf("expected 1 finished span, got %d", len(spans))
			}
			span := spans[0]
			if span.OperationName() != tt.wantSpanName {
				t.Errorf("expected span name %q, got %q", tt.wantSpanName, span.OperationName())
			}
			if got := span.Tag("aggregate.id"); got != agg.ID().String() {
				t.Errorf("expected aggregate.id tag %q, got %v", agg.ID().String(), got)
			}
			if got := span.Tag("aggregate.version"); got != float64(agg.Version()) {
				t.Errorf("expected aggregate.version tag %d, got %v", agg.Version(), got)
			}
			if tt.wantToVersionTag {
				if got := span.Tag("hydrate_options.to_version"); got != float64(tt.wantToVersionVal) {
					t.Errorf("expected hydrate_options.to_version tag %d, got %v", tt.wantToVersionVal, got)
				}
			} else {
				if got := span.Tag("hydrate_options.to_version"); got != nil {
					t.Errorf("expected hydrate_options.to_version tag to be unset, got %v", got)
				}
			}
			if tt.wantErr != nil {
				if got := span.Tag(ext.ErrorMsg); got == nil {
					t.Errorf("expected error to be recorded on span")
				}
			}
		})
	}
}

func TestInstrumentedStore_Save(t *testing.T) {
	mt := mocktracer.Start()
	defer mt.Stop()

	sentinelErr := errors.New("save failed")

	for _, tt := range []struct {
		name           string
		opts           []InstrumentedStoreOption[mockEntity]
		saveOpts       *aggregatestore.SaveOptions
		innerReturnErr error
		wantErr        error
		wantSpanName   string
		wantMetricName string
	}{
		{
			name:           "success",
			saveOpts:       &aggregatestore.SaveOptions{},
			wantSpanName:   "aggregatestore.Save",
			wantMetricName: "aggregatestore.save",
		},
		{
			name:           "nil opts is passed through to inner",
			saveOpts:       nil,
			wantSpanName:   "aggregatestore.Save",
			wantMetricName: "aggregatestore.save",
		},
		{
			name:           "inner error is propagated and span records error",
			saveOpts:       &aggregatestore.SaveOptions{},
			innerReturnErr: sentinelErr,
			wantErr:        sentinelErr,
			wantSpanName:   "aggregatestore.Save",
			wantMetricName: "aggregatestore.save",
		},
		{
			name: "custom namespaces are applied",
			opts: []InstrumentedStoreOption[mockEntity]{
				WithTraceNamespace[mockEntity]("custom.trace"),
				WithMetricNamespace[mockEntity]("custom.metrics"),
			},
			saveOpts:       &aggregatestore.SaveOptions{},
			wantSpanName:   "custom.trace.Save",
			wantMetricName: "custom.metrics.save",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			mt.Reset()

			agg := newMockAggregate(t, 2)

			var gotAgg *aggregatestore.Aggregate[mockEntity]
			var gotOpts *aggregatestore.SaveOptions
			inner := &mockAggregateStore[mockEntity]{
				SaveFn: func(_ context.Context, a *aggregatestore.Aggregate[mockEntity], o *aggregatestore.SaveOptions) error {
					gotAgg = a
					gotOpts = o
					return tt.innerReturnErr
				},
			}

			meter := &mockStatsd{}
			opts := append([]InstrumentedStoreOption[mockEntity]{WithMetricsClient[mockEntity](meter)}, tt.opts...)

			store, err := NewInstrumentedStore(inner, opts...)
			if err != nil {
				t.Fatalf("failed to create store: %v", err)
			}

			gotErr := store.Save(context.Background(), agg, tt.saveOpts)

			if tt.wantErr != nil {
				if !errors.Is(gotErr, tt.wantErr) {
					t.Errorf("expected error %v, got %v", tt.wantErr, gotErr)
				}
			} else if gotErr != nil {
				t.Errorf("unexpected error: %v", gotErr)
			}

			if gotAgg != agg {
				t.Errorf("expected inner to receive aggregate %v, got %v", agg, gotAgg)
			}
			if gotOpts != tt.saveOpts {
				t.Errorf("expected inner to receive opts %v, got %v", tt.saveOpts, gotOpts)
			}

			incrs := meter.incrCalls()
			if len(incrs) != 1 {
				t.Fatalf("expected 1 metric increment, got %d", len(incrs))
			}
			if incrs[0].name != tt.wantMetricName {
				t.Errorf("expected metric name %q, got %q", tt.wantMetricName, incrs[0].name)
			}

			spans := mt.FinishedSpans()
			if len(spans) != 1 {
				t.Fatalf("expected 1 finished span, got %d", len(spans))
			}
			span := spans[0]
			if span.OperationName() != tt.wantSpanName {
				t.Errorf("expected span name %q, got %q", tt.wantSpanName, span.OperationName())
			}
			if got := span.Tag("aggregate.id"); got != agg.ID().String() {
				t.Errorf("expected aggregate.id tag %q, got %v", agg.ID().String(), got)
			}
			if got := span.Tag("aggregate.version"); got != float64(agg.Version()) {
				t.Errorf("expected aggregate.version tag %d, got %v", agg.Version(), got)
			}
			if got := span.Tag("aggregate.unsaved_events"); got != float64(0) {
				t.Errorf("expected aggregate.unsaved_events tag 0, got %v", got)
			}
			if tt.wantErr != nil {
				if got := span.Tag(ext.ErrorMsg); got == nil {
					t.Errorf("expected error to be recorded on span")
				}
			}
		})
	}
}
