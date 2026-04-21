package aggregatestore

import (
	"context"
	"errors"
	"testing"

	"github.com/go-estoria/estoria"
	"github.com/go-estoria/estoria/aggregatestore"
	"github.com/go-estoria/estoria/typeid"
	"github.com/gofrs/uuid/v5"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	noopmetric "go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	nooptrace "go.opentelemetry.io/otel/trace/noop"
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

// testProviders builds an OTel tracer provider (with an in-memory span recorder)
// and a meter provider (with a manual reader) for use in unit tests, along with
// the store options that wire them into the instrumented store.
func testProviders(t *testing.T) (*tracetest.SpanRecorder, *sdkmetric.ManualReader, []InstrumentedStoreOption[mockEntity]) {
	t.Helper()
	recorder := tracetest.NewSpanRecorder()
	tracerProvider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	reader := sdkmetric.NewManualReader()
	meterProvider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	return recorder, reader, []InstrumentedStoreOption[mockEntity]{
		WithTracerProvider[mockEntity](tracerProvider),
		WithMeterProvider[mockEntity](meterProvider),
	}
}

// counterValue returns the cumulative value of the named int64 counter from
// the reader, or -1 if no such counter was recorded.
func counterValue(t *testing.T, reader *sdkmetric.ManualReader, name string) int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collecting metrics: %v", err)
	}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("metric %q has unexpected data type %T", name, m.Data)
			}
			var total int64
			for _, dp := range sum.DataPoints {
				total += dp.Value
			}
			return total
		}
	}
	return -1
}

// findAttribute returns the attribute with the given key from a slice, or
// the zero-value KeyValue and false if not found.
func findAttribute(attrs []attribute.KeyValue, key string) (attribute.KeyValue, bool) {
	for _, kv := range attrs {
		if string(kv.Key) == key {
			return kv, true
		}
	}
	return attribute.KeyValue{}, false
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
			name:    "creates store with defaults",
			opts:    nil,
			wantErr: false,
		},
		{
			name:    "creates store with tracing disabled",
			opts:    []InstrumentedStoreOption[mockEntity]{WithTracingEnabled[mockEntity](false)},
			wantErr: false,
		},
		{
			name:    "creates store with metrics disabled",
			opts:    []InstrumentedStoreOption[mockEntity]{WithMetricsEnabled[mockEntity](false)},
			wantErr: false,
		},
		{
			name: "creates store with noop providers",
			opts: []InstrumentedStoreOption[mockEntity]{
				WithTracerProvider[mockEntity](nooptrace.NewTracerProvider()),
				WithMeterProvider[mockEntity](noopmetric.NewMeterProvider()),
			},
			wantErr: false,
		},
		{
			name: "creates store with custom namespaces",
			opts: []InstrumentedStoreOption[mockEntity]{
				WithMetricNamespace[mockEntity]("custom.metrics"),
				WithTraceNamespace[mockEntity]("custom.trace"),
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

			if store.tracer == nil {
				t.Errorf("expected non-nil tracer")
			}

			if store.meter == nil {
				t.Errorf("expected non-nil meter")
			}

			if store.loadCounter == nil || store.hydrateCounter == nil || store.saveCounter == nil {
				t.Errorf("expected all counters to be initialized")
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
		wantTracingEnabled  bool
		wantMetricsEnabled  bool
		wantMetricNamespace string
		wantTraceNamespace  string
	}{
		{
			name:                "defaults",
			opts:                nil,
			wantTracingEnabled:  true,
			wantMetricsEnabled:  true,
			wantMetricNamespace: "aggregatestore",
			wantTraceNamespace:  "aggregatestore",
		},
		{
			name:                "WithTracingEnabled(false)",
			opts:                []InstrumentedStoreOption[mockEntity]{WithTracingEnabled[mockEntity](false)},
			wantTracingEnabled:  false,
			wantMetricsEnabled:  true,
			wantMetricNamespace: "aggregatestore",
			wantTraceNamespace:  "aggregatestore",
		},
		{
			name:                "WithMetricsEnabled(false)",
			opts:                []InstrumentedStoreOption[mockEntity]{WithMetricsEnabled[mockEntity](false)},
			wantTracingEnabled:  true,
			wantMetricsEnabled:  false,
			wantMetricNamespace: "aggregatestore",
			wantTraceNamespace:  "aggregatestore",
		},
		{
			name:                "WithMetricNamespace",
			opts:                []InstrumentedStoreOption[mockEntity]{WithMetricNamespace[mockEntity]("custom.metrics")},
			wantTracingEnabled:  true,
			wantMetricsEnabled:  true,
			wantMetricNamespace: "custom.metrics",
			wantTraceNamespace:  "aggregatestore",
		},
		{
			name:                "WithTraceNamespace",
			opts:                []InstrumentedStoreOption[mockEntity]{WithTraceNamespace[mockEntity]("custom.trace")},
			wantTracingEnabled:  true,
			wantMetricsEnabled:  true,
			wantMetricNamespace: "aggregatestore",
			wantTraceNamespace:  "custom.trace",
		},
		{
			name: "all options combined",
			opts: []InstrumentedStoreOption[mockEntity]{
				WithTracingEnabled[mockEntity](false),
				WithMetricsEnabled[mockEntity](false),
				WithMetricNamespace[mockEntity]("my.metrics"),
				WithTraceNamespace[mockEntity]("my.traces"),
			},
			wantTracingEnabled:  false,
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

			if store.tracingEnabled != tt.wantTracingEnabled {
				t.Errorf("expected tracingEnabled %v, got %v", tt.wantTracingEnabled, store.tracingEnabled)
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
		name     string
		innerAgg *aggregatestore.Aggregate[mockEntity]
	}{
		{
			name:     "delegates to inner and returns the aggregate",
			innerAgg: aggregatestore.NewAggregate(mockEntity{id: typeid.NewV4("mockentity")}, 0),
		},
		{
			name:     "returns nil when inner returns nil",
			innerAgg: nil,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var gotID uuid.UUID
			inner := &mockAggregateStore[mockEntity]{
				NewFn: func(id uuid.UUID) *aggregatestore.Aggregate[mockEntity] {
					gotID = id
					return tt.innerAgg
				},
			}

			_, _, providerOpts := testProviders(t)
			store, err := NewInstrumentedStore(inner, providerOpts...)
			if err != nil {
				t.Fatalf("failed to create store: %v", err)
			}

			callID, err := uuid.NewV4()
			if err != nil {
				t.Fatalf("generating uuid: %v", err)
			}

			got := store.New(callID)

			if gotID != callID {
				t.Errorf("expected inner New to receive id %v, got %v", callID, gotID)
			}

			if got != tt.innerAgg {
				t.Errorf("expected inner aggregate to be returned, got %v", got)
			}
		})
	}
}

func TestInstrumentedStore_Load(t *testing.T) {
	t.Parallel()

	sentinelErr := errors.New("load failed")

	for _, tt := range []struct {
		name              string
		extraOpts         []InstrumentedStoreOption[mockEntity]
		loadOpts          *aggregatestore.LoadOptions
		innerReturnErr    error
		wantErr           error
		wantSpanName      string
		wantMetricName    string
		wantStatus        codes.Code
		wantToVersionAttr bool
		wantToVersionVal  int64
	}{
		{
			name:              "success with load options",
			loadOpts:          &aggregatestore.LoadOptions{ToVersion: 7},
			wantSpanName:      "aggregatestore.Load",
			wantMetricName:    "aggregatestore.load",
			wantStatus:        codes.Unset,
			wantToVersionAttr: true,
			wantToVersionVal:  7,
		},
		{
			name:              "nil opts does not panic (regression)",
			loadOpts:          nil,
			wantSpanName:      "aggregatestore.Load",
			wantMetricName:    "aggregatestore.load",
			wantStatus:        codes.Unset,
			wantToVersionAttr: false,
		},
		{
			name:              "inner error is propagated and span records error",
			loadOpts:          &aggregatestore.LoadOptions{},
			innerReturnErr:    sentinelErr,
			wantErr:           sentinelErr,
			wantSpanName:      "aggregatestore.Load",
			wantMetricName:    "aggregatestore.load",
			wantStatus:        codes.Error,
			wantToVersionAttr: true,
			wantToVersionVal:  0,
		},
		{
			name: "custom namespaces are applied",
			extraOpts: []InstrumentedStoreOption[mockEntity]{
				WithTraceNamespace[mockEntity]("custom.trace"),
				WithMetricNamespace[mockEntity]("custom.metrics"),
			},
			loadOpts:          &aggregatestore.LoadOptions{ToVersion: 2},
			wantSpanName:      "custom.trace.Load",
			wantMetricName:    "custom.metrics.load",
			wantStatus:        codes.Unset,
			wantToVersionAttr: true,
			wantToVersionVal:  2,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			aggUUID, err := uuid.NewV4()
			if err != nil {
				t.Fatalf("generating uuid: %v", err)
			}

			var gotID uuid.UUID
			var gotOpts *aggregatestore.LoadOptions
			inner := &mockAggregateStore[mockEntity]{
				LoadFn: func(_ context.Context, id uuid.UUID, opts *aggregatestore.LoadOptions) (*aggregatestore.Aggregate[mockEntity], error) {
					gotID = id
					gotOpts = opts
					if tt.innerReturnErr != nil {
						return nil, tt.innerReturnErr
					}
					return aggregatestore.NewAggregate(mockEntity{id: typeid.New("mockentity", id)}, 1), nil
				},
			}

			recorder, reader, providerOpts := testProviders(t)
			opts := append(providerOpts, tt.extraOpts...)

			store, err := NewInstrumentedStore(inner, opts...)
			if err != nil {
				t.Fatalf("failed to create store: %v", err)
			}

			_, gotErr := store.Load(context.Background(), aggUUID, tt.loadOpts)

			if tt.wantErr != nil {
				if !errors.Is(gotErr, tt.wantErr) {
					t.Errorf("expected error %v, got %v", tt.wantErr, gotErr)
				}
			} else if gotErr != nil {
				t.Errorf("unexpected error: %v", gotErr)
			}

			if gotID != aggUUID {
				t.Errorf("expected inner to receive id %v, got %v", aggUUID, gotID)
			}
			if gotOpts != tt.loadOpts {
				t.Errorf("expected inner to receive opts %v, got %v", tt.loadOpts, gotOpts)
			}

			if got := counterValue(t, reader, tt.wantMetricName); got != 1 {
				t.Errorf("expected metric %q value 1, got %d", tt.wantMetricName, got)
			}

			spans := recorder.Ended()
			if len(spans) != 1 {
				t.Fatalf("expected 1 ended span, got %d", len(spans))
			}
			span := spans[0]
			if span.Name() != tt.wantSpanName {
				t.Errorf("expected span name %q, got %q", tt.wantSpanName, span.Name())
			}
			if span.Status().Code != tt.wantStatus {
				t.Errorf("expected span status %v, got %v", tt.wantStatus, span.Status().Code)
			}

			attrs := span.Attributes()
			if kv, ok := findAttribute(attrs, "aggregate.uuid"); !ok {
				t.Errorf("expected aggregate.uuid attribute")
			} else if kv.Value.AsString() != aggUUID.String() {
				t.Errorf("expected aggregate.uuid %q, got %q", aggUUID.String(), kv.Value.AsString())
			}

			kv, ok := findAttribute(attrs, "load_options.to_version")
			if tt.wantToVersionAttr {
				if !ok {
					t.Errorf("expected load_options.to_version attribute")
				} else if kv.Value.AsInt64() != tt.wantToVersionVal {
					t.Errorf("expected load_options.to_version %d, got %d", tt.wantToVersionVal, kv.Value.AsInt64())
				}
			} else if ok {
				t.Errorf("expected load_options.to_version attribute to be unset, got %v", kv.Value.AsInt64())
			}

			if tt.wantErr != nil {
				if len(span.Events()) == 0 {
					t.Errorf("expected error event recorded on span")
				}
			}
		})
	}
}

func TestInstrumentedStore_Hydrate(t *testing.T) {
	t.Parallel()

	sentinelErr := errors.New("hydrate failed")

	for _, tt := range []struct {
		name              string
		extraOpts         []InstrumentedStoreOption[mockEntity]
		hydrateOpts       *aggregatestore.HydrateOptions
		innerReturnErr    error
		wantErr           error
		wantSpanName      string
		wantMetricName    string
		wantStatus        codes.Code
		wantToVersionAttr bool
		wantToVersionVal  int64
	}{
		{
			name:              "success with hydrate options",
			hydrateOpts:       &aggregatestore.HydrateOptions{ToVersion: 5},
			wantSpanName:      "aggregatestore.Hydrate",
			wantMetricName:    "aggregatestore.hydrate",
			wantStatus:        codes.Unset,
			wantToVersionAttr: true,
			wantToVersionVal:  5,
		},
		{
			name:              "nil opts does not panic (regression)",
			hydrateOpts:       nil,
			wantSpanName:      "aggregatestore.Hydrate",
			wantMetricName:    "aggregatestore.hydrate",
			wantStatus:        codes.Unset,
			wantToVersionAttr: false,
		},
		{
			name:              "inner error is propagated and span records error",
			hydrateOpts:       &aggregatestore.HydrateOptions{},
			innerReturnErr:    sentinelErr,
			wantErr:           sentinelErr,
			wantSpanName:      "aggregatestore.Hydrate",
			wantMetricName:    "aggregatestore.hydrate",
			wantStatus:        codes.Error,
			wantToVersionAttr: true,
			wantToVersionVal:  0,
		},
		{
			name: "custom namespaces are applied",
			extraOpts: []InstrumentedStoreOption[mockEntity]{
				WithTraceNamespace[mockEntity]("custom.trace"),
				WithMetricNamespace[mockEntity]("custom.metrics"),
			},
			hydrateOpts:       &aggregatestore.HydrateOptions{ToVersion: 1},
			wantSpanName:      "custom.trace.Hydrate",
			wantMetricName:    "custom.metrics.hydrate",
			wantStatus:        codes.Unset,
			wantToVersionAttr: true,
			wantToVersionVal:  1,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

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

			recorder, reader, providerOpts := testProviders(t)
			opts := append(providerOpts, tt.extraOpts...)

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

			if got := counterValue(t, reader, tt.wantMetricName); got != 1 {
				t.Errorf("expected metric %q value 1, got %d", tt.wantMetricName, got)
			}

			spans := recorder.Ended()
			if len(spans) != 1 {
				t.Fatalf("expected 1 ended span, got %d", len(spans))
			}
			span := spans[0]
			if span.Name() != tt.wantSpanName {
				t.Errorf("expected span name %q, got %q", tt.wantSpanName, span.Name())
			}
			if span.Status().Code != tt.wantStatus {
				t.Errorf("expected span status %v, got %v", tt.wantStatus, span.Status().Code)
			}

			attrs := span.Attributes()
			if kv, ok := findAttribute(attrs, "aggregate.id"); !ok {
				t.Errorf("expected aggregate.id attribute")
			} else if kv.Value.AsString() != agg.ID().String() {
				t.Errorf("expected aggregate.id %q, got %q", agg.ID().String(), kv.Value.AsString())
			}

			if kv, ok := findAttribute(attrs, "aggregate.version"); !ok {
				t.Errorf("expected aggregate.version attribute")
			} else if kv.Value.AsInt64() != agg.Version() {
				t.Errorf("expected aggregate.version %d, got %d", agg.Version(), kv.Value.AsInt64())
			}

			kv, ok := findAttribute(attrs, "hydrate_options.to_version")
			if tt.wantToVersionAttr {
				if !ok {
					t.Errorf("expected hydrate_options.to_version attribute")
				} else if kv.Value.AsInt64() != tt.wantToVersionVal {
					t.Errorf("expected hydrate_options.to_version %d, got %d", tt.wantToVersionVal, kv.Value.AsInt64())
				}
			} else if ok {
				t.Errorf("expected hydrate_options.to_version attribute to be unset, got %v", kv.Value.AsInt64())
			}

			if tt.wantErr != nil {
				if len(span.Events()) == 0 {
					t.Errorf("expected error event recorded on span")
				}
			}
		})
	}
}

func TestInstrumentedStore_Save(t *testing.T) {
	t.Parallel()

	sentinelErr := errors.New("save failed")

	for _, tt := range []struct {
		name           string
		extraOpts      []InstrumentedStoreOption[mockEntity]
		saveOpts       *aggregatestore.SaveOptions
		innerReturnErr error
		wantErr        error
		wantSpanName   string
		wantMetricName string
		wantStatus     codes.Code
	}{
		{
			name:           "success",
			saveOpts:       &aggregatestore.SaveOptions{},
			wantSpanName:   "aggregatestore.Save",
			wantMetricName: "aggregatestore.save",
			wantStatus:     codes.Unset,
		},
		{
			name:           "nil opts is passed through to inner",
			saveOpts:       nil,
			wantSpanName:   "aggregatestore.Save",
			wantMetricName: "aggregatestore.save",
			wantStatus:     codes.Unset,
		},
		{
			name:           "inner error is propagated and span records error",
			saveOpts:       &aggregatestore.SaveOptions{},
			innerReturnErr: sentinelErr,
			wantErr:        sentinelErr,
			wantSpanName:   "aggregatestore.Save",
			wantMetricName: "aggregatestore.save",
			wantStatus:     codes.Error,
		},
		{
			name: "custom namespaces are applied",
			extraOpts: []InstrumentedStoreOption[mockEntity]{
				WithTraceNamespace[mockEntity]("custom.trace"),
				WithMetricNamespace[mockEntity]("custom.metrics"),
			},
			saveOpts:       &aggregatestore.SaveOptions{},
			wantSpanName:   "custom.trace.Save",
			wantMetricName: "custom.metrics.save",
			wantStatus:     codes.Unset,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

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

			recorder, reader, providerOpts := testProviders(t)
			opts := append(providerOpts, tt.extraOpts...)

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

			if got := counterValue(t, reader, tt.wantMetricName); got != 1 {
				t.Errorf("expected metric %q value 1, got %d", tt.wantMetricName, got)
			}

			spans := recorder.Ended()
			if len(spans) != 1 {
				t.Fatalf("expected 1 ended span, got %d", len(spans))
			}
			span := spans[0]
			if span.Name() != tt.wantSpanName {
				t.Errorf("expected span name %q, got %q", tt.wantSpanName, span.Name())
			}
			if span.Status().Code != tt.wantStatus {
				t.Errorf("expected span status %v, got %v", tt.wantStatus, span.Status().Code)
			}

			attrs := span.Attributes()
			if kv, ok := findAttribute(attrs, "aggregate.id"); !ok {
				t.Errorf("expected aggregate.id attribute")
			} else if kv.Value.AsString() != agg.ID().String() {
				t.Errorf("expected aggregate.id %q, got %q", agg.ID().String(), kv.Value.AsString())
			}

			if kv, ok := findAttribute(attrs, "aggregate.version"); !ok {
				t.Errorf("expected aggregate.version attribute")
			} else if kv.Value.AsInt64() != agg.Version() {
				t.Errorf("expected aggregate.version %d, got %d", agg.Version(), kv.Value.AsInt64())
			}

			if kv, ok := findAttribute(attrs, "aggregate.unsaved_events"); !ok {
				t.Errorf("expected aggregate.unsaved_events attribute")
			} else if kv.Value.AsInt64() != 0 {
				t.Errorf("expected aggregate.unsaved_events 0, got %d", kv.Value.AsInt64())
			}

			if tt.wantErr != nil {
				if len(span.Events()) == 0 {
					t.Errorf("expected error event recorded on span")
				}
			}
		})
	}
}
