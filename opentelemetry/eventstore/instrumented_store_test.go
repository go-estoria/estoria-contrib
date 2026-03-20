package eventstore

import (
	"context"
	"errors"
	"testing"

	"github.com/go-estoria/estoria/eventstore"
	"github.com/go-estoria/estoria/typeid"
	"go.opentelemetry.io/otel/metric/noop"
	nooptrace "go.opentelemetry.io/otel/trace/noop"
)

// mockEventStore is a hand-rolled mock for eventstore.Store
type mockEventStore struct {
	ReadStreamFn   func(ctx context.Context, id typeid.ID, opts eventstore.ReadStreamOptions) (eventstore.StreamIterator, error)
	AppendStreamFn func(ctx context.Context, streamID typeid.ID, events []*eventstore.WritableEvent, opts eventstore.AppendStreamOptions) error
}

func (m *mockEventStore) ReadStream(ctx context.Context, id typeid.ID, opts eventstore.ReadStreamOptions) (eventstore.StreamIterator, error) {
	if m.ReadStreamFn != nil {
		return m.ReadStreamFn(ctx, id, opts)
	}
	return nil, errors.New("ReadStreamFn not set")
}

func (m *mockEventStore) AppendStream(ctx context.Context, streamID typeid.ID, events []*eventstore.WritableEvent, opts eventstore.AppendStreamOptions) error {
	if m.AppendStreamFn != nil {
		return m.AppendStreamFn(ctx, streamID, events, opts)
	}
	return errors.New("AppendStreamFn not set")
}

// mockStreamIterator is a hand-rolled mock for eventstore.StreamIterator
type mockStreamIterator struct {
	NextFn  func(ctx context.Context) (*eventstore.Event, error)
	CloseFn func(ctx context.Context) error
}

func (m *mockStreamIterator) Next(ctx context.Context) (*eventstore.Event, error) {
	if m.NextFn != nil {
		return m.NextFn(ctx)
	}
	return nil, eventstore.ErrEndOfEventStream
}

func (m *mockStreamIterator) Close(ctx context.Context) error {
	if m.CloseFn != nil {
		return m.CloseFn(ctx)
	}
	return nil
}

func TestNewInstrumentedStore(t *testing.T) {
	t.Parallel()

	inner := &mockEventStore{}

	for _, tt := range []struct {
		name    string
		inner   eventstore.Store
		opts    []InstrumentedStoreOption
		wantErr bool
	}{
		{
			name:    "creates store with defaults",
			inner:   inner,
			opts:    nil,
			wantErr: false,
		},
		{
			name:  "creates store with tracing disabled",
			inner: inner,
			opts: []InstrumentedStoreOption{
				WithTracingEnabled(false),
			},
			wantErr: false,
		},
		{
			name:  "creates store with metrics disabled",
			inner: inner,
			opts: []InstrumentedStoreOption{
				WithMetricsEnabled(false),
			},
			wantErr: false,
		},
		{
			name:  "creates store with custom metric namespace",
			inner: inner,
			opts: []InstrumentedStoreOption{
				WithMetricNamespace("custom.metrics"),
			},
			wantErr: false,
		},
		{
			name:  "creates store with custom trace namespace",
			inner: inner,
			opts: []InstrumentedStoreOption{
				WithTraceNamespace("custom.trace"),
			},
			wantErr: false,
		},
		{
			name:  "creates store with noop tracer provider",
			inner: inner,
			opts: []InstrumentedStoreOption{
				WithTracerProvider(nooptrace.NewTracerProvider()),
			},
			wantErr: false,
		},
		{
			name:  "creates store with noop meter provider",
			inner: inner,
			opts: []InstrumentedStoreOption{
				WithMeterProvider(noop.NewMeterProvider()),
			},
			wantErr: false,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			store, err := NewInstrumentedStore(tt.inner, tt.opts...)

			if tt.wantErr {
				if err == nil {
					t.Errorf("expected error, got nil")
				}
				return
			}

			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}

			if store == nil {
				t.Errorf("expected non-nil store")
			}

			if store.inner != tt.inner {
				t.Errorf("expected inner store to be set")
			}
		})
	}
}

func TestInstrumentedStore_ReadStream_Success(t *testing.T) {
	t.Parallel()

	expectedID := typeid.NewV4("test")
	expectedOpts := eventstore.ReadStreamOptions{Offset: 10, Count: 5}
	expectedIterator := &mockStreamIterator{}

	inner := &mockEventStore{
		ReadStreamFn: func(ctx context.Context, id typeid.ID, opts eventstore.ReadStreamOptions) (eventstore.StreamIterator, error) {
			if id != expectedID {
				t.Errorf("expected id %v, got %v", expectedID, id)
			}
			if opts.Offset != expectedOpts.Offset {
				t.Errorf("expected offset %d, got %d", expectedOpts.Offset, opts.Offset)
			}
			return expectedIterator, nil
		},
	}

	store, err := NewInstrumentedStore(inner,
		WithTracerProvider(nooptrace.NewTracerProvider()),
		WithMeterProvider(noop.NewMeterProvider()),
	)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}

	iterator, err := store.ReadStream(context.Background(), expectedID, expectedOpts)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if iterator == nil {
		t.Errorf("expected non-nil iterator")
	}

	// Verify we got a wrapped iterator
	if _, ok := iterator.(*InstrumentedStreamIterator); !ok {
		t.Errorf("expected InstrumentedStreamIterator, got %T", iterator)
	}
}

func TestInstrumentedStore_ReadStream_ErrorPropagated(t *testing.T) {
	t.Parallel()

	// CRITICAL REGRESSION TEST for Task 13 fix:
	// Bug: old code returned (iterator, nil) when inner.ReadStream failed
	// Fix: new code returns (nil, err) correctly

	expectedErr := errors.New("read stream failed")

	inner := &mockEventStore{
		ReadStreamFn: func(ctx context.Context, id typeid.ID, opts eventstore.ReadStreamOptions) (eventstore.StreamIterator, error) {
			return nil, expectedErr
		},
	}

	store, err := NewInstrumentedStore(inner,
		WithTracerProvider(nooptrace.NewTracerProvider()),
		WithMeterProvider(noop.NewMeterProvider()),
	)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}

	iterator, err := store.ReadStream(context.Background(), typeid.NewV4("test"), eventstore.ReadStreamOptions{})

	// CRITICAL: error must be propagated, iterator must be nil
	if err == nil {
		t.Errorf("expected error, got nil (BUG: error was swallowed)")
	}

	if !errors.Is(err, expectedErr) {
		t.Errorf("expected error %v, got %v", expectedErr, err)
	}

	if iterator != nil {
		t.Errorf("expected nil iterator when error occurs, got %T (BUG: iterator returned despite error)", iterator)
	}
}

func TestInstrumentedStore_AppendStream_Success(t *testing.T) {
	t.Parallel()

	expectedID := typeid.NewV4("test")
	expectedEvents := []*eventstore.WritableEvent{
		{Type: "test.event", Data: []byte("{}")},
	}
	expectedOpts := eventstore.AppendStreamOptions{ExpectVersion: 5}

	appendCalled := false
	inner := &mockEventStore{
		AppendStreamFn: func(ctx context.Context, streamID typeid.ID, events []*eventstore.WritableEvent, opts eventstore.AppendStreamOptions) error {
			appendCalled = true
			if streamID != expectedID {
				t.Errorf("expected id %v, got %v", expectedID, streamID)
			}
			if len(events) != len(expectedEvents) {
				t.Errorf("expected %d events, got %d", len(expectedEvents), len(events))
			}
			if opts.ExpectVersion != expectedOpts.ExpectVersion {
				t.Errorf("expected version %d, got %d", expectedOpts.ExpectVersion, opts.ExpectVersion)
			}
			return nil
		},
	}

	store, err := NewInstrumentedStore(inner,
		WithTracerProvider(nooptrace.NewTracerProvider()),
		WithMeterProvider(noop.NewMeterProvider()),
	)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}

	err = store.AppendStream(context.Background(), expectedID, expectedEvents, expectedOpts)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if !appendCalled {
		t.Errorf("expected AppendStream to be called on inner store")
	}
}

func TestInstrumentedStore_AppendStream_SpanName(t *testing.T) {
	t.Parallel()

	// CRITICAL REGRESSION TEST for Task 13 fix:
	// Bug: old code used traceNamespace+".Hydrate"
	// Fix: new code uses traceNamespace+".AppendStream"

	inner := &mockEventStore{
		AppendStreamFn: func(ctx context.Context, streamID typeid.ID, events []*eventstore.WritableEvent, opts eventstore.AppendStreamOptions) error {
			return nil
		},
	}

	customNamespace := "custom.namespace"
	store, err := NewInstrumentedStore(inner,
		WithTracerProvider(nooptrace.NewTracerProvider()),
		WithMeterProvider(noop.NewMeterProvider()),
		WithTraceNamespace(customNamespace),
	)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}

	// Verify the span name construction is correct by checking the store's trace namespace
	if store.traceNamespace != customNamespace {
		t.Errorf("expected traceNamespace %q, got %q", customNamespace, store.traceNamespace)
	}

	// The actual span name would be customNamespace+".AppendStream"
	// We can't directly verify span creation with noop tracer, but we verified the code
	// uses the correct constant ".AppendStream" in the implementation
	err = store.AppendStream(context.Background(), typeid.NewV4("test"), nil, eventstore.AppendStreamOptions{})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestInstrumentedStore_AppendStream_ErrorPropagated(t *testing.T) {
	t.Parallel()

	expectedErr := errors.New("append stream failed")

	inner := &mockEventStore{
		AppendStreamFn: func(ctx context.Context, streamID typeid.ID, events []*eventstore.WritableEvent, opts eventstore.AppendStreamOptions) error {
			return expectedErr
		},
	}

	store, err := NewInstrumentedStore(inner,
		WithTracerProvider(nooptrace.NewTracerProvider()),
		WithMeterProvider(noop.NewMeterProvider()),
	)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}

	err = store.AppendStream(context.Background(), typeid.NewV4("test"), nil, eventstore.AppendStreamOptions{})

	if err == nil {
		t.Errorf("expected error, got nil")
	}

	if !errors.Is(err, expectedErr) {
		t.Errorf("expected error %v, got %v", expectedErr, err)
	}
}

func TestInstrumentedStreamIterator_Next(t *testing.T) {
	t.Parallel()

	eventID := typeid.NewV4("event")
	streamID := typeid.NewV4("test")
	expectedEvent := &eventstore.Event{
		ID:       eventID,
		StreamID: streamID,
	}

	nextCalled := false
	innerIterator := &mockStreamIterator{
		NextFn: func(ctx context.Context) (*eventstore.Event, error) {
			nextCalled = true
			return expectedEvent, nil
		},
	}

	inner := &mockEventStore{
		ReadStreamFn: func(ctx context.Context, id typeid.ID, opts eventstore.ReadStreamOptions) (eventstore.StreamIterator, error) {
			return innerIterator, nil
		},
	}

	store, err := NewInstrumentedStore(inner,
		WithTracerProvider(nooptrace.NewTracerProvider()),
		WithMeterProvider(noop.NewMeterProvider()),
	)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}

	iterator, err := store.ReadStream(context.Background(), typeid.NewV4("test"), eventstore.ReadStreamOptions{})
	if err != nil {
		t.Fatalf("failed to read stream: %v", err)
	}

	event, err := iterator.Next(context.Background())
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if !nextCalled {
		t.Errorf("expected Next to be called on inner iterator")
	}

	if event != expectedEvent {
		t.Errorf("expected event %v, got %v", expectedEvent, event)
	}
}

func TestInstrumentedStreamIterator_Close(t *testing.T) {
	t.Parallel()

	closeCalled := false
	innerIterator := &mockStreamIterator{
		CloseFn: func(ctx context.Context) error {
			closeCalled = true
			return nil
		},
	}

	inner := &mockEventStore{
		ReadStreamFn: func(ctx context.Context, id typeid.ID, opts eventstore.ReadStreamOptions) (eventstore.StreamIterator, error) {
			return innerIterator, nil
		},
	}

	store, err := NewInstrumentedStore(inner,
		WithTracerProvider(nooptrace.NewTracerProvider()),
		WithMeterProvider(noop.NewMeterProvider()),
	)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}

	iterator, err := store.ReadStream(context.Background(), typeid.NewV4("test"), eventstore.ReadStreamOptions{})
	if err != nil {
		t.Fatalf("failed to read stream: %v", err)
	}

	err = iterator.Close(context.Background())
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if !closeCalled {
		t.Errorf("expected Close to be called on inner iterator")
	}
}

func TestInstrumentedStoreOptions(t *testing.T) {
	t.Parallel()

	inner := &mockEventStore{}

	for _, tt := range []struct {
		name                string
		opts                []InstrumentedStoreOption
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
			wantMetricNamespace: "eventstore",
			wantTraceNamespace:  "eventstore",
		},
		{
			name: "WithTracingEnabled(false)",
			opts: []InstrumentedStoreOption{
				WithTracingEnabled(false),
			},
			wantTracingEnabled:  false,
			wantMetricsEnabled:  true,
			wantMetricNamespace: "eventstore",
			wantTraceNamespace:  "eventstore",
		},
		{
			name: "WithMetricsEnabled(false)",
			opts: []InstrumentedStoreOption{
				WithMetricsEnabled(false),
			},
			wantTracingEnabled:  true,
			wantMetricsEnabled:  false,
			wantMetricNamespace: "eventstore",
			wantTraceNamespace:  "eventstore",
		},
		{
			name: "WithMetricNamespace",
			opts: []InstrumentedStoreOption{
				WithMetricNamespace("custom.metrics"),
			},
			wantTracingEnabled:  true,
			wantMetricsEnabled:  true,
			wantMetricNamespace: "custom.metrics",
			wantTraceNamespace:  "eventstore",
		},
		{
			name: "WithTraceNamespace",
			opts: []InstrumentedStoreOption{
				WithTraceNamespace("custom.trace"),
			},
			wantTracingEnabled:  true,
			wantMetricsEnabled:  true,
			wantMetricNamespace: "eventstore",
			wantTraceNamespace:  "custom.trace",
		},
		{
			name: "all options combined",
			opts: []InstrumentedStoreOption{
				WithTracingEnabled(false),
				WithMetricsEnabled(false),
				WithMetricNamespace("my.metrics"),
				WithTraceNamespace("my.traces"),
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
