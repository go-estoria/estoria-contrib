package snapshotstore

import (
	"context"
	"errors"
	"fmt"

	"github.com/go-estoria/estoria/snapshotstore"
	"github.com/go-estoria/estoria/typeid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	noopmetric "go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/trace"
	nooptrace "go.opentelemetry.io/otel/trace/noop"
)

const (
	scope = "github.com/go-estoria/estoria-contrib/opentelemetry/snapshotstore"
)

// An InstrumentedStore wraps an event store for OpenTelemetry instrumentation.
//
// The store wraps and emits metrics and traces for the ReadStream and AppendStream methods.
// The metrics are emitted using the OpenTelemetry metric API, and the traces are
// emitted using the OpenTelemetry trace API.
//
// The store can be configured to enable or disable tracing and metrics, and
// to use a custom tracer or meter provider. By default, the store uses the
// global tracer and meter provider from the OpenTelemetry SDK.
//
// The store emits metrics under the "snapshotstore" namespace by default. The
// namespace can be customized using the WithMetricNamespace option.
type InstrumentedStore struct {
	inner          snapshotstore.SnapshotStore
	tracingEnabled bool
	tracer         trace.Tracer
	metricsEnabled bool
	meter          metric.Meter

	metricNamespace string
	traceNamespace  string

	readStreamCounter   metric.Int64Counter
	appendStreamCounter metric.Int64Counter
}

// NewInstrumentedStore creates a new instrumented event store.
func NewInstrumentedStore(inner snapshotstore.SnapshotStore, opts ...InstrumentedStoreOption) (*InstrumentedStore, error) {
	store := &InstrumentedStore{
		inner:           inner,
		tracingEnabled:  true,
		metricsEnabled:  true,
		metricNamespace: "snapshotstore",
		traceNamespace:  "snapshotstore",
	}

	for _, opt := range opts {
		if err := opt(store); err != nil {
			return nil, fmt.Errorf("applying option: %w", err)
		}
	}

	if store.tracer == nil {
		if store.tracingEnabled {
			store.tracer = otel.GetTracerProvider().Tracer(scope)
		} else {
			store.tracer = nooptrace.NewTracerProvider().Tracer(scope)
		}
	}

	if store.meter == nil {
		if store.metricsEnabled {
			store.meter = otel.GetMeterProvider().Meter(scope)
		} else {
			store.meter = noopmetric.NewMeterProvider().Meter(scope)
		}
	}

	if err := store.initializeMetrics(); err != nil {
		return nil, fmt.Errorf("initializing metrics: %w", err)
	}

	return store, nil
}

var _ snapshotstore.SnapshotStore = (*InstrumentedStore)(nil)

// Load loads an aggregate by ID while capturing telemetry.
func (s *InstrumentedStore) ReadSnapshot(ctx context.Context, aggregateID typeid.ID, opts snapshotstore.ReadSnapshotOptions) (_ *snapshotstore.AggregateSnapshot, e error) {
	ctx, span := s.tracer.Start(ctx, s.traceNamespace+".ReadSnapshot", trace.WithAttributes(
		attribute.String("aggregate.id", aggregateID.String()),
		attribute.Int64("options.max_version", opts.MaxVersion),
	))

	defer func() {
		span.RecordError(e)
		if e != nil && !errors.Is(e, snapshotstore.ErrSnapshotNotFound) {
			span.SetStatus(codes.Error, "error reading snapshot")
		}

		s.readStreamCounter.Add(ctx, 1)
		span.End()
	}()

	return s.inner.ReadSnapshot(ctx, aggregateID, opts)
}

// Hydrate hydrates an aggregate while capturing telemetry.
func (s *InstrumentedStore) WriteSnapshot(ctx context.Context, snap *snapshotstore.AggregateSnapshot) (e error) {
	ctx, span := s.tracer.Start(ctx, s.traceNamespace+".WriteSnapshot", trace.WithAttributes(
		attribute.String("snapshot.aggregate.id", snap.AggregateID.String()),
		attribute.Int64("snapshot.aggregate.version", snap.AggregateVersion),
		attribute.Int64("snapshot.data.length", int64(len(snap.Data))),
		attribute.Int64("snapshot.timestamp.nano", snap.Timestamp.UnixNano()),
	))
	defer func() {
		span.RecordError(e)
		if e != nil {
			span.SetStatus(codes.Error, "error writing snapshot")
		}

		s.appendStreamCounter.Add(ctx, 1)
		span.End()
	}()

	return s.inner.WriteSnapshot(ctx, snap)
}

// Create all of the necessary metric instruments.
func (s *InstrumentedStore) initializeMetrics() error {
	if counter, err := s.meter.Int64Counter(s.metricNamespace+".stream.read",
		metric.WithDescription("The number of times the ReadStream method was called"),
	); err != nil {
		return fmt.Errorf("creating ReadStream counter: %w", err)
	} else {
		s.readStreamCounter = counter
	}

	if counter, err := s.meter.Int64Counter(s.metricNamespace+".stream.append",
		metric.WithDescription("The number of times the AppendStream method was called"),
	); err != nil {
		return fmt.Errorf("creating AppendStream counter: %w", err)
	} else {
		s.appendStreamCounter = counter
	}

	return nil
}

// An InstrumentedStoreOption configures an instrumented store.
type InstrumentedStoreOption func(*InstrumentedStore) error

// WithTracingEnabled enables or disables tracing for the store.
//
// By default, tracing is enabled.
func WithTracingEnabled(enabled bool) InstrumentedStoreOption {
	return func(s *InstrumentedStore) error {
		s.tracingEnabled = enabled
		return nil
	}
}

// WithTracerProvider sets the OTEL tracer provider for the store.
func WithTracerProvider(provider trace.TracerProvider) InstrumentedStoreOption {
	return func(s *InstrumentedStore) error {
		s.tracer = provider.Tracer(scope)
		return nil
	}
}

// WithMetricsEnabled enables or disables metrics for the store.
//
// By default, metrics are enabled.
func WithMetricsEnabled(enabled bool) InstrumentedStoreOption {
	return func(s *InstrumentedStore) error {
		s.metricsEnabled = enabled
		return nil
	}
}

// WithMeterProvider sets the OTEL meter provider for the store.
func WithMeterProvider(provider metric.MeterProvider) InstrumentedStoreOption {
	return func(s *InstrumentedStore) error {
		s.meter = provider.Meter(scope)
		return nil
	}
}

// WithMetricNamespace sets the namespace for the metrics emitted by the store.
//
// The default namespace is "aggregatestore". For example, if the namespace is
// set to "customstore", the metrics will be emitted under the following names:
//
//   - customstore.load
//   - customstore.hydrate
//   - customstore.save
//
// Overriding the default namespace is useful when you are layering multiple
// aggregate stores and want to instrument each one while differentiating between
// them in telemetry.
func WithMetricNamespace(namespace string) InstrumentedStoreOption {
	return func(s *InstrumentedStore) error {
		s.metricNamespace = namespace
		return nil
	}
}

// WithTraceNamespace sets the namespace for the traces emitted by the store.
//
// The default namespace is "aggregatestore". For example, if the namespace is
// set to "customstore", the tracer will be emitted under the following names:
//
//   - customstore.Load
//   - customstore.Hydrate
//   - customstore.Save
//
// Overriding the default namespace is useful when you are layering multiple
// aggregate stores and want to instrument each one while differentiating between
// them in telemetry.
func WithTraceNamespace(namespace string) InstrumentedStoreOption {
	return func(s *InstrumentedStore) error {
		s.traceNamespace = namespace
		return nil
	}
}
