package aggregatestore

import (
	"context"
	"fmt"

	"github.com/go-estoria/estoria/aggregatestore"
	"github.com/gofrs/uuid/v5"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	noopmetric "go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/trace"
	nooptrace "go.opentelemetry.io/otel/trace/noop"
)

// Default metric and trace namespace for this store.
const namespaceAggregateStore = "aggregatestore"

const (
	scope = "github.com/go-estoria/estoria-contrib/opentelemetry/aggregatestore"
)

// An InstrumentedStore wraps an aggregate store for OpenTelemetry instrumentation.
//
// The store wraps and emits metrics and traces for the Load, Hydrate, and Save methods.
// The metrics are emitted using the OpenTelemetry metric API, and the traces are
// emitted using the OpenTelemetry trace API.
//
// The store can be configured to enable or disable tracing and metrics, and
// to use a custom tracer or meter provider. By default, the store uses the
// global tracer and meter provider from the OpenTelemetry SDK.
//
// The store emits metrics under the "aggregatestore" namespace by default. The
// namespace can be customized using the WithMetricNamespace option.
type InstrumentedStore[S any] struct {
	inner          aggregatestore.Store[S]
	tracingEnabled bool
	tracer         trace.Tracer
	metricsEnabled bool
	meter          metric.Meter

	metricNamespace string
	traceNamespace  string

	loadCounter    metric.Int64Counter
	hydrateCounter metric.Int64Counter
	saveCounter    metric.Int64Counter
}

// NewInstrumentedStore creates a new instrumented aggregate store.
func NewInstrumentedStore[S any](inner aggregatestore.Store[S], opts ...InstrumentedStoreOption[S]) (*InstrumentedStore[S], error) {
	store := &InstrumentedStore[S]{
		inner:           inner,
		tracingEnabled:  true,
		metricsEnabled:  true,
		metricNamespace: namespaceAggregateStore,
		traceNamespace:  namespaceAggregateStore,
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

var _ aggregatestore.Store[struct{}] = &InstrumentedStore[struct{}]{}

// AggregateType returns the aggregate type name of the inner store.
func (s *InstrumentedStore[S]) AggregateType() string {
	return s.inner.AggregateType()
}

// New creates a new aggregate with the given ID.
func (s *InstrumentedStore[S]) New(id uuid.UUID) *aggregatestore.Aggregate[S] {
	return s.inner.New(id)
}

// Load loads an aggregate by ID while capturing telemetry.
func (s *InstrumentedStore[S]) Load(ctx context.Context, id uuid.UUID, opts *aggregatestore.LoadOptions) (_ *aggregatestore.Aggregate[S], e error) {
	ctx, span := s.tracer.Start(ctx, s.traceNamespace+".Load", trace.WithAttributes(
		attribute.String("aggregate.uuid", id.String()),
	))

	if opts != nil {
		span.SetAttributes(attribute.Int64("load_options.to_version", opts.ToVersion))
	}

	defer func() {
		span.RecordError(e)
		if e != nil {
			span.SetStatus(codes.Error, "error loading aggregate")
		}

		s.loadCounter.Add(ctx, 1)
		span.End()
	}()

	return s.inner.Load(ctx, id, opts)
}

// Hydrate hydrates an aggregate while capturing telemetry.
func (s *InstrumentedStore[S]) Hydrate(ctx context.Context, aggregate *aggregatestore.Aggregate[S], opts *aggregatestore.HydrateOptions) (e error) {
	ctx, span := s.tracer.Start(ctx, s.traceNamespace+".Hydrate", trace.WithAttributes(
		attribute.String("aggregate.id", aggregate.ID().String()),
		attribute.Int64("aggregate.version", aggregate.Version()),
	))
	defer func() {
		span.RecordError(e)
		if e != nil {
			span.SetStatus(codes.Error, "error hydrating aggregate")
		}

		s.hydrateCounter.Add(ctx, 1)
		span.End()
	}()

	if opts != nil {
		span.SetAttributes(attribute.Int64("hydrate_options.to_version", opts.ToVersion))
	}

	return s.inner.Hydrate(ctx, aggregate, opts)
}

// Save saves an aggregate while capturing telemetry.
func (s *InstrumentedStore[S]) Save(ctx context.Context, aggregate *aggregatestore.Aggregate[S], opts *aggregatestore.SaveOptions) (e error) {
	ctx, span := s.tracer.Start(ctx, s.traceNamespace+".Save", trace.WithAttributes(
		attribute.String("aggregate.id", aggregate.ID().String()),
		attribute.Int64("aggregate.version", aggregate.Version()),
	))
	defer func() {
		span.RecordError(e)
		if e != nil {
			span.SetStatus(codes.Error, "error saving aggregate")
		}

		s.saveCounter.Add(ctx, 1)
		span.End()
	}()

	return s.inner.Save(ctx, aggregate, opts)
}

// Create all of the necessary metric instruments.
func (s *InstrumentedStore[S]) initializeMetrics() error {
	counter, err := s.meter.Int64Counter(s.metricNamespace+".load",
		metric.WithDescription("The number of times the Load method was called"),
	)
	if err != nil {
		return fmt.Errorf("creating Load counter: %w", err)
	}

	s.loadCounter = counter

	counter, err = s.meter.Int64Counter(s.metricNamespace+".hydrate",
		metric.WithDescription("The number of times the Hydrate method was called"),
	)
	if err != nil {
		return fmt.Errorf("creating Hydrate counter: %w", err)
	}

	s.hydrateCounter = counter

	counter, err = s.meter.Int64Counter(s.metricNamespace+".save",
		metric.WithDescription("The number of times the Save method was called"),
	)
	if err != nil {
		return fmt.Errorf("creating Save counter: %w", err)
	}

	s.saveCounter = counter

	return nil
}

// An InstrumentedStoreOption configures an instrumented store.
type InstrumentedStoreOption[S any] func(*InstrumentedStore[S]) error

// WithTracingEnabled enables or disables tracing for the store.
//
// By default, tracing is enabled.
func WithTracingEnabled[S any](enabled bool) InstrumentedStoreOption[S] {
	return func(s *InstrumentedStore[S]) error {
		s.tracingEnabled = enabled
		return nil
	}
}

// WithTracerProvider sets the OTEL tracer provider for the store.
func WithTracerProvider[S any](provider trace.TracerProvider) InstrumentedStoreOption[S] {
	return func(s *InstrumentedStore[S]) error {
		s.tracer = provider.Tracer(scope)
		return nil
	}
}

// WithMetricsEnabled enables or disables metrics for the store.
//
// By default, metrics are enabled.
func WithMetricsEnabled[S any](enabled bool) InstrumentedStoreOption[S] {
	return func(s *InstrumentedStore[S]) error {
		s.metricsEnabled = enabled
		return nil
	}
}

// WithMeterProvider sets the OTEL meter provider for the store.
func WithMeterProvider[S any](provider metric.MeterProvider) InstrumentedStoreOption[S] {
	return func(s *InstrumentedStore[S]) error {
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
func WithMetricNamespace[S any](namespace string) InstrumentedStoreOption[S] {
	return func(s *InstrumentedStore[S]) error {
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
func WithTraceNamespace[S any](namespace string) InstrumentedStoreOption[S] {
	return func(s *InstrumentedStore[S]) error {
		s.traceNamespace = namespace
		return nil
	}
}
