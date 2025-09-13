package aggregatestore

import (
	"context"
	"fmt"

	"github.com/go-estoria/estoria"
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
type InstrumentedStore[E estoria.Entity] struct {
	inner          aggregatestore.Store[E]
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
func NewInstrumentedStore[E estoria.Entity](inner aggregatestore.Store[E], opts ...InstrumentedStoreOption[E]) (*InstrumentedStore[E], error) {
	store := &InstrumentedStore[E]{
		inner:           inner,
		tracingEnabled:  true,
		metricsEnabled:  true,
		metricNamespace: "aggregatestore",
		traceNamespace:  "aggregatestore",
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

var _ aggregatestore.Store[estoria.Entity] = &InstrumentedStore[estoria.Entity]{}

// New creates a new aggregate with the given ID.
func (s *InstrumentedStore[E]) New(id uuid.UUID) *aggregatestore.Aggregate[E] {
	return s.inner.New(id)
}

// Load loads an aggregate by ID while capturing telemetry.
func (s *InstrumentedStore[E]) Load(ctx context.Context, id uuid.UUID, opts *aggregatestore.LoadOptions) (_ *aggregatestore.Aggregate[E], e error) {
	ctx, span := s.tracer.Start(ctx, s.traceNamespace+".Load", trace.WithAttributes(
		attribute.String("aggregate.uuid", id.String()),
		attribute.Int64("load_options.to_version", opts.ToVersion)),
	)

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
func (s *InstrumentedStore[E]) Hydrate(ctx context.Context, aggregate *aggregatestore.Aggregate[E], opts *aggregatestore.HydrateOptions) (e error) {
	ctx, span := s.tracer.Start(ctx, s.traceNamespace+".Hydrate", trace.WithAttributes(
		attribute.String("aggregate.id", aggregate.ID().String()),
		attribute.Int64("aggregate.version", aggregate.Version()),
		attribute.Int64("hydrate_options.to_version", opts.ToVersion),
	))
	defer func() {
		span.RecordError(e)
		if e != nil {
			span.SetStatus(codes.Error, "error hydrating aggregate")
		}

		s.hydrateCounter.Add(ctx, 1)
		span.End()
	}()

	return s.inner.Hydrate(ctx, aggregate, opts)
}

// Save saves an aggregate while capturing telemetry.
func (s *InstrumentedStore[E]) Save(ctx context.Context, aggregate *aggregatestore.Aggregate[E], opts *aggregatestore.SaveOptions) (e error) {
	ctx, span := s.tracer.Start(ctx, s.traceNamespace+".Save", trace.WithAttributes(
		attribute.String("aggregate.id", aggregate.ID().String()),
		attribute.Int64("aggregate.version", aggregate.Version()),
		attribute.Int64("aggregate.unsaved_events", int64(len(aggregate.State().UnsavedEvents()))),
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
func (s *InstrumentedStore[E]) initializeMetrics() error {
	if counter, err := s.meter.Int64Counter(s.metricNamespace+".load",
		metric.WithDescription("The number of times the Load method was called"),
	); err != nil {
		return fmt.Errorf("creating Load counter: %w", err)
	} else {
		s.loadCounter = counter
	}

	if counter, err := s.meter.Int64Counter(s.metricNamespace+".hydrate",
		metric.WithDescription("The number of times the Hydrate method was called"),
	); err != nil {
		return fmt.Errorf("creating Hydrate counter: %w", err)
	} else {
		s.hydrateCounter = counter
	}

	if counter, err := s.meter.Int64Counter(s.metricNamespace+".save",
		metric.WithDescription("The number of times the Save method was called"),
	); err != nil {
		return fmt.Errorf("creating Save counter: %w", err)
	} else {
		s.saveCounter = counter
	}

	return nil
}

// An InstrumentedStoreOption configures an instrumented store.
type InstrumentedStoreOption[E estoria.Entity] func(*InstrumentedStore[E]) error

// WithTracingEnabled enables or disables tracing for the store.
//
// By default, tracing is enabled.
func WithTracingEnabled[E estoria.Entity](enabled bool) InstrumentedStoreOption[E] {
	return func(s *InstrumentedStore[E]) error {
		s.tracingEnabled = enabled
		return nil
	}
}

// WithTracerProvider sets the OTEL tracer provider for the store.
func WithTracerProvider[E estoria.Entity](provider trace.TracerProvider) InstrumentedStoreOption[E] {
	return func(s *InstrumentedStore[E]) error {
		s.tracer = provider.Tracer(scope)
		return nil
	}
}

// WithMetricsEnabled enables or disables metrics for the store.
//
// By default, metrics are enabled.
func WithMetricsEnabled[E estoria.Entity](enabled bool) InstrumentedStoreOption[E] {
	return func(s *InstrumentedStore[E]) error {
		s.metricsEnabled = enabled
		return nil
	}
}

// WithMeterProvider sets the OTEL meter provider for the store.
func WithMeterProvider[E estoria.Entity](provider metric.MeterProvider) InstrumentedStoreOption[E] {
	return func(s *InstrumentedStore[E]) error {
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
func WithMetricNamespace[E estoria.Entity](namespace string) InstrumentedStoreOption[E] {
	return func(s *InstrumentedStore[E]) error {
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
func WithTraceNamespace[E estoria.Entity](namespace string) InstrumentedStoreOption[E] {
	return func(s *InstrumentedStore[E]) error {
		s.traceNamespace = namespace
		return nil
	}
}
