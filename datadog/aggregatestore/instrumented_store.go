package aggregatestore

import (
	"context"
	"fmt"

	"github.com/DataDog/datadog-go/v5/statsd"
	"github.com/go-estoria/estoria"
	"github.com/go-estoria/estoria/aggregatestore"
	"github.com/gofrs/uuid/v5"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
)

// An InstrumentedStore wraps an aggregate store for DataDog instrumentation.
//
// The store wraps and emits metrics and traces for the Load, Hydrate, and Save methods.
// The metrics are emitted using the DoogStatsD metric API, and the traces are
// emitted using the DataDog trace API.
//
// The store can be configured to enable or disable tracing and metrics, and
// to use a custom tracer or meter provider. By default, the store uses the
// global tracer and meter provider from the OpenTelemetry SDK.
//
// The store emits metrics under the "aggregatestore" namespace by default. The
// namespace can be customized using the WithMetricNamespace option.
type InstrumentedStore[E estoria.Entity] struct {
	inner          aggregatestore.Store[E]
	metricsEnabled bool
	meter          statsd.ClientInterface

	metricNamespace string
	traceNamespace  string
}

// NewInstrumentedStore creates a new instrumented aggregate store.
func NewInstrumentedStore[E estoria.Entity](inner aggregatestore.Store[E], opts ...InstrumentedStoreOption[E]) (*InstrumentedStore[E], error) {
	store := &InstrumentedStore[E]{
		inner:           inner,
		metricsEnabled:  true,
		metricNamespace: "aggregatestore",
		traceNamespace:  "aggregatestore",
	}

	for _, opt := range opts {
		if err := opt(store); err != nil {
			return nil, fmt.Errorf("applying option: %w", err)
		}
	}

	if store.meter == nil {
		if store.metricsEnabled {
			client, err := statsd.New("localhost:8125")
			if err != nil {
				return nil, fmt.Errorf("creating statsd client: %w", err)
			}

			store.meter = client
		} else {
			store.meter = &statsd.NoOpClient{}
		}
	}

	return store, nil
}

var _ aggregatestore.Store[estoria.Entity] = &InstrumentedStore[estoria.Entity]{}

// New creates a new aggregate with the given ID.
func (s *InstrumentedStore[E]) New(id uuid.UUID) *aggregatestore.Aggregate[E] {
	return s.inner.New(id)
}

// Load loads an aggregate by ID while capturing telemetry.
func (s *InstrumentedStore[E]) Load(ctx context.Context, id uuid.UUID, opts aggregatestore.LoadOptions) (_ *aggregatestore.Aggregate[E], e error) {
	span, ctx := tracer.StartSpanFromContext(ctx, s.traceNamespace+".Load")
	span.SetTag("aggregate.id", id.String())
	span.SetTag("load_options.to_version", opts.ToVersion)

	defer func() {
		s.meter.Incr(s.metricNamespace+".load", nil, 1)
		span.Finish(tracer.WithError(e))
	}()

	return s.inner.Load(ctx, id, opts)
}

// Hydrate hydrates an aggregate while capturing telemetry.
func (s *InstrumentedStore[E]) Hydrate(ctx context.Context, aggregate *aggregatestore.Aggregate[E], opts aggregatestore.HydrateOptions) (e error) {
	span, ctx := tracer.StartSpanFromContext(ctx, s.traceNamespace+".Hydrate")
	span.SetTag("aggregate.id", aggregate.ID().String())
	span.SetTag("aggregate.version", aggregate.Version())
	span.SetTag("hydrate_options.to_version", opts.ToVersion)

	defer func() {
		s.meter.Incr(s.metricNamespace+".hydrate", nil, 1)
		span.Finish(tracer.WithError(e))
	}()

	return s.inner.Hydrate(ctx, aggregate, opts)
}

// Save saves an aggregate while capturing telemetry.
func (s *InstrumentedStore[E]) Save(ctx context.Context, aggregate *aggregatestore.Aggregate[E], opts aggregatestore.SaveOptions) (e error) {
	span, ctx := tracer.StartSpanFromContext(ctx, s.traceNamespace+".Save")
	span.SetTag("aggregate.id", aggregate.ID().String())
	span.SetTag("aggregate.version", aggregate.Version())
	span.SetTag("aggregate.unsaved_events", int64(len(aggregate.State().UnsavedEvents())))

	defer func() {
		s.meter.Incr(s.metricNamespace+".save", nil, 1)
		span.Finish(tracer.WithError(e))
	}()

	return s.inner.Save(ctx, aggregate, opts)
}

// An InstrumentedStoreOption configures an instrumented store.
type InstrumentedStoreOption[E estoria.Entity] func(*InstrumentedStore[E]) error

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
func WithMetricsClient[E estoria.Entity](provider statsd.ClientInterface) InstrumentedStoreOption[E] {
	return func(s *InstrumentedStore[E]) error {
		s.meter = provider
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
