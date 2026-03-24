package eventstore

import (
	"context"
	"errors"
	"fmt"

	"github.com/DataDog/datadog-go/v5/statsd"
	"github.com/go-estoria/estoria/eventstore"
	"github.com/go-estoria/estoria/typeid"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
)

// An InstrumentedStore wraps an event store for Datadog instrumentation.
//
// The store wraps and emits metrics and traces for the ReadStream and AppendStream methods.
// The metrics are emitted using the Datadog StatsD client API, and the traces are
// emitted using the Datadog tracer API.
//
// The store can be configured to enable or disable metrics, and to use a custom
// StatsD client. By default, the store creates a StatsD client connected to
// localhost:8125.
//
// The store emits metrics under the "eventstore" namespace by default. The
// namespace can be customized using the WithMetricNamespace option.
type InstrumentedStore struct {
	inner          eventstore.Store
	metricsEnabled bool
	meter          statsd.ClientInterface

	metricNamespace string
	traceNamespace  string
}

// NewInstrumentedStore creates a new instrumented event store.
func NewInstrumentedStore(inner eventstore.Store, opts ...InstrumentedStoreOption) (*InstrumentedStore, error) {
	store := &InstrumentedStore{
		inner:           inner,
		metricsEnabled:  true,
		metricNamespace: "eventstore",
		traceNamespace:  "eventstore",
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

var _ eventstore.Store = (*InstrumentedStore)(nil)

// ReadStream reads events from a stream while capturing telemetry.
func (s *InstrumentedStore) ReadStream(ctx context.Context, id typeid.ID, opts eventstore.ReadStreamOptions) (_ eventstore.StreamIterator, e error) {
	span, ctx := tracer.StartSpanFromContext(ctx, s.traceNamespace+".ReadStream")
	span.SetTag("stream.id", id.String())
	span.SetTag("options.after_version", opts.AfterVersion)

	defer func() {
		s.meter.Incr(s.metricNamespace+".ReadStream", nil, 1)
		span.Finish(tracer.WithError(e))
	}()

	iterator, err := s.inner.ReadStream(ctx, id, opts)
	if err != nil {
		return nil, err
	}

	return &InstrumentedStreamIterator{
		inner:          iterator,
		meter:          s.meter,
		nextMetric:     s.metricNamespace + ".stream.next",
		traceNamespace: s.traceNamespace,
	}, err
}

// AppendStream appends events to a stream while capturing telemetry.
func (s *InstrumentedStore) AppendStream(ctx context.Context, id typeid.ID, events []*eventstore.WritableEvent, opts eventstore.AppendStreamOptions) (e error) {
	span, ctx := tracer.StartSpanFromContext(ctx, s.traceNamespace+".AppendStream")
	span.SetTag("stream.id", id.String())
	span.SetTag("events.length", int64(len(events)))
	if opts.ExpectVersion != nil {
		span.SetTag("options.expect_version", *opts.ExpectVersion)
	}

	defer func() {
		s.meter.Incr(s.metricNamespace+".AppendStream", nil, 1)
		span.Finish(tracer.WithError(e))
	}()

	return s.inner.AppendStream(ctx, id, events, opts)
}

// An InstrumentedStoreOption configures an instrumented store.
type InstrumentedStoreOption func(*InstrumentedStore) error

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
func WithMeterProvider(client statsd.ClientInterface) InstrumentedStoreOption {
	return func(s *InstrumentedStore) error {
		s.meter = client
		return nil
	}
}

// WithMetricNamespace sets the namespace for the metrics emitted by the store.
//
// The default namespace is "eventstore". For example, if the namespace is
// set to "customstore", the metrics will be emitted under the following names:
//
//   - customstore.ReadStream
//   - customstore.AppendStream
//   - customstore.stream.next
//
// Overriding the default namespace is useful when you are layering multiple
// event stores and want to instrument each one while differentiating between
// them in telemetry.
func WithMetricNamespace(namespace string) InstrumentedStoreOption {
	return func(s *InstrumentedStore) error {
		s.metricNamespace = namespace
		return nil
	}
}

// WithTraceNamespace sets the namespace for the traces emitted by the store.
//
// The default namespace is "eventstore". For example, if the namespace is
// set to "customstore", the traces will be emitted under the following names:
//
//   - customstore.ReadStream
//   - customstore.AppendStream
//   - customstore.StreamIterator.Next
//   - customstore.StreamIterator.Close
//
// Overriding the default namespace is useful when you are layering multiple
// event stores and want to instrument each one while differentiating between
// them in telemetry.
func WithTraceNamespace(namespace string) InstrumentedStoreOption {
	return func(s *InstrumentedStore) error {
		s.traceNamespace = namespace
		return nil
	}
}

type InstrumentedStreamIterator struct {
	inner      eventstore.StreamIterator
	meter      statsd.ClientInterface
	nextMetric string

	traceNamespace string
}

func (i *InstrumentedStreamIterator) Next(ctx context.Context) (_ *eventstore.Event, e error) {
	span, ctx := tracer.StartSpanFromContext(ctx, i.traceNamespace+".StreamIterator.Next")
	defer func() {
		i.meter.Incr(i.nextMetric, nil, 1)
		if errors.Is(e, eventstore.ErrEndOfEventStream) {
			span.Finish()
		} else {
			span.Finish(tracer.WithError(e))
		}
	}()

	return i.inner.Next(ctx)
}

func (i *InstrumentedStreamIterator) Close(ctx context.Context) (e error) {
	span, ctx := tracer.StartSpanFromContext(ctx, i.traceNamespace+".StreamIterator.Close")
	defer func() {
		span.Finish(tracer.WithError(e))
	}()

	return i.inner.Close(ctx)
}
