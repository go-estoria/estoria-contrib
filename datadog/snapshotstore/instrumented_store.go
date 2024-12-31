package snapshotstore

import (
	"context"
	"errors"
	"fmt"

	"github.com/DataDog/datadog-go/v5/statsd"
	"github.com/go-estoria/estoria/snapshotstore"
	"github.com/go-estoria/estoria/typeid"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
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
	metricsEnabled bool
	meter          statsd.ClientInterface

	metricNamespace string
	traceNamespace  string
}

// NewInstrumentedStore creates a new instrumented event store.
func NewInstrumentedStore(inner snapshotstore.SnapshotStore, opts ...InstrumentedStoreOption) (*InstrumentedStore, error) {
	store := &InstrumentedStore{
		inner:           inner,
		metricsEnabled:  true,
		metricNamespace: "snapshotstore",
		traceNamespace:  "snapshotstore",
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

var _ snapshotstore.SnapshotStore = (*InstrumentedStore)(nil)

// Load loads an aggregate by ID while capturing telemetry.
func (s *InstrumentedStore) ReadSnapshot(ctx context.Context, aggregateID typeid.UUID, opts snapshotstore.ReadSnapshotOptions) (_ *snapshotstore.AggregateSnapshot, e error) {
	span, ctx := tracer.StartSpanFromContext(ctx, s.traceNamespace+".ReadSnapshot")
	span.SetTag("aggregate.id", aggregateID.String())
	span.SetTag("options.max_version", opts.MaxVersion)

	defer func() {
		if errors.Is(e, snapshotstore.ErrSnapshotNotFound) {
			span.Finish()
		} else {
			span.Finish(tracer.WithError(e))
		}

		s.meter.Incr(s.metricNamespace+".ReadSnapshot", nil, 1)
	}()

	return s.inner.ReadSnapshot(ctx, aggregateID, opts)
}

// Hydrate hydrates an aggregate while capturing telemetry.
func (s *InstrumentedStore) WriteSnapshot(ctx context.Context, snap *snapshotstore.AggregateSnapshot) (e error) {
	span, ctx := tracer.StartSpanFromContext(ctx, s.traceNamespace+".WriteSnapshot")
	span.SetTag("snapshot.aggregate.id", snap.AggregateID.String())
	span.SetTag("snapshot.aggregate.version", snap.AggregateVersion)
	span.SetTag("snapshot.data.length", int64(len(snap.Data)))
	span.SetTag("snapshot.timestamp.nano", snap.Timestamp.UnixNano())

	defer func() {
		s.meter.Incr(s.metricNamespace+".WriteSnapshot", nil, 1)
		span.Finish(tracer.WithError(e))
	}()

	return s.inner.WriteSnapshot(ctx, snap)
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

// WithMetricsClient sets the statsd client for the store.
func WithMetricsClient(client statsd.ClientInterface) InstrumentedStoreOption {
	return func(s *InstrumentedStore) error {
		s.meter = client
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
