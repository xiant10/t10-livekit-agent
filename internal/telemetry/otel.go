package telemetry

import (
	"context"
	"errors"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/log/global"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// SetupOTel wires endpoint (an OTLP/gRPC host:port, e.g.
// "otel-collector:4317" — no scheme) up as this process's real trace and
// metric pipeline: every StartSession/StartStage span this package produces
// from here on gets batched and exported, and every StartStage duration/
// count is exported as a metric. serviceName identifies this process in the
// backend (e.g. Jaeger's service dropdown) — use a different one per
// process (agent vs. client).
//
// Call the returned shutdown func before the process exits (it flushes
// anything still buffered) — a deferred call in main is the usual spot.
// Skipping SetupOTel entirely is also fine: every telemetry.* call already
// degrades to a correctly-propagating no-op without it, so this is purely
// additive (see the package doc).
func SetupOTel(ctx context.Context, serviceName, endpoint string) (shutdown func(context.Context) error, err error) {
	res, err := resource.New(ctx,
		resource.WithTelemetrySDK(),
		resource.WithHost(),
		resource.WithAttributes(attribute.String("service.name", serviceName)),
	)
	if err != nil {
		return nil, fmt.Errorf("build otel resource: %w", err)
	}

	traceExp, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint(endpoint),
		otlptracegrpc.WithInsecure(),
	)
	if err != nil {
		return nil, fmt.Errorf("dial otlp trace exporter at %q: %w", endpoint, err)
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(traceExp),
		sdktrace.WithResource(res),
	)

	metricExp, err := otlpmetricgrpc.New(ctx,
		otlpmetricgrpc.WithEndpoint(endpoint),
		otlpmetricgrpc.WithInsecure(),
	)
	if err != nil {
		return nil, fmt.Errorf("dial otlp metric exporter at %q: %w", endpoint, err)
	}
	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExp)),
		sdkmetric.WithResource(res),
	)

	// otel.Tracer/otel.Meter handles obtained before this point (including
	// this package's own package-level tracer/meter vars) are delegating
	// proxies that start forwarding to these real providers from here on —
	// no need to recreate this package's tracer/meter/instruments.
	otel.SetTracerProvider(tp)
	otel.SetMeterProvider(mp)

	logExp, err := otlploggrpc.New(ctx,
		otlploggrpc.WithEndpoint(endpoint),
		otlploggrpc.WithInsecure(),
	)
	if err != nil {
		return nil, fmt.Errorf("dial otlp log exporter at %q: %w", endpoint, err)
	}
	lp := sdklog.NewLoggerProvider(
		sdklog.WithProcessor(sdklog.NewBatchProcessor(logExp)),
		sdklog.WithResource(res),
	)
	global.SetLoggerProvider(lp)

	return func(ctx context.Context) error {
		return errors.Join(tp.Shutdown(ctx), mp.Shutdown(ctx), lp.Shutdown(ctx))
	}, nil
}
