package telemetry

// This file owns the small amount of OpenTelemetry plumbing used by the
// deployed measurement path.  The runtime keeps the exporter optional: local
// unit tests use the no-op provider, while Compose points the same provider at
// the OTLP collector.

import (
	"context"
	"net/http"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.37.0"
	"go.opentelemetry.io/otel/trace"
)

var textPropagator = propagation.TraceContext{}

// Tracing is deliberately a small wrapper so callers cannot accidentally
// create unbounded metric labels or forget to shut down a batch exporter.
type Tracing struct {
	tracer   trace.Tracer
	provider *sdktrace.TracerProvider
}

// New creates an OTLP tracer. An empty endpoint returns a valid no-op tracer,
// which keeps package tests independent of a collector.
func NewTracing(ctx context.Context, serviceName, endpoint string) (*Tracing, error) {
	if serviceName == "" {
		serviceName = "durable-agent-runtime"
	}
	if strings.TrimSpace(endpoint) == "" {
		return &Tracing{tracer: otel.GetTracerProvider().Tracer(serviceName)}, nil
	}
	endpoint = strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://")
	exporter, err := otlptracegrpc.New(ctx, otlptracegrpc.WithEndpoint(endpoint), otlptracegrpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	res, err := resource.New(ctx,
		resource.WithAttributes(semconv.ServiceName(serviceName)),
		resource.WithTelemetrySDK(),
	)
	if err != nil {
		return nil, err
	}
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter, sdktrace.WithBatchTimeout(250*time.Millisecond)),
		sdktrace.WithResource(res),
	)
	return &Tracing{tracer: provider.Tracer(serviceName), provider: provider}, nil
}

func (t *Tracing) Start(ctx context.Context, name string, options ...trace.SpanStartOption) (context.Context, trace.Span) {
	if t == nil || t.tracer == nil {
		return trace.NewNoopTracerProvider().Tracer("durable-agent").Start(ctx, name, options...)
	}
	return t.tracer.Start(ctx, name, options...)
}

func (t *Tracing) Shutdown(ctx context.Context) error {
	if t == nil || t.provider == nil {
		return nil
	}
	return t.provider.Shutdown(ctx)
}

func ExtractHTTP(r *http.Request) context.Context {
	if r == nil {
		return context.Background()
	}
	return textPropagator.Extract(r.Context(), propagation.HeaderCarrier(r.Header))
}

func InjectHTTP(ctx context.Context, header http.Header) {
	textPropagator.Inject(ctx, propagation.HeaderCarrier(header))
}

func ExtractMap(ctx context.Context, values map[string]string) context.Context {
	return textPropagator.Extract(ctx, propagation.MapCarrier(values))
}

func InjectMap(ctx context.Context, values map[string]string) {
	textPropagator.Inject(ctx, propagation.MapCarrier(values))
}

// Traceparent returns the W3C parent value for durable outbox/Kafka metadata.
// Empty means the request was not sampled or had no valid incoming context.
func Traceparent(ctx context.Context) string {
	sc := trace.SpanContextFromContext(ctx)
	if !sc.IsValid() {
		return ""
	}
	values := map[string]string{}
	InjectMap(ctx, values)
	return values["traceparent"]
}

func ContextWithTraceparent(ctx context.Context, parent string) context.Context {
	if parent == "" {
		return ctx
	}
	return ExtractMap(ctx, map[string]string{"traceparent": parent})
}
