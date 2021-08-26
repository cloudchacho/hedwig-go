package otel

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/cloudchacho/hedwig-go"
)

const (
	tracerName = "github.com/cloudchacho/hedwig-go/otel"
)

type attributesCarrier struct {
	attributes map[string]string
}

func (ac attributesCarrier) Get(key string) string {
	return ac.attributes[key]
}

func (ac attributesCarrier) Set(key string, value string) {
	ac.attributes[key] = value
}

func (ac attributesCarrier) Keys() []string {
	keys := make([]string, 0, len(ac.attributes))
	for key := range ac.attributes {
		keys = append(keys, key)
	}
	return keys
}

type instrumenter struct {
	tp   trace.TracerProvider
	prop propagation.TextMapPropagator
}

func (o *instrumenter) OnMessageDeserialized(ctx context.Context, message *hedwig.Message) {
	currentSpan := trace.SpanFromContext(ctx)
	currentSpan.SetName(message.Type)
}

func (o *instrumenter) OnPublish(ctx context.Context, message *hedwig.Message, attributes map[string]string) (context.Context, map[string]string, func()) {
	carrier := attributesCarrier{attributes}
	o.prop.Inject(ctx, carrier)

	name := fmt.Sprintf("publish/%s", message.Type)
	ctx, span := o.tp.Tracer(tracerName).Start(ctx, name, trace.WithSpanKind(trace.SpanKindProducer))

	return ctx, carrier.attributes, func() { span.End() }
}

func (o *instrumenter) OnReceive(ctx context.Context, attributes map[string]string) (context.Context, func()) {
	ctx = o.prop.Extract(ctx, attributesCarrier{attributes})

	name := "message_received"
	ctx, span := o.tp.Tracer(tracerName).Start(ctx, name, trace.WithSpanKind(trace.SpanKindConsumer))

	return ctx, func() { span.End() }
}

func NewInstrumenter(tracer trace.TracerProvider, propagator propagation.TextMapPropagator) hedwig.Instrumenter {
	return &instrumenter{tp: tracer, prop: propagator}
}
