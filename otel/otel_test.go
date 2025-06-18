package otel

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

	"github.com/cloudchacho/hedwig-go"
)

// fakeHedwigDataField is a fake data field for testing
type fakeHedwigDataField struct {
	VehicleID string `json:"vehicle_id"`
}

// testTracerProvider is a provider that returns tracer of type testTracer
type testTracerProvider struct{}

func (p testTracerProvider) Tracer(string, ...trace.TracerOption) trace.Tracer {
	return testTracer{}
}

// testTracer is an implementation of Tracer that returns instances of testSpan
type testTracer struct{}

func (t testTracer) Start(ctx context.Context, _ string, _ ...trace.SpanStartOption) (context.Context, trace.Span) {
	span := &testSpan{}
	return trace.ContextWithSpan(ctx, span), span
}

// testSpan is an instance of the span that allows tracking span name
type testSpan struct {
	name string
}

func (*testSpan) SpanContext() trace.SpanContext { return trace.SpanContext{} }

func (*testSpan) IsRecording() bool { return false }

func (*testSpan) SetStatus(codes.Code, string) {}

func (*testSpan) SetAttributes(...attribute.KeyValue) {}

func (*testSpan) End(...trace.SpanEndOption) {}

func (*testSpan) RecordError(error, ...trace.EventOption) {}

func (*testSpan) TracerProvider() trace.TracerProvider { return testTracerProvider{} }

func (*testSpan) AddEvent(string, ...trace.EventOption) {}

func (n *testSpan) SetName(value string) {
	n.name = value
}

func TestOnMessageDeserializedUpdatesName(t *testing.T) {
	a, r := assert.New(t), require.New(t)

	instrumenter := NewInstrumenter(&testTracerProvider{}, propagation.TraceContext{})
	ctx := context.Background()
	span := &testSpan{}
	ctx = trace.ContextWithSpan(ctx, span)

	data := fakeHedwigDataField{
		VehicleID: "C_1234567890123456",
	}
	message, err := hedwig.NewMessage("user-created", "1.0", nil, &data, "myapp")
	r.NoError(err)

	instrumenter.OnMessageDeserialized(ctx, message)
	a.Equal("user-created", span.name)
}

// testProcessor is a simple span processor that let us track when the span is ended
type testProcessor struct {
	onEnd func()
}

func (p *testProcessor) OnStart(context.Context, sdktrace.ReadWriteSpan) {}

func (p *testProcessor) OnEnd(sdktrace.ReadOnlySpan) {
	p.onEnd()
}

func (p *testProcessor) Shutdown(context.Context) error {
	return nil
}

func (p *testProcessor) ForceFlush(context.Context) error {
	return nil
}

func TestOnReceiveStartNewSpan(t *testing.T) {
	a := assert.New(t)

	prop := propagation.TraceContext{}
	tp := sdktrace.NewTracerProvider()
	instrumenter := NewInstrumenter(tp, prop)
	ctx := context.Background()

	attributes := map[string]string{}
	// the trace from upstream service that published the message
	var publishingTraceID trace.TraceID
	var publishingSpanID trace.SpanID
	{
		ctx, publishingSpan := tp.Tracer(tracerName).Start(ctx, "test")
		publishingTraceID = publishingSpan.SpanContext().TraceID()
		publishingSpanID = publishingSpan.SpanContext().SpanID()
		ac := attributesCarrier{attributes}
		prop.Inject(ctx, ac)
		attributes = ac.attributes
		publishingSpan.End()
	}

	// the span in which consumer.ListenForMessages is called
	ctx, consumerSpan := tp.Tracer(tracerName).Start(ctx, "test")

	ended := false
	processor := &testProcessor{func() {
		ended = true
	}}
	tp.RegisterSpanProcessor(processor)
	ctx, finalize := instrumenter.OnReceive(ctx, attributes)
	currentSpan := trace.SpanFromContext(ctx)
	// should have created a new span just for receiver
	a.Equal(publishingTraceID, currentSpan.SpanContext().TraceID())
	a.NotEqual(publishingSpanID, currentSpan.SpanContext().SpanID())
	a.NotEqual(consumerSpan.SpanContext().SpanID(), currentSpan.SpanContext().SpanID())
	a.False(ended)
	// should end the span that was created for receiver
	finalize()
	a.True(ended)
}

func TestOnPublishAttachesCurrentTrace(t *testing.T) {
	a, r := assert.New(t), require.New(t)

	prop := propagation.TraceContext{}
	tp := sdktrace.NewTracerProvider()
	instrumenter := NewInstrumenter(tp, prop)
	ctx := context.Background()

	ctx, publishingSpan := tp.Tracer(tracerName).Start(ctx, "test")
	publishingTraceID := publishingSpan.SpanContext().TraceID()
	publishingSpanID := publishingSpan.SpanContext().SpanID()

	ended := false
	processor := &testProcessor{func() {
		ended = true
	}}
	tp.RegisterSpanProcessor(processor)
	data := fakeHedwigDataField{
		VehicleID: "C_1234567890123456",
	}
	message, err := hedwig.NewMessage("user-created", "1.0", nil, &data, "myapp")
	r.NoError(err)
	attributes := map[string]string{}
	ctx, attributes, finalize := instrumenter.OnPublish(ctx, message, attributes)
	// should have created a new span just for publishing
	currentSpan := trace.SpanFromContext(ctx)
	a.Equal(publishingTraceID, currentSpan.SpanContext().TraceID())
	a.NotEqual(publishingSpanID, currentSpan.SpanContext().SpanID())
	if a.Contains(attributes, "traceparent") {
		a.Contains(attributes["traceparent"], publishingTraceID.String())
		a.Contains(attributes["traceparent"], publishingSpanID.String())
	}
	a.False(ended)
	// should end the span that was created for publishing
	finalize()
	a.True(ended)
}
