package main

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/cloudchacho/hedwig-go"
	hedwigOtel "github.com/cloudchacho/hedwig-go/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

func runConsumer(
	backend hedwig.ConsumerBackend, decoder hedwig.Decoder, registry hedwig.CallbackRegistry,
	instrumenter hedwig.Instrumenter, fakeCallbackErr string) {
	consumer := hedwig.NewQueueConsumer(backend, decoder, nil, registry)
	consumer.WithInstrumenter(instrumenter)
	err := consumer.ListenForMessages(context.Background(), hedwig.ListenRequest{})
	if err != nil {
		panic(fmt.Sprintf("Failed to consume messages: %v", err))
	}
}

func runPublisher(
	backend hedwig.PublisherBackend, encoder hedwig.Encoder, decoder hedwig.Decoder, instrumenter hedwig.Instrumenter,
	dataCreator func() interface{}) {
	ctx := context.Background()
	tp := sdktrace.NewTracerProvider()
	tracer := tp.Tracer("github.com/cloudchacho/hedwig-go/examples")
	ctx, span := tracer.Start(ctx, "publisher")
	defer span.End()
	routing := map[hedwig.MessageTypeMajorVersion]string{
		{
			MessageType:  "user-created",
			MajorVersion: 1,
		}: "dev-user-created-v1",
	}
	publisher := hedwig.NewPublisher(backend, encoder, decoder, routing)
	publisher.WithInstrumenter(instrumenter)
	data := dataCreator()
	message, err := hedwig.NewMessage("user-created", "1.0", map[string]string{"request_id": "123"}, data, "hedwig-go/examples")
	if err != nil {
		panic(fmt.Sprintf("Failed to create message: %v", err))
	}
	messageID, err := publisher.Publish(ctx, message)
	if err != nil {
		panic(fmt.Sprintf("Failed to publish message: %v", err))
	}
	fmt.Printf("[%s/%s], Published message with id %s successfully with publish id: %s\n",
		span.SpanContext().TraceID(), span.SpanContext().SpanID(), message.ID, messageID)
}

func requeueDLQ(backend hedwig.ConsumerBackend, decoder hedwig.Decoder, instrumenter hedwig.Instrumenter) {
	consumer := hedwig.NewQueueConsumer(backend, decoder, nil, nil)
	consumer.WithInstrumenter(instrumenter)
	err := consumer.RequeueDLQ(context.Background(), hedwig.ListenRequest{})
	if err != nil {
		panic(fmt.Sprintf("Failed to requeue messages: %v", err))
	}
}

func main() {
	var consumerBackend hedwig.ConsumerBackend
	var publisherBackend hedwig.PublisherBackend
	var encoder hedwig.Encoder
	var decoder hedwig.Decoder
	var registry hedwig.CallbackRegistry
	var propagator propagation.TextMapPropagator
	var dataCreator func() interface{}

	isProtobuf := false
	if isProtobufStr, found := os.LookupEnv("HEDWIG_PROTOBUF"); found {
		isProtobuf = strings.ToLower(isProtobufStr) == "true"
	}
	backendName := "aws"
	if isGCPStr, found := os.LookupEnv("HEDWIG_GCP"); found && strings.ToLower(isGCPStr) == "true" {
		backendName = "gcp"
	}
	fakeCallbackErr := ""
	if fakeConsumerErrStr, found := os.LookupEnv("FAKE_CALLBACK_ERROR"); found {
		fakeCallbackErr = fakeConsumerErrStr
	}

	if isProtobuf {
		de := protobufEncoderDecoder()
		encoder = de
		decoder = de
		registry = protobufRegistry(fakeCallbackErr)
		dataCreator = protobufDataCreator
	} else {
		de := jsonSchemaEncoderDecoder()
		encoder = de
		decoder = de
		registry = jsonSchemaRegistry(fakeCallbackErr)
		dataCreator = jsonSchemaDataCreator
	}

	if backendName == "aws" {
		b := awsBackend()
		consumerBackend = b
		publisherBackend = b
		propagator = awsPropagator()
	} else {
		b := gcpBackend()
		consumerBackend = b
		publisherBackend = b
		propagator = gcpPropagator()
	}

	instrumenter := hedwigOtel.NewInstrumenter(sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.AlwaysSample())),
	), propagator)

	switch os.Args[1] {
	case "consumer":
		runConsumer(consumerBackend, decoder, registry, instrumenter, fakeCallbackErr)
	case "publisher":
		runPublisher(publisherBackend, encoder, decoder, instrumenter, dataCreator)
	case "requeue-dlq":
		requeueDLQ(consumerBackend, decoder, instrumenter)
	default:
		panic(fmt.Sprintf("unknown command: %s", os.Args[1]))
	}
}
