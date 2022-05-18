package main

import (
	"context"
	"fmt"
	"os"
	"strings"

	"examples/contextlogger"
	hedwiglogrus "examples/logrus"
	hedwigzap "examples/zap"
	hedwigzerolog "examples/zerolog"
	"github.com/cloudchacho/hedwig-go"
	hedwigOtel "github.com/cloudchacho/hedwig-go/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

func runConsumer(
	backend hedwig.ConsumerBackend, decoder hedwig.Decoder, registry hedwig.CallbackRegistry,
	instrumenter hedwig.Instrumenter, logger hedwig.Logger) {
	consumer := hedwig.NewQueueConsumer(backend, decoder, logger, registry)
	consumer.WithInstrumenter(instrumenter)
	err := consumer.ListenForMessages(context.Background(), hedwig.ListenRequest{})
	if err != nil {
		panic(fmt.Sprintf("Failed to consume messages: %v", err))
	}
}

func runPublisher(
	backend hedwig.PublisherBackend, encoderDecoder hedwig.EncoderDecoder, instrumenter hedwig.Instrumenter,
	dataCreator func() interface{}, logger hedwig.Logger) {
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
	publisher := hedwig.NewPublisher(backend, encoderDecoder, routing)
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
	logger.Debug(ctx, "Published message successfully", "id", message.ID, "publish_id", messageID)
}

func requeueDLQ(backend hedwig.ConsumerBackend, decoder hedwig.Decoder, instrumenter hedwig.Instrumenter, logger hedwig.Logger) {
	consumer := hedwig.NewQueueConsumer(backend, decoder, logger, nil)
	consumer.WithInstrumenter(instrumenter)
	err := consumer.RequeueDLQ(context.Background(), hedwig.ListenRequest{})
	if err != nil {
		panic(fmt.Sprintf("Failed to requeue messages: %v", err))
	}
}

func extractor(ctx context.Context) []interface{} {
	spanCtx := trace.SpanContextFromContext(ctx)
	return []interface{}{
		"traceId", spanCtx.TraceID().String(),
		"spanId", spanCtx.SpanID().String(),
	}
}

func main() {
	var consumerBackend hedwig.ConsumerBackend
	var publisherBackend hedwig.PublisherBackend
	var encoderDecoder hedwig.EncoderDecoder
	var registry hedwig.CallbackRegistry
	var propagator propagation.TextMapPropagator
	var dataCreator func() interface{}
	var logger hedwig.Logger

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
	loggerName := os.Getenv("HEDWIG_LOGGER")
	if loggerName == "" {
		loggerName = "context/zerolog"
	}
	useContextLogger := false
	if strings.HasPrefix(loggerName, "context/") {
		useContextLogger = true
		loggerName = strings.TrimPrefix(loggerName, "context/")
	}
	switch loggerName {
	case "zerolog":
		logger = hedwigzerolog.Logger{}
	case "zap":
		logger = hedwigzap.Logger{}
	case "logrus":
		logger = hedwiglogrus.Logger{}
	default:
		panic(fmt.Sprintf("unknown logger: %s", loggerName))
	}
	if useContextLogger {
		logger = contextlogger.Logger{
			Logger:    logger,
			Extractor: extractor,
		}
	}

	if isProtobuf {
		encoderDecoder = protobufEncoderDecoder()
		registry = protobufRegistry(fakeCallbackErr, logger)
		dataCreator = protobufDataCreator
	} else {
		encoderDecoder = jsonSchemaEncoderDecoder()
		registry = jsonSchemaRegistry(fakeCallbackErr, logger)
		dataCreator = jsonSchemaDataCreator
	}

	if backendName == "aws" {
		b := awsBackend(logger)
		consumerBackend = b
		publisherBackend = b
		propagator = awsPropagator()
	} else {
		b := gcpBackend(logger)
		consumerBackend = b
		publisherBackend = b
		propagator = gcpPropagator()
	}

	instrumenter := hedwigOtel.NewInstrumenter(sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.AlwaysSample())),
	), propagator)

	switch os.Args[1] {
	case "consumer":
		runConsumer(consumerBackend, encoderDecoder, registry, instrumenter, logger)
	case "publisher":
		runPublisher(publisherBackend, encoderDecoder, instrumenter, dataCreator, logger)
	case "requeue-dlq":
		requeueDLQ(consumerBackend, encoderDecoder, instrumenter, logger)
	default:
		panic(fmt.Sprintf("unknown command: %s", os.Args[1]))
	}
}
