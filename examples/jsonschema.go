package main

import (
	"context"
	"errors"
	"fmt"

	"github.com/cloudchacho/hedwig-go"
	"github.com/cloudchacho/hedwig-go/jsonschema"
	"go.opentelemetry.io/otel/trace"
)

func jsonSchemaEncoderDecoder() *jsonschema.EncoderDecoder {
	factoryRegistry := map[hedwig.MessageTypeMajorVersion]jsonschema.DataFactory{
		{"user-created", 1}: func() interface{} { return &UserCreatedData{} },
	}
	encoder, err := jsonschema.NewMessageEncoderDecoder("schema.json", factoryRegistry)
	if err != nil {
		panic(fmt.Sprintf("Failed to create encoder: %v", err))
	}
	return encoder
}

type jsonSchemaHandler struct {
	fakeCallbackErr string
}

func (h *jsonSchemaHandler) userCreated(ctx context.Context, message *hedwig.Message) error {
	if h.fakeCallbackErr != "" {
		return errors.New(h.fakeCallbackErr)
	}
	userID := message.Data.(*UserCreatedData).UserID
	span := trace.SpanFromContext(ctx)
	fmt.Printf("[%s/%s] Receive user created message with id %s and user id %s, request id %s and provider metadata %+v\n",
		span.SpanContext().TraceID(), span.SpanContext().SpanID(), message.ID, userID, message.Metadata.Headers["request_id"], message.Metadata.ProviderMetadata)
	return nil
}

func jsonSchemaDataCreator() interface{} {
	return &UserCreatedData{UserID: "U_123"}
}

func jsonSchemaRegistry(fakeCallbackErr string) hedwig.CallbackRegistry {
	handler := &jsonSchemaHandler{fakeCallbackErr: fakeCallbackErr}
	return hedwig.CallbackRegistry{{"user-created", 1}: handler.userCreated}
}
