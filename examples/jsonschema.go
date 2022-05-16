package main

import (
	"context"
	"errors"
	"fmt"

	"github.com/cloudchacho/hedwig-go"
	"github.com/cloudchacho/hedwig-go/jsonschema"
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
	logger          hedwig.Logger
}

func (h *jsonSchemaHandler) userCreated(ctx context.Context, message *hedwig.Message) error {
	if h.fakeCallbackErr != "" {
		return errors.New(h.fakeCallbackErr)
	}
	userID := message.Data.(*UserCreatedData).UserID
	h.logger.Info(ctx, "Receive user created message", "id", message.ID, "user_id", userID, "request_id", message.Metadata.Headers["request_id"],
		"provider_metadata", message.Metadata.ProviderMetadata)
	return nil
}

func jsonSchemaDataCreator() interface{} {
	return &UserCreatedData{UserID: "U_123"}
}

func jsonSchemaRegistry(fakeCallbackErr string, logger hedwig.Logger) hedwig.CallbackRegistry {
	handler := &jsonSchemaHandler{fakeCallbackErr: fakeCallbackErr, logger: logger}
	return hedwig.CallbackRegistry{{"user-created", 1}: handler.userCreated}
}
