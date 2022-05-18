package main

import (
	"context"
	"errors"
	"fmt"

	"github.com/cloudchacho/hedwig-go"
	"github.com/cloudchacho/hedwig-go/protobuf"
	"google.golang.org/protobuf/proto"
)

func protobufEncoderDecoder() *protobuf.EncoderDecoder {
	encoderDecoder, err := protobuf.NewMessageEncoderDecoder(
		[]proto.Message{&UserCreatedV1{}},
	)
	if err != nil {
		panic(fmt.Sprintf("Failed to create encoder: %v", err))
	}
	return encoderDecoder
}

type protobufHandler struct {
	fakeCallbackErr string
	logger          hedwig.Logger
}

func (h *protobufHandler) userCreated(ctx context.Context, message *hedwig.Message) error {
	if h.fakeCallbackErr != "" {
		return errors.New(h.fakeCallbackErr)
	}
	userID := *message.Data.(*UserCreatedV1).UserId
	h.logger.Debug(ctx, "Receive user created message", "id", message.ID, "user_id", userID, "request_id", message.Metadata.Headers["request_id"],
		"provider_metadata", message.Metadata.ProviderMetadata)
	return nil
}

func protobufDataCreator() interface{} {
	return &UserCreatedV1{UserId: proto.String("U_123")}
}

func protobufRegistry(fakeCallbackErr string, logger hedwig.Logger) hedwig.CallbackRegistry {
	handler := &protobufHandler{fakeCallbackErr: fakeCallbackErr, logger: logger}
	return hedwig.CallbackRegistry{{"user-created", 1}: handler.userCreated}
}
