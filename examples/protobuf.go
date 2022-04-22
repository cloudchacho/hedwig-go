package main

import (
	"context"
	"errors"
	"fmt"

	"github.com/cloudchacho/hedwig-go"
	"github.com/cloudchacho/hedwig-go/protobuf"
	"go.opentelemetry.io/otel/trace"
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
}

func (h *protobufHandler) userCreated(ctx context.Context, message *hedwig.Message) error {
	if h.fakeCallbackErr != "" {
		return errors.New(h.fakeCallbackErr)
	}
	userID := message.Data.(*UserCreatedV1).UserId
	span := trace.SpanFromContext(ctx)
	fmt.Printf("[%s/%s] Receive user created message with id %s and user id %s, request id %s and provider metadata %+v\n",
		span.SpanContext().TraceID(), span.SpanContext().SpanID(), message.ID, userID, message.Metadata.Headers["request_id"], message.Metadata.ProviderMetadata)
	return nil
}

func protobufDataCreator() interface{} {
	return &UserCreatedV1{UserId: proto.String("U_123")}
}

func protobufRegistry(fakeCallbackErr string) hedwig.CallbackRegistry {
	handler := &protobufHandler{fakeCallbackErr: fakeCallbackErr}
	return hedwig.CallbackRegistry{{"user-created", 1}: handler.userCreated}
}
