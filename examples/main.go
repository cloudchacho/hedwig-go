package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/cloudchacho/hedwig-go"
	"github.com/cloudchacho/hedwig-go/aws"
	"github.com/cloudchacho/hedwig-go/gcp"
	"github.com/cloudchacho/hedwig-go/jsonschema"
	"github.com/cloudchacho/hedwig-go/protobuf"
	"google.golang.org/protobuf/proto"
)

func settings(isProtobuf bool, backendName string, fakeCallbackErr string) *hedwig.Settings {
	handler := &handler{isProtobuf: isProtobuf, fakeCallbackErr: fakeCallbackErr}
	callbackRegistry := hedwig.CallbackRegistry{{"user-created", 1}: handler.userCreated}
	useMessageAttributes := true
	var queueName string
	if backendName == "aws" {
		queueName = "DEV-MYAPP"
	} else {
		queueName = "dev-myapp"
	}
	return &hedwig.Settings{
		AWSAccessKey:       os.Getenv("AWS_ACCESS_KEY"),
		AWSAccountID:       os.Getenv("AWS_ACCOUNT_ID"),
		AWSRegion:          os.Getenv("AWS_REGION"),
		AWSSecretKey:       os.Getenv("AWS_SECRET_KEY"),
		AWSSessionToken:    os.Getenv("AWS_SESSION_TOKEN"),
		GoogleCloudProject: os.Getenv("GOOGLE_CLOUD_PROJECT"),
		CallbackRegistry:   callbackRegistry,
		PublisherName:      "MYAPP",
		QueueName:          queueName,
		Subscriptions:      []string{"dev-user-created-v1"},
		MessageRouting: map[hedwig.MessageTypeMajorVersion]string{
			{
				MessageType:  "user-created",
				MajorVersion: 1,
			}: "dev-user-created-v1",
		},
		UseTransportMessageAttributes: &useMessageAttributes,
	}
}

func userCreatedDataFactory(isProtobuf bool) hedwig.DataFactory {
	return func() interface{} {
		if isProtobuf {
			return new(UserCreatedV1)
		} else {
			return new(UserCreatedData)
		}
	}
}

func registry(isProtobuf bool) hedwig.DataFactoryRegistry {
	return map[hedwig.MessageTypeMajorVersion]hedwig.DataFactory{
		{"user-created", 1}: userCreatedDataFactory(isProtobuf),
	}
}

type handler struct {
	isProtobuf      bool
	fakeCallbackErr string
}

func (h *handler) userCreated(ctx context.Context, message *hedwig.Message) error {
	if h.fakeCallbackErr != "" {
		return errors.New(h.fakeCallbackErr)
	}
	var userID string
	if h.isProtobuf {
		userID = message.Data.(*UserCreatedV1).UserId
	} else {
		userID = message.Data.(*UserCreatedData).UserID
	}
	fmt.Printf("Receive user created message with id %s and user id %s, request id %s and provider metadata %v\n", message.ID, userID, message.Metadata.Headers["request_id"], message.Metadata.ProviderMetadata)
	return nil
}

func userCreatedData(isProtobuf bool) interface{} {
	if isProtobuf {
		return &UserCreatedV1{
			UserId: "U_123",
		}
	} else {
		return &UserCreatedData{
			UserID: "U_123",
		}
	}
}

func encoder(isProtobuf bool) hedwig.IEncoder {
	var encoder hedwig.IEncoder
	var err error
	factoryRegistry := registry(isProtobuf)
	if isProtobuf {
		encoder, err = protobuf.NewMessageEncoder(
			[]proto.Message{&UserCreatedV1{}},
		)
	} else {
		encoder, err = jsonschema.NewMessageEncoder("schema.json", factoryRegistry)
	}
	if err != nil {
		panic(fmt.Sprintf("Failed to create encoder: %v", err))
	}
	return encoder

}

func backend(settings *hedwig.Settings, backendName string) hedwig.IBackend {
	if backendName == "aws" {
		awsSessionCache := aws.NewAWSSessionsCache()
		return aws.NewBackend(settings, awsSessionCache)
	} else if backendName == "gcp" {
		return gcp.NewBackend(settings)
	} else {
		panic(fmt.Sprintf("unknown backend name: %s", backendName))
	}
}

func runConsumer(isProtobuf bool, backendName string, fakeCallbackErr string) {
	settings := settings(isProtobuf, backendName, fakeCallbackErr)
	backend := backend(settings, backendName)
	consumer := hedwig.NewQueueConsumer(settings, backend, encoder(isProtobuf))
	err := consumer.ListenForMessages(context.Background(), hedwig.ListenRequest{})
	if err != nil {
		panic(fmt.Sprintf("Failed to consume messages: %v", err))
	}
}

func runPublisher(isProtobuf bool, backendName string) {
	settings := settings(isProtobuf, backendName, "")
	validator := hedwig.NewMessageValidator(settings, encoder(isProtobuf))
	backend := backend(settings, backendName)
	publisher := hedwig.NewPublisher(settings, backend, validator)
	data := userCreatedData(isProtobuf)
	message, err := hedwig.NewMessage(settings, "user-created", "1.0", map[string]string{"request_id": "123"}, data)
	if err != nil {
		panic(fmt.Sprintf("Failed to create message: %v", err))
	}
	messageID, err := publisher.Publish(context.Background(), message)
	if err != nil {
		panic(fmt.Sprintf("Failed to publish message: %v", err))
	}
	fmt.Printf("Published message with id %s successfully with publish id: %s\n", message.ID, messageID)
}

func requeueDLQ(isProtobuf bool, backendName string) {
	settings := settings(isProtobuf, backendName, "")
	backend := backend(settings, backendName)
	consumer := hedwig.NewQueueConsumer(settings, backend, encoder(isProtobuf))
	err := consumer.RequeueDLQ(context.Background(), hedwig.ListenRequest{})
	if err != nil {
		panic(fmt.Sprintf("Failed to requeue messages: %v", err))
	}
}

func main() {
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
	if os.Args[1] == "consumer" {
		runConsumer(isProtobuf, backendName, fakeCallbackErr)
	} else if os.Args[1] == "publisher" {
		runPublisher(isProtobuf, backendName)
	} else if os.Args[1] == "requeue-dlq" {
		requeueDLQ(isProtobuf, backendName)
	} else {
		panic(fmt.Sprintf("unknown command: %s", os.Args[1]))
	}
}
