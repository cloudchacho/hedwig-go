package main

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/cloudchacho/hedwig-go"
	"github.com/cloudchacho/hedwig-go/aws"
	"github.com/cloudchacho/hedwig-go/gcp"
	"github.com/cloudchacho/hedwig-go/jsonschema"
	"github.com/cloudchacho/hedwig-go/protobuf"
	"google.golang.org/protobuf/reflect/protoreflect"
)

func settings(isProtobuf bool, publisherBackend string) *hedwig.Settings {
	handler := &handler{isProtobuf: isProtobuf}
	callbackRegistry := hedwig.CallbackRegistry{{"user-created", 1}: handler.userCreated}
	useMessageAttributes := true
	var queueName string
	if publisherBackend == "aws" {
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
		Subscriptions:      []string{"dev-myapp-dev-user-created-v1"},
		MessageRouting: map[hedwig.MessageTypeMajorVersion]string{
			hedwig.MessageTypeMajorVersion{
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
	isProtobuf bool
}

func (h *handler) userCreated(ctx context.Context, message *hedwig.Message) error {
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
			[]protoreflect.Message{(&UserCreatedV1{}).ProtoReflect()},
		)
	} else {
		encoder, err = jsonschema.NewMessageEncoder("schema.json", factoryRegistry)
	}
	if err != nil {
		panic(fmt.Sprintf("Failed to create encoder: %v", err))
	}
	return encoder

}

func backend(settings *hedwig.Settings, publisherBackend string) hedwig.IBackend {
	if publisherBackend == "aws" {
		awsSessionCache := aws.NewAWSSessionsCache()
		return aws.NewAWSBackend(settings, awsSessionCache)
	} else if publisherBackend == "gcp" {
		return gcp.NewGCPBackend(settings)
	} else {
		panic(fmt.Sprintf("unknown backend name: %s", publisherBackend))
	}
}

func runConsumer(isProtobuf bool, publisherBackend string) {
	settings := settings(isProtobuf, publisherBackend)
	backend := backend(settings, publisherBackend)
	consumer := hedwig.NewQueueConsumer(settings, backend, encoder(isProtobuf))
	err := consumer.ListenForMessages(context.Background(), hedwig.ListenRequest{})
	if err != nil {
		panic(fmt.Sprintf("Failed to consume messages: %v", err))
	}
}

func runPublisher(isProtobuf bool, publisherBackend string) {
	settings := settings(isProtobuf, publisherBackend)
	validator := hedwig.NewMessageValidator(settings, encoder(isProtobuf))
	backend := backend(settings, publisherBackend)
	publisher := hedwig.NewPublisher(settings, backend, validator)
	data := userCreatedData(isProtobuf)
	message, err := hedwig.NewMessage(settings, "user-created", "1.0", map[string]string{"request_id": "123"}, data)
	if err != nil {
		panic(fmt.Sprintf("Failed to create message: %v", err))
	}
	messageId, err := publisher.Publish(context.Background(), message)
	if err != nil {
		panic(fmt.Sprintf("Failed to publish message: %v", err))
	}
	fmt.Printf("Published message with id %s successfully with publish id: %s\n", message.ID, messageId)
}

func main() {
	isProtobuf := false
	if isProtobufStr, found := os.LookupEnv("HEDWIG_PROTOBUF"); found {
		isProtobuf = strings.ToLower(isProtobufStr) == "true"
	}
	publisherBackend := "aws"
	if isGCPStr, found := os.LookupEnv("HEDWIG_GCP"); found && strings.ToLower(isGCPStr) == "true" {
		publisherBackend = "gcp"
	}
	if os.Args[1] == "consumer" {
		runConsumer(isProtobuf, publisherBackend)
	} else if os.Args[1] == "publisher" {
		runPublisher(isProtobuf, publisherBackend)
	} else {
		panic(fmt.Sprintf("unknown command: %s", os.Args[1]))
	}
}
