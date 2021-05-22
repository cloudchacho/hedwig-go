package main

import (
	"context"
	"fmt"
	"os"

	"github.com/cloudchacho/hedwig-go"
	"github.com/cloudchacho/hedwig-go/aws"
	"github.com/cloudchacho/hedwig-go/jsonschema"
)

func settings() *hedwig.Settings {
	callbackRegistry := hedwig.CallbackRegistry{{"user-created", 1}: handleUserCreated}
	useMessageAttributes := false
	return &hedwig.Settings{
		AWSAccessKey:     os.Getenv("AWS_ACCESS_KEY"),
		AWSAccountID:     os.Getenv("AWS_ACCOUNT_ID"),
		AWSRegion:        os.Getenv("AWS_REGION"),
		AWSSecretKey:     os.Getenv("AWS_SECRET_KEY"),
		AWSSessionToken:  os.Getenv("AWS_SESSION_TOKEN"),
		CallbackRegistry: &callbackRegistry,
		PublisherName:    "MYAPP",
		QueueName:        "DEV-MYAPP",
		MessageRouting: map[hedwig.MessageRouteKey]string{
			hedwig.MessageRouteKey{
				MessageType:         "user-created",
				MessageMajorVersion: 1,
			}: "dev-user-created-v1",
		},
		UseTransportMessageAttributes: &useMessageAttributes,
	}
}

func dataRegistry() hedwig.DataFactoryRegistry {
	return map[hedwig.DataRegistryKey]hedwig.DataFactory{
		{"user-created", 1}: newUserCreatedData,
	}
}

func handleUserCreated(ctx context.Context, message *hedwig.Message) error {
	fmt.Printf("Receive user created message with id %s and user id %s, request id %s and provider metadata %v\n", message.ID, message.Data.(*UserCreatedData).UserID, message.Metadata.Headers["request_id"], message.Metadata.ProviderMetadata)
	return nil
}

func runConsumer() {
	settings := settings()
	dataRegistry := dataRegistry()
	encoder, err := jsonschema.NewMessageEncoder("schema.json", dataRegistry)
	if err != nil {
		panic(fmt.Sprintf("Failed to create encoder: %v", err))
	}
	validator := hedwig.NewMessageValidator(settings, encoder)
	awsSessionCache := aws.NewAWSSessionsCache()
	backend := aws.NewAWSBackend(settings, awsSessionCache)
	consumer := hedwig.NewQueueConsumer(settings, backend, validator)
	err = consumer.ListenForMessages(context.Background(), hedwig.ListenRequest{})
	if err != nil {
		panic(fmt.Sprintf("Failed to consume messages: %v", err))
	}
}

type UserCreatedData struct {
	UserID string `json:"user_id"`
}

func newUserCreatedData() interface{} {
	return new(UserCreatedData)
}

func runPublisher() {
	settings := settings()
	dataRegistry := dataRegistry()
	encoder, err := jsonschema.NewMessageEncoder("schema.json", dataRegistry)
	if err != nil {
		panic(fmt.Sprintf("Failed to create encoder: %v", err))
	}
	validator := hedwig.NewMessageValidator(settings, encoder)
	awsSessionCache := aws.NewAWSSessionsCache()
	backend := aws.NewAWSBackend(settings, awsSessionCache)
	publisher := hedwig.NewPublisher(settings, backend, validator)
	data := UserCreatedData{
		UserID: "U_123",
	}
	message, err := hedwig.NewMessage(settings, "user-created", "1.0", map[string]string{"request_id": "123"}, data)
	if err != nil {
		panic(fmt.Sprintf("Failed to create message: %v", err))
	}
	messageId, err := publisher.Publish(context.Background(), message)
	if err != nil {
		panic(fmt.Sprintf("Failed to publish message: %v", err))
	}
	fmt.Printf("Published message with id %s successfully with sqs id: %s\n", message.ID, messageId)
}

func main() {
	if os.Args[1] == "consumer" {
		runConsumer()
	} else if os.Args[1] == "publisher" {
		runPublisher()
	}
}
