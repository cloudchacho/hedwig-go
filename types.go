package hedwig

import (
	"context"
	"errors"
)

// DataRegistryKey is a key identifying a data registry
type DataRegistryKey struct {
	// Message type
	MessageType string
	// Message major version
	MessageMajorVersion int
}

// DataFactory is a function that returns a pointer to struct type that a hedwig message data should conform to
type DataFactory func() interface{}

// DataFactoryRegistry is the map of message type and major versions to a factory function
type DataFactoryRegistry map[DataRegistryKey]DataFactory

// DataRegistryKey is a key identifying a hedwig callback
type CallbackKey struct {
	// Message type
	MessageType string
	// Message major version
	MessageMajorVersion int
}

// CallbackFunction is the function signature for a hedwig callback function
type CallbackFunction func(context.Context, *Message) error

type CallbackRegistry map[CallbackKey]CallbackFunction

// ErrRetry should cause the task to retry, but not treat the retry as an error
var ErrRetry = errors.New("Retry error")

type ConsumerCallback func(ctx context.Context, payload []byte, attributes map[string]string, providerMetadata interface{})

type IBackend interface {
	// Receive messages from configured queue(s) and provide it through the callback. This should run indefinitely
	// until the context is cancelled. Provider metadata should include all info necessary to ack/nack a message.
	Receive(ctx context.Context, numMessages uint32, visibilityTimeoutS uint32, callback ConsumerCallback) error

	// NackMessage nacks a message on the queue
	NackMessage(ctx context.Context, providerMetadata interface{}) error

	// AckMessage acknowledges a message on the queue
	AckMessage(ctx context.Context, providerMetadata interface{}) error

	//HandleLambdaEvent(ctx context.Context, settings *Settings, snsEvent events.SNSEvent) error

	// Publish a message represented by the payload, with specified attributes to the specific topic
	Publish(ctx context.Context, message *Message, payload []byte, attributes map[string]string, topic string) (string, error)
}
