package hedwig

import (
	"context"

	"github.com/pkg/errors"
)

// MessageTypeMajorVersion is a tuple of message typa and major version
type MessageTypeMajorVersion struct {
	// Message type
	MessageType string
	// Message major version
	MajorVersion uint
}

// DataFactory is a function that returns a pointer to struct type that a hedwig message data should conform to
type DataFactory func() interface{}

// DataFactoryRegistry is the map of message type and major versions to a factory function
type DataFactoryRegistry map[MessageTypeMajorVersion]DataFactory

// CallbackFunction is the function signature for a hedwig callback function
type CallbackFunction func(context.Context, *Message) error

// CallbackRegistry is a map of message type and major versions to callback functions
type CallbackRegistry map[MessageTypeMajorVersion]CallbackFunction

// MessageRouting is a map of message type and major versions to Hedwig topics
type MessageRouting map[MessageTypeMajorVersion]string

// ErrRetry should cause the task to retry, but not treat the retry as an error
var ErrRetry = errors.New("Retry error")

// SubscriptionProject represents a tuple of subscription name and project for cross-project Google subscriptions
type SubscriptionProject struct {
	// Subscription name
	Subscription string

	// ProjectID
	ProjectID string
}

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
