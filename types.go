package hedwig

import (
	"context"
	"time"

	"github.com/Masterminds/semver"
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

// ConsumerBackend is used for consuming messages from a transport
type ConsumerBackend interface {
	// Receive messages from configured queue(s) and provide it through the callback. This should run indefinitely
	// until the context is canceled. Provider metadata should include all info necessary to ack/nack a message.
	Receive(ctx context.Context, numMessages uint32, visibilityTimeout time.Duration, callback ConsumerCallback) error

	// NackMessage nacks a message on the queue
	NackMessage(ctx context.Context, providerMetadata interface{}) error

	// AckMessage acknowledges a message on the queue
	AckMessage(ctx context.Context, providerMetadata interface{}) error

	//HandleLambdaEvent(ctx context.Context, settings *Settings, snsEvent events.SNSEvent) error

	// RequeueDLQ re-queues everything in the Hedwig DLQ back into the Hedwig queue
	RequeueDLQ(ctx context.Context, numMessages uint32, visibilityTimeout time.Duration) error
}

// PublisherBackend is used to publish messages to a transport
type PublisherBackend interface {
	// Publish a message represented by the payload, with specified attributes to the specific topic
	Publish(ctx context.Context, message *Message, payload []byte, attributes map[string]string, topic string) (string, error)
}

// Encoder is responsible for encoding the message payload in appropriate format for over the wire transport
type Encoder interface {
	// EncodeData encodes the message with appropriate format for transport over the wire
	EncodeData(data interface{}, useMessageTransport bool, metaAttrs MetaAttributes) ([]byte, error)

	// EncodeMessageType encodes the message type with appropriate format for transport over the wire
	EncodeMessageType(messageType string, version *semver.Version) string

	// VerifyKnownMinorVersion checks that message version is known to us
	VerifyKnownMinorVersion(messageType string, version *semver.Version) error
}

// Decoder is responsible for decoding the message payload in appropriate format from over the wire transport format
type Decoder interface {
	// DecodeData validates and decodes data
	DecodeData(messageType string, version *semver.Version, data interface{}) (interface{}, error)

	// ExtractData extracts data from the on-the-wire payload when not using message transport
	ExtractData(messagePayload []byte, attributes map[string]string) (MetaAttributes, interface{}, error)

	// DecodeMessageType decodes message type from meta attributes
	DecodeMessageType(schema string) (string, *semver.Version, error)
}

// GetLoggerFunc returns the logger object
type GetLoggerFunc func(ctx context.Context) ILogger
