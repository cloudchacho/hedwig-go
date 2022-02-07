package hedwig

import (
	"context"

	"github.com/Masterminds/semver"
	"github.com/pkg/errors"
)

// Publisher handles hedwig publishing
type Publisher struct {
	backend      PublisherBackend
	serializer   serializer
	instrumenter Instrumenter
	routing      MessageRouting
}

// Publish a message on Hedwig
func (p *Publisher) Publish(ctx context.Context, message *Message) (string, error) {
	payload, attributes, err := p.serializer.serialize(message)
	if err != nil {
		return "", err
	}

	key := MessageTypeMajorVersion{
		MessageType:  message.Type,
		MajorVersion: uint(message.DataSchemaVersion.Major()),
	}

	topic, ok := p.routing[key]
	if !ok {
		return "", errors.New("Message route is not defined for message")
	}

	if p.instrumenter != nil {
		var finalize func()
		ctx, attributes, finalize = p.instrumenter.OnPublish(ctx, message, attributes)
		defer finalize()
	}

	return p.backend.Publish(ctx, message, payload, attributes, topic)
}

func (p *Publisher) WithInstrumenter(instrumenter Instrumenter) {
	p.instrumenter = instrumenter
}

type serializer interface {
	serialize(message *Message) ([]byte, map[string]string, error)
	withUseTransportMessageAttributes(useTransportMessageAttributes bool)
}

func (p *Publisher) WithUseTransportMessageAttributes(useTransportMessageAttributes bool) {
	p.serializer.withUseTransportMessageAttributes(useTransportMessageAttributes)
}

// NewPublisher creates a new Publisher
// messageRouting: Maps message type and major version to topic names
//   <message type>, <message version> => topic name
// An entry is required for every message type that the app wants to Consumer or publish. It is
// recommended that major versions of a message be published on separate topics.
func NewPublisher(backend PublisherBackend, encoder Encoder, decoder Decoder, routing MessageRouting) *Publisher {
	return &Publisher{
		routing:    routing,
		backend:    backend,
		serializer: newMessageValidator(encoder, decoder),
	}
}

// MessageRouting is a map of message type and major versions to Hedwig topics
type MessageRouting map[MessageTypeMajorVersion]string

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
