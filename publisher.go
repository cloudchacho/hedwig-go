package hedwig

import (
	"context"

	"github.com/pkg/errors"
)

// Publisher handles hedwig publishing
type Publisher struct {
	settings     *Settings
	backend      PublisherBackend
	serializer   serializer
	instrumenter Instrumenter
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

	topic, ok := p.settings.MessageRouting[key]
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
}

// NewPublisher creates a new Publisher
// messageRouting: Maps message type and major version to topic names
//   <message type>, <message version> => topic name
// An entry is required for every message type that the app wants to Consumer or publish. It is
// recommended that major versions of a message be published on separate topics.
func NewPublisher(settings *Settings, backend PublisherBackend, encoder Encoder, decoder Decoder) *Publisher {
	settings.initDefaults()

	return &Publisher{
		settings:   settings,
		backend:    backend,
		serializer: newMessageValidator(settings, encoder, decoder),
	}
}
