/*
 * Author: Michael Ngo
 */

package hedwig

import (
	"context"

	"github.com/pkg/errors"
)

// IPublisher handles all publish related functions
type IPublisher interface {
	// Publish a message on Hedwig infrastructure
	Publish(ctx context.Context, message *Message) (string, error)

	// WithInstrumenter adds a instrumenter to this publisher
	WithInstrumenter(instrumenter Instrumenter) IPublisher
}

// publisher handles hedwig publishing
type publisher struct {
	settings     *Settings
	backend      IBackend
	validator    IMessageValidator
	instrumenter Instrumenter
}

// Publish a message on Hedwig
func (p *publisher) Publish(ctx context.Context, message *Message) (string, error) {
	payload, attributes, err := p.validator.Serialize(message)
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

func (p *publisher) WithInstrumenter(instrumenter Instrumenter) IPublisher {
	p.instrumenter = instrumenter
	return p
}

// NewPublisher creates a new publisher
// messageRouting: Maps message type and major version to topic names
//   <message type>, <message version> => topic name
// An entry is required for every message type that the app wants to consumer or publish. It is
// recommended that major versions of a message be published on separate topics.
func NewPublisher(
	settings *Settings, backend IBackend, validator IMessageValidator,
) IPublisher {
	settings.initDefaults()

	return &publisher{
		settings:  settings,
		backend:   backend,
		validator: validator,
	}
}
