/*
 * Copyright 2017, Automatic Inc.
 * All rights reserved.
 *
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
}

// Publisher handles hedwig publishing for Automatic
type Publisher struct {
	settings  *Settings
	backend   IBackend
	validator IMessageValidator
}

// Publish a message on Hedwig
func (p *Publisher) Publish(ctx context.Context, message *Message) (string, error) {
	payload, attributes, err := p.validator.Serialize(message)
	if err != nil {
		return "", err
	}

	key := MessageRouteKey{
		MessageType:         message.Type,
		MessageMajorVersion: int(message.DataSchemaVersion.Major()),
	}

	topic, ok := p.settings.MessageRouting[key]
	if !ok {
		return "", errors.New("Message route is not defined for message")
	}

	return p.backend.Publish(ctx, message, payload, attributes, topic)
}

// NewPublisher creates a new Publisher
// messageRouting: Maps message type and major version to topic names
//   <message type>, <message version> => topic name
// An entry is required for every message type that the app wants to consumer or publish. It is
// recommended that major versions of a message be published on separate topics.
func NewPublisher(
	settings *Settings, backend IBackend, validator IMessageValidator,
) IPublisher {
	settings.initDefaults()

	return &Publisher{
		settings:  settings,
		backend:   backend,
		validator: validator,
	}
}
