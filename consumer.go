/*
 * Author: Michael Ngo
 */

package hedwig

import (
	"context"

	"github.com/pkg/errors"
)

// ListenRequest represents a request to listen for messages
type ListenRequest struct {
	NumMessages        uint32 // default 1
	VisibilityTimeoutS uint32 // defaults to queue configuration
}

// IQueueConsumer represents a hedwig queue consumer
type IQueueConsumer interface {
	// ListenForMessages starts a hedwig listener for the provided message types
	//
	// This function never returns by default. Possible shutdown methods:
	// 1. Cancel the context - returns immediately.
	// 2. Set a deadline on the context of less than 10 seconds - returns after processing current messages.
	ListenForMessages(ctx context.Context, request ListenRequest) error
}

type consumer struct {
	backend   IBackend
	settings  *Settings
	validator IMessageValidator
}

type queueConsumer struct {
	consumer
}

func (c *queueConsumer) processMessage(ctx context.Context, payload []byte, attributes map[string]string, providerMetadata interface{}) {

	message, err := c.validator.Deserialize(payload, attributes, providerMetadata)
	if err != nil {
		loggingFields := LoggingFields{"message_body": payload}
		c.settings.GetLogger(ctx).Error(err, "invalid message, unable to unmarshal", loggingFields)
		return
	}

	callbackKey := MessageTypeMajorVersion{message.Type, uint(message.DataSchemaVersion.Major())}
	var callback CallbackFunction
	var ok bool
	if callback, ok = c.settings.CallbackRegistry[callbackKey]; !ok {
		loggingFields := LoggingFields{"message_body": payload}
		msg := "no callback defined for message"
		c.settings.GetLogger(ctx).Error(errors.New(msg), msg, loggingFields)
		return
	}

	err = callback(ctx, message)
	switch err {
	case nil:
		err := c.backend.AckMessage(ctx, providerMetadata)
		if err != nil {
			c.settings.GetLogger(ctx).Error(err, "Failed to ack message", LoggingFields{"message_id": message.ID})
		}
		return
	case ErrRetry:
		c.settings.GetLogger(ctx).Debug("Retrying due to exception", LoggingFields{"message_id": message.ID})
	default:
		c.settings.GetLogger(ctx).Error(err, "Retrying due to unknown exception", LoggingFields{"message_id": message.ID})
	}
	err = c.backend.NackMessage(ctx, providerMetadata)
	if err != nil {
		c.settings.GetLogger(ctx).Error(err, "Failed to nack message", LoggingFields{"message_id": message.ID})
	}
}

// ListenForMessages starts a hedwig listener for the provided message types
func (c *queueConsumer) ListenForMessages(ctx context.Context, request ListenRequest) error {
	if request.NumMessages == 0 {
		request.NumMessages = 1
	}

	return c.backend.Receive(ctx, request.NumMessages, request.VisibilityTimeoutS, c.processMessage)
}

func NewQueueConsumer(settings *Settings, backend IBackend, validator IMessageValidator) IQueueConsumer {
	settings.initDefaults()

	return &queueConsumer{
		consumer: consumer{
			backend:   backend,
			settings:  settings,
			validator: validator,
		},
	}
}
