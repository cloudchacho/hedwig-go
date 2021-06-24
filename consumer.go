/*
 * Author: Michael Ngo
 */

package hedwig

import (
	"context"
	"time"

	"github.com/pkg/errors"
)

// ListenRequest represents a request to listen for messages
type ListenRequest struct {
	NumMessages       uint32        // default 1
	VisibilityTimeout time.Duration // defaults to queue configuration
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
	var acked bool
	loggingFields := LoggingFields{"message_body": payload}

	// must ack or nack message, otherwise receive call never returns even on context cancelation
	defer func() {
		if !acked {
			err := c.backend.NackMessage(ctx, providerMetadata)
			if err != nil {
				c.settings.GetLogger(ctx).Error(err, "Failed to nack message", loggingFields)
			}
		}
	}()

	message, err := c.validator.Deserialize(payload, attributes, providerMetadata)
	if err != nil {
		c.settings.GetLogger(ctx).Error(err, "invalid message, unable to unmarshal", loggingFields)
		return
	}

	loggingFields = LoggingFields{"message_id": message.ID, "type": message.Type, "version": message.DataSchemaVersion}

	callbackKey := MessageTypeMajorVersion{message.Type, uint(message.DataSchemaVersion.Major())}
	var callback CallbackFunction
	var ok bool
	if callback, ok = c.settings.CallbackRegistry[callbackKey]; !ok {
		msg := "no callback defined for message"
		c.settings.GetLogger(ctx).Error(errors.New(msg), msg, loggingFields)
		return
	}

	err = callback(ctx, message)
	switch err {
	case nil:
		ackErr := c.backend.AckMessage(ctx, providerMetadata)
		if ackErr != nil {
			c.settings.GetLogger(ctx).Error(ackErr, "Failed to ack message", loggingFields)
		} else {
			acked = true
		}
	case ErrRetry:
		c.settings.GetLogger(ctx).Debug("Retrying due to exception", loggingFields)
	default:
		c.settings.GetLogger(ctx).Error(err, "Retrying due to unknown exception", loggingFields)
	}
}

// ListenForMessages starts a hedwig listener for the provided message types
func (c *queueConsumer) ListenForMessages(ctx context.Context, request ListenRequest) error {
	if request.NumMessages == 0 {
		request.NumMessages = 1
	}

	return c.backend.Receive(ctx, request.NumMessages, request.VisibilityTimeout, c.processMessage)
}

func wrapCallback(function CallbackFunction) CallbackFunction {
	return func(ctx context.Context, message *Message) (err error) {
		defer func() {
			if rErr := recover(); rErr != nil {
				if typedErr, ok := rErr.(error); ok {
					err = errors.Wrapf(typedErr, "callback failed with panic")
				} else {
					err = errors.Errorf("panic: %v", rErr)
				}
			}
		}()
		err = function(ctx, message)
		return
	}
}

func NewQueueConsumer(settings *Settings, backend IBackend, encoder IEncoder) IQueueConsumer {
	settings.initDefaults()

	for key, callback := range settings.CallbackRegistry {
		settings.CallbackRegistry[key] = wrapCallback(callback)
	}

	return &queueConsumer{
		consumer: consumer{
			backend:   backend,
			settings:  settings,
			validator: NewMessageValidator(settings, encoder),
		},
	}
}
