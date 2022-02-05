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

type Consumer struct {
	backend      ConsumerBackend
	settings     *Settings
	deserializer deserializer
	instrumenter Instrumenter
}

type QueueConsumer struct {
	Consumer
}

func (c *Consumer) processMessage(ctx context.Context, payload []byte, attributes map[string]string, providerMetadata interface{}) {
	if c.instrumenter != nil {
		var finalize func()
		ctx, finalize = c.instrumenter.OnReceive(ctx, attributes)
		defer finalize()
	}

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

	message, err := c.deserializer.deserialize(payload, attributes, providerMetadata)
	if err != nil {
		c.settings.GetLogger(ctx).Error(err, "invalid message, unable to unmarshal", loggingFields)
		return
	}

	if c.instrumenter != nil {
		c.instrumenter.OnMessageDeserialized(ctx, message)
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
func (c *QueueConsumer) ListenForMessages(ctx context.Context, request ListenRequest) error {
	if request.NumMessages == 0 {
		request.NumMessages = 1
	}

	return c.backend.Receive(ctx, request.NumMessages, request.VisibilityTimeout, c.processMessage)
}

// RequeueDLQ re-queues everything in the Hedwig DLQ back into the Hedwig queue
func (c *QueueConsumer) RequeueDLQ(ctx context.Context, request ListenRequest) error {
	if request.NumMessages == 0 {
		request.NumMessages = 1
	}

	return c.backend.RequeueDLQ(ctx, request.NumMessages, request.VisibilityTimeout)
}

func (c *QueueConsumer) WithInstrumenter(instrumenter Instrumenter) *QueueConsumer {
	c.instrumenter = instrumenter
	return c
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

type deserializer interface {
	deserialize(messagePayload []byte, attributes map[string]string, providerMetadata interface{}) (*Message, error)
}

func NewQueueConsumer(settings *Settings, backend ConsumerBackend, decoder Decoder) *QueueConsumer {
	settings.initDefaults()

	for key, callback := range settings.CallbackRegistry {
		settings.CallbackRegistry[key] = wrapCallback(callback)
	}

	return &QueueConsumer{
		Consumer: Consumer{
			backend:      backend,
			deserializer: newMessageValidator(settings, nil, decoder),
		},
	}
}
