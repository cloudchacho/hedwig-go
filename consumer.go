package hedwig

import (
	"context"
	"time"

	"github.com/Masterminds/semver"
	"github.com/pkg/errors"
)

// ListenRequest represents a request to listen for messages
type ListenRequest struct {
	NumMessages       uint32        // default 1
	VisibilityTimeout time.Duration // defaults to queue configuration
}

type Consumer struct {
	backend      ConsumerBackend
	deserializer deserializer
	instrumenter Instrumenter
	registry     CallbackRegistry
	getLogger    GetLoggerFunc
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
				c.getLogger(ctx).Error(err, "Failed to nack message", loggingFields)
			}
		}
	}()

	message, err := c.deserializer.deserialize(payload, attributes, providerMetadata)
	if err != nil {
		c.getLogger(ctx).Error(err, "invalid message, unable to unmarshal", loggingFields)
		return
	}

	if c.instrumenter != nil {
		c.instrumenter.OnMessageDeserialized(ctx, message)
	}

	loggingFields = LoggingFields{"message_id": message.ID, "type": message.Type, "version": message.DataSchemaVersion}

	callbackKey := MessageTypeMajorVersion{message.Type, uint(message.DataSchemaVersion.Major())}
	var callback CallbackFunction
	var ok bool
	if callback, ok = c.registry[callbackKey]; !ok {
		msg := "no callback defined for message"
		c.getLogger(ctx).Error(errors.New(msg), msg, loggingFields)
		return
	}

	err = callback(ctx, message)
	switch err {
	case nil:
		ackErr := c.backend.AckMessage(ctx, providerMetadata)
		if ackErr != nil {
			c.getLogger(ctx).Error(ackErr, "Failed to ack message", loggingFields)
		} else {
			acked = true
		}
	case ErrRetry:
		c.getLogger(ctx).Debug("Retrying due to exception", loggingFields)
	default:
		c.getLogger(ctx).Error(err, "Retrying due to unknown exception", loggingFields)
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
	withUseTransportMessageAttributes(useTransportMessageAttributes bool)
}

func (v *QueueConsumer) withUseTransportMessageAttributes(useTransportMessageAttributes bool) {
	v.deserializer.withUseTransportMessageAttributes(useTransportMessageAttributes)
}

func (v *QueueConsumer) initDefaults() {
	if v.getLogger == nil {
		stdLogger := &StdLogger{}
		v.getLogger = func(_ context.Context) Logger { return stdLogger }
	}
}

func NewQueueConsumer(backend ConsumerBackend, decoder Decoder, getLogger GetLoggerFunc, registry CallbackRegistry) *QueueConsumer {
	for key, callback := range registry {
		registry[key] = wrapCallback(callback)
	}

	c := &QueueConsumer{
		Consumer: Consumer{
			getLogger:    getLogger,
			registry:     registry,
			backend:      backend,
			deserializer: newMessageValidator(nil, decoder),
		},
	}
	c.initDefaults()
	return c
}

// CallbackFunction is the function signature for a hedwig callback function
type CallbackFunction func(context.Context, *Message) error

// CallbackRegistry is a map of message type and major versions to callback functions
type CallbackRegistry map[MessageTypeMajorVersion]CallbackFunction

// ErrRetry should cause the task to retry, but not treat the retry as an error
var ErrRetry = errors.New("Retry error")

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

type ConsumerCallback func(ctx context.Context, payload []byte, attributes map[string]string, providerMetadata interface{})

// Decoder is responsible for decoding the message payload in appropriate format from over the wire transport format
type Decoder interface {
	// DecodeData validates and decodes data
	DecodeData(messageType string, version *semver.Version, data interface{}) (interface{}, error)

	// ExtractData extracts data from the on-the-wire payload when not using message transport
	ExtractData(messagePayload []byte, attributes map[string]string) (MetaAttributes, interface{}, error)

	// DecodeMessageType decodes message type from meta attributes
	DecodeMessageType(schema string) (string, *semver.Version, error)
}
