package hedwig

import (
	"context"
	"sync"
	"time"

	"github.com/Masterminds/semver"
	"github.com/pkg/errors"
)

// ListenRequest represents a request to listen for messages
type ListenRequest struct {
	// How many messages to fetch at one time
	NumMessages uint32 // default 1

	// How long should the message be hidden from other consumers?
	VisibilityTimeout time.Duration // defaults to queue configuration

	// How many goroutines to spin for processing messages concurrently
	NumConcurrency uint32 // default 1
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

	message, err := c.deserializer.deserialize(payload, attributes, providerMetadata, nil)
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
	if request.NumConcurrency == 0 {
		request.NumConcurrency = 1
	}

	messageCh := make(chan ReceivedMessage)

	wg := &sync.WaitGroup{}
	// start n concurrent workers to receive messages from the channel
	for i := uint32(0); i < request.NumConcurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					// drain channel before returning
					for receivedMessage := range messageCh {
						c.processMessage(ctx, receivedMessage.Payload, receivedMessage.Attributes, receivedMessage.ProviderMetadata)
					}
					return
				case receivedMessage := <-messageCh:
					c.processMessage(ctx, receivedMessage.Payload, receivedMessage.Attributes, receivedMessage.ProviderMetadata)
				}
			}
		}()
	}
	// wait for all receive goroutines to finish
	defer wg.Wait()

	// close channel to indicate no more message will be published and receive goroutines spawned above should return
	defer close(messageCh)

	return c.backend.Receive(ctx, request.NumMessages, request.VisibilityTimeout, messageCh)
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

func (c *QueueConsumer) WithUseTransportMessageAttributes(useTransportMessageAttributes bool) {
	c.deserializer.withUseTransportMessageAttributes(useTransportMessageAttributes)
}

func (c *QueueConsumer) initDefaults() {
	if c.getLogger == nil {
		stdLogger := &StdLogger{}
		c.getLogger = func(_ context.Context) Logger { return stdLogger }
	}
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
	deserialize(messagePayload []byte, attributes map[string]string, providerMetadata interface{}, overrideUseMsgAttrs *bool) (*Message, error)
	withUseTransportMessageAttributes(useTransportMessageAttributes bool)
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
	// Receive messages from configured queue(s) and provide it through the channel. This should run indefinitely
	// until the context is canceled. Provider metadata should include all info necessary to ack/nack a message.
	// The channel must not be closed by the backend.
	Receive(ctx context.Context, numMessages uint32, visibilityTimeout time.Duration, messageCh chan<- ReceivedMessage) error

	// NackMessage nacks a message on the queue
	NackMessage(ctx context.Context, providerMetadata interface{}) error

	// AckMessage acknowledges a message on the queue
	AckMessage(ctx context.Context, providerMetadata interface{}) error

	//HandleLambdaEvent(ctx context.Context, settings *Settings, snsEvent events.SNSEvent) error

	// RequeueDLQ re-queues everything in the Hedwig DLQ back into the Hedwig queue
	RequeueDLQ(ctx context.Context, numMessages uint32, visibilityTimeout time.Duration) error
}

// ReceivedMessage is the message as received by a transport backend.
type ReceivedMessage struct {
	Payload          []byte
	Attributes       map[string]string
	ProviderMetadata interface{}
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
